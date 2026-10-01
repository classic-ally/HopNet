//! Publish-credential refresh: heals the stale-node-URL coupling.
//!
//! The GUI node binds an ephemeral loopback port per launch and rewrites the
//! keychain `base_url`; a daemon that read its credentials once at startup
//! would publish into the previous port forever. The wrapper re-reads the
//! platform credential source (a cheap SecItem query on the Swift side) — but
//! only after a pass observed the node unreachable, so the steady state does
//! zero keychain traffic.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use ingress_core::publish::{
    EditItem, PublishError, PublishItem, PublishOutcome, Publisher, ResolveOutcome, TombstoneOp,
};

/// Current publish credentials as the platform sees them.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct FfiPublishCredentials {
    /// Node base URL WITHOUT `/api` (same contract as `FfiDaemonOptions`).
    pub node_url: String,
    /// RFC-012 device token (`{device_id}.{secret}`).
    pub device_token: String,
}

/// Implemented in Swift (keychain read). Called at most once per publish
/// tick, and only after a tick observed the node unreachable. `None` =
/// credentials missing (deprovisioned) — the current client is kept and the
/// pass keeps parking.
#[uniffi::export(with_foreign)]
pub trait PublishCredentialsProvider: Send + Sync {
    fn current(&self) -> Option<FfiPublishCredentials>;
}

/// Builds a fresh inner publisher from a fresh credential set.
type RebuildFn<P> = Box<dyn Fn(&FfiPublishCredentials) -> Result<P, String> + Send + Sync>;

/// Wraps the real publisher; rebuilds it from fresh credentials after an
/// unreachable pass. Generic over the inner publisher (and rebuild closure)
/// so tests can observe rebuilds without a network.
pub(crate) struct RefreshingPublisher<P: Publisher> {
    provider: Arc<dyn PublishCredentialsProvider>,
    rebuild: RebuildFn<P>,
    /// The inner publisher plus the credentials it was built from. Held
    /// only to clone or swap the `Arc`, never across a call: the daemon runs
    /// publishes concurrently, and a guard held over `.await` would
    /// serialize them all behind one lock.
    state: Mutex<(Arc<P>, FfiPublishCredentials)>,
    /// Set when a call returns NodeUnreachable; consumed (and cleared) by
    /// the next call's refresh probe. Every pass opens with `resolve`, so
    /// the probe naturally runs at tick start.
    stale: AtomicBool,
}

impl<P: Publisher> RefreshingPublisher<P> {
    pub(crate) fn new(
        inner: P,
        built_from: FfiPublishCredentials,
        provider: Arc<dyn PublishCredentialsProvider>,
        rebuild: impl Fn(&FfiPublishCredentials) -> Result<P, String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            provider,
            rebuild: Box::new(rebuild),
            state: Mutex::new((Arc::new(inner), built_from)),
            stale: AtomicBool::new(false),
        }
    }

    /// One refresh probe per unreachable observation: clear the flag first
    /// so a still-unreachable node re-arms it rather than looping reads.
    fn refresh_if_stale(&self) {
        if !self.stale.swap(false, Ordering::AcqRel) {
            return;
        }
        let Some(fresh) = self.provider.current() else {
            return;
        };
        let mut state = self.state.lock().expect("publisher state mutex");
        if fresh == state.1 {
            return;
        }
        // A malformed credential set cannot beat the working-but-stale one;
        // on Err keep the old client and let the pass park again. Calls
        // already in flight keep their clone of the old client and finish on
        // it.
        if let Ok(inner) = (self.rebuild)(&fresh) {
            *state = (Arc::new(inner), fresh);
        }
    }

    /// Refresh if an earlier call found the node unreachable, then hand out
    /// the current client.
    fn current(&self) -> Arc<P> {
        self.refresh_if_stale();
        self.state.lock().expect("publisher state mutex").0.clone()
    }

    fn note<T>(&self, result: &Result<T, PublishError>) {
        if matches!(result, Err(PublishError::NodeUnreachable(_))) {
            self.stale.store(true, Ordering::Release);
        }
    }
}

#[async_trait::async_trait]
impl<P: Publisher> Publisher for RefreshingPublisher<P> {
    async fn publish(&self, item: PublishItem) -> Result<PublishOutcome, PublishError> {
        let result = self.current().publish(item).await;
        self.note(&result);
        result
    }

    async fn resolve(
        &self,
        library_id: Option<&str>,
        cloud_ids: &[String],
    ) -> Result<ResolveOutcome, PublishError> {
        let result = self.current().resolve(library_id, cloud_ids).await;
        self.note(&result);
        result
    }

    async fn propagate_tombstone(
        &self,
        consensus_photo_id: &str,
        op: TombstoneOp,
    ) -> Result<(), PublishError> {
        let result = self
            .current()
            .propagate_tombstone(consensus_photo_id, op)
            .await;
        self.note(&result);
        result
    }

    async fn publish_edit(&self, item: EditItem) -> Result<(), PublishError> {
        let result = self.current().publish_edit(item).await;
        self.note(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use ingress_core::publish::Responsibility;

    use super::*;

    /// One scripted resolve: its result, and optionally a gate the call
    /// holds on (inside the inner publisher) until a permit is added.
    type Step = (
        Result<(), PublishError>,
        Option<Arc<tokio::sync::Semaphore>>,
    );

    /// Inner mock: scripted resolve results, records which credential set
    /// it was built from.
    struct MockInner {
        built_from: String,
        results: Arc<Mutex<Vec<Step>>>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl Publisher for MockInner {
        async fn publish(&self, _item: PublishItem) -> Result<PublishOutcome, PublishError> {
            unreachable!("tests drive the wrapper through resolve only")
        }

        async fn propagate_tombstone(
            &self,
            _consensus_photo_id: &str,
            _op: TombstoneOp,
        ) -> Result<(), PublishError> {
            unreachable!("tests drive the wrapper through resolve only")
        }

        async fn publish_edit(&self, _item: EditItem) -> Result<(), PublishError> {
            unreachable!("tests drive the wrapper through resolve only")
        }

        async fn resolve(
            &self,
            _library_id: Option<&str>,
            _cloud_ids: &[String],
        ) -> Result<ResolveOutcome, PublishError> {
            self.calls.lock().unwrap().push(self.built_from.clone());
            let (result, gate) = self.results.lock().unwrap().remove(0);
            if let Some(gate) = gate {
                let _permit = gate.acquire().await.unwrap();
            }
            match result {
                Ok(()) => Ok(ResolveOutcome {
                    responsibility: Responsibility::Holder,
                    entries: Vec::new(),
                }),
                Err(e) => Err(e),
            }
        }
    }

    struct MockProvider {
        queries: AtomicUsize,
        creds: Mutex<Option<FfiPublishCredentials>>,
    }

    impl PublishCredentialsProvider for MockProvider {
        fn current(&self) -> Option<FfiPublishCredentials> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            self.creds.lock().unwrap().clone()
        }
    }

    fn creds(url: &str) -> FfiPublishCredentials {
        FfiPublishCredentials {
            node_url: url.into(),
            device_token: "d.t".into(),
        }
    }

    fn unreachable_err() -> Result<(), PublishError> {
        Err(PublishError::NodeUnreachable("refused".into()))
    }

    struct Fixture {
        publisher: RefreshingPublisher<MockInner>,
        provider: Arc<MockProvider>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    fn fixture(scripted: Vec<Result<(), PublishError>>, provider_creds: Option<&str>) -> Fixture {
        gated_fixture(
            scripted.into_iter().map(|r| (r, None)).collect(),
            provider_creds,
        )
    }

    fn gated_fixture(scripted: Vec<Step>, provider_creds: Option<&str>) -> Fixture {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let results = Arc::new(Mutex::new(scripted));
        let provider = Arc::new(MockProvider {
            queries: AtomicUsize::new(0),
            creds: Mutex::new(provider_creds.map(creds)),
        });
        let inner = MockInner {
            built_from: "initial".into(),
            results: results.clone(),
            calls: calls.clone(),
        };
        let (c, r) = (calls.clone(), results.clone());
        let publisher = RefreshingPublisher::new(
            inner,
            creds("initial"),
            provider.clone() as Arc<dyn PublishCredentialsProvider>,
            move |fresh| {
                Ok(MockInner {
                    built_from: fresh.node_url.clone(),
                    results: r.clone(),
                    calls: c.clone(),
                })
            },
        );
        Fixture {
            publisher,
            provider,
            calls,
        }
    }

    async fn resolve(f: &Fixture) -> Result<ResolveOutcome, PublishError> {
        f.publisher.resolve(None, &[]).await
    }

    // Should not: touch the credential source while the node is reachable.
    #[tokio::test]
    async fn reachable_passes_never_query_the_provider() {
        let f = fixture(vec![Ok(()), Ok(()), Ok(())], Some("fresh"));
        for _ in 0..3 {
            resolve(&f).await.unwrap();
        }
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 0);
        assert_eq!(*f.calls.lock().unwrap(), vec!["initial"; 3]);
    }

    // Impact: this is the healing path for the GUI's ephemeral-port restart —
    // a stranded daemon follows the node to its new port without a restart.
    // Should: re-read credentials after an unreachable pass and rebuild the
    // client when they changed.
    #[tokio::test]
    async fn unreachable_then_changed_creds_rebuilds() {
        let f = fixture(vec![unreachable_err(), Ok(())], Some("fresh"));
        resolve(&f).await.unwrap_err();
        resolve(&f).await.unwrap();
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 1);
        assert_eq!(*f.calls.lock().unwrap(), vec!["initial", "fresh"]);
    }

    // Should: keep the existing client when the re-read credentials are
    // unchanged, re-arming (not looping) the refresh probe.
    #[tokio::test]
    async fn unchanged_creds_keep_the_client_and_rearm() {
        let f = fixture(
            vec![unreachable_err(), unreachable_err(), Ok(())],
            Some("initial"),
        );
        resolve(&f).await.unwrap_err();
        resolve(&f).await.unwrap_err(); // one query, no rebuild
        resolve(&f).await.unwrap(); // re-armed: queries again
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 2);
        assert_eq!(*f.calls.lock().unwrap(), vec!["initial"; 3]);
    }

    // Should not: drop a working-but-stale client when the platform reports
    // credentials missing (deprovisioned mid-run) — the pass keeps parking.
    #[tokio::test]
    async fn missing_creds_keep_the_client() {
        let f = fixture(vec![unreachable_err(), Ok(())], None);
        resolve(&f).await.unwrap_err();
        resolve(&f).await.unwrap();
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 1);
        assert_eq!(*f.calls.lock().unwrap(), vec!["initial", "initial"]);
    }

    // Should: delegate non-unreachable errors without arming the probe.
    #[tokio::test]
    async fn transient_errors_do_not_arm_the_probe() {
        let f = fixture(
            vec![Err(PublishError::Transient("500".into())), Ok(())],
            Some("fresh"),
        );
        resolve(&f).await.unwrap_err();
        resolve(&f).await.unwrap();
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 0);
    }

    /// Wait (bounded) until the inner publisher has been entered `n` times.
    async fn entered(f: &Fixture, n: usize) -> bool {
        for _ in 0..200 {
            if f.calls.lock().unwrap().len() >= n {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        false
    }

    // Impact: regression guard. The wrapper used to hold its lock across the
    // inner call, so the daemon's concurrent publish window ran one photo at
    // a time in production even though the unwrapped publisher tests showed
    // it concurrent.
    // Should: let concurrent calls run inside the inner publisher at the same
    // time.
    #[tokio::test]
    async fn concurrent_calls_overlap_inside_the_inner_publisher() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let f = Arc::new(gated_fixture(
            vec![(Ok(()), Some(gate.clone())), (Ok(()), Some(gate.clone()))],
            None,
        ));
        let a = tokio::spawn({
            let f = f.clone();
            async move { resolve(&f).await }
        });
        let b = tokio::spawn({
            let f = f.clone();
            async move { resolve(&f).await }
        });

        let both_inside = entered(&f, 2).await;
        gate.add_permits(2);
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        assert!(both_inside, "second call never reached the inner publisher");
    }

    // Should: rebuild the client for new calls while an earlier call is still
    // in flight, and let that call finish on the client it started with.
    #[tokio::test]
    async fn refresh_does_not_wait_for_in_flight_calls() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let f = Arc::new(gated_fixture(
            vec![
                (Ok(()), Some(gate.clone())),
                (unreachable_err(), None),
                (Ok(()), None),
            ],
            Some("fresh"),
        ));
        let held = tokio::spawn({
            let f = f.clone();
            async move { resolve(&f).await }
        });
        assert!(entered(&f, 1).await);

        resolve(&f).await.unwrap_err();
        resolve(&f).await.unwrap();
        assert_eq!(
            *f.calls.lock().unwrap(),
            vec!["initial", "initial", "fresh"]
        );

        gate.add_permits(1);
        held.await.unwrap().unwrap();
        assert_eq!(f.provider.queries.load(Ordering::SeqCst), 1);
    }
}
