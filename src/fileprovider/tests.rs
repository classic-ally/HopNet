#[cfg(target_os = "macos")]
use super::keychain::{
    FileProviderConfig, KeychainEnvironment, load_config, remove_config, store_config,
};
use uuid::Uuid;

#[cfg(target_os = "macos")]
#[tokio::test]
async fn test_keychain_setup() {
    // Clean up any existing test data first
    let _ = remove_config(KeychainEnvironment::Test);

    // Generate random test configuration to avoid false positives from stale data
    let random_api_key = format!("test-api-key-{}", Uuid::new_v4());
    let random_port = 30000 + (rand::random::<u16>() % 10000); // Random port 30000-39999
    let test_base_url = format!("http://localhost:{}", random_port);

    let test_config = FileProviderConfig::new(random_api_key.clone(), test_base_url.clone());

    // Store test configuration
    store_config(&test_config, KeychainEnvironment::Test)
        .expect("Failed to store test config in keychain");

    // Load it back
    let loaded_config =
        load_config(KeychainEnvironment::Test).expect("Failed to load test config from keychain");

    // Verify it matches our randomly generated values (not any stale data)
    assert_eq!(
        loaded_config.api_key, random_api_key,
        "API key should match the randomly generated value"
    );
    assert_eq!(
        loaded_config.base_url, test_base_url,
        "Base URL should match the randomly generated value"
    );

    println!("✅ Keychain test setup working correctly with random values");
    println!("   API Key: {}", loaded_config.api_key);
    println!("   Base URL: {}", loaded_config.base_url);

    // Clean up test data
    let _ = remove_config(KeychainEnvironment::Test);

    // Verify cleanup worked by trying to load again (should fail)
    match load_config(KeychainEnvironment::Test) {
        Ok(_) => panic!("Expected keychain cleanup to remove test config, but it still exists"),
        Err(_) => println!("✅ Keychain cleanup successful - config no longer loadable"),
    }
}

// ---------------------------------------------------------------------------
// /health consensus liveness (consensus-shell-wedge fix)

#[cfg(test)]
mod health {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use tower::ServiceExt;

    use crate::consensus::malachite::EngineHandle;
    use hopnet_common::fileprovider::{HealthResponse, HealthStatus};

    /// Test AppState marked set-up (this_node row present), no engine yet.
    fn setup_app_state() -> crate::AppState {
        let app_state = crate::consensus::tests::create_test_app_state();
        let conn = app_state.db_pool.get().unwrap();
        conn.execute(
            "INSERT INTO this_node (internal_id, node_id, privkey) VALUES (1, 1, X'00')",
            [],
        )
        .unwrap();
        app_state
    }

    /// Install a synthetic engine handle; returns the liveness flag and the
    /// decided-height sender (kept alive by the caller).
    fn install_engine(
        app_state: &crate::AppState,
        height: u64,
    ) -> (Arc<AtomicBool>, tokio::sync::watch::Sender<u64>) {
        let (input_tx, _input_rx) = tokio::sync::mpsc::channel(1);
        std::mem::forget(_input_rx);
        let (decided_tx, decided_rx) = tokio::sync::watch::channel(height);
        let (round_tx, round_rx) = tokio::sync::watch::channel(None);
        std::mem::forget(round_tx);
        let running = Arc::new(AtomicBool::new(true));
        app_state
            .malachite
            .set(EngineHandle {
                input_tx,
                decided: decided_rx,
                round: round_rx,
                sync_inflight: Arc::new(AtomicBool::new(false)),
                running: running.clone(),
            })
            .ok()
            .expect("engine handle installed once");
        (running, decided_tx)
    }

    fn fetch_health(app_state: &crate::AppState) -> HealthResponse {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let app = axum::Router::new()
                .route(
                    "/health",
                    axum::routing::get(crate::fileprovider::routes::get_health),
                )
                .with_state(app_state.clone());
            let resp = app
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/health")
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
                .await
                .unwrap();
            serde_json::from_slice(&bytes).unwrap()
        })
    }

    // Should: report ready for a set-up node with a live consensus shell,
    // carrying the decided height and the node version in the payload.
    #[test]
    fn health_ready_with_live_shell_reports_height() {
        let app_state = setup_app_state();
        let (_running, _decided_tx) = install_engine(&app_state, 7);
        let body = fetch_health(&app_state);
        assert!(matches!(body.status, HealthStatus::Ready), "{body:?}");
        assert_eq!(body.consensus_height, Some(7));
        assert_eq!(body.node_version, crate::version::effective_running_code());
    }

    // Impact: the exact zombie of the 2026-08-17 wedge — a node whose
    // consensus shell stopped kept answering Ready for 42 minutes, and the
    // failure was only found by comparing heights across hosts by hand.
    // This pins that the flag the shell's Drop guard clears is the flag
    // /health reads.
    // Should: report not_ready once the shell's liveness flag clears.
    // Should not: hide the last decided height (operators compare it
    // across nodes).
    #[test]
    fn health_not_ready_when_shell_dead() {
        let app_state = setup_app_state();
        let (running, _decided_tx) = install_engine(&app_state, 7);
        running.store(false, Ordering::SeqCst);
        let body = fetch_health(&app_state);
        assert!(matches!(body.status, HealthStatus::NotReady), "{body:?}");
        assert_eq!(body.consensus_height, Some(7));
    }

    // Impact: main.rs's restart path logs a failed spawn_engine and keeps
    // serving — the same zombie shape as a died shell, distinguishable only
    // by the missing handle. A set-up node without an engine must not
    // report Ready.
    // Should: report not_ready for a set-up node whose engine never
    // started, with no height in the payload.
    #[test]
    fn health_not_ready_when_engine_never_started() {
        let app_state = setup_app_state();
        let body = fetch_health(&app_state);
        assert!(matches!(body.status, HealthStatus::NotReady), "{body:?}");
        assert_eq!(body.consensus_height, None);
    }
}
