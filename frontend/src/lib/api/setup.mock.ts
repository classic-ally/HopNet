import { TRANSPORT_FAILURE, type SetupApi } from './setup';

/**
 * An in-memory `SetupApi` for stories and component tests. Nothing in the app
 * imports this, so it tree-shakes out of production bundles.
 *
 * The passphrase is deliberately plain, readable words rather than realistic
 * entropy: whoever is driving the flow by hand has to read three of them off
 * the display step and type them back into the verify step, and a genuine
 * high-entropy passphrase makes that tedious without testing anything extra.
 */

export const MOCK_PASSPHRASE =
    'anchor bramble cinder driftwood ember fathom glimmer harbour ' +
    'iron juniper kindle lantern';

export const MOCK_PUBKEY = 'k51qzi5uqu5dhqhw8gxk4vd9k3rn4kfvbz2mjrs8x0p7hwzf3cq9tuvn6yg2ab';

export const MOCK_TOKEN = 'mock.session.token';

export interface MockSetupOptions {
    /**
     * Artificial latency per call. Non-zero by default so the spinners and
     * disabled-button states — the parts of the flow most likely to regress
     * unnoticed — are actually visible while clicking through.
     */
    latencyMs?: number;
    passphrase?: string;
    pubkey?: string;
    /**
     * Make every call fail with this status. Use `TRANSPORT_FAILURE` for the
     * no-backend case, or a real code (401, 503) to exercise the mapped
     * messages.
     */
    failWith?: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function mockSetupApi(options: MockSetupOptions = {}): SetupApi {
    const {
        latencyMs = 600,
        passphrase = MOCK_PASSPHRASE,
        pubkey = MOCK_PUBKEY,
        failWith,
    } = options;

    async function gate(): Promise<{ ok: false; status: number; detail?: string } | null> {
        if (latencyMs > 0) await sleep(latencyMs);
        if (failWith === undefined) return null;
        return failWith === TRANSPORT_FAILURE
            ? { ok: false, status: TRANSPORT_FAILURE, detail: 'Failed to fetch' }
            : { ok: false, status: failWith };
    }

    return {
        async createNetwork() {
            return (await gate()) ?? { ok: true, passphrase };
        },
        async fetchPubkey() {
            return (await gate()) ?? { ok: true, pubkey };
        },
        async login() {
            return (await gate()) ?? { ok: true, token: MOCK_TOKEN };
        },
    };
}
