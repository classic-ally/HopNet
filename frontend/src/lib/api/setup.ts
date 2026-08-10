import { API_BASE_URL } from '../stores';

/**
 * Every network call the setup and login flow makes, behind one injectable
 * interface. Each component in the flow takes a `SetupApi` prop defaulting to
 * `liveSetupApi`, so Storybook and tests can walk the whole state machine
 * against a fake with no backend running. The invariant this file exists to
 * hold: nothing in the flow calls `fetch` directly.
 *
 * Failures are returned rather than thrown, because every caller already
 * branches on the status to choose its message. `status: TRANSPORT_FAILURE`
 * marks a request that never got an HTTP reply at all.
 */

/** No HTTP response — DNS failure, closed port, abort. */
export const TRANSPORT_FAILURE = 0;

export type CreateNetworkResult =
    | { ok: true; passphrase: string }
    | { ok: false; status: number; detail?: string };

export type PubkeyResult =
    | { ok: true; pubkey: string }
    | { ok: false; status: number; detail?: string };

export type LoginResult =
    | { ok: true; token: string }
    | { ok: false; status: number; detail?: string };

export interface SetupApi {
    /** POST /setup — genesis. Resolves with the passphrase the node minted. */
    createNetwork(username: string, nodeName: string): Promise<CreateNetworkResult>;

    /**
     * GET /setup — this node's public key, for the pairing QR. The endpoint
     * answers 404 before genesis and 200 after, carrying the pubkey in the
     * body either way, so both count as success here.
     */
    fetchPubkey(): Promise<PubkeyResult>;

    /** POST /login — exchanges credentials for a session token. */
    login(username: string, passphrase: string, rememberMe: boolean): Promise<LoginResult>;
}

function detailOf(error: unknown): string {
    return error instanceof Error ? error.message : 'Unknown error';
}

export const liveSetupApi: SetupApi = {
    async createNetwork(username, nodeName) {
        try {
            const response = await fetch(`${API_BASE_URL}/setup`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, node_name: nodeName }),
            });
            if (!response.ok) return { ok: false, status: response.status };
            const data = await response.json();
            return { ok: true, passphrase: data.passphrase };
        } catch (error) {
            return { ok: false, status: TRANSPORT_FAILURE, detail: detailOf(error) };
        }
    },

    async fetchPubkey() {
        try {
            const response = await fetch(`${API_BASE_URL}/setup`);
            if (response.status !== 404 && !response.ok) {
                return { ok: false, status: response.status };
            }
            return { ok: true, pubkey: await response.json() };
        } catch (error) {
            return { ok: false, status: TRANSPORT_FAILURE, detail: detailOf(error) };
        }
    },

    async login(username, passphrase, rememberMe) {
        try {
            const response = await fetch(`${API_BASE_URL}/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username, passphrase, remember_me: rememberMe }),
            });
            if (!response.ok) return { ok: false, status: response.status };
            const data = await response.json();
            return { ok: true, token: data.token };
        } catch (error) {
            return { ok: false, status: TRANSPORT_FAILURE, detail: detailOf(error) };
        }
    },
};
