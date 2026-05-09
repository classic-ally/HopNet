// Verb pools for `StatusSpinner`. Pick categories per op site; BASE is
// auto-merged into every list because HopNet is a crypto-heavy storage app
// and those verbs always apply.

export const BASE = ['Encrypting', 'Computing', 'Crunching', 'Hashing', 'Working'];

export const WHIMSY = [
    'Forging', 'Weaving', 'Kindling', 'Conjuring', 'Hatching',
    'Stitching', 'Sprouting', 'Plotting', 'Convening', 'Brewing',
];

/// First-time genesis: keypair generation, DB init, passphrase derive.
export const GENESIS = ['Bootstrapping', 'Provisioning', 'Initializing', 'Seeding'];

/// Auth flows: passphrase verify, key unwrap.
export const AUTH = ['Verifying', 'Deriving', 'Unsealing', 'Authenticating'];

/// Network ops: pairing, peer discovery, transport handshake.
export const NETWORK = ['Connecting', 'Pairing', 'Handshaking', 'Dialing', 'Greeting', 'Routing'];

/// Storage ops: chunking, indexing, write paths.
export const STORAGE = ['Chunking', 'Indexing', 'Persisting', 'Allocating', 'Storing'];

/// Merge BASE + caller-provided lists, dedupe order-preserving.
export function mergeStatusWords(...lists: string[][]): string[] {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const w of [...BASE, ...lists.flat()]) {
        if (!seen.has(w)) { seen.add(w); out.push(w); }
    }
    return out;
}
