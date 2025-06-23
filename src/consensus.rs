/// CONSENSUS ARCHITECTURE
/// Key notes:
/// - Using ed25519 over threshold w/ distributed key generation (e.g. BLS)
///   - Verifier: O(nodes) signature verification vs O(1), but ed is ~10x faster per op
///   - Signer: both O(1), ed is ~5x faster per op
///   - ed25519_dalek library gives us batch verify, ~6-8x faster 128+ batch
///   - Conclusion: probably faster until nodes is multiple hundreds (O(nodes) dominates)
/// 
///   - Gives us audit trail (which node did what?) over BLS
///   - Simpler implementation (no secret sharing polynomial pain)
///   - But, more vulnerable to mistakes
///     - No cryptographic protection against committing undersigned changes
///   - O(nodes) sigs broadcast for changes introduces network overhead
///     - BLS: 96 bytes per sig
///     - ed: 64 bytes per sig -> 6.4kb per 100 nodes
/// 
///   - We may want to address this later based on % overhead stats

use ed25519_dalek::{SigningKey, VerifyingKey};
use rand_core::OsRng;

pub fn generate_ed25519_key() -> (SigningKey, VerifyingKey) {
    let mut csprng = OsRng;
    let private_key= SigningKey::generate(&mut csprng);
    let public_key = private_key.verifying_key();

    return (private_key, public_key);
}