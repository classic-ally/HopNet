//! Reed-Solomon chunking math (RFC-002 encoding, moved verbatim).
//!
//! Stage A carries only the pure chunk/padding arithmetic; the streaming
//! encode/decode pipeline moves here with `api::put`/`api::get` at Stages B/F.

// Fundamental constraint: individual fragment size limit for network performance
pub const MAX_FRAGMENT_SIZE: usize = 4 * 1024 * 1024; // 4MB

// Fixed fragment count per chunk for predictable modulo placement
pub const ORIGINAL_FRAGMENTS_PER_CHUNK: usize = 10;
pub const RECOVERY_FRAGMENTS_PER_CHUNK: usize = 20;
pub const TOTAL_FRAGMENTS_PER_CHUNK: usize =
    ORIGINAL_FRAGMENTS_PER_CHUNK + RECOVERY_FRAGMENTS_PER_CHUNK;

// Derived: logical chunk size is constrained by fragment size and count
// This ensures each fragment in a full chunk is exactly MAX_FRAGMENT_SIZE
pub const CHUNK_SIZE: usize = MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK; // 40MB

/// Returns (num_chunks, total_original_fragments, total_recovery_fragments)
///
/// Each chunk is encoded independently with 10 original + 20 recovery fragments
/// Chunk size is fixed at 40MB, so files >40MB are split into multiple chunks
///
/// Special case: Returns (0, 0, 0) for empty files, which should be handled
/// separately without Reed-Solomon encoding (no fragments created)
pub fn calculate_chunked_fragments(file_size: usize) -> (usize, usize, usize) {
    // Empty files: no chunks, no fragments
    // These are handled specially in the upload path (skipping process_uploaded_file entirely)
    if file_size == 0 {
        return (0, 0, 0);
    }

    // Calculate number of logical chunks
    let num_chunks = file_size.div_ceil(CHUNK_SIZE);

    // Each chunk has fixed 10 original + 20 recovery fragments
    let total_original = num_chunks * ORIGINAL_FRAGMENTS_PER_CHUNK;
    let total_recovery = num_chunks * RECOVERY_FRAGMENTS_PER_CHUNK;

    (num_chunks, total_original, total_recovery)
}

/// Calculate optimal number of original and recovery chunks based on file size
/// DEPRECATED: Use calculate_chunked_fragments() instead for Phase 4+
pub fn calculate_optimal_chunks(file_size: usize) -> (usize, usize) {
    // Calculate minimum chunks needed to stay under fragment size limit
    let min_original_chunks = if file_size == 0 {
        10 // Empty files still need minimum chunks for Reed-Solomon
    } else {
        file_size.div_ceil(MAX_FRAGMENT_SIZE)
    };

    // Ensure at least 10 original chunks for good Reed-Solomon efficiency
    let original_chunks = min_original_chunks.max(10);

    // Use 2:1 redundancy ratio (2 recovery for every 1 original)
    let recovery_chunks = original_chunks * 2;

    (original_chunks, recovery_chunks)
}

pub fn calculate_chunk_padding(file_size: usize, num_chunks: usize) -> usize {
    if num_chunks == 0 {
        return 0; // Defensive: avoid division by zero
    }

    // Calculate padding needed for the chosen number of chunks
    let mut remainder = if file_size == 0 {
        0
    } else {
        (num_chunks - (file_size % num_chunks)) % num_chunks
    };

    // Ensure chunk length is even
    let chunk_len_after_padding = if file_size + remainder == 0 {
        0
    } else {
        (file_size + remainder) / num_chunks
    };

    if chunk_len_after_padding % 2 != 0 {
        remainder += num_chunks;
    }

    remainder
}

/// Calculate padding needed to ensure even chunk sizes
/// Returns (padded_file, added_bytes)
pub fn calculate_padding_and_chunks(mut file: Vec<u8>, num_chunks: usize) -> (Vec<Vec<u8>>, u8) {
    let original_len = file.len();

    let remainder = calculate_chunk_padding(original_len, num_chunks);

    // Apply padding in one go
    if remainder > 0 {
        file.resize(original_len + remainder, 0);
    }
    let added_bytes = remainder as u8;

    // Split into chunks
    let chunks = if file.is_empty() {
        vec![vec![]; num_chunks] // Empty chunks for empty file
    } else {
        let chunk_size = file.len() / num_chunks;
        let mut chunks: Vec<Vec<u8>> = Vec::with_capacity(num_chunks);
        while !file.is_empty() {
            let current_len = file.len();
            let chunk = file.split_off(current_len - chunk_size);
            chunks.push(chunk);
        }
        chunks.reverse();
        chunks
    };

    (chunks, added_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_math_edges() {
        // Should: empty → no fragments; ≤40MB → 1 chunk (10+20); 40MB+1 → 2 chunks.
        // Impact: wrong chunk counts corrupt placement + reconstruction indices.
        assert_eq!(calculate_chunked_fragments(0), (0, 0, 0));
        assert_eq!(calculate_chunked_fragments(1), (1, 10, 20));
        assert_eq!(calculate_chunked_fragments(CHUNK_SIZE), (1, 10, 20));
        assert_eq!(calculate_chunked_fragments(CHUNK_SIZE + 1), (2, 20, 40));
    }

    #[test]
    fn padding_reassembles() {
        // Should: chunks concatenate back to the padded input; added_bytes strips it.
        for len in [1usize, 9, 10, 4096, 12345] {
            let data: Vec<u8> = (0..len as u32).map(|i| (i % 256) as u8).collect();
            let (chunks, added) = calculate_padding_and_chunks(data.clone(), 10);
            assert_eq!(chunks.len(), 10);
            let total: Vec<u8> = chunks.concat();
            assert_eq!(total.len(), len + added as usize);
            assert_eq!(&total[..len], &data[..]);
            // Even chunk length invariant
            assert_eq!(total.len() % 10, 0);
            assert_eq!((total.len() / 10) % 2, 0);
        }
    }
}
