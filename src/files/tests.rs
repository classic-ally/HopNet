/// File processing functions test module
#[cfg(test)]
mod tests {
    use crate::files::functions::{
        calculate_chunked_fragments, calculate_padding_and_chunks,
        MAX_FRAGMENT_SIZE, CHUNK_SIZE, ORIGINAL_FRAGMENTS_PER_CHUNK, RECOVERY_FRAGMENTS_PER_CHUNK
    };
    use rand::prelude::*;

    /// Helper function to generate random test data
    fn generate_random_data(size: usize) -> Vec<u8> {
        let mut rng = rand::rng();
        (0..size).map(|_| rng.r#gen::<u8>()).collect()
    }

    /// Test calculate_chunked_fragments function for Phase 4 chunked Reed-Solomon
    #[test]
    fn test_calculate_chunked_fragments() {
        // Empty file: special case, no chunks
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(0);
        assert_eq!(num_chunks, 0, "Empty file should have 0 chunks (handled separately)");
        assert_eq!(total_original, 0, "Empty file should have 0 original fragments");
        assert_eq!(total_recovery, 0, "Empty file should have 0 recovery fragments");

        // 1 byte file: 1 chunk (< 40MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1);
        assert_eq!(num_chunks, 1, "1-byte file should have 1 chunk");
        assert_eq!(total_original, 10, "Should have 10 original fragments");
        assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

        // 1KB file: 1 chunk (< 40MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1024);
        assert_eq!(num_chunks, 1, "1KB file should have 1 chunk");
        assert_eq!(total_original, 10, "Should have 10 original fragments");
        assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

        // 1MB file: 1 chunk (< 40MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1024 * 1024);
        assert_eq!(num_chunks, 1, "1MB file should have 1 chunk");
        assert_eq!(total_original, 10, "Should have 10 original fragments");
        assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

        // 40MB file: exactly 1 chunk
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(CHUNK_SIZE);
        assert_eq!(num_chunks, 1, "40MB file should have 1 chunk");
        assert_eq!(total_original, 10, "Should have 10 original fragments");
        assert_eq!(total_recovery, 20, "Should have 20 recovery fragments");

        // 45MB file: 2 chunks (chunk 0: 40MB, chunk 1: 5MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(45 * 1024 * 1024);
        assert_eq!(num_chunks, 2, "45MB file should have 2 chunks");
        assert_eq!(total_original, 20, "Should have 20 original fragments (10 per chunk)");
        assert_eq!(total_recovery, 40, "Should have 40 recovery fragments (20 per chunk)");

        // 100MB file: 3 chunks (40MB + 40MB + 20MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(100 * 1024 * 1024);
        assert_eq!(num_chunks, 3, "100MB file should have 3 chunks");
        assert_eq!(total_original, 30, "Should have 30 original fragments (10 per chunk)");
        assert_eq!(total_recovery, 60, "Should have 60 recovery fragments (20 per chunk)");

        // 1GB file: 26 chunks (25 × 40MB + 1 × 24MB)
        let (num_chunks, total_original, total_recovery) = calculate_chunked_fragments(1024 * 1024 * 1024);
        assert_eq!(num_chunks, 26, "1GB file should have 26 chunks");
        assert_eq!(total_original, 260, "Should have 260 original fragments (10 per chunk)");
        assert_eq!(total_recovery, 520, "Should have 520 recovery fragments (20 per chunk)");

        // Verify constants relationship: CHUNK_SIZE should equal MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK
        assert_eq!(
            CHUNK_SIZE,
            MAX_FRAGMENT_SIZE * ORIGINAL_FRAGMENTS_PER_CHUNK,
            "CHUNK_SIZE must be derived from MAX_FRAGMENT_SIZE × ORIGINAL_FRAGMENTS_PER_CHUNK"
        );
    }

    /// Test calculate_padding_and_chunks function used for splitting chunks into fragments
    #[test]
    fn test_calculate_padding_and_chunks() {
        // Test with data that divides evenly into even-sized chunks (no padding needed)
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

        assert_eq!(chunks.len(), 2, "Should create 2 chunks");
        assert_eq!(padding, 0, "No padding needed when chunk length is already even");
        assert_eq!(chunks[0].len(), 6, "Each chunk should have 6 bytes");
        assert_eq!(chunks[1].len(), 6, "Each chunk should have 6 bytes");

        // Test with uneven division requiring padding for even chunk length
        // 10 bytes / 2 chunks = 5 bytes each (odd), needs padding to make 6 bytes each
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

        assert_eq!(chunks.len(), 2, "Should create 2 chunks");
        assert_eq!(padding, 2, "Should add 2 bytes of padding to make chunk length even");
        assert_eq!(chunks[0].len(), 6, "Each chunk should have 6 bytes (5 + padding)");
        assert_eq!(chunks[1].len(), 6, "Each chunk should have 6 bytes (5 + padding)");

        // Test with data requiring padding for both uneven division AND even chunk length
        // 5 bytes / 2 chunks = 2.5, rounds up to 3 bytes each (odd)
        // Must add 3 bytes total padding to make 8 bytes → 4 bytes per chunk (even)
        let test_data = vec![1, 2, 3, 4, 5];
        let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);

        assert_eq!(chunks.len(), 2, "Should create 2 chunks");
        assert_eq!(padding, 3, "Should add 3 bytes of padding to ensure even chunk length");
        assert_eq!(chunks[0].len(), 4, "Each chunk should have 4 bytes (even length)");
        assert_eq!(chunks[1].len(), 4, "Each chunk should have 4 bytes (even length)");
    }

    /// Test that chunk content is preserved correctly
    #[test]
    fn test_chunk_content_preservation() {
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (chunks, _padding) = calculate_padding_and_chunks(test_data.clone(), 2);
        
        // Reconstruct data from chunks to verify preservation
        let mut reconstructed = Vec::new();
        for chunk in chunks {
            reconstructed.extend_from_slice(&chunk);
        }
        
        // Should match original data (with possible padding at end)
        assert_eq!(&reconstructed[..test_data.len()], test_data.as_slice(), 
            "Original data should be preserved in chunks");
    }

    /// Test padding edge cases with odd numbers
    #[test]
    fn test_padding_edge_cases() {
        // Test files that don't divide evenly into chunks
        let odd_cases = vec![
            (1, 3),    // 1 byte into 3 chunks
            (5, 2),    // 5 bytes into 2 chunks  
            (7, 3),    // 7 bytes into 3 chunks
            (100, 7),  // 100 bytes into 7 chunks
        ];
        
        for (data_size, num_chunks) in odd_cases {
            let test_data = generate_random_data(data_size);
            let (chunks, _padding) = calculate_padding_and_chunks(test_data.clone(), num_chunks);
            
            assert_eq!(chunks.len(), num_chunks, "Should create exactly {} chunks", num_chunks);
            
            // All chunks should have equal size (with padding)
            let expected_chunk_size = chunks[0].len();
            for (i, chunk) in chunks.iter().enumerate() {
                assert_eq!(chunk.len(), expected_chunk_size, 
                    "Chunk {} should have even length for input size {}", i, data_size);
            }
        }
    }
}