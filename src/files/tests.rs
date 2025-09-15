/// File processing functions test module
#[cfg(test)]
mod tests {
    use crate::files::functions::{
        calculate_optimal_chunks, calculate_padding_and_chunks, 
        calculate_chunk_padding, calculate_encrypted_chunk_length, 
        MAX_FRAGMENT_SIZE
    };
    use rand::prelude::*;
    
    /// Helper function to generate random test data
    fn generate_random_data(size: usize) -> Vec<u8> {
        let mut rng = rand::rng();
        (0..size).map(|_| rng.r#gen::<u8>()).collect()
    }

    /// Test calculate_optimal_chunks function
    #[test]
    fn test_calculate_optimal_chunks() {
        // Test edge cases and common sizes
        assert_eq!(calculate_optimal_chunks(0), (10, 20));        // Empty file still needs minimum chunks
        assert_eq!(calculate_optimal_chunks(1), (10, 20));        // 1 byte -> 10 chunks
        assert_eq!(calculate_optimal_chunks(1024), (10, 20));     // 1KB -> 10 chunks  
        assert_eq!(calculate_optimal_chunks(1024 * 1024), (10, 20)); // 1MB -> 10 chunks
        assert_eq!(calculate_optimal_chunks(10 * 1024 * 1024), (10, 20)); // 10MB -> 10 chunks
        assert_eq!(calculate_optimal_chunks(40 * 1024 * 1024), (10, 20)); // 40MB -> 10 chunks
        assert_eq!(calculate_optimal_chunks(45 * 1024 * 1024), (12, 24)); // 45MB -> 12 chunks
        assert_eq!(calculate_optimal_chunks(100 * 1024 * 1024), (25, 50)); // 100MB -> 25 chunks
    }

    /// Test calculate_padding_and_chunks function
    #[test]
    fn test_calculate_padding_and_chunks() {
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);
        
        assert_eq!(chunks.len(), 2, "Should create 2 chunks");
        assert_eq!(padding, 0, "No padding needed for even division");
        
        // Test with uneven division requiring padding
        let test_data = vec![1, 2, 3, 4, 5];
        let (chunks, padding) = calculate_padding_and_chunks(test_data.clone(), 2);
        
        assert_eq!(chunks.len(), 2, "Should create 2 chunks");
        assert_eq!(padding, 1, "Should add 1 byte of padding");
        assert_eq!(chunks[0].len(), 3, "First chunk should have 3 bytes");
        assert_eq!(chunks[1].len(), 3, "Second chunk should have 3 bytes (including padding)");
    }

    /// Test that chunk sizes never exceed MAX_FRAGMENT_SIZE
    #[test]
    fn test_chunk_size_consistency() {
        // Test various file sizes to ensure chunks stay within bounds
        let test_sizes = vec![
            1,                          // 1 byte
            1024,                       // 1KB
            1024 * 1024,               // 1MB
            10 * 1024 * 1024,          // 10MB
            50 * 1024 * 1024,          // 50MB
            100 * 1024 * 1024,         // 100MB
            200 * 1024 * 1024,         // 200MB
        ];
        
        for file_size in test_sizes {
            let (passes, chunk_size, _num_chunks, expected_max) = test_file_size(file_size);
            assert!(passes, 
                "File size {} bytes: chunk size {} exceeds max fragment size {}", 
                file_size, chunk_size, expected_max);
        }
        
        // Helper function to test a specific file size
        fn test_file_size(file_size: usize) -> (bool, usize, usize, usize) {
            let (num_original_chunks, _num_recovery_chunks) = calculate_optimal_chunks(file_size);
            let needed_padding = calculate_chunk_padding(file_size, num_original_chunks);
            let chunk_size = (file_size + needed_padding) / num_original_chunks;
            let encrypted_chunk_size = calculate_encrypted_chunk_length(chunk_size);
            
            (encrypted_chunk_size <= MAX_FRAGMENT_SIZE, chunk_size, num_original_chunks, MAX_FRAGMENT_SIZE)
        }
    }

    /// Test that files larger than 40MB use optimal chunk counts for stable boundaries
    #[test]
    fn test_large_file_chunk_stability() {
        // Test file sizes around chunk boundaries to ensure stable behavior
        let boundary_tests = vec![
            40 * 1024 * 1024,      // Exactly 40MB (should use 10 chunks)
            41 * 1024 * 1024,      // Just over 40MB (should calculate dynamically)
            80 * 1024 * 1024,      // 80MB
            120 * 1024 * 1024,     // 120MB
        ];
        
        for file_size in boundary_tests {
            let (is_stable, chunk_count, chunk_size, total_with_recovery, expected_max) = test_large_file_stability(file_size);
            
            if file_size <= 40 * 1024 * 1024 {
                assert_eq!(chunk_count, 10, "Files <= 40MB should use minimum 10 chunks");
            } else {
                assert!(chunk_count >= 10, "Files > 40MB should use at least 10 chunks");
                assert!(chunk_count <= file_size / (1024 * 1024), "Chunk count should be reasonable for file size");
            }
            
            assert!(is_stable, 
                "File size {} MB: chunk size {} should not exceed max fragment size {}", 
                file_size / (1024 * 1024), chunk_size, expected_max);
            
            // Verify 2:1 redundancy ratio
            assert_eq!(total_with_recovery, chunk_count * 3, 
                "Should have 1 original + 2 recovery = 3x fragments");
        }
        
        // Helper function to test chunk stability for large files
        fn test_large_file_stability(file_size: usize) -> (bool, usize, usize, usize, usize) {
            let (num_original_chunks, num_recovery_chunks) = calculate_optimal_chunks(file_size);
            let needed_padding = calculate_chunk_padding(file_size, num_original_chunks);
            let chunk_size = (file_size + needed_padding) / num_original_chunks;
            let encrypted_chunk_size = calculate_encrypted_chunk_length(chunk_size);
            let total_fragments = num_original_chunks + num_recovery_chunks;
            
            (encrypted_chunk_size <= MAX_FRAGMENT_SIZE, 
             num_original_chunks, 
             chunk_size, 
             total_fragments, 
             MAX_FRAGMENT_SIZE)
        }
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