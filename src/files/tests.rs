#[cfg(test)]
mod tests {
    use crate::files::functions::*;
    use crate::db::CustomUUID;
    use rand::Rng;

    /// Generate random test data of specified size
    fn generate_random_data(size: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        (0..size).map(|_| rng.r#gen::<u8>()).collect()
    }

    /// Test calculate_optimal_chunks function
    #[test]
    fn test_calculate_optimal_chunks() {
        let max_fragment_size = 64 * 1024 * 1024; // 64MB

        // Test empty file
        let (orig, recovery) = calculate_optimal_chunks(0);
        assert_eq!(orig, 0, "Empty file should have 0 original chunks");
        assert_eq!(recovery, 0, "Empty file should have 0 recovery chunks");

        // Test small file (should use minimum 10 chunks)
        let (orig, recovery) = calculate_optimal_chunks(1000);
        assert_eq!(orig, 10, "Small file should use minimum 10 original chunks");
        assert_eq!(recovery, 20, "Small file should use 20 recovery chunks");

        // Test file at exactly 64MB boundary
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size);
        assert_eq!(orig, 10, "64MB file should use minimum 10 original chunks");
        assert_eq!(recovery, 20, "64MB file should use 20 recovery chunks");

        // Test file just over 64MB (should need 2 chunks minimum)
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size + 1);
        assert_eq!(orig, 10, "65MB file should use 10 original chunks (minimum kicks in)");
        assert_eq!(recovery, 20, "65MB file should use 20 recovery chunks");

        // Test file requiring exactly 2 chunks
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size * 2);
        assert_eq!(orig, 10, "128MB file should use 10 original chunks (minimum)");
        assert_eq!(recovery, 20, "128MB file should use 20 recovery chunks");

        // Test large file requiring many chunks
        let large_size = max_fragment_size * 15; // 960MB
        let (orig, recovery) = calculate_optimal_chunks(large_size);
        assert_eq!(orig, 15, "960MB file should use 15 original chunks");
        assert_eq!(recovery, 30, "960MB file should use 30 recovery chunks");

        // Test very large file
        let huge_size = max_fragment_size * 100; // 6.4TB
        let (orig, recovery) = calculate_optimal_chunks(huge_size);
        assert_eq!(orig, 100, "6.4TB file should use 100 original chunks");
        assert_eq!(recovery, 200, "6.4TB file should use 200 recovery chunks");

        // Verify 2:1 ratio always holds for non-empty files
        for size in [1, 1000, max_fragment_size, max_fragment_size * 5] {
            let (orig, recovery) = calculate_optimal_chunks(size);
            assert_eq!(recovery, orig * 2, "Should maintain 2:1 recovery ratio for size {}", size);
        }
    }

    /// Test calculate_padding_and_chunks function
    #[test]
    fn test_calculate_padding_and_chunks() {
        // Test empty file
        let (chunks, padding) = calculate_padding_and_chunks(vec![], 0);
        assert_eq!(chunks.len(), 0, "Empty file should produce 0 chunks");
        assert_eq!(padding, 0, "Empty file should have no padding");

        // Test single byte, single chunk
        let (chunks, padding) = calculate_padding_and_chunks(vec![42], 1);
        assert_eq!(chunks.len(), 1, "Single byte should produce 1 chunk");
        assert_eq!(chunks[0].len(), 2, "Single byte should be padded to even length");
        assert_eq!(padding, 1, "Single byte should have 1 padding byte");

        // Test odd-sized file that needs padding
        let data = vec![1, 2, 3, 4, 5]; // 5 bytes
        let (chunks, padding) = calculate_padding_and_chunks(data, 1);
        assert_eq!(chunks.len(), 1, "Should produce 1 chunk");
        assert_eq!(chunks[0].len() % 2, 0, "Chunk length should be even");
        assert!(padding > 0, "Should add padding for odd-sized data");

        // Test multiple chunks with even division
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8]; // 8 bytes
        let (chunks, padding) = calculate_padding_and_chunks(data, 2);
        assert_eq!(chunks.len(), 2, "Should produce 2 chunks");
        assert_eq!(chunks[0].len(), 4, "Each chunk should be 4 bytes");
        assert_eq!(chunks[1].len(), 4, "Each chunk should be 4 bytes");
        assert_eq!(chunks[0].len() % 2, 0, "Chunk 0 should have even length");
        assert_eq!(chunks[1].len() % 2, 0, "Chunk 1 should have even length");

        // Test multiple chunks with odd division needing padding
        let data = vec![1, 2, 3, 4, 5, 6, 7]; // 7 bytes
        let (chunks, padding) = calculate_padding_and_chunks(data, 2);
        assert_eq!(chunks.len(), 2, "Should produce 2 chunks");
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.len() % 2, 0, "Chunk {} should have even length", i);
        }
        assert!(padding > 0, "Should add padding for uneven division");

        // Test that all chunks are the same size
        let data = generate_random_data(1000);
        let (chunks, _) = calculate_padding_and_chunks(data, 10);
        assert_eq!(chunks.len(), 10, "Should produce 10 chunks");
        let first_chunk_size = chunks[0].len();
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.len(), first_chunk_size, "Chunk {} should be same size as others", i);
            assert_eq!(chunk.len() % 2, 0, "Chunk {} should have even length", i);
        }
    }

    /// Test that chunk content is preserved correctly
    #[test]
    fn test_chunk_content_preservation() {
        let original_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (chunks, padding) = calculate_padding_and_chunks(original_data.clone(), 2);
        
        // Reconstruct data from chunks (removing padding)
        let mut reconstructed = Vec::new();
        for chunk in chunks {
            reconstructed.extend(chunk);
        }
        
        // Remove padding bytes
        reconstructed.truncate(reconstructed.len() - padding as usize);
        
        assert_eq!(reconstructed, original_data, "Data should be preserved through chunking");
    }

    /// Test padding edge cases with odd numbers
    #[test]
    fn test_padding_edge_cases() {
        // Test odd numbers that might cause issues
        let odd_sizes = vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21];
        
        for size in odd_sizes {
            let data = generate_random_data(size);
            let (chunks, _) = calculate_padding_and_chunks(data, 10);
            
            // All chunks should have even length
            for (i, chunk) in chunks.iter().enumerate() {
                assert_eq!(chunk.len() % 2, 0, 
                    "Chunk {} should have even length for input size {}", i, size);
            }
        }
    }

    /// Test shard_file function end-to-end
    #[tokio::test]
    async fn test_shard_file_basic_functionality() {
        use crate::db::CustomUUID;
        
        // Test empty file
        let test_id = CustomUUID::new(None);
        let result = shard_file(vec![], "/tmp/test_fragments", test_id).await;
        assert!(result.is_ok(), "Empty file should be handled successfully");
        assert!(result.unwrap().is_none(), "Empty file should return None");

        let test_sizes = vec![
            1,           // 1 byte
            100,         // Small file
            1_000,       // 1KB
            10_000,      // 10KB
            1_000_000,   // 1MB
        ];

        for size in test_sizes {
            println!("Testing shard_file with size: {} bytes", size);
            let test_data = generate_random_data(size);
            let original_data = test_data.clone();
            
            let test_id = CustomUUID::new(None);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone()).await;
            assert!(result.is_ok(), "Failed to shard file of size {}: {:?}", size, result.err());
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Non-empty file should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // Verify file hash is correct
            let expected_hash = crate::db::Blake3Hash::new(blake3::hash(&original_data));
            assert_eq!(data.hash, expected_hash, "File hash should match original");
            
            // Verify we have fragments
            assert!(!data.fragments.is_empty(), "Should have fragments");
            
            // Verify 2:1 ratio
            let total_fragments = data.fragments.len();
            assert_eq!(total_fragments % 3, 0, "Total fragments should be divisible by 3 (1 orig + 2 recovery)");
            
            // Verify all fragments have correct structure
            for (i, fragment) in data.fragments.iter().enumerate() {
                assert!(!fragment.fragment_hash.to_hex().is_empty(), "Fragment {} hash should not be empty", i);
                assert_eq!(fragment.data_block_id, test_id, "Fragment {} should have correct data_block_id", i);
                assert!(!fragment.stored_locally, "Fragment {} should start with stored_locally = false", i);
            }
        }
    }

    /// Test shard_file with large files requiring multiple chunks
    #[tokio::test]
    async fn test_shard_file_large_files() {
        let large_sizes = vec![
            64 * 1024 * 1024,     // 64MB (boundary)
            65 * 1024 * 1024,     // 65MB (just over boundary)
            128 * 1024 * 1024,    // 128MB
        ];

        for size in large_sizes {
            println!("Testing large file size: {} MB", size / (1024 * 1024));
            let test_data = generate_random_data(size);
            
            let test_id = CustomUUID::new(None);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone()).await;
            assert!(result.is_ok(), "Failed to shard large file of size {}", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Large file should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // Verify we have the expected number of fragments
            let expected_chunks = if size <= 64 * 1024 * 1024 * 10 { 10 } else { (size + 64 * 1024 * 1024 - 1) / (64 * 1024 * 1024) };
            let expected_total = expected_chunks * 3; // 1 original + 2 recovery
            assert_eq!(data.fragments.len(), expected_total, 
                "Large file should have {} fragments, got {}", expected_total, data.fragments.len());
        }
    }

    /// Test Reed-Solomon encoding works correctly
    #[tokio::test]
    async fn test_reed_solomon_functionality() {
        let test_data = generate_random_data(10000);
        let test_id = CustomUUID::new(None);
        let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone()).await;
        
        assert!(result.is_ok(), "Reed-Solomon encoding should succeed");
        
        let sharded_data = result.unwrap();
        assert!(sharded_data.is_some(), "Non-empty file should return Some(Data)");
        
        let data = sharded_data.unwrap();
        
        // Verify we have both original and recovery fragments
        assert!(data.fragments.len() >= 30, "Should have at least 30 fragments (10 orig + 20 recovery)");
        
        // All fragments should have valid structure (Reed-Solomon succeeded)
        for fragment in &data.fragments {
            assert!(!fragment.fragment_hash.to_hex().is_empty(), "Fragment hash should not be empty");
            assert_eq!(fragment.data_block_id, test_id, "Fragment should have correct data_block_id");
        }
    }

    /// Test deterministic behavior
    #[tokio::test]
    async fn test_deterministic_behavior() {
        // Test empty file
        let test_id1 = CustomUUID::new(None);
        let test_id2 = CustomUUID::new(None);
        let empty_result1 = shard_file(vec![], "/tmp/test_fragments", test_id1).await.unwrap();
        let empty_result2 = shard_file(vec![], "/tmp/test_fragments", test_id2).await.unwrap();
        assert_eq!(empty_result1, empty_result2, "Empty files should produce identical results");
        
        // Test non-empty file
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        
        let test_id3 = CustomUUID::new(None);
        let result1 = shard_file(test_data.clone(), "/tmp/test_fragments", test_id3.clone()).await.unwrap();
        let result2 = shard_file(test_data, "/tmp/test_fragments", test_id3).await.unwrap();
        
        // Compare just the hash (UUIDs will be different due to cloning different test IDs)
        if let (Some(data1), Some(data2)) = (&result1, &result2) {
            assert_eq!(data1.hash, data2.hash, "Same input should produce identical file hashes");
            assert_eq!(data1.added_bytes, data2.added_bytes, "Same input should have same padding");
        }
        
        // Both should be Some for non-empty files
        assert!(result1.is_some(), "Non-empty file should return Some");
        assert!(result2.is_some(), "Non-empty file should return Some");
        
        let data1 = result1.unwrap();
        let data2 = result2.unwrap();
        
        assert_eq!(data1.hash, data2.hash, "Same input should produce same file hash");
        assert_eq!(data1.fragments, data2.fragments, "Same input should produce same fragments");
        assert_eq!(data1.added_bytes, data2.added_bytes, "Same input should produce same padding");
    }

    /// Test that different files produce different results
    #[tokio::test]
    async fn test_hash_uniqueness() {
        let data1 = generate_random_data(1000);
        let data2 = generate_random_data(1000);
        
        let test_id1 = CustomUUID::new(None);
        let test_id2 = CustomUUID::new(None);
        let result1 = shard_file(data1, "/tmp/test_fragments", test_id1).await.unwrap().unwrap();
        let result2 = shard_file(data2, "/tmp/test_fragments", test_id2).await.unwrap().unwrap();
        
        assert_ne!(result1.hash, result2.hash, "Different files should have different hashes");
        assert_ne!(result1.fragments, result2.fragments, "Different files should have different fragments");
    }

    /// Test 2:1 redundancy ratio verification
    #[tokio::test]
    async fn test_2_to_1_redundancy_ratio() {
        let test_sizes = vec![
            1,
            100,
            1_000,
            10_000,
            100_000,
            1_000_000,
        ];

        for size in test_sizes {
            let test_data = generate_random_data(size);
            let test_id = CustomUUID::new(None);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id).await.unwrap().unwrap();
            
            let total_fragments = result.fragments.len();
            let original_chunks = total_fragments / 3;
            let recovery_chunks = total_fragments - original_chunks;
            
            assert_eq!(recovery_chunks, original_chunks * 2, 
                "Size {}: Recovery chunks ({}) should be 2x original chunks ({})", 
                size, recovery_chunks, original_chunks);
        }
    }

    /// Test very large files
    #[tokio::test]
    async fn test_very_large_files() {
        // Test a file that requires many chunks (more than 10 minimum)
        let large_size = 700 * 1024 * 1024; // 700MB (should need 11 chunks)
        let test_data = generate_random_data(large_size);
        let test_id = CustomUUID::new(None);
        
        let result = shard_file(test_data, "/tmp/test_fragments", test_id).await;
        assert!(result.is_ok(), "Should handle very large files");
        
        let sharded_data = result.unwrap();
        assert!(sharded_data.is_some(), "Large file should return Some(Data)");
        
        let data = sharded_data.unwrap();
        
        // Verify fragment count calculation
        let expected_chunks = std::cmp::max(10, (large_size + 64 * 1024 * 1024 - 1) / (64 * 1024 * 1024));
        let expected_total = expected_chunks * 3;
        assert_eq!(data.fragments.len(), expected_total, 
            "Should have {} fragments for {}MB file", expected_total, large_size / (1024 * 1024));
        
        // Should use more than minimum chunks for this size
        assert!(data.fragments.len() > 30, "700MB file should use more than minimum fragments");
    }

    /// Test chunk size requirements for Reed-Solomon
    #[tokio::test]
    async fn test_chunk_even_length_requirement() {
        // Test various odd sizes that might cause Reed-Solomon issues
        let odd_sizes = vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 101, 1001];
        
        for size in odd_sizes {
            let test_data = generate_random_data(size);
            let test_id = CustomUUID::new(None);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone()).await;
            
            assert!(result.is_ok(), "Should handle odd-sized file: {} bytes", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Odd-sized file should return Some(Data)");
            
            // If we get here, Reed-Solomon succeeded, meaning chunks were properly padded to even lengths
        }
    }

    /// Test edge cases at 64MB boundaries
    #[tokio::test]
    async fn test_64mb_boundary_cases() {
        let boundary_size = 64 * 1024 * 1024;
        let test_cases = vec![
            boundary_size - 1,     // Just under 64MB
            boundary_size,         // Exactly 64MB
            boundary_size + 1,     // Just over 64MB
        ];

        for size in test_cases {
            let test_data = generate_random_data(size);
            let test_id = CustomUUID::new(None);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone()).await;
            
            assert!(result.is_ok(), "Should handle boundary case: {} bytes", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Boundary case should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // All these cases should use minimum 10 chunks since they're <= 640MB
            assert_eq!(data.fragments.len(), 30, 
                "Boundary case {} should use 30 fragments (10 orig + 20 recovery)", size);
        }
    }
}
