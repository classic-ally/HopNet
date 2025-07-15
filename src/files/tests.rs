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
        let max_fragment_size = 4 * 1024 * 1024; // 4MB

        // Test empty file
        let (orig, recovery) = calculate_optimal_chunks(0);
        assert_eq!(orig, 0, "Empty file should have 0 original chunks");
        assert_eq!(recovery, 0, "Empty file should have 0 recovery chunks");

        // Test small file (should use minimum 10 chunks)
        let (orig, recovery) = calculate_optimal_chunks(1000);
        assert_eq!(orig, 10, "Small file should use minimum 10 original chunks");
        assert_eq!(recovery, 20, "Small file should use 20 recovery chunks");

        // Test file at exactly 4MB boundary
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size);
        assert_eq!(orig, 10, "4MB file should use minimum 10 original chunks");
        assert_eq!(recovery, 20, "4MB file should use 20 recovery chunks");

        // Test file just over 4MB (should need 2 chunks minimum)
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size + 1);
        assert_eq!(orig, 10, "5MB file should use 10 original chunks (minimum kicks in)");
        assert_eq!(recovery, 20, "5MB file should use 20 recovery chunks");

        // Test file requiring exactly 2 chunks
        let (orig, recovery) = calculate_optimal_chunks(max_fragment_size * 2);
        assert_eq!(orig, 10, "8MB file should use 10 original chunks (minimum)");
        assert_eq!(recovery, 20, "8MB file should use 20 recovery chunks");

        // Test large file requiring many chunks
        let large_size = max_fragment_size * 15; // 60MB
        let (orig, recovery) = calculate_optimal_chunks(large_size);
        assert_eq!(orig, 15, "60MB file should use 15 original chunks");
        assert_eq!(recovery, 30, "60MB file should use 30 recovery chunks");

        // Test very large file
        let huge_size = max_fragment_size * 100; // 400MB
        let (orig, recovery) = calculate_optimal_chunks(huge_size);
        assert_eq!(orig, 100, "400MB file should use 100 original chunks");
        assert_eq!(recovery, 200, "400MB file should use 200 recovery chunks");

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

    /// Test that chunk sizes never exceed MAX_FRAGMENT_SIZE
    #[test]
    fn test_chunk_size_consistency() {
        const MAX_FRAGMENT_SIZE: usize = 4 * 1024 * 1024; // 4MB
        
        // Helper function to test a specific file size
        fn test_file_size(file_size: usize) -> (bool, usize, usize, usize) {
            let (num_chunks, _) = calculate_optimal_chunks(file_size);
            let padding = calculate_chunk_padding(file_size, num_chunks);
            let final_chunk_size = if num_chunks == 0 { 0 } else { (file_size + padding) / num_chunks };
            let exceeds = final_chunk_size > MAX_FRAGMENT_SIZE;
            
            (exceeds, final_chunk_size, padding, num_chunks)
        }
        
        let mut violations = Vec::new();
        
        println!("Testing chunk size consistency...");
        println!("MAX_FRAGMENT_SIZE = {} bytes ({} MB)", MAX_FRAGMENT_SIZE, MAX_FRAGMENT_SIZE / (1024 * 1024));
        
        // Test various file sizes
        let test_cases = vec![
            // Edge cases around 4MB boundary
            4 * 1024 * 1024 - 1,      // Just under 4MB
            4 * 1024 * 1024,          // Exactly 4MB  
            4 * 1024 * 1024 + 1,      // Just over 4MB
            
            // Around 10-chunk boundary (40MB)
            40 * 1024 * 1024 - 1,     // Forces 10 chunks, max padding
            40 * 1024 * 1024,         // Exactly 40MB
            40 * 1024 * 1024 + 1,     // Forces 11 chunks
            
            // Small files (force 10 chunks)
            1024,                     // 1KB → 10 chunks
            1024 * 1024,              // 1MB → 10 chunks  
            10 * 1024 * 1024,         // 10MB → 10 chunks
            
            // Large files
            100 * 1024 * 1024,        // 100MB
            500 * 1024 * 1024,        // 500MB
        ];
        
        for file_size in test_cases {
            let (exceeds, final_chunk_size, padding, num_chunks) = test_file_size(file_size);
            
            if exceeds {
                violations.push((file_size, final_chunk_size, padding, num_chunks));
                println!("VIOLATION: {} bytes → chunk_size: {}, padding: {}, chunks: {}", 
                         file_size, final_chunk_size, padding, num_chunks);
            }
        }
        
        // Exhaustive test around boundary conditions
        println!("Exhaustive test around 4MB boundaries...");
        
        // Test all file sizes around multiples of 4MB that could trigger edge cases
        for base_size in [4 * 1024 * 1024, 8 * 1024 * 1024, 40 * 1024 * 1024] {
            for offset in -100..=100 {
                let file_size = (base_size as i64 + offset) as usize;
                if file_size == 0 { continue; }
                
                let (exceeds, final_chunk_size, padding, num_chunks) = test_file_size(file_size);
                
                if exceeds {
                    violations.push((file_size, final_chunk_size, padding, num_chunks));
                    println!("VIOLATION: {} bytes → chunk_size: {}, padding: {}, chunks: {}", 
                             file_size, final_chunk_size, padding, num_chunks);
                }
            }
        }
        
        // Test worst-case padding scenarios
        println!("Testing worst-case padding scenarios...");
        
        // Find file sizes that maximize padding for different chunk counts
        for target_chunks in [10, 11, 12, 25, 26] {
            // File size that creates maximum basic remainder
            let base_file_size = target_chunks * MAX_FRAGMENT_SIZE - 1;
            
            // Test around this size to find worst padding
            for offset in -50..=50 {
                let file_size = (base_file_size as i64 + offset) as usize;
                if file_size == 0 { continue; }
                
                let (actual_chunks, _) = calculate_optimal_chunks(file_size);
                if actual_chunks != target_chunks { continue; }
                
                let (exceeds, final_chunk_size, padding, num_chunks) = test_file_size(file_size);
                
                if exceeds {
                    violations.push((file_size, final_chunk_size, padding, num_chunks));
                    println!("VIOLATION: {} bytes → chunk_size: {}, padding: {}, chunks: {}", 
                             file_size, final_chunk_size, padding, num_chunks);
                }
            }
        }
        
        // Summary
        if !violations.is_empty() {
            panic!("❌ VIOLATIONS FOUND: {}\nThe following file sizes produce chunks larger than MAX_FRAGMENT_SIZE:\n{:#?}", 
                   violations.len(), violations);
        }
        
        println!("✅ NO VIOLATIONS FOUND");
        println!("All chunk sizes respect the MAX_FRAGMENT_SIZE limit");
    }

    /// Test that files larger than 40MB use optimal chunk counts for stable boundaries
    #[test]
    fn test_large_file_chunk_stability() {
        const MAX_FRAGMENT_SIZE: usize = 4 * 1024 * 1024; // 4MB
        const MINIMUM_CHUNKS: usize = 10;
        const LARGE_FILE_THRESHOLD: usize = MINIMUM_CHUNKS * MAX_FRAGMENT_SIZE; // 40MB
        
        // Helper function to test chunk stability for large files
        fn test_large_file_stability(file_size: usize) -> (bool, usize, usize, usize, usize) {
            let (num_chunks, _) = calculate_optimal_chunks(file_size);
            let padding = calculate_chunk_padding(file_size, num_chunks);
            let final_chunk_size = if num_chunks == 0 { 0 } else { (file_size + padding) / num_chunks };
            
            // For large files, optimal chunk count should be ceiling division by MAX_FRAGMENT_SIZE
            let optimal_chunks = (file_size + MAX_FRAGMENT_SIZE - 1) / MAX_FRAGMENT_SIZE;
            let uses_optimal_chunks = num_chunks == optimal_chunks;
            
            (uses_optimal_chunks, final_chunk_size, padding, num_chunks, optimal_chunks)
        }
        
        let mut suboptimal_cases = Vec::new();
        
        println!("Testing large file chunk stability (files > 40MB should use optimal chunk counts)...");
        println!("MAX_FRAGMENT_SIZE = {} bytes ({} MB)", MAX_FRAGMENT_SIZE, MAX_FRAGMENT_SIZE / (1024 * 1024));
        println!("LARGE_FILE_THRESHOLD = {} bytes ({} MB)", LARGE_FILE_THRESHOLD, LARGE_FILE_THRESHOLD / (1024 * 1024));
        println!("Optimal chunks = ceiling(file_size / 4MB) for maximum chunk stability\\n");
        
        // Test various large file sizes that should use optimal chunk counts
        let test_cases = vec![
            // Just over 40MB threshold
            LARGE_FILE_THRESHOLD + 1,
            LARGE_FILE_THRESHOLD + 1024,
            LARGE_FILE_THRESHOLD + 1024 * 1024,
            
            // Common large file sizes
            50 * 1024 * 1024,   // 50MB
            64 * 1024 * 1024,   // 64MB (old boundary)
            100 * 1024 * 1024,  // 100MB
            200 * 1024 * 1024,  // 200MB
            500 * 1024 * 1024,  // 500MB
            1024 * 1024 * 1024, // 1GB
        ];
        
        for file_size in &test_cases {
            let (uses_optimal, final_chunk_size, padding, actual_chunks, optimal_chunks) = test_large_file_stability(*file_size);
            
            println!("{:>12} bytes ({:>5} MB) → actual: {:>3} chunks, optimal: {:>3} chunks, chunk_size: {:>10} bytes, optimal: {}", 
                     file_size, file_size / (1024 * 1024), actual_chunks, optimal_chunks, final_chunk_size, 
                     if uses_optimal { "✓" } else { "✗" });
            
            if !uses_optimal {
                suboptimal_cases.push((*file_size, final_chunk_size, padding, actual_chunks, optimal_chunks));
            }
        }
        
        // Exhaustive sweep around boundaries where chunk count changes
        println!("\\nExhaustive sweep around chunk count boundaries...");
        
        // Test around multiples of 4MB where chunk count would increase
        for multiplier in 11..=50 { // Test 44MB to 200MB range
            let base_size = multiplier * MAX_FRAGMENT_SIZE;
            
            // Test around the exact boundary where chunk count changes
            for offset in -1000..=1000 {
                let file_size = (base_size as i64 + offset) as usize;
                if file_size <= LARGE_FILE_THRESHOLD { continue; } // Skip small files
                
                let (uses_optimal, final_chunk_size, padding, actual_chunks, optimal_chunks) = test_large_file_stability(file_size);
                
                if !uses_optimal {
                    suboptimal_cases.push((file_size, final_chunk_size, padding, actual_chunks, optimal_chunks));
                    println!("SUBOPTIMAL: {} bytes → actual: {} chunks, optimal: {} chunks, chunk_size: {} bytes", 
                             file_size, actual_chunks, optimal_chunks, final_chunk_size);
                }
            }
        }
        
        // Test edge cases where files are just under chunk boundaries
        println!("\\nTesting files just under chunk count boundaries...");
        
        for target_chunks in 11..=25 {
            // File size just under requiring `target_chunks` 
            let boundary_file_size = target_chunks * MAX_FRAGMENT_SIZE - 1;
            if boundary_file_size <= LARGE_FILE_THRESHOLD { continue; }
            
            let (uses_optimal, final_chunk_size, padding, actual_chunks, optimal_chunks) = test_large_file_stability(boundary_file_size);
            
            println!("{:>12} bytes (just under {} chunks) → actual: {} chunks, optimal: {} chunks, optimal: {}", 
                     boundary_file_size, target_chunks, actual_chunks, optimal_chunks, 
                     if uses_optimal { "✓" } else { "✗" });
            
            if !uses_optimal {
                suboptimal_cases.push((boundary_file_size, final_chunk_size, padding, actual_chunks, optimal_chunks));
            }
        }
        
        // Summary
        if !suboptimal_cases.is_empty() {
            println!("\\n❌ SUBOPTIMAL CHUNKING FOUND: {}", suboptimal_cases.len());
            println!("The following large file sizes don't use optimal chunk counts:");
            for (file_size, chunk_size, padding, actual, optimal) in &suboptimal_cases {
                let mb = file_size / (1024 * 1024);
                println!("  {}MB file: {} chunks (should be {}), chunk_size: {}MB", 
                         mb, actual, optimal, chunk_size / (1024 * 1024));
            }
            println!("\\nThis means incremental edits will cause unnecessary rechunking and reuploads.");
            panic!("Large files should use optimal chunk counts for stable chunk boundaries");
        }
        
        println!("\\n✅ ALL LARGE FILES USE OPTIMAL CHUNKING");
        println!("All files larger than 40MB use optimal chunk counts for stable boundaries");
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
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        // Test empty file
        let test_id = CustomUUID::new(None);
        let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
        let result = shard_file(vec![], "/tmp/test_fragments", test_id, &per_file_key).await;
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
            let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone(), &per_file_key).await;
            assert!(result.is_ok(), "Failed to shard file of size {}: {:?}", size, result.err());
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Non-empty file should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // Verify file hash is correct (with data_block_id appended for privacy)
            let expected_hash = {
                let mut hasher = blake3::Hasher::new();
                hasher.update(&original_data);
                hasher.update(test_id.as_bytes());
                crate::db::Blake3Hash::new(hasher.finalize())
            };
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
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        let large_sizes = vec![
            16 * 1024 * 1024,     // 16MB (boundary for 4 chunks)
            17 * 1024 * 1024,     // 17MB (just over boundary)
            50 * 1024 * 1024,     // 50MB
        ];

        for size in large_sizes {
            println!("Testing large file size: {} MB", size / (1024 * 1024));
            let test_data = generate_random_data(size);
            
            let test_id = CustomUUID::new(None);
            let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone(), &per_file_key).await;
            assert!(result.is_ok(), "Failed to shard large file of size {}", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Large file should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // Verify we have the expected number of fragments
            let expected_chunks = if size <= 4 * 1024 * 1024 * 10 { 10 } else { (size + 4 * 1024 * 1024 - 1) / (4 * 1024 * 1024) };
            let expected_total = expected_chunks * 3; // 1 original + 2 recovery
            assert_eq!(data.fragments.len(), expected_total, 
                "Large file should have {} fragments, got {}", expected_total, data.fragments.len());
        }
    }

    /// Test Reed-Solomon encoding works correctly
    #[tokio::test]
    async fn test_reed_solomon_functionality() {
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        let test_data = generate_random_data(10000);
        let test_id = CustomUUID::new(None);
        let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
        let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone(), &per_file_key).await;
        
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
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        // Test empty file
        let test_id1 = CustomUUID::new(None);
        let test_id2 = CustomUUID::new(None);
        let per_file_key1 = ChaCha20Poly1305::generate_key(&mut OsRng);
        let per_file_key2 = ChaCha20Poly1305::generate_key(&mut OsRng);
        let empty_result1 = shard_file(vec![], "/tmp/test_fragments", test_id1, &per_file_key1).await.unwrap();
        let empty_result2 = shard_file(vec![], "/tmp/test_fragments", test_id2, &per_file_key2).await.unwrap();
        assert_eq!(empty_result1, empty_result2, "Empty files should produce identical results");
        
        // Test non-empty file
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        
        let test_id3 = CustomUUID::new(None);
        let per_file_key3 = ChaCha20Poly1305::generate_key(&mut OsRng);
        let result1 = shard_file(test_data.clone(), "/tmp/test_fragments", test_id3.clone(), &per_file_key3).await.unwrap();
        let result2 = shard_file(test_data, "/tmp/test_fragments", test_id3, &per_file_key3).await.unwrap();
        
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
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        let data1 = generate_random_data(1000);
        let data2 = generate_random_data(1000);
        
        let test_id1 = CustomUUID::new(None);
        let test_id2 = CustomUUID::new(None);
        let per_file_key1 = ChaCha20Poly1305::generate_key(&mut OsRng);
        let per_file_key2 = ChaCha20Poly1305::generate_key(&mut OsRng);
        let result1 = shard_file(data1, "/tmp/test_fragments", test_id1, &per_file_key1).await.unwrap().unwrap();
        let result2 = shard_file(data2, "/tmp/test_fragments", test_id2, &per_file_key2).await.unwrap().unwrap();
        
        assert_ne!(result1.hash, result2.hash, "Different files should have different hashes");
        assert_ne!(result1.fragments, result2.fragments, "Different files should have different fragments");
    }

    /// Test 2:1 redundancy ratio verification
    #[tokio::test]
    async fn test_2_to_1_redundancy_ratio() {
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
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
            let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id, &per_file_key).await.unwrap().unwrap();
            
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
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        // Test a file that requires many chunks (more than 10 minimum)
        let large_size = 100 * 1024 * 1024; // 100MB (should need 25 chunks)
        let test_data = generate_random_data(large_size);
        let test_id = CustomUUID::new(None);
        let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
        
        let result = shard_file(test_data, "/tmp/test_fragments", test_id, &per_file_key).await;
        assert!(result.is_ok(), "Should handle very large files");
        
        let sharded_data = result.unwrap();
        assert!(sharded_data.is_some(), "Large file should return Some(Data)");
        
        let data = sharded_data.unwrap();
        
        // Verify fragment count calculation
        let expected_chunks = std::cmp::max(10, (large_size + 4 * 1024 * 1024 - 1) / (4 * 1024 * 1024));
        let expected_total = expected_chunks * 3;
        assert_eq!(data.fragments.len(), expected_total, 
            "Should have {} fragments for {}MB file", expected_total, large_size / (1024 * 1024));
        
        // Should use more than minimum chunks for this size
        assert!(data.fragments.len() > 30, "100MB file should use more than minimum fragments");
    }

    /// Test chunk size requirements for Reed-Solomon
    #[tokio::test]
    async fn test_chunk_even_length_requirement() {
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        // Test various odd sizes that might cause Reed-Solomon issues
        let odd_sizes = vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 101, 1001];
        
        for size in odd_sizes {
            let test_data = generate_random_data(size);
            let test_id = CustomUUID::new(None);
            let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone(), &per_file_key).await;
            
            assert!(result.is_ok(), "Should handle odd-sized file: {} bytes", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Odd-sized file should return Some(Data)");
            
            // If we get here, Reed-Solomon succeeded, meaning chunks were properly padded to even lengths
        }
    }

    /// Test edge cases at 4MB boundaries
    #[tokio::test]
    async fn test_4mb_boundary_cases() {
        use chacha20poly1305::{ChaCha20Poly1305, KeyInit, aead::OsRng};
        
        let boundary_size = 4 * 1024 * 1024;
        let test_cases = vec![
            boundary_size - 1,     // Just under 4MB
            boundary_size,         // Exactly 4MB
            boundary_size + 1,     // Just over 4MB
        ];

        for size in test_cases {
            let test_data = generate_random_data(size);
            let test_id = CustomUUID::new(None);
            let per_file_key = ChaCha20Poly1305::generate_key(&mut OsRng);
            let result = shard_file(test_data, "/tmp/test_fragments", test_id.clone(), &per_file_key).await;
            
            assert!(result.is_ok(), "Should handle boundary case: {} bytes", size);
            
            let sharded_data = result.unwrap();
            assert!(sharded_data.is_some(), "Boundary case should return Some(Data)");
            
            let data = sharded_data.unwrap();
            
            // All these cases should use minimum 10 chunks since they're <= 40MB
            assert_eq!(data.fragments.len(), 30, 
                "Boundary case {} should use 30 fragments (10 orig + 20 recovery)", size);
        }
    }
}
