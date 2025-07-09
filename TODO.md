[] - Switch any IDs in database to U versions (UINTEGER etc) and the corresponding rust side to u32 etc
[] - Zero-byte file handling in post_files: currently they meet criteria for chunking but should not be created
[] - Fix get_file_fragments to handle empty files (zero-byte files with data_id: None)