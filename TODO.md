[] - Switch any IDs in database to U versions (UINTEGER etc) and the corresponding rust side to u32 etc
[] - Zero-byte file handling in post_files: currently they meet criteria for chunking but should not be created
[] - Fix get_file_fragments to handle empty files (zero-byte files with data_id: None)
[] - There are bugs in file upload+download not recovering the same thing for very large files. We should switch to raptorQ encoding as our next step rather than spending the time to correct this, or at the very least apply reed-solomon on smaller segments to rework+fix mem consumption.