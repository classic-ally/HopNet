use super::*;
use aes_siv::{siv::Aes256Siv, Key, Nonce};
use either::Either;

use crate::files::functions::decrypt_path;

use duckdb::Transaction;

pub fn get_files(
    db: &Arc<Mutex<Connection>>,
    path: String,
    key: &Key<Aes256Siv>,
    nonce: &Nonce
) -> Result<Vec<Inode>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT owner_id, path, type, data_id FROM inodes WHERE path LIKE ? AND path NOT LIKE ?").map_err(|_| DatabaseError::RecallError)?;
            let like_path = format!("{}/%", path);
            let not_like_path = format!("{}/%", like_path);
            dbg!(&like_path, &not_like_path);
            let inodes = stmt.query_map(params![like_path, not_like_path], |row| {
                let path = row.get(1)?;
                let decrypted_path = decrypt_path(path, key, nonce)?;
                let data_id: Option<CustomUUID> = row.get(3)?;
                Ok(Inode {
                    owner: Either::Left(row.get(0)?),
                    path: decrypted_path,
                    inode_type: row.get(2)?,
                    data_id: data_id.map(Either::Left),
                })
            }).map_err(|_| DatabaseError::ProcessingError)?;
            
            Ok(inodes.collect::<Result<Vec<_>, _>>().map_err(|_| DatabaseError::ProcessingError)?)
        }
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn insert_files(
    db: &Arc<Mutex<Connection>>,
    inodes: Vec<Inode>,
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;

            // STEP 1: Collect all paths that need to be inserted
            let new_paths: Vec<String> = inodes.iter()
                .map(|inode| inode.path.clone())
                .collect();
            
            // STEP 2: Find all missing parent directories in one query
            let missing_parents = find_missing_parents(&tx, &new_paths)?;
            
            // STEP 3: Insert missing parent directories first
            if !missing_parents.is_empty() {
                insert_parent_directories(&tx, &missing_parents, &inodes)?;
            }
            
            for inode in inodes {
                // Handle the data_id which can be Either<CustomUUID, DataRecord>
                let data_id: Option<CustomUUID> = match inode.data_id {
                    Some(either::Either::Left(uuid)) => {
                        // If it's already a UUID, use it directly
                        Ok(Some(uuid))
                    },
                    Some(either::Either::Right(data_record)) => {
                        // If it's a DataRecord, we need to insert it first
                        let data_id = data_record.id;
                                                
                        // Access fragment hashes directly (now Vec<FragmentHash>)
                        let data = &data_record.data;
                        
                        // Insert into data_blocks table
                        tx.execute(
                            "INSERT INTO data_blocks (id, access_list, modified_at, file_hash, fragment_count, added_bytes) VALUES (?, ?, ?, ?, ?, ?)",
                            params![
                                data_id,
                                data_record.access_list,
                                data_record.modified_at,
                                data.hash,
                                data.fragments.len() as i32,
                                data.added_bytes
                            ]
                        ).map_err(|_| DatabaseError::InsertError)?;
                        
                        // Insert fragment hashes into fragment_hashes table
                        for fragment in &data.fragments {
                            tx.execute(
                                "INSERT INTO fragment_hashes (data_block_id, fragment_index, fragment_hash, chunk_type, stored_locally) VALUES (?, ?, ?, ?, ?)",
                                params![fragment.data_block_id, fragment.fragment_index, fragment.fragment_hash, fragment.chunk_type, fragment.stored_locally]
                            ).map_err(|_| DatabaseError::InsertError)?;
                        }
                        
                        Ok(Some(data_id))
                    },
                    None => Ok(None)
                }?;
                
                // Get the owner_id from the inode
                let owner_id = match inode.owner {
                    either::Either::Left(user_id) => user_id,
                    either::Either::Right(user) => user.user_id,
                };

                // Insert into inodes table
                tx.execute(
                    "INSERT INTO inodes (owner_id, path, type, data_id) VALUES (?, ?, ?, ?)",
                    params![
                        owner_id,
                        inode.path,
                        inode.inode_type,
                        data_id
                    ]
                ).map_err(|_| DatabaseError::InsertError)?;
            }
            
            // Commit the transaction
            tx.commit().map_err(|_| DatabaseError::InsertError)?;
            
            Ok(())
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}

// Helper function to find missing parent directories
fn find_missing_parents(
    tx: &Transaction,
    new_paths: &[String]
) -> Result<Vec<String>, DatabaseError> {
    if new_paths.is_empty() {
        return Ok(Vec::new());
    }
    
    // Create a temporary table with the new paths
    tx.execute(
        "CREATE TEMP TABLE temp_new_paths (path VARCHAR)",
        []
    ).map_err(|_| DatabaseError::InsertError)?;
    
    // Insert new paths into temp table
    for path in new_paths {
        tx.execute(
            "INSERT INTO temp_new_paths VALUES (?)",
            params![path]
        ).map_err(|_| DatabaseError::InsertError)?;
    }
    
    // Find all missing parent directories
    let query = r#"
        WITH RECURSIVE path_parents AS (
            -- Generate all required parent paths
            SELECT DISTINCT 
                '/' || array_to_string(
                    list_slice(
                        string_split(ltrim(tnp.path, '/'), '/'), 
                        1, 
                        i
                    ), 
                    '/'
                ) as parent_path
            FROM temp_new_paths tnp,
            LATERAL (
                SELECT unnest(
                    generate_series(1, array_length(string_split(ltrim(tnp.path, '/'), '/')) - 1)
                ) as i
            )
            WHERE array_length(string_split(ltrim(tnp.path, '/'), '/')) > 1
        )
        SELECT DISTINCT pp.parent_path
        FROM path_parents pp
        LEFT JOIN inodes i ON pp.parent_path = i.path
        WHERE i.path IS NULL
        ORDER BY pp.parent_path
    "#;
    
    let mut stmt = tx.prepare(query).map_err(|_| DatabaseError::ProcessingError)?;
    let rows = stmt.query_map([], |row| {
        Ok(row.get::<_, String>(0)?)
    }).map_err(|_| DatabaseError::ProcessingError)?;
    
    let mut missing_parents = Vec::new();
    for row in rows {
        missing_parents.push(row.map_err(|_| DatabaseError::ProcessingError)?);
    }
    
    // Clean up temp table
    tx.execute("DROP TABLE temp_new_paths", [])
        .map_err(|_| DatabaseError::ProcessingError)?;
    
    Ok(missing_parents)
}

// Helper function to insert parent directories
fn insert_parent_directories(
    tx: &Transaction,
    missing_parents: &[String],
    inodes: &[Inode]
) -> Result<(), DatabaseError> {
    // Get owner_id from the first inode (assuming same owner for batch)
    let owner_id = match &inodes[0].owner {
        either::Either::Left(user_id) => *user_id,
        either::Either::Right(user) => user.user_id,
    };
    
    // Insert each missing parent directory
    for parent_path in missing_parents {
        
        tx.execute(
            "INSERT INTO inodes (owner_id, path, type, data_id) VALUES (?, ?, 'folder', NULL)",
            params![
                owner_id,
                parent_path
            ]
        ).map_err(|_| DatabaseError::InsertError)?;
    }
    
    Ok(())
}

pub fn delete_files(
    db: &Arc<Mutex<Connection>>,
    path: String,
) -> Result<(), DatabaseError> {
    match db.lock() {
        Ok(mut db_lock) => {
            let tx = db_lock.transaction().map_err(|_| DatabaseError::LockError)?;
            
            // Delete the file/folder and all its children
            tx.execute(
                "DELETE FROM inodes WHERE path = ? OR path LIKE ?",
                params![path, format!("{}/%", path)]
            ).map_err(|_| DatabaseError::ProcessingError)?;
            
            tx.commit().map_err(|_| DatabaseError::ProcessingError)?;
            Ok(())
        }
        Err(e) => {
            dbg!(e);
            Err(DatabaseError::LockError)
        }
    }
}

pub fn get_file_fragments(
    db: &Arc<Mutex<Connection>>,
    encrypted_path: String,
) -> Result<crate::files::functions::FileReassemblyData, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            // Query for a specific file by path and get its fragments with reassembly info
            let mut stmt = db_lock.prepare(
                "SELECT db.file_hash, db.added_bytes, fh.fragment_index, fh.fragment_hash, fh.chunk_type, fh.stored_locally
                 FROM inodes i 
                 JOIN data_blocks db ON i.data_id = db.id 
                 JOIN fragment_hashes fh ON db.id = fh.data_block_id
                 WHERE i.path = ? AND i.type = 'file'
                 ORDER BY fh.fragment_index"
            ).map_err(|_| DatabaseError::RecallError)?;
            
            let rows = stmt.query_map(params![encrypted_path], |row| {
                let file_hash: Blake3Hash = row.get(0)?;
                let added_bytes: u8 = row.get(1)?;
                let fragment_index: i32 = row.get(2)?;
                let fragment_hash: Blake3Hash = row.get(3)?;
                let chunk_type: crate::db::ChunkType = row.get(4)?;
                let stored_locally: bool = row.get(5)?;
                Ok((file_hash, added_bytes, fragment_index, fragment_hash, chunk_type, stored_locally))
            }).map_err(|_| DatabaseError::ProcessingError)?;
            
            let mut file_hash: Option<Blake3Hash> = None;
            let mut added_bytes: Option<u8> = None;
            let mut original_fragments = std::collections::HashMap::new();
            let mut recovery_fragments = std::collections::HashMap::new();
            
            for row in rows {
                let (f_hash, a_bytes, fragment_index, fragment_hash, chunk_type, stored_locally) = row.map_err(|_| DatabaseError::ProcessingError)?;
                
                if file_hash.is_none() {
                    file_hash = Some(f_hash);
                    added_bytes = Some(a_bytes);
                }
                
                match chunk_type {
                    crate::db::ChunkType::Original => {
                        original_fragments.insert(fragment_index as usize, (fragment_hash, stored_locally));
                    }
                    crate::db::ChunkType::Recovery => {
                        recovery_fragments.insert(fragment_index as usize, (fragment_hash, stored_locally));
                    }
                }
            }
            
            match (file_hash, added_bytes) {
                (Some(file_hash), Some(added_bytes)) => Ok(crate::files::functions::FileReassemblyData {
                    original_fragments,
                    recovery_fragments,
                    added_bytes,
                    expected_file_hash: file_hash,
                }),
                _ => Err(DatabaseError::RecallError), // File not found
            }
        }
        Err(_) => Err(DatabaseError::LockError)
    }
}