use super::*;
use either::Either;


pub fn get_files(
    db: &Arc<Mutex<Connection>>,
    path: String,
) -> Result<Vec<Inode>, DatabaseError> {
    match db.lock() {
        Ok(db_lock) => {
            let mut stmt = db_lock.prepare("SELECT id, owner_id, path, type, data_id FROM inodes WHERE path LIKE ?").map_err(|_| DatabaseError::RecallError)?;
            let like_path = format!("{}%", path);
            let inodes = stmt.query_map(params![like_path], |row| {
                Ok(Inode {
                    id: row.get(0)?,
                    owner: Either::Left(row.get(1)?),
                    path: row.get(2)?,
                    inode_type: row.get(3)?,
                    data_id: Either::Left(row.get(4)?),
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
            
            for inode in inodes {
                // Handle the data_id which can be Either<CustomUUID, DataRecord>
                let data_id = match inode.data_id {
                    either::Either::Left(uuid) => {
                        // If it's already a UUID, use it directly
                        uuid
                    },
                    either::Either::Right(data_record) => {
                        // If it's a DataRecord, we need to insert it first
                        let data_id = data_record.id;
                                                
                        // Convert Data to individual fragment hashes
                        let data = &data_record.data;
                        
                        // Helper function to extract hash from DataBlockRepresentation
                        let extract_hash = |fragment: &crate::db::DataBlockRepresentation| -> Result<Blake3Hash, DatabaseError> {
                            match fragment {
                                crate::db::DataBlockRepresentation::Hash(hash) => Ok(hash.clone()),
                                crate::db::DataBlockRepresentation::Data(_) => {
                                    // Return error for Data type since encryption will be applied
                                    Err(DatabaseError::InvalidPayload)
                                }
                            }
                        };
                        
                        // Insert into data_blocks table
                        tx.execute(
                            "INSERT INTO data_blocks (
                                id, access_list, modified_at, file_hash,
                                fragment_hash_01, fragment_hash_02, fragment_hash_03, fragment_hash_04, fragment_hash_05,
                                fragment_hash_06, fragment_hash_07, fragment_hash_08, fragment_hash_09, fragment_hash_10,
                                fragment_hash_11, fragment_hash_12, fragment_hash_13, fragment_hash_14, fragment_hash_15,
                                fragment_hash_16, fragment_hash_17, fragment_hash_18, fragment_hash_19, fragment_hash_20,
                                fragment_hash_21, fragment_hash_22, fragment_hash_23, fragment_hash_24, fragment_hash_25,
                                fragment_hash_26, fragment_hash_27, fragment_hash_28, fragment_hash_29, fragment_hash_30,
                                added_bytes
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            params![
                                data_id,
                                data_record.access_list,
                                data_record.modified_at,
                                data.hash,
                                extract_hash(&data.fragment_01)?,
                                extract_hash(&data.fragment_02)?,
                                extract_hash(&data.fragment_03)?,
                                extract_hash(&data.fragment_04)?,
                                extract_hash(&data.fragment_05)?,
                                extract_hash(&data.fragment_06)?,
                                extract_hash(&data.fragment_07)?,
                                extract_hash(&data.fragment_08)?,
                                extract_hash(&data.fragment_09)?,
                                extract_hash(&data.fragment_10)?,
                                extract_hash(&data.fragment_11)?,
                                extract_hash(&data.fragment_12)?,
                                extract_hash(&data.fragment_13)?,
                                extract_hash(&data.fragment_14)?,
                                extract_hash(&data.fragment_15)?,
                                extract_hash(&data.fragment_16)?,
                                extract_hash(&data.fragment_17)?,
                                extract_hash(&data.fragment_18)?,
                                extract_hash(&data.fragment_19)?,
                                extract_hash(&data.fragment_20)?,
                                extract_hash(&data.fragment_21)?,
                                extract_hash(&data.fragment_22)?,
                                extract_hash(&data.fragment_23)?,
                                extract_hash(&data.fragment_24)?,
                                extract_hash(&data.fragment_25)?,
                                extract_hash(&data.fragment_26)?,
                                extract_hash(&data.fragment_27)?,
                                extract_hash(&data.fragment_28)?,
                                extract_hash(&data.fragment_29)?,
                                extract_hash(&data.fragment_30)?,
                                data.added_bytes
                            ]
                        ).map_err(|_| DatabaseError::InsertError)?;
                        
                        data_id
                    }
                };
                
                // Get the owner_id from the inode
                let owner_id = match inode.owner {
                    either::Either::Left(user_id) => user_id,
                    either::Either::Right(user) => user.user_id,
                };

                // Insert into inodes table
                tx.execute(
                    "INSERT INTO inodes (id, owner_id, path, type, data_id) VALUES (?, ?, ?, ?, ?)",
                    params![
                        inode.id,
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