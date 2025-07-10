use axum::{
    extract::{
        Multipart, Path, Query, State
    },
    Json,
    response::Response,
    http::header,
    body::Body
};
use reqwest::StatusCode;

use crate::{db::{DataRecord, Inode, Blake3Hash, DatabaseError}, files::functions::{encrypt_part, encrypt_path}};
use serde::{Deserialize, Serialize};

use super::*;
use crate::{db::{AccessList, CustomUUID}, files::functions::shard_file};
use either::Either::{Left, Right};
use crate::consensus::{functions::consensus_middleware, types::Transaction};

#[derive(Deserialize)]
pub struct GetQueryParams {
    path: String
}

#[derive(Serialize)]
pub struct FileFragmentsResponse {
    pub file_hash: Blake3Hash,
    pub fragments: Vec<(Blake3Hash, crate::db::ChunkType)>,
}

pub async fn get_files(
    State(app_state): State<AppState>,
    Query(params): Query<GetQueryParams>
) -> Result<Json<Vec<Inode>>, StatusCode> {
    // let's encrypt the path so we can search for it
    let enc_path = encrypt_path(params.path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::get_files(&app_state.db, enc_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?) {
        Ok(files) => {
            Ok(Json(files))
        }
        Err(e) => {
            dbg!(e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn get_file_fragments(
    State(app_state): State<AppState>,
    Path(path): Path<String>
) -> Result<Response<Body>, StatusCode> {
    // Convert the path: /files/ -> "/" and /files/test -> "/test"
    let file_path = if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", path)
    };
    
    // Extract filename from path for Content-Disposition header
    let filename = path.split('/').last().unwrap_or("download");
    
    // Encrypt the path for database lookup
    let enc_path = encrypt_path(file_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    // Get file reassembly data from database
    let file_data = match db::get_file_fragments(&app_state.db, enc_path) {
        Ok(data) => data,
        Err(DatabaseError::RecallError) => return Err(StatusCode::NOT_FOUND),
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    
    // Reassemble the file from fragments
    let file_contents = match functions::reassemble_file(&app_state.fragments_dir, file_data) {
        Ok(contents) => contents,
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    
    // Build response with proper headers
    let response = Response::builder()
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(file_contents))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    Ok(response)
}

pub async fn post_files(
    State(app_state): State<AppState>,
    mut multipart: Multipart
) -> Result<(), StatusCode> {
    // read path part first
    // need path in later file processing
    let mut inodes: Vec<Inode> = Vec::new();
    let mut has_files = false;

    match multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        Some(part) => {
            if part.name() != Some("path") {
                return Err(StatusCode::BAD_REQUEST);
            }
            let unencrypted_path = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let path = encrypt_path(unencrypted_path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            
            while let Some(part) = multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
                match part.name() {
                    Some("file") => {
                        has_files = true;
                        // instantiate data
                        let filename = part.file_name().map(|s| s.to_string()).ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                        let filedata = part.bytes().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        // encrypt filename - deterministic AES-SIV
                        let filepath = path.clone() + &encrypt_part(&filename, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        let accessors = AccessList{
                            keys: vec![
                                (1, "will be value".to_string()),
                            ].into_iter().collect(),
                        };

                        // Generate data block ID before sharding
                        let dataid = CustomUUID::new(None);
                        
                        let data_option = shard_file(filedata.to_vec(), &app_state.fragments_dir, dataid.clone()).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        // assemble inode for database
                        let inode = match data_option {
                            Some(data) => {
                                // assemble data record for database
                                let datarecord = DataRecord {
                                    id: dataid,
                                    access_list: accessors,
                                    modified_at: None,
                                    data: data
                                };
                                
                                Inode {
                                    owner: Left(0),
                                    path: filepath,
                                    inode_type: crate::db::InodeType::File,
                                    data_id: Some(Right(datarecord))
                                }
                            }
                            None => {
                                // Empty file - no data record
                                Inode {
                                    owner: Left(0),
                                    path: filepath,
                                    inode_type: crate::db::InodeType::File,
                                    data_id: None
                                }
                            }
                        };
                        inodes.push(inode);
                        
                    }
                    Some(_) => {}
                    None => {}
                }
            }
            
            // If no files were found, create a folder
            if !has_files {
                let folder_inode = Inode {
                    owner: Left(0),
                    path: path,
                    inode_type: crate::db::InodeType::Folder,
                    data_id: None
                };
                inodes.push(folder_inode);
            }
        }
        None => return Err(StatusCode::BAD_REQUEST)
    }
    
    // Insert the collected inodes into the database via consensus
    match bincode::serde::encode_to_vec(&inodes, bincode::config::standard()) {
        Ok(encoded_inodes) => {
            let transaction = Transaction {
                function: "insert_files".to_string(),
                payload: encoded_inodes,
            };
            let transactions = vec![transaction];

            // Use consensus middleware to ensure distributed agreement
            match consensus_middleware(&app_state, transactions).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    dbg!(e);
                    return Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }

}

pub async fn delete_files(
    State(app_state): State<AppState>,
    Query(params): Query<GetQueryParams>
) -> Result<(), StatusCode> {
    let enc_path = encrypt_path(params.path, app_state.get_siv_key()?, app_state.get_siv_nonce()?).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::delete_files(&app_state.db, enc_path) {
        Ok(_) => return Ok(()),
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
