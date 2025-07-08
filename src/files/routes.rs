use axum::{
    extract::{
        Multipart, Query, State
    },
    Json
};
use reqwest::StatusCode;

use crate::{db::{DataRecord, Inode}, files::functions::{encrypt_part, encrypt_path}};
use serde::Deserialize;

use super::*;
use crate::{db::{AccessList, CustomUUID}, files::functions::shard_file};
use either::Either::{Left, Right};

#[derive(Deserialize)]
pub struct GetQueryParams {
    path: String
}

pub async fn get_files(
    State(app_state): State<AppState>,
    Query(params): Query<GetQueryParams>
) -> Result<Json<Vec<Inode>>, StatusCode> {
    // let's encrypt the path so we can search for it
    let enc_path = encrypt_path(params.path, &app_state.siv_key, &app_state.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::get_files(&app_state.db, enc_path, &app_state.siv_key, &app_state.siv_nonce) {
        Ok(files) => {
            Ok(Json(files))
        }
        Err(e) => {
            dbg!(e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn put_folder(
    State(app_state): State<AppState>,
    Query(params): Query<GetQueryParams>
) -> Result<(), StatusCode> {
    let path = encrypt_path(params.path, &app_state.siv_key, &app_state.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let folder_inode = Inode {
        owner: Left(0),
        path: path,
        inode_type: crate::db::InodeType::Folder,
        data_id: None
    };

    let inodes = vec![folder_inode];

    match db::insert_files(&app_state.db, inodes) {
        Ok(_) => return Ok(()),
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

pub async fn post_files(
    State(app_state): State<AppState>,
    mut multipart: Multipart
) -> Result<(), StatusCode> {
    // read path part first
    // need path in later file processing
    let mut inodes: Vec<Inode> = Vec::new();

    match multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        Some(part) => {
            if part.name() != Some("path") {
                return Err(StatusCode::BAD_REQUEST);
            }
            let unencrypted_path = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let path = encrypt_path(unencrypted_path, &app_state.siv_key, &app_state.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            while let Some(part) = multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
                match part.name() {
                    Some("file") => {
                        // instantiate data
                        let filename = part.file_name().map(|s| s.to_string()).ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                        let filedata = part.bytes().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        // encrypt filename - deterministic AES-SIV
                        let filepath = path.clone() + &encrypt_part(&filename, &app_state.siv_key, &app_state.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        let accessors = AccessList{
                            keys: vec![
                                (1, "will be value".to_string()),
                            ].into_iter().collect(),
                        };

                        let data = shard_file(filedata.to_vec()).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                        let dataid = CustomUUID::new(None);
                        // assemble data record for database
                        let datarecord = DataRecord {
                            id: dataid,
                            access_list: accessors,
                            modified_at: None,
                            data: data
                        };
                        // assemble inode for database
                        let inode = Inode {
                            owner: Left(0),
                            path: filepath,
                            inode_type: crate::db::InodeType::File,
                            data_id: Some(Right(datarecord))
                        };
                        inodes.push(inode);
                        
                    }
                    Some(_) => {}
                    None => {}
                }
            }
        }
        None => return Err(StatusCode::BAD_REQUEST)
    }
    
    // Insert the collected inodes into the database
    match db::insert_files(&app_state.db, inodes) {
        Ok(_) => return Ok(()),
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
    let enc_path = encrypt_path(params.path, &app_state.siv_key, &app_state.siv_nonce).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match db::delete_files(&app_state.db, enc_path) {
        Ok(_) => return Ok(()),
        Err(e) => {
            dbg!(e);
            return Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}
