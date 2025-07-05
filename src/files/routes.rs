use axum::{
    extract::{
        State,
        Multipart
    },
    response::IntoResponse,
    Json
};
use reqwest::StatusCode;

use crate::db::{DataRecord, Inode};

use super::*;
use crate::{db::{AccessList, CustomUUID}, files::functions::shard_file};
use either::Either::{Left, Right};

pub async fn get_files(
    State(app_state): State<AppState>
) -> impl IntoResponse {
    match db::get_files(&app_state.db, String::new()) {
        Ok(files) => {
            (StatusCode::OK, Json(files))
        }
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(Vec::<Inode>::new())
        )
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
            let path = part.text().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            while let Some(part) = multipart.next_field().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
                match part.name() {
                    Some("file") => {
                        // instantiate data
                        let filename = part.file_name().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                        let filedata = part.bytes().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
                            id: CustomUUID::new(None),
                            owner: Left(0),
                            path: path.clone(),
                            inode_type: crate::db::InodeType::Folder,
                            data_id: Right(datarecord)
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