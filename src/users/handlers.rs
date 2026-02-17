use crate::{db::{DatabaseError, users::{insert_user_tx, update_user_profile_tx}}, handlers::{HandlerResult, TransactionHandler}, types::User, consensus::types::Transaction};
use crate::AppState;
use super::types::UpdateUserProfilePayload;

pub struct InsertUserHandler;

impl TransactionHandler for InsertUserHandler {
    fn name(&self) -> &'static str { "insert_user" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        match bincode::serde::decode_from_slice::<User, _>(&tx.rpc.payload, bincode::config::standard()) {
            Ok((user_data, _)) => {
                insert_user_tx(db_tx, user_data)?;
                Ok(())
            },
            Err(_) => Err(DatabaseError::InvalidPayload),
        }
    }
}

inventory::submit! {
    &InsertUserHandler as &dyn TransactionHandler
}

pub struct UpdateUserProfileHandler;

impl TransactionHandler for UpdateUserProfileHandler {
    fn name(&self) -> &'static str { "update_user_profile" }

    fn process(&self, _state: &AppState, tx: &Transaction, _execute: bool, db_tx: &duckdb::Transaction) -> HandlerResult {
        let (payload, _) = bincode::serde::decode_from_slice::<UpdateUserProfilePayload, _>(
            &tx.rpc.payload, bincode::config::standard()
        ).map_err(|_| DatabaseError::InvalidPayload)?;

        // Authorization: must be the authenticated user
        let user = tx.user.as_ref().ok_or(DatabaseError::AuthorizationError)?;
        if user.id != payload.user_id {
            return Err(DatabaseError::AuthorizationError);
        }

        // Validation: user exists
        db_tx.query_row(
            "SELECT 1 FROM users WHERE user_id = ?",
            [payload.user_id],
            |_| Ok(())
        ).map_err(|_| DatabaseError::NotFound)?;

        // Validation: name fields <= 32 chars
        if let Some(Some(ref name)) = payload.first_name {
            if name.len() > 32 { return Err(DatabaseError::InvalidPayload); }
        }
        if let Some(Some(ref name)) = payload.last_name {
            if name.len() > 32 { return Err(DatabaseError::InvalidPayload); }
        }

        // Validation: avatar <= 128KB
        if let Some(Some(ref bytes)) = payload.avatar {
            tracing::debug!("Avatar payload size: {} bytes", bytes.len());
            if bytes.len() > 128_000 {
                tracing::warn!("Avatar rejected: {} bytes exceeds 128KB limit", bytes.len());
                return Err(DatabaseError::InvalidPayload);
            }
        }

        update_user_profile_tx(
            db_tx,
            payload.user_id,
            payload.first_name.as_ref().map(|v| v.as_deref()),
            payload.last_name.as_ref().map(|v| v.as_deref()),
            payload.avatar.as_ref().map(|v| v.as_deref()),
        )?;

        Ok(())
    }
}

inventory::submit! {
    &UpdateUserProfileHandler as &dyn TransactionHandler
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_payload_roundtrip_with_avatar() {
        // Simulate a realistic avatar: 256x256 JPEG encoded image
        let img = image::RgbImage::from_fn(256, 256, |x, y| {
            image::Rgb([(x % 256) as u8, (y % 256) as u8, ((x + y) % 256) as u8])
        });
        let dynamic = image::DynamicImage::ImageRgb8(img);
        let mut buf = std::io::Cursor::new(Vec::new());
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buf, 85);
        dynamic.write_with_encoder(encoder).unwrap();
        let jpeg_bytes = buf.into_inner();

        println!("Generated 256x256 JPEG avatar: {} bytes", jpeg_bytes.len());
        assert!(jpeg_bytes.len() < 128_000, "JPEG avatar {} bytes exceeds 128KB handler limit", jpeg_bytes.len());

        let payload = UpdateUserProfilePayload {
            user_id: 1,
            first_name: Some(Some("Alice".to_string())),
            last_name: Some(None), // clear last name
            avatar: Some(Some(jpeg_bytes.clone())),
        };

        // Encode then decode — this is the exact path the consensus handler takes
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let (decoded, _) = bincode::serde::decode_from_slice::<UpdateUserProfilePayload, _>(
            &encoded, bincode::config::standard()
        ).unwrap();

        assert_eq!(decoded.user_id, 1);
        assert_eq!(decoded.first_name, Some(Some("Alice".to_string())));
        assert_eq!(decoded.last_name, Some(None));
        assert_eq!(decoded.avatar.as_ref().unwrap().as_ref().unwrap().len(), jpeg_bytes.len());
    }

    #[test]
    fn test_avatar_resize_pipeline() {
        // Simulate the full route pipeline: large input image → resize → JPEG
        let large_img = image::RgbImage::from_fn(2048, 1536, |x, y| {
            image::Rgb([
                ((x * 7 + y * 3) % 256) as u8,
                ((x * 11 + y * 5) % 256) as u8,
                ((x * 13 + y * 17) % 256) as u8,
            ])
        });

        // Encode as PNG (what the frontend canvas toBlob produces)
        let mut png_buf = std::io::Cursor::new(Vec::new());
        large_img.write_to(&mut png_buf, image::ImageFormat::Png).unwrap();
        let png_bytes = png_buf.into_inner();
        println!("Input PNG: {} bytes ({}x{})", png_bytes.len(), 2048, 1536);

        // Server-side pipeline: decode → resize → JPEG (matches put_avatar route)
        let img = image::load_from_memory(&png_bytes).unwrap();
        let resized = img.resize_to_fill(256, 256, image::imageops::FilterType::Lanczos3);

        let mut jpeg_buf = std::io::Cursor::new(Vec::new());
        let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg_buf, 85);
        resized.write_with_encoder(encoder).unwrap();
        let jpeg_bytes = jpeg_buf.into_inner();
        println!("Output JPEG: {} bytes (256x256)", jpeg_bytes.len());

        assert!(jpeg_bytes.len() < 128_000,
            "Resized avatar JPEG {} bytes exceeds 128KB handler limit", jpeg_bytes.len());

        // Verify it round-trips through the consensus payload
        let payload = UpdateUserProfilePayload {
            user_id: 42,
            first_name: None,
            last_name: None,
            avatar: Some(Some(jpeg_bytes)),
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let (decoded, _) = bincode::serde::decode_from_slice::<UpdateUserProfilePayload, _>(
            &encoded, bincode::config::standard()
        ).unwrap();
        assert!(decoded.avatar.unwrap().unwrap().len() > 0);
    }

    #[test]
    fn test_profile_payload_no_change_fields() {
        // All None = no changes — should round-trip cleanly
        let payload = UpdateUserProfilePayload {
            user_id: 5,
            first_name: None,
            last_name: None,
            avatar: None,
        };
        let encoded = bincode::serde::encode_to_vec(&payload, bincode::config::standard()).unwrap();
        let (decoded, _) = bincode::serde::decode_from_slice::<UpdateUserProfilePayload, _>(
            &encoded, bincode::config::standard()
        ).unwrap();
        assert_eq!(decoded.user_id, 5);
        assert!(decoded.first_name.is_none());
        assert!(decoded.avatar.is_none());
    }
}