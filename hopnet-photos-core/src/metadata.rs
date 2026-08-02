use crate::error::PhotosCoreError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq)]
#[serde(default)]
pub struct PhotoMetadata {
    pub date_taken: String,
    pub media_type: i32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera_make: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub camera_model: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub latitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub longitude: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub orientation: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_type: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_index: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_group_pick: Option<i32>,
}

impl PhotoMetadata {
    pub fn to_json(&self) -> Result<Vec<u8>, PhotosCoreError> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, PhotosCoreError> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_json_round_trip_full() {
        let meta = PhotoMetadata {
            date_taken: "2025-01-01T12:00:00Z".into(),
            media_type: 0,
            width: Some(1920),
            height: Some(1080),
            duration_ms: None,
            camera_make: Some("Apple".into()),
            camera_model: Some("iPhone 16".into()),
            latitude: Some(51.5074),
            longitude: Some(-0.1278),
            orientation: Some(1),
            group_id: Some("group-1".into()),
            group_type: Some(0),
            group_index: Some(0),
            is_group_pick: Some(1),
        };
        let json = meta.to_json().unwrap();
        let round = PhotoMetadata::from_json(&json).unwrap();
        assert_eq!(meta, round);
    }

    #[test]
    fn metadata_json_round_trip_minimal() {
        let meta = PhotoMetadata {
            date_taken: "2025-01-01T00:00:00Z".into(),
            media_type: 0,
            ..Default::default()
        };
        let json = meta.to_json().unwrap();
        let round = PhotoMetadata::from_json(&json).unwrap();
        assert_eq!(round.date_taken, "2025-01-01T00:00:00Z");
        assert_eq!(round.media_type, 0);
        assert!(round.width.is_none());
    }

    #[test]
    fn metadata_unknown_fields_tolerated() {
        let json = r#"{"date_taken":"2025-01-01T00:00:00Z","media_type":0,"unknown_field":42}"#;
        let _meta = PhotoMetadata::from_json(json.as_bytes()).unwrap();
    }

    #[test]
    fn metadata_null_maps_to_none() {
        let json = r#"{"date_taken":"2025-01-01T00:00:00Z","media_type":0,"width":null}"#;
        let meta = PhotoMetadata::from_json(json.as_bytes()).unwrap();
        assert!(meta.width.is_none());
    }

    #[test]
    fn metadata_missing_optional_maps_to_none() {
        let json = r#"{"date_taken":"2025-01-01T00:00:00Z","media_type":0}"#;
        let meta = PhotoMetadata::from_json(json.as_bytes()).unwrap();
        assert!(meta.width.is_none());
    }
}
