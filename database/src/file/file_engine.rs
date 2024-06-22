use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};

use prost::Message;

use super::proto;
use super::record::{HtmlRecord, JsonRecord, Record};
use crate::errors::NullDbReadError;

#[derive(Debug, Clone, Copy)]
pub enum FileEngine {
    Json,
    Html,
    Proto,
}

/// FileEngine is an enum that represents the different file engines that can be used to store records.
impl FileEngine {
    /// new creates a new FileEngine from a string, valid options are json, html and proto.
    pub fn new(engine: &str) -> Self {
        match engine {
            "json" => FileEngine::Json,
            "html" => FileEngine::Html,
            "proto" => FileEngine::Proto,
            _ => panic!("Invalid file engine"),
        }
    }

    /// deserialize a string into a Record.
    ///
    /// # Errors
    ///
    /// This function will return an error if the value is not a valid record.
    pub fn deserialize(&self, value: &str) -> anyhow::Result<Record, NullDbReadError> {
        match self {
            FileEngine::Json => {
                let json: JsonRecord =
                    serde_json::from_str(value).map_err(|e| NullDbReadError::Corrupted)?;
                Ok(Record::Json(json))
            }
            FileEngine::Html => {
                let html: HtmlRecord = from_str(value).map_err(|e| NullDbReadError::Corrupted)?;
                Ok(Record::Html(html))
            }
            FileEngine::Proto => {
                let proto: proto::ProtoRecord = proto::ProtoRecord::decode(value.as_bytes())
                    .map_err(|e| NullDbReadError::Corrupted)?;
                Ok(Record::Proto(proto))
            }
        }
    }

    /// serialize a Record into a byte vector.
    pub fn serialize(&self, record: Record) -> Vec<u8> {
        record.serialize()
    }

    /// new_record creates a new Record from a key, index, tombstone and value.
    pub fn new_record(
        &self,
        key: String,
        index: u64,
        tombstone: Option<bool>,
        value: Option<String>,
    ) -> Record {
        match self {
            FileEngine::Json => Record::Json(JsonRecord {
                key,
                index,
                tombstone,
                value,
            }),
            FileEngine::Html => Record::Html(HtmlRecord {
                key,
                index,
                class: None,
                value,
            }),
            FileEngine::Proto => Record::Proto(proto::ProtoRecord {
                key,
                index,
                tombstone,
                value,
            }),
        }
    }

    /// new_tombstone_record creates a new tombstone Record from a key and index.
    /// Shortcut for new_record with tombstone set to true and value set to None.
    pub fn new_tombstone_record(&self, key: String, index: u64) -> Record {
        match self {
            FileEngine::Json => Record::Json(JsonRecord {
                key,
                index,
                tombstone: Some(true),
                value: None,
            }),
            FileEngine::Html => Record::Html(HtmlRecord {
                key,
                index,
                class: Some("tombstone".to_string()),
                value: None,
            }),
            FileEngine::Proto => Record::Proto(proto::ProtoRecord {
                key,
                index,
                tombstone: Some(true),
                value: None,
            }),
        }
    }
}
