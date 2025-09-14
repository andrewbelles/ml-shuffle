//! 
//! src/sink.rs  Andrew Belles  Sept 13th, 2025 
//!
//! Defines methods for conversion of raw data 
//! to compressed json. Provides functions to index features 
//!  
//!

use std::{fs, path::{Path, PathBuf}};
use serde_json::{Map, Value}; 

use crate::errors::CrawlerError; 

#[derive(Debug, Clone, Copy)]
pub enum RawType {
    SpotifyTrack, 
    MusicBrainzRecording,  
    ABHighLevel, 
    ABLowLevel, 
    LastFmTopTags 
}

pub struct DiskZstdSink {
    root: PathBuf,
    level: i32 
}

impl DiskZstdSink {
    pub fn new(root: impl AsRef<Path>, level: i32) -> Self {
        Self { root: root.as_ref().to_path_buf(), level: level.clamp(0, 21)}
    }

    pub fn write_json(&self, kind: RawType, key: &str, mut json: Value) -> 
        Result<PathBuf, CrawlerError> {

        match kind {
            RawType::SpotifyTrack => Self::prune_spotify_track(&mut json),
            _ => {},
        }

        let rpath = Self::rel_path(kind, Self::sanitize_key(key));
        let path = self.root.join(rpath);

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e|
                CrawlerError::Db(
                    format!("create dir {}: {e}", parent.display())
            ))?;
        }

        let temp = tempfile::NamedTempFile::new_in(path.parent().unwrap())
            .map_err(|e| CrawlerError::Db(
                format!("tempfile in {}: {e}", path.parent().unwrap().display())
            ))?;

        {
            let mut enc = zstd::stream::write::Encoder::new(
                temp.as_file(),
                self.level 
            ).map_err(|e| CrawlerError::Db(
                format!("zstd encoder: {e}")
            ))?;
            
            serde_json::to_writer(&mut enc, &json)
                .map_err(|e| CrawlerError::Db(
                    format!("serialize json: {e}")
                ))?;
            enc.finish().map_err(|e| CrawlerError::Db(
                format!("zstd finish: {e}")
            ))?;
        }

        temp.persist(&path).map_err(|e|
            CrawlerError::Db(format!("persist {}: {e}", path.display())))?;

        Ok(path)
    }

    fn rel_path(kind: RawType, key: String) -> PathBuf {
        let end = format!("{key}.json.zst");
        match kind {
            RawType::SpotifyTrack => 
                ["raw","spotify","track", &end],
            RawType::MusicBrainzRecording => 
                ["raw", "musicbrainz","recording", &end],
            RawType::ABHighLevel => 
                ["raw", "acousticbrainz", "high-level", &end],
            RawType::ABLowLevel => 
                ["raw", "acousticbrainz", "low-level", &end],
            RawType::LastFmTopTags => 
                ["raw", "lastfm", "toptags", &end],
        }.into_iter().collect()
    }

    fn sanitize_key(key: &str) -> String {
        key.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }

    /// Whitelists fields that should be written to json 
    fn prune_spotify_track(v: &mut Value) {
        let s = |p: &str| v.pointer(p)
            .and_then(Value::as_str)
            .map(|x| x.to_string());
        
        let v_i64 = |p: &str| v.pointer(p)
            .and_then(Value::as_i64);

        let v_b = |p: &str| v.pointer(p)
            .and_then(Value::as_bool);
    
        let mut album = Map::new(); 
        
        if let Some(x) = s("/album/id") {
            album.insert("id".into(), Value::String(x)); 
        }

        if let Some(x) = s("/album/name") {
            album.insert("name".into(), Value::String(x));
        }

        if let Some(x) = s("/album/release_date") {
            album.insert("release_date".into(), Value::String(x));
        }

        let artists = v.get("artists")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter().filter_map(|a| {
                    let mut obj = Map::new(); 
                    if let Some(id) = a.get("id")
                        .and_then(Value::as_str) {
                        obj.insert(
                            "id".into(), Value::String(id.to_string())
                        );
                    }
                    if let Some(name) = a.get("name") 
                        .and_then(Value::as_str) {
                        obj.insert(
                            "name".into(), Value::String(name.to_string())
                        );
                    }
                    if obj.is_empty() {
                        None 
                    } else {
                        Some(Value::Object(obj))
                    }
                }).collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let ext_isrc = s("/external_ids/isrc");
        let mut root = Map::new(); 
        if let Some(x) = v.get("id").and_then(Value::as_str) {
            root.insert("id".into(), Value::String(x.to_string()));
        }
        if let Some(x) = v.get("name").and_then(Value::as_str) {
            root.insert("name".into(), Value::String(x.to_string()));
        }
        if let Some(x) = v_i64("/duration_ms") {
            root.insert("duration_ms".into(), Value::Number(x.into()));
        }
        if let Some(x) = v_b("/explicit") {
            root.insert("explicit".into(), Value::Bool(x));
        }
        if let Some(x) = v.get("popularity").and_then(Value::as_i64) {
            root.insert("popularity".into(), Value::Number(x.into()));
        }

        if !album.is_empty() {
            root.insert("album".into(), Value::Object(album));
        }

        if !artists.is_empty() {
            root.insert("artists".into(), Value::Array(artists));
        }

        if let Some(isrc) = ext_isrc {
            let mut ext = Map::new(); 
            ext.insert("isrc".into(), Value::String(isrc));
            root.insert("external_ids".into(), Value::Object(ext));
        }

        *v = Value::Object(root);
    }

    pub fn extract_high_level(v: &serde_json::Value) -> 
        (Vec<(String, f64)>, Vec<(String, String)>) {
        let mut nums: Vec<(String, f64)> = Vec::new();
        let mut texts: Vec<(String, String)> = Vec::new();

        let Some(object) = v.get("highlevel").and_then(|x| x.as_object()) else {
            return (nums, texts);
        };

        for (classifier, node) in object {
            if let Some(value) = node.get("value").and_then(|x| x.as_str()) {
                let key = Self::sanitize_key(&format!(
                        "ab.highlevel.{classifier}.value")
                );
                texts.push((key, value.to_string()));
            }
            if let Some(all) = node.get("all").and_then(|x| x.as_object()) {
                for (label, p) in all {
                    if let Some(prob) = p.as_f64() {
                        let key = Self::sanitize_key(&format!(
                            "ab.highlevel.{classifier}.all.{label}"
                        ));
                        nums.push((key, prob));
                    }
                }
            }
        }

        (nums, texts)
    }

    // Walk through lowlevel features and extract 
    fn extract_low_level_helper(
        prefix: &str, 
        v: &Value, 
        out: &mut Vec<(String, f64)>
    ) {
        match v {
            Value::Number(n) => {
                if let Some(x) = n.as_f64() {
                    let key = Self::sanitize_key(prefix);
                    out.push((key, x));
                }
            },
            Value::Bool(b) => {
                let key = Self::sanitize_key(prefix);
                out.push((key, if *b {1.0} else {0.0}));
            },
            Value::Array(arr) => {
                for (i, element) in arr.iter().enumerate() {
                    let key = format!("{prefix}.{i:02}");
                    Self::extract_low_level_helper(&key, element, out);
                }
            },
            Value::Object(map) => {
                for (key, value) in map {
                    let key = if prefix.is_empty() {
                        format!("ab.lowlevel.{key}")
                    } else {
                        format!("{prefix}.{key}")
                    };
                    Self::extract_low_level_helper(&key, value, out);
                }
            },
            _ => {}
        }
    }

    pub fn extract_low_level(v: &Value) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = Vec::new(); 
        let Some(root) = v.get("lowlevel") else {
            return out; 
        };

        // Call to recursive helper 
        Self::extract_low_level_helper("", root, &mut out);
        out 
    }

    pub fn extract_toptags(v: &Value) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = Vec::new(); 
        let Some(tags) = v.pointer("/toptags/tag").and_then(|x| x.as_array()) else {
            return out; 
        };

        let mut sum: f64 = 0.0; 
        let mut temp: Vec<(String, f64)> = Vec::with_capacity(tags.len());

        for tag in tags {
            let Some(name) = tag.get("name").and_then(|x| x.as_str()) else {
                continue; 
            };
            let count = tag.get("count")
                .and_then(|x| x.as_str().and_then(|s| s.parse::<f64>().ok())
                .or_else(|| x.as_f64()))
                .unwrap_or(0.0);
            
            let tag_key = Self::sanitize_key(&format!("lastfm.toptags.{name}.count"));
            temp.push((tag_key, count));
            sum += count; 
        }

        out.extend(temp.iter().cloned());
        if sum > 0.0 {
            for (key, count) in &temp {
                let per = key.replace(".count", ".p");
                out.push((per, *count / sum));
            }
        }
        out 
    }
}
