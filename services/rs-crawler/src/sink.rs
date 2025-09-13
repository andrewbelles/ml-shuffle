use std::{fs, path::{Path, PathBuf}};
use serde_json::Value; 

use crate::errors::CrawlerError; 

#[derive(Debug, Clone, Copy)]
pub enum RawType {
    SpotifyTrack, 
    // TODO: More for features 
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
            // TODO: Other prune funcs for features, etc. 
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

    fn rel_path(kind: RawType, sanitize_key: String) -> PathBuf {
        match kind {
            RawType::SpotifyTrack => 
                PathBuf::from("raw/spotify/track").join(
                    format!("{sanitize_key}.json.zst")
                ),
            // TODO: Other relative paths for features, etc. 
        }
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

    /// Drops keys that we do not to store/handle 
    fn prune_spotify_track(v: &mut Value) {
        Self::drop_key(v, "available_markets");
        Self::drop_key(v, "preview_url");
        Self::drop_key(v, "href");
        Self::drop_key(v, "uri");
        Self::drop_key(v, "type");
        Self::drop_key(v, "is_local");
        Self::drop_key(v, "disc_number");
        Self::drop_key(v, "track_number");

        Self::drop_path(v, &["album", "available_markets"]);
        Self::drop_path(v, &["album", "external_urls"]);
        Self::drop_path(v, &["album", "images"]);
        Self::drop_path(v, &["album", "album_type"]);
        Self::drop_path(v, &["album", "total_tracks"]);
        Self::drop_path(v, &["album", "release_date_precision"]);

        Self::drop_path(v, &["album", "artists"]);
        
        Self::drop_keys_recursive(v, 
            &["href", "uri", "type", "external_urls"]);
    }

    fn drop_key(v: &mut Value, key: &str) {
        if let Some(object) = v.as_object_mut() {
            object.remove(key);
        }
    }

    fn drop_path(v: &mut Value, path: &[&str]) {
        if path.is_empty() {
            return; 
        } 

        let (last_key, parents) = path.split_last().unwrap();
        let mut curr = v; 
        for segment in parents {
            match curr {
                Value::Object(map) => {
                    if !map.contains_key(*segment) {
                        return; 
                    }
                    curr = map.get_mut(*segment).unwrap();
                }
                _ => return, 
            }
        }
        if let Value::Object(map) = curr {
            map.remove(*last_key);
        }
    }

    fn drop_array_obj_key(v: &mut Value, arr_path: &[&str], child_key: &str) {
        let mut curr = v; 
        for segment in arr_path {
            match curr {
                Value::Object(map) => {
                    if !map.contains_key(*segment) {
                        return; 
                    }
                    curr = map.get_mut(*segment).unwrap(); 
                },
                _ => return, 
            }
        }
        if let Value::Array(arr) = curr {
            for element in arr {
                if let Value::Object(object) = element {
                    object.remove(child_key);
                }
            }
        }
    }

    fn drop_keys_recursive(v: &mut Value, keys: &[&str]) {
        match v {
            Value::Object(map) => {
                for key in keys {
                    map.remove(*key);
                }
                for val in map.values_mut() {
                    Self::drop_keys_recursive(val, keys);
                }
            }
            Value::Array(arr) => {
                for element in arr {
                    Self::drop_keys_recursive(element, keys);
                }
            }
            _ => {}
        }
    }
}
