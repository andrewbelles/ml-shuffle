
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrackRecord {
    pub internal_id: uuid::Uuid,
    pub isrc: Option<String>, 
    pub spotify_id: Option<String>, 
    pub mbid: Option<String>,
    pub title: String, 
    pub artist_all: Vec<String>, 
    pub album: Option<String>, 
    pub duration_ms: Option<u32>, 
    pub release_data: Option<String>, 
    pub fetch_time: chrono::DateTime<chrono::Utc>
}
