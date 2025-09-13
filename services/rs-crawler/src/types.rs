use serde::{Deserialize, Serialize};

// International standard recording code
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Isrc(pub String);


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpotifyTrackId(pub String);


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MbRecordingId(pub String); 


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MbReleaseId(pub String);


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackKey {
    pub spotify_id: Option<SpotifyTrackId>, 
    pub isrc: Option<Isrc>, 
    pub title: Option<String>,
    pub artist_name: Option<String>,
    pub duration_ms: Option<u32> 
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalLink {
    pub internal_track_uuid: uuid::Uuid, 
    pub mb_recording_id: Option<MbRecordingId>,
    pub mb_release_id: Option<MbReleaseId>, 
    pub isrc: Option<Isrc>, 
    pub spotify_track_id: Option<SpotifyTrackId>, 
    pub confidence: f32
}
