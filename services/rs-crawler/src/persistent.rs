//! src/persistent.rs  Andrew Belles  Sept 12th, 2025  
//! 
//! Defines module for persisting raw data to memory 
//!

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{sqlite::SqlitePoolOptions, Pool, Row, Sqlite};
use uuid::Uuid; 

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpotifyTrack {
    pub spotify_id: Option<String>, 
    pub isrc: Option<String>, 
    pub title: String, 
    pub artist_all: Vec<String>, 
    pub album: Option<String>, 
    pub duration_ms: Option<u32>, 
    pub release_date: Option<String>, 
    pub explicit: Option<bool>, 
    pub popularity: Option<i32> 
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobType {
    Link, 
    Features
}

impl JobType {
    pub fn as_str(self) -> &'static str {
        match self {
            JobType::Link => "link",
            JobType::Features => "features"
        }
    }
    pub fn parse(s: &str) -> Option<JobType> {
        match s {
            "link" => Some(JobType::Link),
            "features" => Some(JobType::Features),
            _ => None 
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending, 
    Active, 
    Done, 
    Failed
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Active  => "active",
            JobStatus::Done    => "done",
            JobStatus::Failed  => "failed"
        }
    }
    pub fn parse(s: &str) -> Option<JobStatus> {
        match s {
            "pending" => Some(JobStatus::Pending),
            "active"  => Some(JobStatus::Active),
            "done"    => Some(JobStatus::Done),
            "failed"  => Some(JobStatus::Failed),
            _ => None 
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub job_id: i64,
    pub track_id: String, 
    pub kind: JobType,
    pub attempt: i64
}

#[derive(Debug, Clone)]
pub struct Track {
    pub id: String, 
    pub spotify_id: Option<String>, 
    pub isrc: Option<String>, 
    pub mb_recording_id: Option<String>, 
    pub linked_ok: bool, 
    pub features_ok: bool,
    pub updated_at: i64 
}

pub struct Persistent {
    pool: Pool<Sqlite>
}

impl Persistent {

    // TODO: Create a From<sqlx::error> for CrawlerError 
    pub async fn init(database_url: &str) -> Result<Self, _> {
        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .connect(database_url)
            .await
            .with_context(|| format!("connecting to sqlite at {database_url}"));

        sqlx::query("PRAGMA journal_mode=WAL;").execute(&pool).await?; 
        sqlx::query("PRAGMA foreign_keys=ON;").execute(&pool).await?; 
        sqlx::query("PRAGMA synchronous=NORMAL;").execute(&pool).await?;

        let this = Self { pool };
        this.ensure_schema().await?; 
        Ok( this )
    }

    async fn ensure_schema(&self) -> Result<(), _> {
        // ensure that schema exists  
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tracks (
              id                TEXT PRIMARY KEY,           
              spotify_id        TEXT UNIQUE,
              isrc              TEXT UNIQUE,
              mb_recording_id   TEXT UNIQUE,
              title             TEXT,
              artist_all        TEXT,                       
              album             TEXT,
              duration_ms       INTEGER,
              release_date      TEXT,
              explicit          INTEGER,                    
              popularity        INTEGER,
              linked_ok         INTEGER NOT NULL DEFAULT 0,
              features_ok       INTEGER NOT NULL DEFAULT 0, 
              created_at        INTEGER NOT NULL,
              updated_at        INTEGER NOT NULL
            );
            "#
        ).execute(&self.pool).await?; 

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_jobs_pending ON jobs(kind, status);"
        ).execute(&self.pool).await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tracks_spotify ON tracks(spotify_id);"
        ).execute(&self.pool).await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tracks_mbid ON tracks(mb_recording_id);"
        ).execute(&self.pool).await?; 

        Ok(())
    } 

    fn now() -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
    }

    pub async fn upsert_track(&self, track: &SpotifyTrack) -> 
        Result<(String, bool), _> {

        if let Some(existing) = self.get_track(&track.spotify_id).await? {
            sqlx::query(
                r#"
                UPDATE tracks
                   SET title = COALESCE(?1, title),
                       artist_all = COALESCE(?2, artist_all),
                       album = COALESCE(?3, album),
                       duration_ms = COALESCE(?4, duration_ms),
                       release_date = COALESCE(?5, release_date),
                       explicit = COALESCE(?6, explicit),
                       popularity = COALESCE(?7, popularity),
                       updated_at = ?8
                 WHERE id = ?9;
                "#
            )
            .bind(Some(&track.title))
            .bind(Some(serde_json::to_string(&track.artist_all)?))
            .bind(track.album.as_ref())
            .bind(track.duration_ms)
            .bind(track.release_date)
            .bind(track.explicit.map(|b| if b {1} else {0}))
            .bind(track.popularity)
            .bind(Self::now())
            .bind(&existing)
            .execute(&self.pool)
            .await?; 

            if let Some(isrc) = &track.isrc {
                let _ = sqlx::query(
                    "UPDATE tracks SET isrc = COALESCE(isrc, ?1) WHERE id = ?2;"
                )
                .bind(isrc)
                .bind(&existing)
                .execute(&self.pool)
                .await; 
            }
            return Ok((existing, false));
        }

        let id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO tracks (
                id, spotify_id, isrc, title, artist_all, album, duration_ms, 
                release_date, explicit, popularity, linked_ok, 
                features_ok, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, 0, ?11, ?11);
            "#
        )
        .bind(&id)
        .bind(&track.spotify_id)
        .bind(t.isrc.as_ref())
        .bind(&track.title)
        .bind(serde_json::to_string(&track.artist_all)?)
        .bind(track.album.as_ref())
        .bind(track.duration_ms)
        .bind(track.release_date.as_ref())
        .bind(track.explicit.map(|b| if b {1} else {0}))
        .bind(track.popularity)
        .bind(Self::now())
        .execute(&self.pool)
        .await?; 

        Ok((id, true))
    }
}
