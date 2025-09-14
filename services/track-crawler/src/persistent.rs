//! 
//! src/persistent.rs  Andrew Belles  Sept 12th, 2025  
//! 
//! Defines module for persisting raw data to memory 
//! We define the memory as an sqlite database for features 
//! and raw json for tracks (methods defined in src/sink.rs)
//!

use std::str::FromStr;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use sqlx::{sqlite::SqlitePoolOptions, sqlite::SqliteConnectOptions, Pool, Row, Sqlite};
use uuid::Uuid; 
use crate::errors::CrawlerError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpotifyTrack {
    pub spotify_id: Option<String>, 
    pub isrc: Option<String>, 
    pub title: String, 
    pub artist_all: Vec<String>, 
    pub album: Option<String>, 
    pub duration_ms: Option<i64>, 
    pub release_date: Option<String>, 
    pub explicit: Option<bool>, 
    pub popularity: Option<i32> 
}

impl SpotifyTrack {
    pub fn new(track: &serde_json::Value) -> Self {
        Self {
            spotify_id: track.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()),
            isrc: track.pointer("/external_ids/isrc").and_then(|v| v.as_str()).map(str::to_string),
            title: track.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            artist_all: track.get("artists").and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter().filter_map(|a| a.get("name").and_then(|v| v.as_str()))
                       .map(|name| name.to_string()).collect()
                }).unwrap_or_else(|| Vec::new()),
            album: track.pointer("/album/name").and_then(|v| v.as_str()).map(str::to_string),
            duration_ms: track.get("duration_ms").and_then(|v| v.as_i64()),
            release_date: track.pointer("/album/release_date").and_then(|v| v.as_str()).map(str::to_string),
            explicit: track.get("explicit").and_then(|v| v.as_bool()),
            popularity: track.get("popularity").and_then(|v| v.as_i64()).map(|x| x as i32),
        }
    }
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
    pub title: Option<String>, 
    pub spotify_id: Option<String>, 
    pub artist_all: Vec<String>,
    pub isrc: Option<String>, 
    pub mb_recording_id: Option<String>, 
    pub linked_ok: bool, 
    pub features_ok: bool,
    pub updated_at: i64 
}

impl Track {
    pub fn first_artist(&self) -> &str {
        self.artist_all
            .first()
            .map(String::as_str)
            .unwrap_or("unknown")
    }
}

pub struct Persistent {
    pool: Pool<Sqlite>
}

impl Persistent {

    async fn ensure_schema(pool: &Pool<Sqlite>) -> Result<(), CrawlerError> {
        // ensure that schema exists  
        sqlx::query(
            r"
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
            "
        ).execute(pool).await?; 

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS jobs (
              job_id      INTEGER PRIMARY KEY AUTOINCREMENT,
              track_id    TEXT NOT NULL,
              kind        TEXT NOT NULL CHECK (kind IN ('link','features')),
              status      TEXT NOT NULL CHECK (status IN (
                  'pending','active',
                  'done','failed')
                  ) DEFAULT 'pending',
              attempt     INTEGER NOT NULL DEFAULT 0,
              last_error  TEXT,
              created_at  INTEGER NOT NULL,
              updated_at  INTEGER NOT NULL,
              UNIQUE(track_id, kind),
              FOREIGN KEY(track_id) REFERENCES tracks(id) ON DELETE CASCADE
            );
            "
        ).execute(pool).await?; 

        sqlx::query(
        r"
        CREATE TABLE IF NOT EXISTS raw_files (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          track_id    TEXT NOT NULL,
          source      TEXT NOT NULL,
          subtype     TEXT NOT NULL,
          key         TEXT NOT NULL,
          rel_path    TEXT NOT NULL,
          created_at  INTEGER NOT NULL,
          UNIQUE (source, subtype, key)
        );"
        ).execute(pool).await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_raw_files_track ON raw_files(
                track_id);"
        ).execute(pool).await?;

        sqlx::query(
        r"
        CREATE TABLE IF NOT EXISTS features (
          track_id    TEXT NOT NULL,
          source      TEXT NOT NULL,
          feature     TEXT NOT NULL,
          dtype       TEXT NOT NULL CHECK (dtype IN ('num','text')),
          num_value   REAL,
          text_value  TEXT,
          updated_at  INTEGER NOT NULL,
          PRIMARY KEY (track_id, source, feature)
        );"
        ).execute(pool).await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_features_track ON features(track_id);")
            .execute(pool).await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_jobs_pending ON jobs(kind, status);"
        ).execute(pool).await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tracks_spotify ON tracks(spotify_id);"
        ).execute(pool).await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tracks_mbid ON tracks(mb_recording_id);"
        ).execute(pool).await?; 

        Ok(())
    } 

    pub async fn init(database_url: &str) -> Result<Self, CrawlerError> {
        let is_memory = database_url == "sqlite::memory:";

        let mut opts = SqliteConnectOptions::from_str(database_url)?
            .create_if_missing(true);

        // WAL is file-only; don’t set it for in-memory
        if !is_memory {
            opts = opts.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                       .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);
        }

        let pool = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(if is_memory {1} else {8})
            .connect_with(opts)
            .await?;

        // Always create schema right away
        Self::ensure_schema(&pool).await?;

        Ok(Self { pool })
    }


    fn now() -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
    }

    pub async fn count_jobs(&self, kind: JobType, status: JobStatus) -> 
        Result<i64, CrawlerError> {
        let count = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE kind = ?1 AND status = ?2;"
        )
        .bind(kind.as_str())
        .bind(status.as_str())
        .fetch_one(&self.pool)
        .await?; 
        Ok(count)
    }

    pub async fn upsert_track(&self, track: &SpotifyTrack) -> 
        Result<(String, bool), CrawlerError> {
        // ensure spotify_id is not None 
        let id: &str = track 
            .spotify_id 
            .as_deref() 
            .ok_or_else(|| CrawlerError::Db("missing spotify_id".into()))?;

        if let Some(existing) = self.get_track_id(id).await? {
            sqlx::query(
                r"
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
                "
            )
            .bind(Some(&track.title))
            .bind(Some(serde_json::to_string(&track.artist_all)?))
            .bind(track.album.as_ref())
            .bind(track.duration_ms)
            .bind(track.release_date.clone())
            .bind(track.explicit.map(i32::from))
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
            r"
            INSERT INTO tracks (
                id, spotify_id, isrc, title, artist_all, album, duration_ms, 
                release_date, explicit, popularity, linked_ok, 
                features_ok, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, 0, ?11, ?11);
            "
        )
        .bind(&id)
        .bind(&track.spotify_id)
        .bind(track.isrc.as_ref())
        .bind(&track.title)
        .bind(serde_json::to_string(&track.artist_all)?)
        .bind(track.album.as_ref())
        .bind(track.duration_ms)
        .bind(track.release_date.as_ref())
        .bind(track.explicit.map(i32::from))
        .bind(track.popularity)
        .bind(Self::now())
        .execute(&self.pool)
        .await?; 

        Ok((id, true))
    }

    pub async fn get_track_id(&self, spotify_id: &str) -> 
        Result<Option<String>, CrawlerError> {
        let row = sqlx::query("SELECT id FROM tracks WHERE spotify_id = ?1 LIMIT 1;")
            .bind(spotify_id)
            .fetch_optional(&self.pool)
            .await?; 
        Ok( row.map(|r| r.get::<String, _>("id")))
    }

    pub async fn set_mbid(&self, track_id: &str, mbid: &str) -> Result<(), CrawlerError> {
        sqlx::query(
            "UPDATE tracks SET mb_recording_id = ?1, linked_ok = 1, updated_at = ?2 WHERE id = ?3"
        )
        .bind(mbid)
        .bind(Self::now())
        .bind(track_id)
        .execute(&self.pool)
        .await?; 
        Ok(())
    }

    pub async fn mark_features_ok(&self, track_id: &str) -> Result<(), CrawlerError> {
        sqlx::query(
            "UPDATE tracks SET features_ok = 1, updated_at = ?1 WHERE id = ?2;"
        )
        .bind(Self::now())
        .bind(track_id)
        .execute(&self.pool)
        .await?; 
        Ok(())
    } 

    pub async fn enqueue_job_if_missing(&self, track_id: &str, kind: JobType) ->
        Result<(), CrawlerError> {
        sqlx::query(
            r"
            INSERT OR IGNORE INTO jobs (
            track_id, kind, status, 
            attempt, created_at, updated_at
            )
            VALUES (?1, ?2, 'pending', 0, ?3, ?3);
            "
        )
        .bind(track_id)
        .bind(kind.as_str())
        .bind(Self::now())
        .execute(&self.pool)
        .await?; 
        Ok(())
    }

    pub async fn claim_one_job(&self, kind: JobType) -> 
        Result<Option<Job>, CrawlerError> {
        let mut tx = self.pool.begin().await?; 

        let row = sqlx::query(
            r"
            SELECT job_id, track_id, kind, attempt 
              FROM jobs 
            WHERE kind = ?1 AND status = 'pending'
            ORDER BY created_at ASC 
            LIMIT 1;
            "
        )
        .bind(kind.as_str())
        .fetch_optional(&mut *tx)
        .await?; 

        let Some(row) = row else {
            tx.rollback().await?; 
            return Ok(None);
        };

        let job_id   = row.get::<i64, _>("job_id");
        let track_id = row.get::<String, _>("track_id");
        let kind     = row.get::<String, _>("kind");
        let attempt  = row.get::<i64, _>("attempt");
        let now      = Self::now();

        let updated = sqlx::query(
            r"
            UPDATE jobs 
                SET status = 'active',
                    attempt = attempt + 1, 
                    updated_at = ?1 
                WHERE job_id = ?2 AND status = 'pending';
            "
        )
        .bind(now)
        .bind(job_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();

        if updated == 0 {
            tx.rollback().await?; 
            return Ok(None);
        }
        
        tx.commit().await?; 

        let kind = JobType::parse(&kind).ok_or_else(
            || CrawlerError::Parse("bad kind in DB".to_string())
        )?;
        Ok(Some(Job { job_id, track_id, kind, attempt }))
    }

    pub async fn complete_job(&self, job_id: i64) -> Result<(), CrawlerError> {
        sqlx::query(
            r"
            UPDATE jobs SET status='done', updated_at = ?1, 
                last_error = NULL WHERE job_id = ?2;
            "
        )
        .bind(Self::now())
        .bind(job_id)
        .execute(&self.pool)
        .await?; 

        Ok(())
    }

    pub async fn fail_job(&self, job_id: i64, err: &str) -> Result<(), CrawlerError> {
        sqlx::query(
            "UPDATE jobs SET status='failed', updated_at = ?1, 
                last_error = ?2 WHERE job_id = ?3;"
        )
        .bind(Self::now())
        .bind(err) 
        .bind(job_id)
        .execute(&self.pool)
        .await?; 

        Ok(())
    }

    pub async fn ensure_track(&self, track: &SpotifyTrack) -> 
        Result<String, CrawlerError> {
        let (track_id, _) = self.upsert_track(track).await?; 
        let linked: Option<i64> = sqlx::query_scalar(
            "SELECT linked_ok FROM tracks WHERE id = ?1;"
        )
        .bind(&track_id)
        .fetch_optional(&self.pool)
        .await? 
        .flatten();

        if linked.unwrap_or(0) == 0 {
            self.enqueue_job_if_missing(&track_id, JobType::Link).await?; 
        }
        Ok(track_id)
    }

    pub async fn enqueue_features(&self, track_id: &str) -> Result<(), CrawlerError> {
        let linking_and_features: (i64, i64) = sqlx::query_as(
            "SELECT linked_ok, features_ok FROM tracks WHERE id = ?1;"
        )
        .bind(track_id)
        .fetch_optional(&self.pool)
        .await? 
        .unwrap_or((0,0));

        if linking_and_features.0 == 1 && linking_and_features.1 == 0 {
            self.enqueue_job_if_missing(track_id, JobType::Features).await?; 
        }

        Ok(())
    }

    pub async fn get_track_metadata(&self, track_id: &str) -> 
        Result<Option<Track>, CrawlerError> {
        let row = sqlx::query(
            r"
            SELECT id, spotify_id, title, artist_all, isrc, mb_recording_id, 
                linked_ok, features_ok,
            updated_at
                FROM tracks where id = ?1;
            "
        )
        .bind(track_id)
        .fetch_optional(&self.pool)
        .await?; 

        Ok(row.map(|r| { 
            let artist_all_json: Option<String> = r.try_get("artist_all").ok();
            let artist_all: Vec<String> = artist_all_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .unwrap_or_default();

            Track {
                id: r.get("id"),
                title: r.try_get("title").ok(),
                spotify_id: r.try_get("spotify_id").ok(),
                artist_all,
                isrc: r.try_get("isrc").ok(),
                mb_recording_id: r.try_get("mb_recording_id").ok(),
                linked_ok: r.get::<i64, _>("linked_ok") == 1,
                features_ok: r.get::<i64, _>("features_ok") == 1, 
                updated_at: r.get("updated_at")
            }
        }))
    }

    pub async fn index_raw_file(
        &self, track_id: &str, source: &str, subtype: &str, key: &str, rel_path: &str
    ) -> Result<(), CrawlerError> {
        sqlx::query(r"
            INSERT OR IGNORE INTO raw_files(
                track_id, source, subtype, key, rel_path, created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6);")
        .bind(track_id)
        .bind(source)
        .bind(subtype)
        .bind(key)
        .bind(rel_path)
        .bind(Self::now())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_features_num(
        &self,
        track_id: &str,
        source: &str,
        items: &[(String, f64)],
    ) -> Result<(), CrawlerError> {
        let mut tx = self.pool.begin().await?;
        for (feature, value) in items {
            sqlx::query(r"
                INSERT INTO features (
                    track_id, source, feature, dtype, num_value, text_value, updated_at
                )
                VALUES (?1, ?2, ?3, 'num', ?4, NULL, ?5)
                ON CONFLICT(track_id, source, feature) DO UPDATE SET
                    dtype='num', num_value=excluded.num_value, text_value=NULL, 
                    updated_at=excluded.updated_at;")
            .bind(track_id)
            .bind(source)
            .bind(feature)
            .bind(value)
            .bind(Self::now())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Batch upsert text features (transactional)
    pub async fn upsert_features_text(
        &self,
        track_id: &str,
        source: &str,
        items: &[(String, String)],
    ) -> Result<(), CrawlerError> {
        let mut tx = self.pool.begin().await?;
        for (feature, value) in items {
            sqlx::query(r"
                INSERT INTO features (
                    track_id, source, feature, dtype, num_value, text_value, updated_at
                )
                VALUES (?1, ?2, ?3, 'text', NULL, ?4, ?5)
                ON CONFLICT(track_id, source, feature) DO UPDATE SET
                    dtype='text', num_value=NULL, text_value=excluded.text_value,
                    updated_at=excluded.updated_at;")
            .bind(track_id)
            .bind(source)
            .bind(feature)
            .bind(value)
            .bind(Self::now())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}
