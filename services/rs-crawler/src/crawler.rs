//!
//! src/crawler.rs  Andrew Belles  Sept 13th, 2025 
//!
//! Defines the crawler interface
//!
//!
//!

use std::{sync::Arc, time::{Duration, Instant}};

use rand::{rngs::SmallRng, Rng, SeedableRng};
use tokio::{sync::Semaphore, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken; 
use tracing::{debug, error, info, warn};
use uuid::Uuid; 

use crate::{config::{AcousticBrainzConfig, HttpConfig, LoggingConfig}, fetch::LastFmClient};
use crate::fetch::*;    // all clients are imported 
use crate::persistent::{Job, JobType, Persistent};
use crate::sink::{DiskZstdSink, RawType};
use crate::errors::CrawlerError;
use crate::config::AppConfig; 

#[derive(Debug)]
struct RateGate {
    min_interval: Duration, 
    state: tokio::sync::Mutex<Instant> 
}

impl RateGate {
    fn new(min_interval: Duration) -> Self {
        Self { 
            min_interval, 
            state: tokio::sync::Mutex::new(Instant::now() - min_interval)
        }
    }
    async fn wait(&self) {
        let mut last = self.state.lock().await; 
        let elapsed = last.elapsed();
        if elapsed < self.min_interval { 
            sleep(self.min_interval - elapsed).await; 
        }
        *last = Instant::now();
    }
}

/// Simple function to generate random wait for http_with_retry
fn generate_backoff(ms: u64, attempt: usize, rng: &mut SmallRng) -> Duration {
    let exp = (1_u64 << attempt.min(6)) * ms; 
    let jitter = rng.gen_range(50..=200) as u64; 
    Duration::from_millis(exp + jitter)
}

async fn http_with_retry(
    request: reqwest::RequestBuilder, 
    max_retries: usize, 
    backoff_ms: u64
) -> Result<serde_json::Value, CrawlerError> {
    let mut rng = SmallRng::from_entropy();
    let mut attempt = 0_usize; 
    loop {
        let response = request.try_clone()
            .ok_or_else(|| CrawlerError::Http("non-cloneable request".to_string()))?
            .send()
            .await;
        match response {
            Ok(resp) => {
                if resp.status().is_success() {
                    let v = resp.json::<serde_json::Value>().await?; 
                    return Ok(v);
                }
                let status = resp.status(); 
                let body = resp.text().await.unwrap_or_default();
                let retryable = status.as_u16() == 429 || status.is_server_error(); 
                if !retryable || attempt >= max_retries {
                    return Err(CrawlerError::Http("http.retry".to_string()));
                }
                let backoff = generate_backoff(backoff_ms, attempt, &mut rng);
                warn!(status = %status, backoff = ?backoff.as_millis(), "http.retry");
                sleep(backoff).await; 
                attempt += 1;
            },
            Err(e) => {
                if attempt >= max_retries {
                    return Err(e.into());
                }
                let backoff = generate_backoff(backoff_ms, attempt, &mut rng);
                warn!(backoff = ?backoff.as_millis(), "http.retry.error");
                sleep(backoff).await; 
                attempt += 1; 
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CrawlerLimits {
    pub musicbrainz_limit: usize, 
    pub musicbrainz_ms: u64,
    pub feature_limit: usize, 
    pub queue_poll_ms: u64, 
    pub http_max_retry: usize, 
    pub http_backoff_ms: u64
}

impl Default for CrawlerLimits {
    fn default() -> Self {
        Self {
            musicbrainz_limit: 1, 
            musicbrainz_ms: 1100,
            feature_limit: 4, 
            queue_poll_ms: 300, 
            http_max_retry: 3, 
            http_backoff_ms: 500 
        }
    }
}

#[derive(Clone)]
pub struct Clients {
    pub spotify: Arc<SpotifyClient>,
    pub musicbrainz: Arc<MusicBrainzClient>, 
    pub acousticbrainz: Arc<AcousticBrainzClient>, 
    pub lastfm: Arc<LastFmClient> 
}

pub struct Crawler {
    // backbone 
    http: HttpConfig,
    logging: LoggingConfig, 
    limits: CrawlerLimits, 
    db: Arc<Persistent>, 
    clients: Clients, 
    sink: Arc<DiskZstdSink>, 

    // concurrency handlers 
    musicbrainz_handler: Arc<Semaphore>, 
    features_handler: Arc<Semaphore>, 
    musicbrainz_rate: Arc<RateGate>,

    // handles daemon exit 
    shutdown: CancellationToken
}

impl Crawler {
    pub fn new(
        cfg: &AppConfig, 
        db: Persistent, 
        clients: Clients, 
        sink: DiskZstdSink,
        limits: CrawlerLimits 
    ) -> Self {
       let musicbrainz_handler = Arc::new(Semaphore::new(limits.musicbrainz_limit));
       let features_handler    = Arc::new(Semaphore::new(limits.feature_limit));
       let musicbrainz_rate    = Arc::new(RateGate::new(
           Duration::from_millis(limits.musicbrainz_ms)
       ));

       Self {
           http: cfg.http.clone(),
           logging: cfg.logging.clone(),
           limits, 
           db: Arc::new(db),
           clients, 
           sink: Arc::new(sink),
           musicbrainz_handler, 
           features_handler, 
           musicbrainz_rate,
           shutdown: CancellationToken::new()
       }
    } 

    pub fn shutdown(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub async fn run(self) -> Result<(), CrawlerError> {
        info!( 
            mb_conc = self.limits.musicbrainz_limit,
            feat_conc = self.limits.feature_limit,
            "crawler.start",
        );

        let link_handle = self.spawn_link_workers(); 
        let feat_handle = self.spawn_feature_workers(); 

        let shutdown = self.shutdown.clone();
        let trigger = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!(msg = "shutting crawler daemon down", "crawler.signal");
                shutdown.cancel();
            }
        });

        tokio::select! {
            _ = self.shutdown.cancelled() => {
                info!(reason = "shutdown token", "crawler.stop");
            }
            r = link_handle => {
                if let Err(e) = r { 
                    error!(error = ?e, "link_workers task found error"); 
                }
                self.shutdown.cancel(); 
            }
            r = feat_handle => {
                if let Err(e) = r { 
                    error!(error = ?e, "feat_workers task found error"); 
                }
                self.shutdown.cancel(); 
            }
        }
        let _ = trigger.await; 
        info!("crawler.exit");
        Ok(())
    }

    fn spawn_link_workers(&self) -> JoinHandle<()> {
        let this = self.clone_for_task(); 
        tokio::spawn(async move { this.link_loop().await })
    }

    fn spawn_feature_workers(&self) -> JoinHandle<()> {
        let this = self.clone_for_task(); 
        tokio::spawn(async move { this.features_loop().await })
    }

    fn clone_for_task(&self) -> Self {
        Self {
            http: self.http.clone(), 
            logging: self.logging.clone(), 
            limits: self.limits.clone(), 
            db: self.db.clone(), 
            clients: self.clients.clone(), 
            sink: self.sink.clone(), 
            musicbrainz_handler: self.musicbrainz_handler.clone(),
            features_handler: self.features_handler.clone(),
            musicbrainz_rate: self.musicbrainz_rate.clone(), 
            shutdown: self.shutdown.clone(), 
        }
    }

    async fn link_loop(&self) {
        info!("crawler.link.loop.start");
        let poll = Duration::from_millis(self.limits.queue_poll_ms);
        while !self.shutdown.is_cancelled() {
            self.musicbrainz_rate.wait().await;

            let Some(job) = match self.db.claim_one_job(JobType::Link).await {
                Ok(v) => v, 
                Err(e) => {
                    error!(error = ?e, "claim_one_job(Link) failed");
                    sleep(poll).await; 
                    continue; 
                },
            } else {
                sleep(poll).await; 
                continue; 
            };

            let _permit = match self.musicbrainz_handler.acquire().await {
                Ok(p) => p, 
                Err(_) => break 
            }; 

            if let Err(e) = self.process_link_job(job).await {
                error!(error = ?e, "link job failed");
            }
        }
        info!("crawler.link.loop.stop");
    }

    async fn process_link_job(&self, job: Job) -> Result<(), CrawlerError> {
        debug!(
            job_id = job.job_id, track = %job.track_id, 
            attempt = job.attempt, "link.process");

        let meta = self.db.get_track_metadata(&job.track_id).await 
            .map_err(CrawlerError::Db("link failure".to_string()))?; 

        let mbid = if let Some(isrc) = meta.isrc.as_deref() {
            self.lookup_mbid_by_isrc(isrc).await? 
        } else {
            self.lookup_mbid_by_query(&meta.title, &meta.first_artist()).await?
        };

        self.db.set_mbid(&job.track_id, &mbid).await?; 
        self.db.complete_job(job.job_id).await?;
        
        if let Err(e) = self.db.enqueue_features(job.track_id).await {
            warn!(error = ?e, "enqueue_features");
        }
        info!(job_id = job.job_id, track = %job.track_id, mbid = %mbid, "link.done");
        Ok(())
    }

    async fn lookup_mbid_by_isrc(&self, isrc: &str) -> Result<String, CrawlerError> {
        let resp = self.clients.musicbrainz.lookup_isrc(isrc);
        let value = http_with_retry(
            resp, self.limits.http_max_retry,
            self.limits.http_backoff_ms
        ).await?;
        let records = value["recordings"].as_array().unwrap();
        let mbid = records.iter() 
            .filter_map(|r| r.get("id").and_then(|x| x.as_str()))
            .next()
            .ok_or_else(|| CrawlerError::Http("no recording for ISRC".to_string()))?;
        Ok(mbid.to_string())
    }

    async fn lookup_mbid_by_query(&self, title: &str, artist: &str) -> 
        Result<String, CrawlerError> {
        let query = format!("recording:\"{}\" AND artist:\"{}\"", title, artist);
        let resp = self.clients.musicbrainz.search_recording(&query, 10, 0);
        let value = http_with_retry(
            resp, self.limits.http_max_retry,
            self.limits.http_backoff_ms
        ).await?;
        let records = value["recordings"].as_array().unwrap();
        let mbid = records.iter() 
            .filter_map(|r| r.get("id").and_then(|x| x.as_str()))
            .next()
            .ok_or_else(|| CrawlerError::Http("no recording for ISRC".to_string()))?;
        Ok(mbid.to_string())
    }

    async fn features_loop(&self) {
        info!("crawler.features.loop.start");
        let poll = Duration::from_millis(self.limits.queue_poll_ms);
        while !self.shutdown.is_cancelled() {
            let Some(job) = match self.db.claim_one_job(JobType::Features).await {
                Ok(v) => v,
                Err(e) => {
                    error!(error = ?e, "claim_one_job(Features) failed");
                    continue; 
                }
            } else {
                sleep(poll).await; 
                continue; 
            };

            let _permit = match self.features_handler.acquire().await {
                Ok(p) => p, 
                Err(_) => break 
            };
            if let Err(e) = self.process_features_job(job).await {
                error!(error = ?e, "features job failed");
            }
        }
        info!("crawler.features.loop.stop");
    }

    async fn process_features_job(&self, job: Job) -> Result<(), CrawlerError> {
        debug!(job_id = job.job_id, track = %job.track_id, attempt = job.attempt, 
            "features.process");

        let meta = self.db.get_track_metadata(&job.track_id).await 
            .map_err(CrawlerError::Db("no metadata for id".to_string()))?; 
        let mbid = meta.mbid.as_deref().ok_or_else(
            CrawlerError::NotFound("No mbid found".to_string()
        ))?;

        let highlevel = self.clients.acousticbrainz.features(mbid, "high-level");
        let highlevel = http_with_retry(
            highlevel, 
            self.limits.http_max_retry, 
            self.limits.http_backoff_ms
        );

        let path_highlevel = self.sink.write_json(RawType::ABHighLevel, mbid, &highlevel);
        self.db.index_raw_file(
            &job.track_id, 
            "acousticbrainz", 
            "high-level",
            mbid, 
            path_highlevel
        ).await?;

        let (highlevel_numeric, highlevel_text) = DiskZstdSink::extract_high_level(
            &highlevel
        );

        self.db.upsert_features_num(job.track_id, "acousticbrainz", &highlevel_numeric)
            .await?; 
        self.db.upsert_features_text(job.track_id, "acousticbrainz", &highlevel_text)
            .await?; 

        let lowlevel = self.clients.acousticbrainz.features(mbid, "low-level");
        let lowlevel = http_with_retry(
            lowlevel, 
            self.limits.http_max_retry, 
            self.limits.http_backoff_ms
        );

        let path_lowlevel = self.sink.write_json(RawType::ABLowLevel, mbid, &lowlevel);
        self.db.index_raw_file(
            &job.track_id, 
            "acousticbrainz", 
            "low-level",
            mbid, 
            path_lowlevel
        ).await?;

        let lowlevel_numeric = DiskZstdSink::extract_low_level(&lowlevel); 

        self.db.upsert_features_num(job.track_id, "acousticbrainz", &lowlevel_numeric)
            .await?; 

        // Get tags from mbid, if fails get conventionally else warning 
        let mut tags = {
            let resp = self.clients.lastfm.track_top_tags_by_mbid(mbid);
            http_with_retry(resp, self.limits.http_max_retry, self.limits.http_backoff_ms)
                .await 
        };
        
        if tags.is_err() {
            let artist = meta.first_artist(); 
            let resp = self.clients.lastfm.track_top_tags(&artist, meta.title);
            tags = http_with_retry(
                resp,
                self.limits.http_max_retry, 
                self.limits.http_backoff_ms
            ).await;
        }

        if let Ok(tags) = tags {
            let key = meta.mbid.as_deref().unwrap_or_else(|| { 
                meta.spotify_id.as_deref().unwrap_or("unknown");       
            });
            let path_tags = self.sink.write_json(RawType::LastFmTopTags, key, &tags)
                .await?;             
            self.db.index_raw_file(job.track_id, "lastfm", "toptags", key, path_tags)
                .await?; 
        } else {
            warn!(track = %job.track_id, "lastfm tags missing");
        }

        self.db.mark_features_ok(job.track_id).await?; 
        self.db.complete_job(job.job_id).await?; 
        info!(job_id = job.job_id, track = %job.track_id, "features.done");

        Ok(())
    }
}
