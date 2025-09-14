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

use crate::{config::{self, HttpConfig, LoggingConfig}, fetch::LastFmClient, persistent};
use crate::fetch::*;    // all clients are imported 
use crate::persistent::{Job, JobType, Persistent, JobStatus};
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
                let _body = resp.text().await.unwrap_or_default();
                let retryable = status.as_u16() == 429 || status.is_server_error(); 
                if !retryable || attempt >= max_retries {
                    return Err(CrawlerError::Http(
                        format!("status {} after {} retries", status, attempt)
                    ));
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

impl Clients {
    pub fn new(
        spotify: SpotifyClient, 
        musicbrainz: MusicBrainzClient, 
        acousticbrainz: AcousticBrainzClient, 
        lastfm: LastFmClient
    ) ->Self {
        Self {
            spotify: Arc::new(spotify),
            musicbrainz: Arc::new(musicbrainz),
            acousticbrainz: Arc::new(acousticbrainz),
            lastfm: Arc::new(lastfm)
        }
    }
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
        let feed_handle = self.spawn_feed_worker(); 

        let shutdown = self.shutdown.clone();
        let trigger = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                warn!(msg = "shutting crawler daemon down", "crawler.signal");
                shutdown.cancel();
            }
        });

        tokio::select! {
            () = self.shutdown.cancelled() => {
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
            r = feed_handle => {
                if let Err(e) = r {
                    error!(error = ?e, "feed_workers task found error");
                }
                self.shutdown().cancel();
            }
        }
        let _ = trigger.await; 
        info!("crawler.exit");
        Ok(())
    }

    fn spawn_feed_worker(&self) -> JoinHandle<()> {
        let this = self.clone_for_task();
        tokio::spawn(async move { this.feed_loop().await })
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

            match self.db.claim_one_job(JobType::Link).await {
                Ok(Some(job)) => {
                    let _permit = match self.musicbrainz_handler.acquire().await {
                        Ok(p) => p, 
                        Err(_) => break
                    };
                    if let Err(e) = self.process_link_job(job).await {
                        error!(error = ?e, "link job failed");
                    }
                }
                Ok(None) => { 
                    sleep(poll).await; 
                }
                Err(e) => {
                    error!(error = ?e, "claim_one_job(Link) failed");
                    sleep(poll).await; 
                }
            }
        }
        info!("crawler.link.loop.stop");
    }

    async fn process_link_job(&self, job: Job) -> Result<(), CrawlerError> {
        debug!(
            job_id = job.job_id, track = %job.track_id, 
            attempt = job.attempt, "link.process");

        let meta = match self.db.get_track_metadata(&job.track_id).await? {
            Some(m) => m,
            None => {
                self.db.fail_job(job.job_id, "track not found").await?; 
                info!(job_id = job.job_id, track = %job.track_id, "link.skip.no_track");
                return Ok(())
            }
        };

        let mbid = if let Some(isrc) = meta.isrc.as_deref() {
            self.lookup_mbid_by_isrc(isrc).await? 
        } else {
            let title  = meta.title.as_deref().unwrap_or("");
            let artist = meta.first_artist();
            self.lookup_mbid_by_query(title, artist).await?
        };

        self.db.set_mbid(&job.track_id, &mbid).await?; 
        self.db.complete_job(job.job_id).await?;
        
        if let Err(e) = self.db.enqueue_features(&job.track_id).await {
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
            .ok_or_else(|| CrawlerError::Http("no recording for ISRC".into()))?
            .to_string();
        Ok(mbid)
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
            .ok_or_else(|| CrawlerError::Http("no recording for ISRC".into()))?
            .to_string();
        Ok(mbid)
    }

    async fn features_loop(&self) {
        info!("crawler.features.loop.start");
        let poll = Duration::from_millis(self.limits.queue_poll_ms);
        while !self.shutdown.is_cancelled() {
            match self.db.claim_one_job(JobType::Features).await {
                Ok(Some(job)) => {
                    let _permit = match self.features_handler.acquire().await {
                        Ok(p) => p, 
                        Err(_) => break
                    };
                    if let Err(e) = self.process_features_job(job).await {
                        error!(error = ?e, "features job failed");
                    }
                }
                Ok(None) => { 
                    sleep(poll).await; 
                }
                Err(e) => {
                    error!(error = ?e, "claim_one_job(Features) failed");
                    sleep(poll).await; 
                }
            }
        }
        info!("crawler.features.loop.stop");
    }

    async fn process_features_job(&self, job: Job) -> Result<(), CrawlerError> {
        debug!(job_id = job.job_id, track = %job.track_id, attempt = job.attempt, 
            "features.process");

        let meta = match self.db.get_track_metadata(&job.track_id).await 
            .map_err(|e| CrawlerError::Db(format!("get_track_metadata: {e}")))?
        {
            Some(m) => m, 
            None => {
                self.db.fail_job(job.job_id, "track not found").await?; 
                info!(job_id = job.job_id, track = %job.track_id, "skip.no_track");
                return Ok(());
            }
        };

        let mbid = meta.mb_recording_id
            .as_deref()
            .ok_or_else(|| CrawlerError::NotFound("No mbid found".into()))?;
        let highlevel = http_with_retry(
            self.clients.acousticbrainz.features(mbid, "high-level"), 
            self.limits.http_max_retry, 
            self.limits.http_backoff_ms
        ).await?;

        let path_highlevel = self.sink.write_json(
            RawType::ABHighLevel, 
            mbid, 
            highlevel.clone()
        )?;
        self.db.index_raw_file(
            &job.track_id, 
            "acousticbrainz", 
            "high-level",
            mbid, 
            path_highlevel.to_str().unwrap_or_default()
        ).await?;

        let (highlevel_numeric, highlevel_text) = DiskZstdSink::extract_high_level(
            &highlevel
        );

        self.db.upsert_features_num(&job.track_id, "acousticbrainz", &highlevel_numeric)
            .await?; 
        self.db.upsert_features_text(&job.track_id, "acousticbrainz", &highlevel_text)
            .await?; 

        let lowlevel = self.clients.acousticbrainz.features(mbid, "low-level");
        let lowlevel = http_with_retry(
            lowlevel, 
            self.limits.http_max_retry, 
            self.limits.http_backoff_ms
        ).await?;

        let path_lowlevel = self.sink.write_json(
            RawType::ABLowLevel, 
            mbid, 
            lowlevel.clone()
        )?;
        self.db.index_raw_file(
            &job.track_id, 
            "acousticbrainz", 
            "low-level",
            mbid, 
            path_lowlevel.to_str().unwrap_or_default()
        ).await?;

        let lowlevel_numeric = DiskZstdSink::extract_low_level(&lowlevel); 

        self.db.upsert_features_num(&job.track_id, "acousticbrainz", &lowlevel_numeric)
            .await?; 

        // Get tags from mbid, if fails get conventionally else warning 
        let mut tags = {
            let resp = self.clients.lastfm.track_top_tags_by_mbid(mbid);
            http_with_retry(resp, self.limits.http_max_retry, self.limits.http_backoff_ms)
                .await 
        };
        
        if tags.is_err() {
            let title  = meta.title.as_deref().unwrap_or("");
            let artist = meta.first_artist(); 
            tags = http_with_retry(
                self.clients.lastfm.track_top_tags(&artist, &title),
                self.limits.http_max_retry, 
                self.limits.http_backoff_ms
            ).await;
        }

        if let Ok(tags) = tags {
            let mbid = meta.mb_recording_id
                .as_deref()
                .ok_or_else(|| CrawlerError::NotFound("No mbid found".into()))?;
            let path_tags = self.sink.write_json(
                RawType::LastFmTopTags, 
                mbid, 
                tags
            )?;
                             
            self.db.index_raw_file(
                &job.track_id, 
                "lastfm", 
                "toptags", 
                mbid, 
                path_tags.to_str().unwrap_or_default()
            ).await?; 
        } else {
            warn!(track = %job.track_id, "lastfm tags missing");
        }

        self.db.mark_features_ok(&job.track_id).await?; 
        self.db.complete_job(job.job_id).await?; 
        info!(job_id = job.job_id, track = %job.track_id, "features.done");

        Ok(())
    }

    async fn refresh_token(
        client: &SpotifyClient, 
        cfg: &config::SpotifyConfig, 
        max_retry: usize,
        backoff_ms: u64
    ) -> Result<(String, tokio::time::Instant), CrawlerError> {
        let response = http_with_retry(
            client.token_request().basic_auth(
                &cfg.client_id, 
                Some(&cfg.client_secret)
            ), 
            max_retry, 
            backoff_ms
        ).await?; 
        let token_str = response["access_token"].as_str() 
            .ok_or_else(|| CrawlerError::Http("no access_token in response".into()))?
            .to_string();
        let expires_in = response["expires_in"].as_u64().unwrap_or(3600);
        let expire_time = tokio::time::Instant::now() + std::time::Duration::from_secs(expires_in - 60);
        Ok((token_str, expire_time))
    }

    async fn insert_tracks(&self, search: serde_json::Value, token: &str) -> bool{
        let items = search.pointer("/tracks/items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if items.is_empty() {
            debug!("no tracks found for query");
            return false; 
        } 
        
        let ids: Vec<&str> = items.iter()
            .filter_map(|i| i.get("id").and_then(|v| v.as_str()))
            .collect();
        let ids = ids.join(",");
        
        let tracks = http_with_retry(
            self.clients.spotify.batch_track(
                &ids, 
                token
            ),
            self.limits.http_max_retry,
            self.limits.http_backoff_ms
        ).await; 

        let tracks = match tracks {
            Ok(value) => value, 
            Err(e) => {
                warn!(error = ?e, "spotify batch request failed");
                sleep(Duration::from_millis(self.limits.queue_poll_ms))
                    .await;
                return false; 
            }
        };

        let tracks = tracks.get("tracks")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mut count = 0; 
        for track in tracks {
            if track.is_null() {
                continue; 
            }

            let spotify_track = persistent::SpotifyTrack::new(&track);
            match self.db.ensure_track(&spotify_track).await {
                Ok(track_id) => {
                    count += 1; 
                    debug!(
                        track = %track_id, 
                        title = %spotify_track.title,
                        "track ensured in db"
                    );
                },
                Err(e) => {
                    error!(error = ?e, track = ?spotify_track.spotify_id,
                        "failed to ensure track in db");
                }
            }

            if let Some(spotify_id) = spotify_track.spotify_id.as_deref() {
                match self.sink.write_json(
                    RawType::SpotifyTrack, 
                    spotify_id, 
                    track.clone()) {

                    Ok(path) => {
                        if let Err(e) = self.db.index_raw_file(
                            spotify_id,
                            "spotify",
                            "track", 
                            spotify_id,
                            path.to_str().unwrap_or_default()
                        ).await {
                            warn!(error = ?e, spotify_id = %spotify_id, 
                                "index_raw_file spotify");
                        }
                    }
                    Err(e) => {
                        warn!(error = ?e, spotify_id = %spotify_id, "write_json spotify");
                    }
                }
            }
        }

        if count > 0 {
            info!("feed.added {} new tracks from Spotify", count);
        }
        true 
    }

    async fn feed_loop(&self) {
        info!("crawler.feed.loop.start");
        let min_pending: i64 = 50; 
        let mut bearer_token: Option<String> = None; 
        let mut token_expiry = tokio::time::Instant::now(); 

        while !self.shutdown.is_cancelled() {
            let pending_links = match self.db.count_jobs(
                    JobType::Link, JobStatus::Pending
                ).await {
                Ok(count) =>  count, 
                Err(e) => {
                    error!(error = ?e, "count_jobs failed");
                    sleep(Duration::from_millis(self.limits.queue_poll_ms)).await; 
                    continue; 
                }
            }; 

            if pending_links >= min_pending {
                sleep(Duration::from_millis(self.limits.queue_poll_ms)).await; 
                continue; 
            }

            if bearer_token.is_none() || tokio::time::Instant::now() >= token_expiry {
                match Self::refresh_token(
                    &self.clients.spotify, 
                    &self.clients.spotify.cfg,
                    self.limits.http_max_retry,
                    self.limits.http_backoff_ms
                ).await {
                    Ok((token, exp)) => {
                        bearer_token = Some(token);
                        token_expiry = exp; 
                        debug!("fetched new spotify token (expires: ~{:?})",
                            token_expiry);
                    }
                    Err(e) => {
                        error!(error = ?e, "spotify token request failed");
                        bearer_token = None; 
                    }
                }
            }
            if bearer_token.is_some() {
                let year: u32 = SmallRng::from_entropy().gen_range(1950..=2025);
                let offset: u32 = SmallRng::from_entropy().gen_range(0..1000);
                let query = format!("year:{year}");
                debug!(%query, %offset, "spotify search");

                let search = http_with_retry(
                    self.clients.spotify.search(
                        &query, 
                        50_u32, 
                        offset, 
                        bearer_token.as_ref().unwrap()
                    ), 
                    self.limits.http_max_retry,
                    self.limits.http_backoff_ms
                ).await;
                
                let search = match search {
                    Ok(value) => value, 
                    Err(e) => {
                        warn!(error = ?e, "spotify search failed");
                        sleep(Duration::from_millis(self.limits.queue_poll_ms)).await;
                        continue; 
                    }
                };
                let token = bearer_token.as_deref().unwrap(); 
                if !self.insert_tracks(search, token).await {
                    warn!("insert_tracks failed");
                    sleep(Duration::from_millis(self.limits.queue_poll_ms)).await; 
                    continue; 
                }
            }
            sleep(Duration::from_millis(self.limits.queue_poll_ms)).await; 
        }
        info!("crawler.feed.loop.stop");
    }
}
