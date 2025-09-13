use serde::Deserialize;
use url::Url; 
use std::time; 
use crate::CrawlerError; 

/// Constants for HTTP Config  
pub const HTTP_TIMEOUT: u64 = 8000;
pub const HTTP_CONNECT_TIMEOUT: u64 = 2000;
pub const HTTP_POOL_MAX_IDLE: usize = 16;
pub const HTTP_POOL_IDLE_TIMEOUT: u64 = 90000;
pub const HTTP_MAX_REDIRECTS: u8 = 4;

pub const RETRY_MAX_ATTEMPTS: u8 = 4;
pub const RETRY_BASE_BACKOFF: u64 = 250;
pub const RETRY_JITTER: bool = true;
pub const RETRYABLE_STATUSES: [u16; 5] = [429, 500, 502, 503, 504];

/// Wrapper over env::var to return an invalid enviroment var error
fn env_check(s: &str) -> Result<String, CrawlerError> {
    match std::env::var(s) {
        Ok(v) if !v.trim().is_empty() => Ok(v),
        _ => Err(CrawlerError::Config(format!("{s} was not set"))),
    }
}

/// Ensures that url is https 
fn ensure_https(url: &Url) -> Result<(), String> {
    if url.scheme() == "https" {
        Ok(())
    } else {
        Err(format!("URL must be https: {url}"))
    }
}

fn ensure_host(url: &Url, expected_host: &str) -> Result<(), String> {
    match url.host_str() {
        Some(h) if h.eq_ignore_ascii_case(expected_host) => Ok(()),
        Some(h) => Err(
            format!("Unexpected host for {url} (got {h}, expected {expected_host})")
        ),
        None => Err(format!("URL missing host: {url}"))
    }
}

/// Configuration for Identity expected by musicbrainz 
#[derive(Debug, Clone)]
pub enum AppEnv{ Dev, Staging, Prod }

#[derive(Debug, Clone)]
pub struct IdentityConfig {
    pub app_env: AppEnv,        // will almost always be Dev 
    pub mb_user_agent: String,
}

fn build_identity() -> Result<IdentityConfig, CrawlerError> {
    let application   = env_check("APPLICATION")?; 
    let header        = env_check("MUSIC_BRAINZ_HEADER")?;
    let mb_user_agent = format!("{application} {header}");

    let app_env = AppEnv::Dev; 
    Ok( IdentityConfig { app_env, mb_user_agent } )
}

/// Configuration that Spotify expects when hitting endpoints 
#[derive(Debug, Clone)]
pub struct SpotifyConfig {
    pub client_id: String, 
    pub client_secret: String, 
    pub token_url: Url, 
    pub api_base: Url, 
}

fn build_spotify() -> Result<SpotifyConfig, CrawlerError> {
    let client_id     = env_check("SPOTIFY_CLIENT_ID")?;
    let client_secret = env_check("SPOTIFY_CLIENT_SECRET")?;

    // form urls 
    let token_url = std::env::var("SPOTIFY_TOKEN_URL")
        .unwrap_or_else(|_| "https://accounts.spotify.com/api/token".to_string());

    let api_base  = std::env::var("SPOTIFY_API_BASE")
        .unwrap_or_else(|_| "https://api.spotify.com/v1/".to_string());

    let token_url = Url::parse(&token_url)
        .map_err(|_| CrawlerError::Config(
                format!("SPOTIFY_TOKEN_URL invalid")
        ))?;

    let mut api_base  = Url::parse(&api_base)
        .map_err(|_| CrawlerError::Config(
                format!("SPOTIFY_API_BASE invalid")
        ))?;

    // ensure valid https and hostname for both urls 
    ensure_https(&token_url).map_err(|e| CrawlerError::Config(e))?;
    ensure_https(&api_base).map_err(|e| CrawlerError::Config(e))?;
    ensure_host(&token_url, "accounts.spotify.com")
        .map_err(|e| CrawlerError::Config(e))?;
    ensure_host(&api_base, "api.spotify.com")
        .map_err(|e| CrawlerError::Config(e))?;

    if !api_base.path().ends_with('/') {
        let mut path = api_base.path().to_string(); 
        path.push('/');
        api_base.set_path(&path);
    }

    Ok( SpotifyConfig { client_id, client_secret, token_url, api_base })
}

/// 
/// Configuration for musicbrainz api 
///
#[derive(Debug, Clone)]
pub struct MusicBrainzConfig {
    pub base_url: Url,         // https://musicbrainz.org/etc  
    pub user_agent: String,    // app/version (ex@mail.com)
    pub inc_recording: String, // 
    pub search_limit: u32,     // default 5
    pub search_offset: u32,    // default 0 
    pub max_rps: f32,          // default 1.0 
    pub duration_tol: u32      // default 1500 
}

fn build_musicbrainz(identity: &IdentityConfig) -> 
    Result<MusicBrainzConfig, CrawlerError> {
        
    let env_to_uint = |s: &str, default: u32| -> u32 {
        match std::env::var(s) {
            Ok(s) => {
                match s.parse::<u32>() {
                    Ok(value) => value,
                    _ => default
                }
            },
            Err(_) => {
                default
            }
        } 
    };

    let env_to_float = |s: &str, default: f32| -> f32 {
        match std::env::var(s) {
            Ok(s) => {
                match s.parse::<f32>() {
                    Ok(value) => value,
                    _ => default
                }
            },
            Err(_) => {
                default
            }
        } 
    };


    let base_url = std::env::var("MB_BASE_URL")
        .unwrap_or_else(|_| "https://musicbrainz.org/ws/2/".to_string());

    // get url
    let mut base_url = Url::parse(&base_url)
        .map_err(|e| CrawlerError::Config(
                format!("MB_BASE_URL invalid {e}")
        ))?;

    // https and hostname check 
    ensure_https(&base_url)
        .map_err(CrawlerError::Config)?;
    ensure_host(&base_url, "musicbrainz.org")
        .map_err(CrawlerError::Config)?;

    // ensure trailing slash
    if !base_url.path().ends_with('/') {
        let mut path = base_url.path().to_string();
        path.push('/');
        base_url.set_path(&path);
    }

    // set values to either env specified or default values 
    let inc_recording = std::env::var("MB_INC_RECORDING")
        .unwrap_or_else(|_| "artist-credits+isrcs+releases".to_string());
    let search_limit  = env_to_uint("MB_SEARCH_LIMIT", 5);
    let search_offset = env_to_uint("MB_SEARCH_OFFSET", 0);
    let max_rps       = env_to_float("MB_MAX_RPS", 1.0);
    let duration_tol  = env_to_uint("MB_SEARCH_DURATION_TOL", 1500);

    Ok( MusicBrainzConfig {
        base_url,
        user_agent: identity.mb_user_agent.clone(),
        inc_recording,
        search_limit,
        search_offset,
        max_rps,
        duration_tol,
    })
}   

#[derive(Debug, Clone)]
pub struct AcoustIdConfig {
    pub api_key: String, 
    pub base_url: Url, 
    pub meta: String 
}

fn build_acoustid() -> Result<AcoustIdConfig, CrawlerError> {
    let api_key = env_check("ACOUST_ID")?;

    let base_url = std::env::var("ACOUST_BASE_URL")
        .unwrap_or_else(|_| "https:/api.acoustid.org/v2/".to_string());
    let mut base_url = Url::parse(&base_url)
        .map_err(|e| CrawlerError::Config(
            format!("ACOUST_BASE_URL invalid {e}")
        ))?;

    ensure_https(&base_url)
        .map_err(CrawlerError::Config)?;
    ensure_host(&base_url, "api.acoustid.org")
        .map_err(CrawlerError::Config)?;

    // ensure trailing slash
    if !base_url.path().ends_with('/') {
        let mut path = base_url.path().to_string();
        path.push('/');
        base_url.set_path(&path);
    }

    let meta = std::env::var("ACOUSTID_META")
        .unwrap_or_else(|_| 
            "recordings+recordingids+releaseids+tracks+compress".to_string()
        );

    Ok( AcoustIdConfig { api_key, base_url, meta } )
}

/// 
/// Configuration for Http timeouts, retries, etc. 
///
#[derive(Debug, Clone)]
pub struct RetryConfig { 
    pub max_attempts: u8, 
    pub base_backoff: time::Duration, 
    pub jitter: bool, 
    pub retryable_statuses: Vec<u16> 
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: RETRY_MAX_ATTEMPTS, 
            base_backoff: time::Duration::from_millis(RETRY_BASE_BACKOFF),
            jitter: RETRY_JITTER, 
            retryable_statuses: RETRYABLE_STATUSES.to_vec()
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub timeout: time::Duration, 
    pub connect_timeout: time::Duration, 
    pub pool_max_idle_per_host: usize, 
    pub pool_idle_timeout: time::Duration, 
    pub max_redirects: u8, 
    pub retry: RetryConfig
} 

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout: time::Duration::from_millis(HTTP_TIMEOUT),
            connect_timeout: time::Duration::from_millis(HTTP_CONNECT_TIMEOUT),
            pool_max_idle_per_host: HTTP_POOL_MAX_IDLE,
            pool_idle_timeout: time::Duration::from_millis(HTTP_POOL_IDLE_TIMEOUT),
            max_redirects: HTTP_MAX_REDIRECTS, 
            retry: RetryConfig::default()
        }
    }
}

/// 
/// Configuration for persistent storage in sqlite db or in compressed .json
///

// 
// Enum for types of compression used for data 
//
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawCompression {
    None, 
    Gzip, 
    Zstd
}

// 
// Configuration struct 
//
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub db_url: String, 
    pub raw_store_root: String, 
    pub raw_compression: RawCompression,
    pub schema_version: u16,
    pub http_cache_dir: String
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            db_url: "sqlite:./data/dev.db".to_string(), 
            raw_store_root: "./data/raw".to_string(),
            raw_compression: RawCompression::Zstd,
            schema_version: 1, 
            http_cache_dir: "./data/http-cache".to_string()
        }
    }
}

///
/// Configuration for matching already existing songs within database to 
/// ensure we don't ingest a large number of repeats 
///

//
// enum for how to normalized a title 
//
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TitleNorm {
    AsciiFoldLowerTrim,
    LowerTrim,
    None,
}

#[derive(Debug, Clone, Copy)]
pub struct MatchingConfig {
    pub min_mb_score: u8,        // 0..100; drop weak search hits
    pub duration_tol: u32,       // allowable difference on duration
    pub require_isrc_echo: bool, // if you started with an ISRC, must MB echo it?
    pub prefer_same_isrc: bool,  // if not required, still bonus matching ISRC
    pub title_norm: TitleNorm,   // how to normalize similar titles  
    pub ambiguity_margin: f32,   // top1 - top2 composite gap to auto-accept
}

impl Default for MatchingConfig {
    fn default() -> Self {
        Self {
            min_mb_score: 70,
            duration_tol: 1_500,
            require_isrc_echo: false,
            prefer_same_isrc: true,
            title_norm: TitleNorm::AsciiFoldLowerTrim,
            ambiguity_margin: 0.05,
        }
    }
}

/// 
/// Configuration for rules pertaining to each specific api in terms of 
/// rate limits, threads used, etc. 
///

#[derive(Debug, Clone)]
pub struct ConcurrencyConfig {
    pub max_inflight: u32, 
    pub spotify_concurrency: u16, 
    pub musicbrainz_concurrency: u16, 
    pub acoustid_concurrency: u16, 
    pub task_channel_capacity: usize,        // bound queue capacity
    pub retry_channel_capacity: usize, 
    pub queue_poll_interval: time::Duration, // how often to pull queue  
    pub shutdown_grace: time::Duration       // allocated time to exit 
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            max_inflight: 32, 
            spotify_concurrency: 8, 
            musicbrainz_concurrency: 1, 
            acoustid_concurrency: 4, 
            task_channel_capacity: 1024, 
            retry_channel_capacity: 256, 
            queue_poll_interval: time::Duration::from_millis(250),
            shutdown_grace: time::Duration::from_millis(5)
        }
    }
}

/// 
/// Configuration for Logger 
///

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty, 
    Json
}

// 
// Configuration for OpenTelemetry Logging backend 
//
#[derive(Debug, Clone)]
pub struct OtelConfig {
    pub enabled: bool, 
    pub service_name: String, 
    pub sample_ratio: f64 
}

#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub filter_directives: String, 
    pub format: LogFormat, 
    pub with_ansi: bool, 
    pub include_file_line: bool, 
    pub include_target: bool, 
    pub include_span_events: bool, 
    pub capture_error_sources: bool, 
    pub otel: OtelConfig 
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            filter_directives: "info,rs_id_linker=debug,reqwest=warn".to_string(),
            format: LogFormat::Json,
            with_ansi: true, 
            include_file_line: true, 
            include_target: true, 
            include_span_events: true, 
            capture_error_sources: true, 
            otel: OtelConfig {
                enabled: true, 
                service_name: "rs-id-linker".to_string(),
                sample_ratio: 0.0
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AcousticBrainzConfig {
    pub base_url: String 
}

impl Default for AcousticBrainzConfig {
    fn default() -> Self {
        Self {
            base_url: "https://acousticbrainz.org/".to_string(), 
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LastFmConfig {
    pub base_url: String, 
    pub api_key: String
}

fn build_lastfm() -> Result<LastFmConfig, CrawlerError> {
    let api_key = env_check("LASTFM_API_KEY")?;

    Ok(LastFmConfig {
        base_url: "https://ws.audioscrobbler.com/2.0/".to_string(),
        api_key
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiscogsConfig {
    pub base_url: String, 
    pub api_key: String
}

fn build_discogs() -> Result<DiscogsConfig, CrawlerError> {
    let api_key = env_check("DISCOGS_API_KEY")?;

    Ok(DiscogsConfig {
        base_url: "https://api.discogs.com/".to_string(),
        api_key
    })
}


///
/// AppConfig which holds all requests, etc. needed by fetch module 
///
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub identity: IdentityConfig, 
    pub spotify: SpotifyConfig, 
    pub acousticbrainz: AcousticBrainzConfig, 
    pub lastfm: LastFmConfig, 
    pub discogs: DiscogsConfig,
    pub musicbrainz: MusicBrainzConfig, 
    // pub acoustid: AcoustIdConfig, 
    pub http: HttpConfig, 
    pub persistence: PersistenceConfig, 
    pub matching: MatchingConfig, 
    pub concurrency: ConcurrencyConfig, 
    pub logging: LoggingConfig
}

///
/// Return all environment variables to caller at program start. 
///
pub fn load_config() -> Result<AppConfig, CrawlerError> {
    dotenvy::dotenv().ok();

    let identity    = build_identity()?; 
    let spotify     = build_spotify()?;
    let acousticbrainz = AcousticBrainzConfig::default(); 
    let lastfm      = build_lastfm()?; 
    let discogs     = build_discogs()?; 
    let musicbrainz = build_musicbrainz(&identity)?;
    // let acoustid    = build_acoustid()?;
    let http        = HttpConfig::default(); 
    let persistence = PersistenceConfig::default();    
    let matching    = MatchingConfig::default(); 
    let concurrency = ConcurrencyConfig::default(); 
    let logging     = LoggingConfig::default(); 

    Ok( AppConfig { 
        identity, spotify, acousticbrainz, lastfm, discogs, musicbrainz, 
        http, persistence, matching, concurrency, logging
    } )
}
