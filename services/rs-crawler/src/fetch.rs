//!
//! src/fetch.rs  Andrew Belles  Sept 10th, 2025 
//!
//! Defines methods for hitting specified endpoints and 
//! returning unparsed data, handling retries, etc. 
//!

use url::Url;
use reqwest::{Client, header, redirect, RequestBuilder};
use crate::config::{HttpConfig, IdentityConfig, MusicBrainzConfig, SpotifyConfig}; 
use crate::errors; 

/// Client building functionality 
fn client_helper(http: &HttpConfig) -> reqwest::ClientBuilder {
    Client::builder()
        .timeout(http.timeout)
        .connect_timeout(http.connect_timeout)
        .pool_max_idle_per_host(http.pool_max_idle_per_host)
        .pool_idle_timeout(Some(http.pool_idle_timeout))
        .redirect(redirect::Policy::limited(http.max_redirects as usize))
}

fn client_with_headers(http: &HttpConfig, headers: header::HeaderMap) ->
    Result<Client, errors::CrawlerError> {
    client_helper(http)
        .default_headers(headers)
        .build()
        .map_err(|e| errors::CrawlerError::Http(format!("build client: {e}")))
}

pub fn base_client(http: &HttpConfig) -> Result<Client, errors::CrawlerError> {
    let mut h = header::HeaderMap::new();
    h.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
    client_with_headers(http, h)
}

pub fn musicbrainz_client(http: &HttpConfig, id: &IdentityConfig) -> 
    Result<Client, errors::CrawlerError> {

    let mut h = header::HeaderMap::new(); 
    h.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
    h.insert(
        header::USER_AGENT, 
        header::HeaderValue::from_str(&id.mb_user_agent)
            .map_err(|e| errors::CrawlerError::Config(
                format!("invalid mb user-agent {e}")
            ))?
    );
    client_with_headers(http, h)
}

#[derive(Clone, Debug)]
pub struct SpotifyClient {
    pub http: Client, 
    pub cfg: SpotifyConfig
}

impl SpotifyClient {
    pub fn new(http_config: &HttpConfig, cfg: &SpotifyConfig) -> 
        Result<Self, errors::CrawlerError> {

        let http = base_client(http_config)?; 
        Ok( Self { 
            http, 
            cfg: cfg.clone()
        })
    }

    pub fn token_request(&self) -> reqwest::RequestBuilder {
        self.http 
            .post(self.cfg.token_url.clone())
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body("grant_type=client_credentials")
    }

    /// GET /v1/tracks/{id}
    pub fn track(&self, track_id: &str, bearer: &str) -> reqwest::RequestBuilder {
        let url = self.cfg.api_base.join(&format!("tracks/{track_id}")).unwrap();
        self.http.get(url).bearer_auth(bearer)
    }

    /// GET /v1/tracks?ids=...
    pub fn batch_track(&self, ids_csv: &str, bearer: &str) -> reqwest::RequestBuilder {
        let url = self.cfg.api_base.join("tracks").unwrap();
        self.http.get(url).bearer_auth(bearer).query(&[("ids", ids_csv)])
    }

    /// GET /v1/audio-features/{id}
    pub fn audio_features(&self, track_id: &str, bearer: &str) -> 
        reqwest::RequestBuilder {
        let url = self.cfg.api_base.join(
            &format!("audio-features/{track_id}")
        ).unwrap();
        self.http.get(url).bearer_auth(bearer)
    }

    /// GET /v1/audio-features?ids=
    pub fn batch_audio_features(&self, ids_csv: &str, bearer: &str) -> 
        reqwest::RequestBuilder {
        let url = self.cfg.api_base.join("audio-features").unwrap();
        self.http.get(url).bearer_auth(bearer).query(&[("ids", ids_csv)])
    }

    /// GET /v1/search?type=track&q=...&limit=&offset=
    pub fn search(&self, query: &str, limit: u32, offset: u32, bearer: &str) ->
        reqwest::RequestBuilder {
        let url = self.cfg.api_base.join("search").unwrap();
        self.http.get(url).bearer_auth(bearer).query(&[
            ("type", "track"),
            ("q", query),
            ("limit", &limit.to_string()),
            ("offset", &offset.to_string())
        ])
    }
}

#[derive(Debug, Clone)]
pub struct MusicBrainzClient {
    pub http: Client, 
    pub base: Url, 
    pub inc_recording: String 
}

impl MusicBrainzClient {
    pub fn new(
        http_config: &HttpConfig, 
        id: &IdentityConfig, 
        cfg: &MusicBrainzConfig) -> Result<Self, errors::CrawlerError> {
        let http = musicbrainz_client(http_config, id)?; 
        Ok( Self{ 
            http, 
            base: cfg.base_url.clone(),
            inc_recording: cfg.inc_recording.clone()
        })
    }

    /// GET /ws/v2/isrc/{ISRC}?fmt=json
    pub fn lookup_isrc(&self, isrc: &str) -> RequestBuilder {
        let url = self.base.join(&format!("isrc/{isrc}?fmt=json")).unwrap();
        self.http.get(url)
    }

    /// GET /ws/2/recording/{MBID}?fmt=json&inc=artist-credits+isrcs+releases
    pub fn lookup_recording(&self, mbid: &str) -> RequestBuilder {
        let mut url = self.base.join(&format!("recording/{mbid}")).unwrap();
        url.set_query(Some(&format!("fmt=json&inc={}", self.inc_recording)));
        self.http.get(url)
    }

    /// GET /ws/v2/recording?query=...&fmt=json&limit=&offset=
    pub fn search_recording(&self, lucene: &str, limit: u32, offset: u32) -> 
        RequestBuilder {
        let url = self.base.join("recording").unwrap();
        self.http.get(url).query(&[
            ("query", lucene),
            ("fmt", "json"),
            ("limit", &limit.to_string()),
            ("offset", &offset.to_string())
        ])
    }

    /// GET /ws/2/release/{MBID}?fmt=json&inc=...
    pub fn lookup_release(&self, mbid: &str, inc: &str) -> RequestBuilder {
        let mut url = self.base.join(&format!("release/{mbid}")).unwrap();
        url.set_query(Some(&format!("fmt=json&inc={inc}")));
        self.http.get(url)
    }
}
