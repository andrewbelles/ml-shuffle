//!
//! src/fetch.rs  Andrew Belles  Sept 10th, 2025 
//!
//! Defines methods for hitting specified endpoints and 
//! returning unparsed data, handling retries, etc. 
//!

use url::Url;
use reqwest::{Client, header, redirect, RequestBuilder};
use crate::config::{
    HttpConfig, IdentityConfig, MusicBrainzConfig, SpotifyConfig,
    AcousticBrainzConfig, LastFmConfig, DiscogsConfig
}; 
use crate::CrawlerError; 

/// Client building functionality 
fn client_helper(http: &HttpConfig) -> reqwest::ClientBuilder  {
    Client::builder()
        .timeout(http.timeout)
        .connect_timeout(http.connect_timeout)
        .pool_max_idle_per_host(http.pool_max_idle_per_host)
        .pool_idle_timeout(Some(http.pool_idle_timeout))
        .redirect(redirect::Policy::limited(http.max_redirects as usize))
}

fn client_with_headers(http: &HttpConfig, headers: header::HeaderMap) ->
    Result<Client, CrawlerError> {
    client_helper(http)
        .default_headers(headers)
        .build()
        .map_err(|e| CrawlerError::Http(format!("build client: {e}")))
}

fn client_with_headers_and_agent(
    http: &HttpConfig, 
    headers: header::HeaderMap,
    user_agent: &str
) -> Result<Client, CrawlerError> {
    client_helper(http)
        .default_headers(headers)
        .user_agent(user_agent)
        .build()
        .map_err(|e| CrawlerError::Http(format!("build client: {e}")))
}


pub fn base_client(http: &HttpConfig) -> Result<Client, CrawlerError> {
    let mut h = header::HeaderMap::new();
    h.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
    client_with_headers(http, h)
}

pub fn musicbrainz_client(http: &HttpConfig, id: &IdentityConfig) -> 
    Result<Client, CrawlerError> {

    let mut h = header::HeaderMap::new(); 
    h.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
    h.insert(
        header::USER_AGENT, 
        header::HeaderValue::from_str(&id.mb_user_agent)
            .map_err(|e| CrawlerError::Config(
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
        Result<Self, CrawlerError> {

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
        cfg: &MusicBrainzConfig) -> Result<Self, CrawlerError> {
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

#[derive(Clone, Debug)]
pub struct AcousticBrainzClient {
    pub http: Client, 
    pub base: Url 
}

impl AcousticBrainzClient {
    pub fn new(
        http_config: &HttpConfig, 
        identity: &IdentityConfig, 
        acousticbrainz: &AcousticBrainzConfig
    ) -> Result<Self, CrawlerError> {
        let mut headers = header::HeaderMap::new(); 
        headers.insert(header::ACCEPT, header::HeaderValue::from_static(
            "application/json"
        ));
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_str(&identity.mb_user_agent)
                .map_err(|e| CrawlerError::Config(
                    format!("invalid user agent: {e}")
                ))?
        );
        let http = client_with_headers(http_config, headers)?;

        let base = acousticbrainz.base_url.clone();
        Ok( Self{ http, base })
    }

    /// GET {base}/api/v1/{mbid}/{level}
    /// Ensure level is either high_level or low_level (TODO?) 
    pub fn features(&self, mb_recording_id: &str, level: &str) -> RequestBuilder {
        let url = self.base.join(
            &format!("api/v1/{mb_recording_id}/{level}")
        ).unwrap();
        self.http.get(url)
    }
}

#[derive(Clone, Debug)]
pub struct LastFmClient {
    pub http: Client, 
    pub cfg: LastFmConfig,
}

impl LastFmClient {
    pub fn new(http_cfg: &HttpConfig, last_cfg: &LastFmConfig) -> 
        Result<Self, CrawlerError> {
        let mut headers = header::HeaderMap::new(); 
        headers.insert(
            header::ACCEPT, 
            header::HeaderValue::from_static("application/json")
        );
        let http = client_with_headers(http_cfg, headers)?; 
        Ok( Self{ http, cfg: last_cfg.clone() })
    }

    /// GET /?method=track.getTopTags&artist=...&track=...&api_key=...&format=json
    pub fn track_top_tags(&self, artist: &str, track: &str) -> RequestBuilder {
        self.http.get(self.cfg.base_url.clone()).query(&[
            ("method", "track.getTopTags"),
            ("artist", artist),
            ("track", track),
            ("api_key", &self.cfg.api_key),
            ("format", "json"),
        ])
    }

    /// GET /?method=track.getTopTags&mbid=...&api_key=...&format=json
    pub fn track_top_tags_by_mbid(&self, mbid: &str) -> RequestBuilder {
        self.http.get(self.cfg.base_url.clone()).query(&[
            ("method", "track.getTopTags"),
            ("mbid", mbid),
            ("api_key", &self.cfg.api_key),
            ("format", "json"),
        ])
    }

    /// GET /?method=track.getInfo&artist=...&track=...&api_key=...&format=json
    pub fn track_info(&self, artist: &str, track: &str) -> RequestBuilder {
        self.http.get(self.cfg.base_url.clone()).query(&[
            ("method", "track.getInfo"),
            ("artist", artist),
            ("track", track),
            ("api_key", &self.cfg.api_key),
            ("format", "json"),
        ])
    }

    /// GET /?method=track.getSimilar&artist=...&track=...&limit=...&api_key=...&format=json
    pub fn track_similar(&self, artist: &str, track: &str, limit: u32) -> RequestBuilder {
        self.http.get(self.cfg.base_url.clone()).query(&[
            ("method", "track.getSimilar"),
            ("artist", artist),
            ("track", track),
            ("limit", &limit.to_string()),
            ("api_key", &self.cfg.api_key),
            ("format", "json"),
        ])
    }
}

#[derive(Clone, Debug)]
pub struct DiscogsClient {
    pub http: Client, 
    pub cfg: DiscogsConfig
}

impl DiscogsClient {
    pub fn new(http_cfg: &HttpConfig, identity: &IdentityConfig, dg_cfg: &DiscogsConfig) 
        -> Result<Self, CrawlerError> {
        let mut headers = header::HeaderMap::new(); 
        headers.insert(
            header::USER_AGENT,
            header::HeaderValue::from_str(&identity.mb_user_agent)
                .map_err(|e| CrawlerError::Config(format!("invalid user_agent: {e}")))?,
        );
        let http = client_with_headers(http_cfg, headers)?;

        Ok(Self { http, cfg: dg_cfg.clone() })
    }

    /// GET /database/search?artist=...&track=...&type=release&per_page=&page=
    pub fn search_release(&self, artist: &str, track: &str, per_page: u32, page: u32) -> RequestBuilder {
        let url = self.cfg.base_url.join("database/search").unwrap();
        let rb = self.http.get(url).query(&[
            ("artist", artist),
            ("track", track),
            ("type", "release"),
            ("per_page", &per_page.to_string()),
            ("page", &page.to_string()),
        ]);
        rb.header(header::AUTHORIZATION, format!("Discogs token={}", self.cfg.api_key))
    }

    /// GET /releases/{id}
    pub fn release(&self, release_id: u64) -> RequestBuilder {
        let url = self.cfg.base_url.join(&format!("releases/{release_id}")).unwrap();
        let rb = self.http.get(url);
        rb.header(header::AUTHORIZATION, format!("Discogs token={}", self.cfg.api_key))
    }

    /// GET /masters/{id}
    pub fn master(&self, master_id: u64) -> RequestBuilder {
        let url = self.cfg.base_url.join(&format!("masters/{master_id}")).unwrap();
        let rb = self.http.get(url);
        rb.header(header::AUTHORIZATION, format!("Discogs token={}", self.cfg.api_key))
    }
}
