//!
//! src/main.rs  Andrew Belles  Sept 13, 2025 
//! 
//! Main source file of unit tests for modules as well as 
//! calls to all handlers etc. that define the crawler 
//!
//!

mod crawler; 
mod config; 
mod fetch;
mod persistent; 
mod sink;
mod logging; 

mod errors; 
use crate::errors::CrawlerError;

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let cfgs = config::load_config()?; 
    let db   = persistent::Persistent::init("../data/raw.db").await?;

    let spotify = fetch::SpotifyClient::new(&cfgs.http, &cfgs.spotify)?;
    let musicbrainz = fetch::MusicBrainzClient::new(
        &cfgs.http, 
        &cfgs.identity,
        &cfgs.musicbrainz
    )?;
    let acousticbrainz = fetch::AcousticBrainzClient::new(
        &cfgs.http,
        &cfgs.identity, 
        &cfgs.acousticbrainz
    )?;
    let lastfm = fetch::LastFmClient::new(&cfgs.http, &cfgs.lastfm)?;
    let clients = crawler::Clients::new(spotify, musicbrainz, acousticbrainz, lastfm);

    let disk = sink::DiskZstdSink::new("../data/raw/", 3);
    let limits = crawler::CrawlerLimits::default();

    let _logger = logging::init_logging(&cfgs.logging);
    let crawler = crawler::Crawler::new(&cfgs, db, clients, disk, limits);

    let () = crawler.run().await?;

    Ok(())
}

/// Unit Tests 
/// Spotify Test
#[cfg(test)]
mod tests {
    use crate::CrawlerError;
    use super::*;

    fn live() -> bool {
        std::env::var("LIVE_HTTP").ok().as_deref() == Some("1")
    } 

    #[tokio::test]
    #[allow(dead_code)]
    async fn spotify_client_testbench() -> 
        Result<(), CrawlerError> {
        dotenvy::dotenv().ok();

        if !live() {
            eprintln!("Set LIVE_HTTP=1 to run");
            return Ok(())
        }
        
        let cfgs = config::load_config()?;
        let spotify = fetch::SpotifyClient::new(&cfgs.http, &cfgs.spotify)?;

        let token_response = spotify.token_request()
            .basic_auth(&cfgs.spotify.client_id, Some(&cfgs.spotify.client_secret))
            .send()
            .await?;
        assert!(token_response.status().is_success());

        let token: serde_json::Value = token_response.json().await?;
        let bearer = token["access_token"].as_str().unwrap();

        println!("token: {}",  serde_json::to_string_pretty(&token)?);
        println!("bearer: {bearer}");

        // Breathe Deeper -  Tame Impala, Lil Yatchy
        let track_response = spotify.track("6GtOsEzNUhJghrIf6UTbRV", bearer)
            .send()
            .await?;
        assert!(track_response.status().is_success());

        let track: serde_json::Value = track_response.json().await?;
        println!("track: {}", serde_json::to_string_pretty(&track)?);

        Ok(())
    }

    #[tokio::test]
    #[allow(dead_code)]
    async fn musicbrainz_client_testbench() -> Result<(), CrawlerError> {
        dotenvy::dotenv().ok();

        if !live() {
            eprintln!("Set LIVE_HTTP=1 to run");
            return Ok(())
        }

        let cfgs = config::load_config()?;
        let musicbrainz = fetch::MusicBrainzClient::new(
            &cfgs.http, &cfgs.identity, &cfgs.musicbrainz)?;

        let response = musicbrainz.lookup_isrc("AUUM71900929")
            .send()
            .await?;
        assert!(response.status().is_success());

        let isrc: serde_json::Value = response.json().await?; 
        println!("isrc: {}", serde_json::to_string_pretty(&isrc)?);

        Ok(())
    }

    #[tokio::test]
    #[allow(dead_code)]
    async fn spotify_track_and_upsert_testbench() -> Result<(), CrawlerError> {
        dotenvy::dotenv().ok();
        if !live() {
            eprintln!("Set LIVE_HTTP=1 to run");
            return Ok(())
        }

        let db_url = "../data/raw.db"; 

        eprintln!("cwd = {}", std::env::current_dir().unwrap().display());
        eprintln!("db  = {db_url}");

        let persistent = crate::persistent::Persistent::init(db_url).await?;
        let cfgs = config::load_config()?;
        let spotify = fetch::SpotifyClient::new(&cfgs.http, &cfgs.spotify)?;

        let token_response = spotify.token_request()
            .basic_auth(&cfgs.spotify.client_id, Some(&cfgs.spotify.client_secret))
            .send()
            .await?;
        assert!(token_response.status().is_success());

        let token: serde_json::Value = token_response.json().await?;
        let bearer = token["access_token"].as_str().unwrap();

        println!("token: {}",  serde_json::to_string_pretty(&token)?);
        println!("bearer: {bearer}");

        // Breathe Deeper -  Tame Impala, Lil Yatchy
        let track_response = spotify.track("6GtOsEzNUhJghrIf6UTbRV", bearer)
            .send()
            .await?;
        assert!(track_response.status().is_success());

        let track_json: serde_json::Value = track_response.json().await?;
        println!("track: {}", serde_json::to_string_pretty(&track_json)?);

        let input = crate::persistent::SpotifyTrack {
            spotify_id: Some(track_json["id"].as_str().unwrap().to_string()),
            isrc: track_json["external_ids"]["isrc"].as_str().map(str::to_string),
            title: track_json["name"].as_str().unwrap().to_string(),
            artist_all: track_json["artists"].as_array()
                .unwrap()
                .iter()
                .filter_map(|a| a["name"].as_str())
                .map(str::to_string)
                .collect(),
            album: track_json["album"]["name"].as_str().map(str::to_string),
            duration_ms: track_json["duration_ms"].as_i64(),
            release_date: track_json["album"]["release_date"].as_str().map(
                str::to_string),
            explicit: track_json["explicit"].as_bool(),
            popularity: track_json["popularity"].as_i64().map(|x| x as i32),
        };

        let (uuid, _) = persistent.upsert_track(&input).await?; 

        let fetched = persistent.get_track_metadata(&uuid).await?
            .expect("track should exist");
        let formatted = serde_json::json!({
            "id": fetched.id,
            "spotify_id": fetched.spotify_id,
            "isrc": fetched.isrc,
            "mb_recording_id": fetched.mb_recording_id,
            "linked_ok": fetched.linked_ok,
            "features_ok": fetched.features_ok,
            "updated_at": fetched.updated_at
        });

        println!("row: \n{}", serde_json::to_string_pretty(&formatted)?);

        let sink = sink::DiskZstdSink::new("../data", 3);
        let spotify_id = track_json["id"].as_str().unwrap(); 
        let path = sink.write_json(
            sink::RawType::SpotifyTrack, spotify_id, track_json.clone()
        )?;

        println!("wrote raw data to {}", path.display());

        Ok(())
    }

    #[tokio::test]
    #[allow(dead_code)]
    async fn track_pipeline_testbench() -> Result<(), CrawlerError> {
        dotenvy::dotenv().ok();
        if !live() {
            eprintln!("Set LIVE_HTTP=1 to run");
            return Ok(())
        }

        let cfgs = config::load_config()?; 
        let spotify = fetch::SpotifyClient::new(
            &cfgs.http, &cfgs.spotify)?; 
        let musicb  = fetch::MusicBrainzClient::new(
            &cfgs.http, &cfgs.identity, &cfgs.musicbrainz)?;
        let acoustb = fetch::AcousticBrainzClient::new(
            &cfgs.http, &cfgs.identity, &cfgs.acousticbrainz)?;
        let lastfm  = fetch::LastFmClient::new(
            &cfgs.http, &cfgs.lastfm)?; 

        let token_response = spotify.token_request()
            .basic_auth(&cfgs.spotify.client_id, Some(&cfgs.spotify.client_secret))
            .send()
            .await?; 

        assert!(token_response.status().is_success(), 
            "spotify token status: {}", token_response.status());

        let token: serde_json::Value = token_response.json().await?; 
        let bearer = token["access_token"].as_str().expect("spotify access_token missing");

        let track_id = "6GtOsEzNUhJghrIf6UTbRV";
        let track_response = spotify.track(track_id, bearer).send().await?; 

        assert!(track_response.status().is_success(), 
            "spotify track status: {}", track_response.status());

        let track: serde_json::Value = track_response.json().await?; 
        println!("spotify.track:\n{}", serde_json::to_string_pretty(&track)?);

        let isrc = track.pointer("/external_ids/isrc")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CrawlerError::Parse(
                    "spotify track missing external_ids".into())
            )?.to_string(); 
        
        let track_title = track.get("name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(); 

        let artists_array = track.get("artists")
            .and_then(|v| v.as_array())
            .unwrap();

        let first_artist = artists_array.first() 
            .and_then(|a| a.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        // Hit MusicBrainz to get MBID from ISRC 

        let mb_response = musicb.lookup_isrc(&isrc).send().await?; 
        assert!(mb_response.status().is_success(), 
            "musicbrainz isrc status: {}", mb_response.status()); 

        let mb: serde_json::Value = mb_response.json().await?; 
        println!("musicbrainz.isrc:\n{}", serde_json::to_string_pretty(&mb)?);

        let mbid = mb.get("recordings")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.iter()
                .find_map(|r| r.get("id").and_then(|v| v.as_str())))
            .ok_or_else(|| CrawlerError::Parse(
                "no recordings found for ISRC".into()
            ))?.to_string();

        println!("resolved MBID: {mbid}");

        let acoust_response = acoustb.features(&mbid, "high-level")
            .send()
            .await?; 
        assert!(acoust_response.status().is_success(), 
            "acousticbrainz high-level status: {}", acoust_response.status());
        let acoust_high: serde_json::Value = acoust_response.json().await?; 
        println!("acousticbrainz.high-level:\n{}", 
            serde_json::to_string_pretty(&acoust_high)?);

        let acoust_response = acoustb.features(&mbid, "low-level")
            .send()
            .await?; 
        assert!(acoust_response.status().is_success(), 
            "acousticbrainz low-level status: {}", acoust_response.status());
        let acoust_low: serde_json::Value = acoust_response.json().await?; 
        println!("acousticbrainz.low-level:\n{}", 
            serde_json::to_string_pretty(&acoust_low)?);

        let lastfm_response = lastfm.track_top_tags_by_mbid(&mbid)
            .send()
            .await?; 
        let mut tags: Option<serde_json::Value> = None; 
        if lastfm_response.status().is_success() {
            let v: serde_json::Value = lastfm_response.json().await?; 
            if v.get("toptags").is_some() {
                tags = Some(v);
            }
        }

        if tags.is_none() {
            let lastfm_response = lastfm.track_top_tags(&first_artist, &track_title)
                .send()
                .await?; 
            assert!(lastfm_response.status().is_success(),
                "last.fm toptags: {}", lastfm_response.status());
            tags = Some(lastfm_response.json().await?);
        }

        let tags = tags.expect("no last.fm toptags found");
        println!("lastfm.toptags:\n{}", serde_json::to_string_pretty(&tags)?);
        assert!(tags.get("toptags").is_some(), 
            "expected toptags key in response");

        Ok(())
    }
}
