//!
//! src/main.rs  Andrew Belles  Sept 13, 2025 
//! 
//! Main source file of unit tests for modules as well as 
//! calls to all handlers etc. that define the crawler 
//!
//!


mod config; 
mod errors; 
mod logging; 

mod fetch;
mod persistent; 
mod sink;

use crate::errors::CrawlerError;

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let cfgs = config::load_config()?;
    let _    = logging::init_logging(&cfgs.logging)?;

    println!("Configuration: {:#?}", cfgs);
    
    tracing::info!(
        service="rs-id-linker", 
        version=%env!("CARGO_PKG_VERSION"), 
        "starting"
    );

    let spotify     = fetch::SpotifyClient::new(&cfgs.http, &cfgs.spotify)?;
    let musicbrainz = fetch::MusicBrainzClient::new(
        &cfgs.http, 
        &cfgs.identity, 
        &cfgs.musicbrainz
    )?;
    let acoust     = fetch::AcoustIdClient::new(&cfgs.http, &cfgs.acoustid)?;

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
}
