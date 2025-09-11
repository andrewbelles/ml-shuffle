mod config; 
mod errors; 
mod logging; 

mod fetch;

#[tokio::main]
async fn main() -> Result<(), errors::CrawlerError> {
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
    use super::*;

    #[tokio::test]
    #[allow(dead_code)]
    async fn spotify_client_testbench() -> Result<(), errors::CrawlerError> {
        
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
    async fn musicbrainz_client_testbench() -> Result<(), errors::CrawlerError> {
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
}
