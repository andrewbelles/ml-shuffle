mod config; 
mod errors; 
mod logging; 

#[tokio::main]
async fn main() -> Result<(), errors::CrawlerError> {
    let cfgs   = config::load_config()?;
    let logger = logging::init_logging(&cfgs.logging)?;

    println!("Configuration: {:#?}", cfgs);
    
    tracing::info!(
        service="rs-id-linker", 
        version=%env!("CARGO_PKG_VERSION"), 
        "starting"
    );

    Ok(())
}
