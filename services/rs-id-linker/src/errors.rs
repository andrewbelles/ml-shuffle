use thiserror::Error; 

#[derive(Error, Debug)]
pub enum CrawlerError {
    #[error("config error: {0}")]
    Config(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("rate limited: retry {0:?}")]
    RateLimited(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("db error: {0}")]
    Db(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error)
}

impl From<reqwest::Error> for CrawlerError {
    fn from(e: reqwest::Error) -> Self { CrawlerError::Http(e.to_string()) }
}

impl From<serde_json::Error> for CrawlerError {
    fn from(e: serde_json::Error) -> Self { CrawlerError::Parse(e.to_string()) }
}
