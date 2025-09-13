# services 

NB: Spotify's Terms of Service strictly prohibit the use of their API/Data for use in ai/ml models. Therefore any information crawlers gain from the Spotify API are guarenteed to not be in the learning data. They are exclusively used to gain access to available tracks, artists, release information, etc. which is used by other crawlers to get learning data (From external sources that **do not include spotify**)

## crawler-services 

Compendum of source files (Mostly in Rust) to crawl and ingest data to be utilized by model. 

### rs-crawler 

This crawler's primary goal is to collect internation standard recording codes, as well as any other identification for tracks. On top of that, this crawler should collect information regarding release date, artist name, features, etc. 

While ids are fetched, they are enqueued and a different process within the crawler begins the process of collecting features for each enqueued track. Two other processes handle the writing of track ids (and other metadata) and the features to our feature set. 
