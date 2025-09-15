# services 

NB: Spotify's Terms of Service strictly prohibit the use of their API/Data for use in ai/ml models. Therefore any information crawlers gain from the Spotify API are guarenteed to not be in the learning data. They are exclusively used to gain access to available tracks, artists, release information, etc. which is used by other crawlers to get learning data (From external sources that **do not include spotify**)

## track-crawler 

This crawler's primary goal is to collect internation standard recording codes, as well as any other identification for tracks. On top of that, this crawler collects information regarding release date, artist name, features, etc. 

While ids are fetched, they are enqueued and a different process within the crawler begins the process of collecting features for each enqueued track. Two other processes handle the writing of track ids (and other metadata) and the features to our feature set. 

Features are pulled from acousticbrainz high and low level features as well as lastfm's toptags for each song. A large number of features are pulled with an emphasis on quantity

## py-processor 

TODO: This service's goal is to implement all python scripts that pre-process data in some way 
