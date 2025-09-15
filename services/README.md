# services 

NB: Spotify's Terms of Service strictly prohibit the use of their API/Data for use in ai/ml models. Therefore any information crawlers gain from the Spotify API are guarenteed to not be in the learning data. They are exclusively used to gain access to available tracks, artists, release information, etc. which is used by other crawlers to get learning data (From external sources that **do not include spotify**)

## track-crawler 

This crawler's primary goal is to collect internation standard recording codes, as well as any other identification for tracks. On top of that, this crawler collects information regarding release date, artist name, features, etc. 

While ids are fetched, they are enqueued and a different process within the crawler begins the process of collecting features for each enqueued track. Two other processes handle the writing of track ids (and other metadata) and the features to our feature set. 

Features are pulled from acousticbrainz high and low level features as well as lastfm's toptags for each song. A large number of features are pulled with an emphasis on quantity

The crawler as of now has pulled ~5k songs each with ~2.2k features. This represents the raw data matrix and will be processed through unsupervised learning methods to determine a managable reduced feature space. 

## py-processor 

The following source python source files define scripts for processing raw data: 
+`matrix.py`: Pivots raw json into a `.parquet` numeric table of features per song 
+`RF-PCA.py`: Script that embeds data using the Random Forest algorithm, then performs randomized PCA to reduce feature space down to 32-128 features.  
