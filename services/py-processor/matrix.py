#!/usr/bin/env python3 
# 
# matrix.py  Andrew Belles  Sept 14th, 2025  
# 
# Generates a single matrix given sinked data
# 
# 

import argparse, json, sqlite3
from pathlib import Path
import numpy as np 
import pandas as pd 
import zstandard as zstd


def features(path: str) -> pd.DataFrame:
    db = sqlite3.connect(path)
    try: 
        data = pd.read_sql_query(
            """
            SELECT 
                t.spotify_id AS track_id, 
                f.source, 
                f.feature, 
                f.num_value
            FROM features f 
            JOIN tracks t 
            ON t.id = f.track_id 
            WHERE f.dtype = 'num' AND t.spotify_id IS NOT NULL 
            """,
            db
        )
    finally: 
        db.close() 

    if data.empty:
        return pd.DataFrame().set_index(pd.Index([], name="track_id"))

    data["col"] = data["source"] + "." + data["feature"]
    wide = (
        data.pivot_table(
            index="track_id", 
            columns="col", 
            values="num_value", 
            aggfunc="mean"
        )
        .sort_index(axis=1)
        .astype(np.float64)
    )
    wide.index.name = "track_id"
    return wide 

def metadata(path: str, include_numeric=True) -> pd.DataFrame: 
    db = sqlite3.connect(path)
    try: 
        meta = pd.read_sql_query(
            """
            SELECT 
                spotify_id AS track_id, 
                id AS internal_id, 
                isrc, 
                mb_recording_id, 
                duration_ms, 
                popularity, 
                explicit
            FROM tracks
            """,
            db 
        )
    finally: 
        db.close() 

    meta = meta.set_index("track_id")
    if include_numeric:
        for col in ("duration_ms", "popularity"):
            if col in meta.columns: 
                meta[col] = pd.to_numeric(meta[col], errors="coerce")
        if "explicit" in meta.columns:
            meta["explicit"] = meta["explicit"].fillna(0).astype("float32")
    return meta

def spotify_raw(path: str) -> pd.DataFrame:

    db = sqlite3.connect(path)
    try: 
        rel = pd.read_sql_query(
            """
            SELECT track_id, rel_path, key 
            FROM raw_files 
            WHERE source='spotify' AND subtype='track'
            """,
            db
        )
    finally: 
        db.close() 
    return rel

def numeric_from_raw(root: str | Path, rel: pd.DataFrame) -> pd.DataFrame:
    root = Path(root) 
    cols = [ "track_id" ]

    if rel.empty:
        return pd.DataFrame(
            index=pd.Index([], name="track_id"),
            columns=pd.Index(cols)
        ).astype("float32")

    rows = []
    for row in rel.itertuples(index=False):
        p = Path(row.rel_path)
        if not p.is_absolute():
            p = root / p 
        
        try: 
            with open(p, "rb") as fptr: 
                comp = zstd.ZstdDecompressor()
                data = comp.decompress(fptr.read())
        except Exception as _: 
            continue 

        rows.append({ "track_id": row.track_id })

    data = (pd.DataFrame.from_records(rows, columns=cols)
        .drop_duplicates(subset=["track_id"])
        .set_index("track_id"))
    return data.astype("float32")

def main(): 
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--raw", default="../data/raw/raw")
    parser.add_argument("--out", default="../data/matrix.parquet")

    args = parser.parse_args()

    path = args.db 
    raw  = args.raw 

    wide = features(path)
    rel  = spotify_raw(path)
    root = Path(raw)
    mat  = numeric_from_raw(root, rel)
    mat  = mat.add_prefix("spotify.")
    wide = wide.join(mat, how="left")

    out  = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(out, index=True)

if __name__ == "__main__":
    main()
