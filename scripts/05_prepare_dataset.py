"""
06_prepare_dataset.py

Bereitet die importierten Post-Daten für die Analyse vor.

Ziele:
- Nur relevante Originalposts für die Analyse verwenden
- Zeitbezogene Felder ergänzen
- Wahlkampfphasen zuweisen
- Ergebnis in einer separaten Tabelle speichern

Ergebnis:
Tabelle `posts_prepared`
"""

from pathlib import Path
import sqlite3
import pandas as pd


DATABASE_PATH = Path("database") / "election_posts.db"

PHASE_A_START = pd.Timestamp("2024-11-06", tz="UTC")
PHASE_A_END = pd.Timestamp("2024-12-26 23:59:59", tz="UTC")
PHASE_B_START = pd.Timestamp("2024-12-27", tz="UTC")
PHASE_B_END = pd.Timestamp("2025-02-23 23:59:59", tz="UTC")


DROP_PREPARED_TABLE_SQL = "DROP TABLE IF EXISTS posts_prepared;"


CREATE_PREPARED_TABLE_SQL = """
CREATE TABLE posts_prepared (
    post_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    handle TEXT NOT NULL,
    party TEXT,
    created_at TEXT NOT NULL,
    date_only TEXT NOT NULL,
    year INTEGER NOT NULL,
    week INTEGER NOT NULL,
    year_week TEXT NOT NULL,
    phase TEXT NOT NULL,
    text TEXT NOT NULL,
    like_count INTEGER DEFAULT 0,
    retweet_count INTEGER DEFAULT 0,
    reply_count INTEGER DEFAULT 0,
    quote_count INTEGER DEFAULT 0,
    is_reply INTEGER DEFAULT 0,
    is_retweet INTEGER DEFAULT 0,
    source TEXT
);
"""


CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_posts_prepared_handle ON posts_prepared(handle);",
    "CREATE INDEX IF NOT EXISTS idx_posts_prepared_phase ON posts_prepared(phase);",
    "CREATE INDEX IF NOT EXISTS idx_posts_prepared_year_week ON posts_prepared(year_week);",
]


SELECT_POSTS_SQL = """
SELECT
    post_id,
    account_name,
    handle,
    party,
    created_at,
    text,
    like_count,
    retweet_count,
    reply_count,
    quote_count,
    is_reply,
    is_retweet,
    source
FROM posts;
"""


INSERT_PREPARED_SQL = """
INSERT INTO posts_prepared (
    post_id,
    account_name,
    handle,
    party,
    created_at,
    date_only,
    year,
    week,
    year_week,
    phase,
    text,
    like_count,
    retweet_count,
    reply_count,
    quote_count,
    is_reply,
    is_retweet,
    source
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def load_posts(database_path: Path) -> pd.DataFrame:
    with sqlite3.connect(database_path) as connection:
        df = pd.read_sql_query(SELECT_POSTS_SQL, connection)
    return df


def filter_original_posts(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df["is_reply"] == 0) & (df["is_retweet"] == 0)].copy()
    return df


def add_datetime_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ergänzt Datums- und Wochenfelder für die Analyse.
    Alle Zeitstempel werden einheitlich in UTC geparst.
    Unterstützt gemischte Datumsformate aus Testdaten und API-Daten.
    """
    df["created_at"] = pd.to_datetime(
        df["created_at"],
        errors="coerce",
        utc=True,
        format="mixed"
    )
    df = df.dropna(subset=["created_at"]).copy()

    iso_calendar = df["created_at"].dt.isocalendar()

    df["date_only"] = df["created_at"].dt.strftime("%Y-%m-%d")
    df["year"] = iso_calendar["year"].astype(int)
    df["week"] = iso_calendar["week"].astype(int)
    df["year_week"] = (
        df["year"].astype(str) + "-W" + df["week"].astype(str).str.zfill(2)
    )

    return df


def assign_phase(timestamp: pd.Timestamp) -> str:
    ts = timestamp.tz_convert("UTC")

    if PHASE_A_START <= ts <= PHASE_A_END:
        return "Phase A"
    if PHASE_B_START <= ts <= PHASE_B_END:
        return "Phase B"
    return "Outside Scope"


def add_phase(df: pd.DataFrame) -> pd.DataFrame:
    df["phase"] = df["created_at"].apply(assign_phase)
    df = df[df["phase"] != "Outside Scope"].copy()
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["created_at"] = df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S%z")

    text_columns = ["account_name", "handle", "party", "text", "source"]
    for col in text_columns:
        df[col] = df[col].fillna("").astype(str)

    numeric_columns = [
        "year",
        "week",
        "like_count",
        "retweet_count",
        "reply_count",
        "quote_count",
        "is_reply",
        "is_retweet",
    ]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df = df[
        [
            "post_id",
            "account_name",
            "handle",
            "party",
            "created_at",
            "date_only",
            "year",
            "week",
            "year_week",
            "phase",
            "text",
            "like_count",
            "retweet_count",
            "reply_count",
            "quote_count",
            "is_reply",
            "is_retweet",
            "source",
        ]
    ].copy()

    return df


def recreate_prepared_table(database_path: Path) -> None:
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute(DROP_PREPARED_TABLE_SQL)
        cursor.execute(CREATE_PREPARED_TABLE_SQL)

        for statement in CREATE_INDEXES_SQL:
            cursor.execute(statement)

        connection.commit()


def save_prepared_posts(df: pd.DataFrame, database_path: Path) -> int:
    rows = list(df.itertuples(index=False, name=None))

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.executemany(INSERT_PREPARED_SQL, rows)
        connection.commit()

    return len(rows)


def main() -> None:
    print("Lade Rohdaten aus der Datenbank ...")
    df = load_posts(DATABASE_PATH)
    print(f"Geladene Zeilen: {len(df)}")

    print("Filtere auf Originalposts ...")
    df = filter_original_posts(df)
    print(f"Zeilen nach Filterung: {len(df)}")

    print("Ergänze Datumsfelder ...")
    df = add_datetime_features(df)
    print(f"Zeilen nach Datums-Parsing: {len(df)}")

    print("Ordne Wahlkampfphasen zu ...")
    df["debug_phase"] = df["created_at"].apply(assign_phase)
    print(df["debug_phase"].value_counts(dropna=False))

    df = df[df["debug_phase"] != "Outside Scope"].copy()
    df["phase"] = df["debug_phase"]
    df = df.drop(columns=["debug_phase"])

    print(f"Zeilen innerhalb des Scopes: {len(df)}")

    print("Normalisiere Datensatz ...")
    df = normalize_columns(df)

    print("Erstelle Zieltabelle neu ...")
    recreate_prepared_table(DATABASE_PATH)

    print("Speichere vorbereitete Daten ...")
    inserted_rows = save_prepared_posts(df, DATABASE_PATH)

    print(f"Vorbereitung abgeschlossen. Gespeicherte Zeilen: {inserted_rows}")


if __name__ == "__main__":
    main()