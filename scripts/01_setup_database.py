"""
02_setup_database.py

Legt die SQLite-Datenbank für das Projekt an und erstellt
die zentrale Tabelle `posts`, falls sie noch nicht existiert.

Ziel:
Eine saubere, reproduzierbare Datenbasis für die spätere
Import-, Aufbereitungs- und Analysepipeline schaffen.
"""

from pathlib import Path
import sqlite3


DATABASE_DIR = Path("database")
DATABASE_PATH = DATABASE_DIR / "election_posts.db"


CREATE_POSTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS posts (
    post_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL,
    handle TEXT NOT NULL,
    party TEXT,
    created_at TEXT NOT NULL,
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
    "CREATE INDEX IF NOT EXISTS idx_posts_handle ON posts(handle);",
    "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_posts_party ON posts(party);",
]


def ensure_database_directory() -> None:
    """
    Stellt sicher, dass der Datenbankordner existiert.
    """
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)


def create_database() -> None:
    """
    Erstellt die SQLite-Datenbank und die Tabelle `posts`
    inklusive sinnvoller Indizes.
    """
    ensure_database_directory()

    with sqlite3.connect(DATABASE_PATH) as connection:
        cursor = connection.cursor()

        cursor.execute(CREATE_POSTS_TABLE_SQL)

        for statement in CREATE_INDEXES_SQL:
            cursor.execute(statement)

        connection.commit()


def main() -> None:
    """
    Haupteinstiegspunkt des Skripts.
    """
    create_database()
    print(f"Datenbank erfolgreich eingerichtet: {DATABASE_PATH}")


if __name__ == "__main__":
    main()