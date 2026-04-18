"""
04_collect_x_posts_api.py

Ruft Posts der ausgewählten politischen X-Accounts über die offizielle X API ab
und speichert sie direkt in die SQLite-Datenbank.

!!ACHTUNG KOSTET GELD BEI NUTZUNG!!

Ziel:
Echte Rohdaten für den definierten Untersuchungszeitraum sammeln und in das
bestehende Schema der Tabelle `posts` überführen.

Wichtige Verbesserung:
Das Skript validiert den Account-Lookup strikt, verarbeitet nur gültige User-IDs,
legt die Datenbankstruktur bei Bedarf automatisch an, verhindert unsaubere
Mischläufe auf bestehenden Daten und stoppt sofort bei kritischen API-Fehlern
wie fehlenden Credits.

Methodische Logik:
Accounts mit 0 Originalposts im Untersuchungszeitraum gelten als fachliche
Nullfälle und nicht als technischer Fehler.
"""

from pathlib import Path
import os
import sqlite3
import sys
import time
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

DATABASE_DIR = Path("database")
DATABASE_PATH = DATABASE_DIR / "election_posts.db"
ACCOUNT_LOOKUP_PATH = Path("data") / "raw" / "account_lookup.csv"

BASE_URL_TEMPLATE = "https://api.x.com/2/users/{user_id}/tweets"

START_TIME = "2024-11-06T00:00:00Z"
END_TIME = "2025-02-24T00:00:00Z"

MAX_RESULTS = 100
REQUEST_TIMEOUT_SECONDS = 60
EXPECTED_ACCOUNT_COUNT = 10
REQUEST_PAUSE_SECONDS = 1

ALLOW_APPEND_TO_EXISTING_POSTS = False

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

INSERT_SQL = """
INSERT OR IGNORE INTO posts (
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def get_bearer_token() -> str:
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        raise ValueError("Kein Bearer Token gefunden. Bitte X_BEARER_TOKEN in der .env setzen.")
    return token


def build_headers(bearer_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "election-x-analysis"
    }


def ensure_database_directory() -> None:
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)


def ensure_database_ready(database_path: Path) -> None:
    ensure_database_directory()

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute(CREATE_POSTS_TABLE_SQL)

        for statement in CREATE_INDEXES_SQL:
            cursor.execute(statement)

        connection.commit()

    print(f"Datenbankstruktur bereit: {database_path}")


def load_account_lookup(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Account-Lookup-Datei nicht gefunden: {csv_path}")
    return pd.read_csv(csv_path)


def validate_account_lookup(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {
        "account_name",
        "handle",
        "party",
        "user_id",
        "status_code",
        "error",
        "lookup_ok",
    }

    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(
            "In account_lookup.csv fehlen Pflichtspalten: "
            + ", ".join(sorted(missing_columns))
        )

    if len(df) != EXPECTED_ACCOUNT_COUNT:
        print(
            f"WARNUNG: account_lookup.csv enthält {len(df)} Zeilen, "
            f"erwartet werden {EXPECTED_ACCOUNT_COUNT}."
        )

    handles = df["handle"].astype(str).str.strip().tolist()
    duplicate_handles = sorted({handle for handle in handles if handles.count(handle) > 1})
    if duplicate_handles:
        raise ValueError(
            "Doppelte Handles im Account-Lookup gefunden: "
            + ", ".join(f"@{handle}" for handle in duplicate_handles)
        )

    df = df.copy()
    df["user_id_str"] = df["user_id"].astype(str).str.strip()
    df["lookup_ok_normalized"] = df["lookup_ok"].astype(str).str.strip().str.lower()
    df["status_code_numeric"] = pd.to_numeric(df["status_code"], errors="coerce")

    valid_mask = (
        df["lookup_ok_normalized"].isin(["true", "1"])
        & df["status_code_numeric"].eq(200)
        & ~df["user_id"].isna()
        & ~df["user_id_str"].isin(["", "nan", "none", "null"])
    )

    valid_df = df.loc[valid_mask].copy()
    invalid_df = df.loc[~valid_mask].copy()

    print(f"Valide Accounts im Lookup: {len(valid_df)}/{len(df)}")

    if not invalid_df.empty:
        print("\nAccounts mit ungültigem oder fehlgeschlagenem Lookup:")
        print(
            invalid_df[
                ["account_name", "handle", "user_id", "status_code", "error", "lookup_ok"]
            ].to_string(index=False)
        )

    if valid_df.empty:
        raise ValueError(
            "Es wurden keine validen Accounts im Lookup gefunden. "
            "Bitte zuerst 03_fetch_user_ids.py erfolgreich ausführen."
        )

    return valid_df[
        ["account_name", "handle", "party", "user_id", "status_code", "error", "lookup_ok"]
    ].copy()


def get_existing_posts_state(database_path: Path) -> dict[str, Any]:
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()

        cursor.execute("SELECT COUNT(*) FROM posts")
        total_posts = int(cursor.fetchone()[0])

        cursor.execute("SELECT COUNT(DISTINCT handle) FROM posts")
        distinct_handles = int(cursor.fetchone()[0])

        cursor.execute(
            """
            SELECT handle, COUNT(*) AS post_count
            FROM posts
            GROUP BY handle
            ORDER BY handle
            """
        )
        handle_rows = cursor.fetchall()

    return {
        "total_posts": total_posts,
        "distinct_handles": distinct_handles,
        "handle_rows": handle_rows,
    }


def ensure_safe_database_state(database_path: Path) -> None:
    state = get_existing_posts_state(database_path)

    if state["total_posts"] == 0:
        print("Datenbankprüfung: Tabelle posts ist leer. Sauberer Lauf möglich.")
        return

    print(
        f"Datenbankprüfung: Tabelle posts enthält bereits {state['total_posts']} Zeilen "
        f"und {state['distinct_handles']} unterschiedliche Handles."
    )

    if state["handle_rows"]:
        print("\nBereits vorhandene Handles in posts:")
        existing_df = pd.DataFrame(state["handle_rows"], columns=["handle", "post_count"])
        print(existing_df.to_string(index=False))

    if not ALLOW_APPEND_TO_EXISTING_POSTS:
        raise ValueError(
            "ABBRUCH: Tabelle posts enthält bereits Daten. "
            "Für einen sauberen Re-Run bitte posts vorab leeren oder "
            "ALLOW_APPEND_TO_EXISTING_POSTS bewusst auf True setzen."
        )

    print(
        "\nWARNUNG: ALLOW_APPEND_TO_EXISTING_POSTS=True. "
        "Der Lauf hängt auf bestehende Daten an."
    )


def extract_error_text(response: requests.Response) -> str:
    try:
        payload = response.json()
        if isinstance(payload, dict):
            detail = payload.get("detail")
            title = payload.get("title")
            errors = payload.get("errors")

            if detail:
                return str(detail)

            if title:
                return str(title)

            if errors:
                return str(errors)

        return response.text.strip() or "Unbekannter API-Fehler"
    except ValueError:
        return response.text.strip() or "Unbekannter API-Fehler"


def classify_api_error(status_code: int | None, error_text: str) -> str:
    error_text_lower = (error_text or "").lower()

    if status_code == 402 or "does not have any credits" in error_text_lower:
        return "credit_exhausted"

    if status_code == 429:
        return "rate_limit"

    if status_code in {500, 502, 503, 504}:
        return "server_error"

    if status_code in {401, 403}:
        return "auth_error"

    if status_code == 404:
        return "not_found"

    if status_code is None:
        return "request_exception"

    return "api_error"


def is_reply(tweet: dict[str, Any]) -> int:
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "replied_to" for ref in referenced))


def is_retweet(tweet: dict[str, Any]) -> int:
    referenced = tweet.get("referenced_tweets", [])
    return int(any(ref.get("type") == "retweeted" for ref in referenced))


def map_tweet_to_row(tweet: dict[str, Any], account_name: str, handle: str, party: str) -> tuple:
    metrics = tweet.get("public_metrics", {})

    return (
        str(tweet.get("id")),
        account_name,
        handle,
        party,
        tweet.get("created_at"),
        tweet.get("text", ""),
        int(metrics.get("like_count", 0)),
        int(metrics.get("retweet_count", 0)),
        int(metrics.get("reply_count", 0)),
        int(metrics.get("quote_count", 0)),
        is_reply(tweet),
        is_retweet(tweet),
        "x_api",
    )


def fetch_posts_for_user(user_id: str, headers: dict[str, str]) -> dict[str, Any]:
    all_tweets: list[dict[str, Any]] = []
    next_token = None
    page_count = 0

    while True:
        params = {
            "max_results": MAX_RESULTS,
            "start_time": START_TIME,
            "end_time": END_TIME,
            "tweet.fields": "created_at,public_metrics,referenced_tweets",
            "exclude": "replies,retweets",
        }

        if next_token:
            params["pagination_token"] = next_token

        url = BASE_URL_TEMPLATE.format(user_id=user_id)

        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            error_text = f"RequestException: {exc}"
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": None,
                "error": error_text,
                "error_type": classify_api_error(None, error_text),
                "pages_fetched": page_count,
            }

        if response.status_code != 200:
            error_text = extract_error_text(response)
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": error_text,
                "error_type": classify_api_error(response.status_code, error_text),
                "pages_fetched": page_count,
            }

        try:
            payload = response.json()
        except ValueError:
            error_text = "Antwort konnte nicht als JSON geparst werden."
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": error_text,
                "error_type": "invalid_json",
                "pages_fetched": page_count,
            }

        tweets = payload.get("data", [])
        meta = payload.get("meta", {})

        if tweets and not isinstance(tweets, list):
            error_text = "API-Antwort enthält kein gültiges Listenformat in 'data'."
            return {
                "ok": False,
                "tweets": all_tweets,
                "status_code": response.status_code,
                "error": error_text,
                "error_type": "invalid_payload",
                "pages_fetched": page_count,
            }

        all_tweets.extend(tweets)
        page_count += 1

        next_token = meta.get("next_token")
        if not next_token:
            break

        time.sleep(REQUEST_PAUSE_SECONDS)

    return {
        "ok": True,
        "tweets": all_tweets,
        "status_code": 200,
        "error": None,
        "error_type": None,
        "pages_fetched": page_count,
    }


def insert_rows(rows: list[tuple], database_path: Path) -> int:
    if not rows:
        return 0

    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        before_changes = connection.total_changes
        cursor.executemany(INSERT_SQL, rows)
        connection.commit()
        after_changes = connection.total_changes

    inserted_count = after_changes - before_changes
    return inserted_count


def fetch_distinct_handles_from_database(database_path: Path) -> set[str]:
    with sqlite3.connect(database_path) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT DISTINCT handle FROM posts")
        rows = cursor.fetchall()

    return {str(row[0]).strip() for row in rows if row and row[0] is not None}


def print_final_summary(
    valid_accounts_df: pd.DataFrame,
    processed_handles: list[str],
    failed_accounts: list[dict[str, Any]],
    zero_post_accounts: list[dict[str, Any]],
    total_rows_mapped: int,
    total_rows_inserted: int,
    db_handles: set[str],
    missing_expected_nonzero_handles: list[str],
) -> None:
    print("\nDatenerhebung abgeschlossen.")
    print(f"Valide Zielaccounts aus Lookup: {len(valid_accounts_df)}")
    print(f"Erfolgreich technisch verarbeitete Accounts: {len(processed_handles)}")
    print(f"Insgesamt abgerufene Posts: {total_rows_mapped}")
    print(f"Insgesamt neu gespeicherte Posts: {total_rows_inserted}")
    print(f"Handles in Tabelle posts: {len(db_handles)}")

    if failed_accounts:
        print("\nAccounts mit technischem Abruffehler:")
        print(pd.DataFrame(failed_accounts).to_string(index=False))

    if zero_post_accounts:
        print("\nAccounts mit 0 Posts im Untersuchungszeitraum:")
        print(pd.DataFrame(zero_post_accounts).to_string(index=False))

    if missing_expected_nonzero_handles:
        print("\nFEHLENDE Handles in Tabelle posts trotz erfolgreichem Non-Zero-Abruf:")
        for handle in missing_expected_nonzero_handles:
            print(f"- @{handle}")


def main() -> None:
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)

    ensure_database_ready(DATABASE_PATH)
    ensure_safe_database_state(DATABASE_PATH)

    accounts_df = load_account_lookup(ACCOUNT_LOOKUP_PATH)
    valid_accounts_df = validate_account_lookup(accounts_df)

    total_rows_mapped = 0
    total_rows_inserted = 0
    processed_handles: list[str] = []
    failed_accounts: list[dict[str, Any]] = []
    zero_post_accounts: list[dict[str, Any]] = []
    nonzero_post_handles: set[str] = set()
    abort_run_immediately = False

    for _, account in valid_accounts_df.iterrows():
        account_name = str(account["account_name"]).strip()
        handle = str(account["handle"]).strip()
        party = str(account["party"]).strip()
        user_id = str(account["user_id"]).strip()

        print(f"\nRufe Posts ab für @{handle} ({account_name}) | user_id={user_id} ...")
        result = fetch_posts_for_user(user_id, headers)

        if not result["ok"]:
            print(
                f"FEHLER beim Abruf für @{handle}: "
                f"status_code={result['status_code']} | "
                f"error_type={result['error_type']} | "
                f"error={result['error']}"
            )

            failed_accounts.append(
                {
                    "account_name": account_name,
                    "handle": handle,
                    "user_id": user_id,
                    "status_code": result["status_code"],
                    "error_type": result["error_type"],
                    "error": result["error"],
                }
            )

            if result["error_type"] == "credit_exhausted":
                print(
                    "\nKRITISCHER ABBRUCH: X API Credits sind aufgebraucht. "
                    "Der Lauf wird sofort gestoppt, um keine weiteren Requests zu senden."
                )
                abort_run_immediately = True
                break

            time.sleep(REQUEST_PAUSE_SECONDS)
            continue

        tweets = result["tweets"]
        rows = [
            map_tweet_to_row(tweet, account_name, handle, party)
            for tweet in tweets
        ]

        inserted_count = insert_rows(rows, DATABASE_PATH)

        total_rows_mapped += len(rows)
        total_rows_inserted += inserted_count
        processed_handles.append(handle)

        print(
            f"Erfolgreich verarbeitet für @{handle}: "
            f"{len(rows)} Posts abgerufen | {inserted_count} neue Zeilen gespeichert | "
            f"Seiten: {result['pages_fetched']}"
        )

        if len(rows) == 0:
            zero_post_accounts.append(
                {
                    "account_name": account_name,
                    "handle": handle,
                    "user_id": user_id,
                }
            )
            print(
                f"WARNUNG: Für @{handle} wurden 0 Posts im definierten Zeitraum zurückgegeben."
            )
        else:
            nonzero_post_handles.add(handle)

        time.sleep(REQUEST_PAUSE_SECONDS)

    db_handles = fetch_distinct_handles_from_database(DATABASE_PATH)
    missing_expected_nonzero_handles = sorted(nonzero_post_handles - db_handles)

    print_final_summary(
        valid_accounts_df=valid_accounts_df,
        processed_handles=processed_handles,
        failed_accounts=failed_accounts,
        zero_post_accounts=zero_post_accounts,
        total_rows_mapped=total_rows_mapped,
        total_rows_inserted=total_rows_inserted,
        db_handles=db_handles,
        missing_expected_nonzero_handles=missing_expected_nonzero_handles,
    )

    run_success = (
        len(valid_accounts_df) == EXPECTED_ACCOUNT_COUNT
        and len(processed_handles) == EXPECTED_ACCOUNT_COUNT
        and not failed_accounts
        and not missing_expected_nonzero_handles
    )

    if abort_run_immediately:
        print(
            "\nABBRUCH: Der Lauf wurde wegen erschöpfter X API Credits vorzeitig gestoppt."
        )
        sys.exit(1)

    if not run_success:
        print(
            "\nABBRUCH: Die Datenerhebung ist technisch unvollständig. "
            "Bitte Lookup, Datenbankzustand und API-Abruf prüfen, bevor die Pipeline fortgesetzt wird."
        )
        sys.exit(1)

    print(
        "\nDatenerhebung technisch erfolgreich abgeschlossen."
    )
    print(
        "Hinweis: Accounts mit 0 Posts im Untersuchungszeitraum wurden als fachliche Nullfälle protokolliert."
    )


if __name__ == "__main__":
    main()