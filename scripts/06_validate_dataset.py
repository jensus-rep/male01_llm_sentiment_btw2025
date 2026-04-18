"""
07_validate_dataset.py

Validiert die finale Datenbasis für die Analysepipeline.

Ziele:
- Existenz der erwarteten Tabellen prüfen
- Schema von posts und posts_prepared dokumentieren
- Kernkennzahlen der Datenbasis reproduzierbar prüfen
- Filterwirkung zwischen Rohdaten und Analysebasis herleiten
- Account-Verteilungen und Phasenlogik ausgeben
- Validierungsbericht als JSON und TXT speichern

Hinweis:
Das Skript verändert keine Daten. Es prüft ausschließlich den aktuellen
Stand der SQLite-Datenbank und erzeugt einen nachvollziehbaren Audit-Report.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import json
import sqlite3


DATABASE_PATH = Path("database") / "election_posts.db"

OUTPUT_DIR = Path("data") / "validation"
OUTPUT_JSON_PATH = OUTPUT_DIR / "validation_report.json"
OUTPUT_TXT_PATH = OUTPUT_DIR / "validation_summary.txt"

EXPECTED_TABLES = ["posts", "posts_prepared"]

EXPECTED_ACCOUNTS = [
    "Alice Weidel",
    "Christian Lindner",
    "Friedrich Merz",
    "Heidi Reichinnek",
    "Jan van Aken",
    "Lars Klingbeil",
    "Markus Söder",
    "Olaf Scholz",
    "Robert Habeck",
    "Sahra Wagenknecht",
]

SCOPE_START_DATE = "2024-11-06"
SCOPE_END_DATE = "2025-02-24"


def ensure_output_directory() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_scalar(cursor: sqlite3.Cursor, query: str, params: tuple = ()) -> int | str | None:
    cursor.execute(query, params)
    row = cursor.fetchone()
    return row[0] if row else None


def fetch_rows(cursor: sqlite3.Cursor, query: str, params: tuple = ()) -> list[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    query = """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table' AND name = ?;
    """
    return int(fetch_scalar(cursor, query, (table_name,)) or 0) == 1


def get_table_schema(cursor: sqlite3.Cursor, table_name: str) -> list[dict]:
    rows = fetch_rows(cursor, f"PRAGMA table_info({table_name});")

    schema = []
    for row in rows:
        schema.append(
            {
                "cid": int(row[0]),
                "name": row[1],
                "type": row[2],
                "notnull": int(row[3]),
                "default_value": row[4],
                "pk": int(row[5]),
            }
        )
    return schema


def get_basic_checks(cursor: sqlite3.Cursor, table_name: str) -> dict:
    row_count = int(fetch_scalar(cursor, f"SELECT COUNT(*) FROM {table_name};") or 0)

    missing_post_id = int(
        fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE post_id IS NULL OR TRIM(post_id) = '';
            """
        )
        or 0
    )

    missing_created_at = int(
        fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE created_at IS NULL OR TRIM(created_at) = '';
            """
        )
        or 0
    )

    missing_text = int(
        fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE text IS NULL OR TRIM(text) = '';
            """
        )
        or 0
    )

    duplicate_rows = fetch_rows(
        cursor,
        f"""
        SELECT post_id, COUNT(*) AS duplicate_count
        FROM {table_name}
        GROUP BY post_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC, post_id ASC;
        """
    )

    duplicate_post_ids = len(duplicate_rows)
    duplicate_examples = [
        {
            "post_id": row[0],
            "count": int(row[1]),
        }
        for row in duplicate_rows[:10]
    ]

    min_created_at = fetch_scalar(
        cursor,
        f"SELECT MIN(created_at) FROM {table_name};"
    )
    max_created_at = fetch_scalar(
        cursor,
        f"SELECT MAX(created_at) FROM {table_name};"
    )

    return {
        "row_count": row_count,
        "missing_post_id": missing_post_id,
        "missing_created_at": missing_created_at,
        "missing_text": missing_text,
        "duplicate_post_ids": duplicate_post_ids,
        "duplicate_examples": duplicate_examples,
        "min_created_at": min_created_at,
        "max_created_at": max_created_at,
    }


def get_scope_checks(cursor: sqlite3.Cursor, table_name: str) -> dict:
    in_scope_count = int(
        fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE date(substr(created_at, 1, 10)) BETWEEN ? AND ?;
            """,
            (SCOPE_START_DATE, SCOPE_END_DATE),
        )
        or 0
    )

    outside_scope_count = int(
        fetch_scalar(
            cursor,
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE date(substr(created_at, 1, 10)) < ?
               OR date(substr(created_at, 1, 10)) > ?;
            """,
            (SCOPE_START_DATE, SCOPE_END_DATE),
        )
        or 0
    )

    return {
        "scope_start_date": SCOPE_START_DATE,
        "scope_end_date": SCOPE_END_DATE,
        "in_scope_count": in_scope_count,
        "outside_scope_count": outside_scope_count,
    }


def get_account_distribution(cursor: sqlite3.Cursor, table_name: str) -> list[dict]:
    rows = fetch_rows(
        cursor,
        f"""
        SELECT account_name, COUNT(*) AS post_count
        FROM {table_name}
        GROUP BY account_name
        ORDER BY post_count DESC, account_name ASC;
        """
    )

    return [
        {
            "account_name": row[0],
            "post_count": int(row[1]),
        }
        for row in rows
    ]


def get_handle_distribution(cursor: sqlite3.Cursor, table_name: str) -> list[dict]:
    rows = fetch_rows(
        cursor,
        f"""
        SELECT handle, COUNT(*) AS post_count
        FROM {table_name}
        GROUP BY handle
        ORDER BY post_count DESC, handle ASC;
        """
    )

    return [
        {
            "handle": row[0],
            "post_count": int(row[1]),
        }
        for row in rows
    ]


def get_posts_flag_checks(cursor: sqlite3.Cursor) -> dict:
    reply_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts
            WHERE COALESCE(is_reply, 0) = 1;
            """
        )
        or 0
    )

    retweet_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts
            WHERE COALESCE(is_retweet, 0) = 1;
            """
        )
        or 0
    )

    original_non_retweet_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts
            WHERE COALESCE(is_reply, 0) = 0
              AND COALESCE(is_retweet, 0) = 0;
            """
        )
        or 0
    )

    return {
        "reply_count": reply_count,
        "retweet_count": retweet_count,
        "original_non_retweet_count": original_non_retweet_count,
    }


def get_prepared_flag_checks(cursor: sqlite3.Cursor) -> dict:
    reply_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts_prepared
            WHERE COALESCE(is_reply, 0) = 1;
            """
        )
        or 0
    )

    retweet_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts_prepared
            WHERE COALESCE(is_retweet, 0) = 1;
            """
        )
        or 0
    )

    outside_scope_count = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts_prepared
            WHERE phase = 'Outside Scope';
            """
        )
        or 0
    )

    return {
        "reply_count": reply_count,
        "retweet_count": retweet_count,
        "outside_scope_phase_count": outside_scope_count,
    }


def get_exclusion_breakdown(cursor: sqlite3.Cursor) -> dict:
    posts_total = int(fetch_scalar(cursor, "SELECT COUNT(*) FROM posts;") or 0)
    posts_prepared_total = int(fetch_scalar(cursor, "SELECT COUNT(*) FROM posts_prepared;") or 0)

    difference = posts_total - posts_prepared_total

    excluded_not_in_prepared = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts p
            LEFT JOIN posts_prepared pp
                ON p.post_id = pp.post_id
            WHERE pp.post_id IS NULL;
            """
        )
        or 0
    )

    excluded_replies = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts p
            LEFT JOIN posts_prepared pp
                ON p.post_id = pp.post_id
            WHERE pp.post_id IS NULL
              AND COALESCE(p.is_reply, 0) = 1;
            """
        )
        or 0
    )

    excluded_retweets = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts p
            LEFT JOIN posts_prepared pp
                ON p.post_id = pp.post_id
            WHERE pp.post_id IS NULL
              AND COALESCE(p.is_retweet, 0) = 1;
            """
        )
        or 0
    )

    excluded_outside_scope = int(
        fetch_scalar(
            cursor,
            """
            SELECT COUNT(*)
            FROM posts p
            LEFT JOIN posts_prepared pp
                ON p.post_id = pp.post_id
            WHERE pp.post_id IS NULL
              AND (
                    date(substr(p.created_at, 1, 10)) < ?
                 OR date(substr(p.created_at, 1, 10)) > ?
              );
            """,
            (SCOPE_START_DATE, SCOPE_END_DATE),
        )
        or 0
    )

    excluded_other = max(
        0,
        excluded_not_in_prepared - excluded_replies - excluded_retweets - excluded_outside_scope,
    )

    return {
        "posts_total": posts_total,
        "posts_prepared_total": posts_prepared_total,
        "difference": difference,
        "excluded_not_in_prepared": excluded_not_in_prepared,
        "excluded_replies": excluded_replies,
        "excluded_retweets": excluded_retweets,
        "excluded_outside_scope": excluded_outside_scope,
        "excluded_other": excluded_other,
    }


def get_phase_distribution(cursor: sqlite3.Cursor) -> list[dict]:
    rows = fetch_rows(
        cursor,
        """
        SELECT phase, COUNT(*) AS post_count
        FROM posts_prepared
        GROUP BY phase
        ORDER BY post_count DESC, phase ASC;
        """
    )

    return [
        {
            "phase": row[0],
            "post_count": int(row[1]),
        }
        for row in rows
    ]


def get_expected_accounts_check(observed_accounts: list[str]) -> dict:
    expected_set = set(EXPECTED_ACCOUNTS)
    observed_set = set(observed_accounts)

    missing_expected_accounts = sorted(expected_set - observed_set)
    unexpected_observed_accounts = sorted(observed_set - expected_set)

    return {
        "expected_accounts": EXPECTED_ACCOUNTS,
        "observed_accounts": sorted(observed_accounts),
        "expected_count": len(EXPECTED_ACCOUNTS),
        "observed_count": len(observed_accounts),
        "missing_expected_accounts": missing_expected_accounts,
        "unexpected_observed_accounts": unexpected_observed_accounts,
    }


def derive_validation_status(report: dict) -> dict:
    fatal_errors: list[str] = []
    warnings: list[str] = []

    for table_name, exists in report["tables"].items():
        if not exists:
            fatal_errors.append(f"Tabelle fehlt: {table_name}")

    if fatal_errors:
        return {
            "passed": False,
            "fatal_errors": fatal_errors,
            "warnings": warnings,
        }

    posts_checks = report["posts_checks"]
    prepared_checks = report["posts_prepared_checks"]
    exclusion = report["exclusion_breakdown"]
    posts_flags = report["posts_flags"]
    prepared_flags = report["posts_prepared_flags"]
    expected_accounts = report["expected_accounts_check"]

    if posts_checks["missing_post_id"] > 0:
        fatal_errors.append("posts enthält fehlende post_id-Werte")

    if prepared_checks["missing_post_id"] > 0:
        fatal_errors.append("posts_prepared enthält fehlende post_id-Werte")

    if posts_checks["duplicate_post_ids"] > 0:
        fatal_errors.append("posts enthält doppelte post_id-Werte")

    if prepared_checks["duplicate_post_ids"] > 0:
        fatal_errors.append("posts_prepared enthält doppelte post_id-Werte")

    if prepared_flags["reply_count"] > 0:
        fatal_errors.append("posts_prepared enthält Replies")

    if prepared_flags["retweet_count"] > 0:
        fatal_errors.append("posts_prepared enthält Retweets")

    if prepared_flags["outside_scope_phase_count"] > 0:
        fatal_errors.append("posts_prepared enthält Phase 'Outside Scope'")

    if exclusion["difference"] != exclusion["excluded_not_in_prepared"]:
        fatal_errors.append("Differenz zwischen posts und posts_prepared ist nicht konsistent hergeleitet")

    if posts_checks["missing_created_at"] > 0:
        warnings.append("posts enthält fehlende created_at-Werte")

    if prepared_checks["missing_created_at"] > 0:
        warnings.append("posts_prepared enthält fehlende created_at-Werte")

    if posts_checks["missing_text"] > 0:
        warnings.append("posts enthält fehlende oder leere Texte")

    if prepared_checks["missing_text"] > 0:
        warnings.append("posts_prepared enthält fehlende oder leere Texte")

    if report["posts_scope_checks"]["outside_scope_count"] > 0:
        warnings.append("posts enthält Datensätze außerhalb des Untersuchungszeitraums")

    if report["posts_prepared_scope_checks"]["outside_scope_count"] > 0:
        warnings.append("posts_prepared enthält Datensätze außerhalb des Untersuchungszeitraums")

    if expected_accounts["missing_expected_accounts"]:
        warnings.append(
            "Nicht alle erwarteten Zielaccounts sind in der finalen Datenbank beobachtbar: "
            + ", ".join(expected_accounts["missing_expected_accounts"])
        )

    if posts_flags["original_non_retweet_count"] != prepared_checks["row_count"]:
        warnings.append(
            "Anzahl der Originalposts ohne Retweets in posts entspricht nicht exakt der Zeilenzahl von posts_prepared"
        )

    return {
        "passed": len(fatal_errors) == 0,
        "fatal_errors": fatal_errors,
        "warnings": warnings,
    }


def build_text_summary(report: dict) -> str:
    lines: list[str] = []

    lines.append("VALIDIERUNGSREPORT DATENBASIS")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"Datenbank: {report['database_path']}")
    lines.append(f"Erstellt am (UTC): {report['generated_at_utc']}")
    lines.append("")

    lines.append("VALIDIERUNGSSTATUS")
    lines.append("-" * 70)
    lines.append(f"Bestanden: {'JA' if report['validation_status']['passed'] else 'NEIN'}")

    if report["validation_status"]["fatal_errors"]:
        lines.append("Fatale Fehler:")
        for item in report["validation_status"]["fatal_errors"]:
            lines.append(f"  - {item}")
    else:
        lines.append("Fatale Fehler: keine")

    if report["validation_status"]["warnings"]:
        lines.append("Warnungen:")
        for item in report["validation_status"]["warnings"]:
            lines.append(f"  - {item}")
    else:
        lines.append("Warnungen: keine")

    lines.append("")
    lines.append("TABELLEN")
    lines.append("-" * 70)
    for table_name, exists in report["tables"].items():
        lines.append(f"{table_name}: {'vorhanden' if exists else 'FEHLT'}")

    lines.append("")
    lines.append("BASISCHECKS POSTS")
    lines.append("-" * 70)
    for key, value in report["posts_checks"].items():
        if key != "duplicate_examples":
            lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("BASISCHECKS POSTS_PREPARED")
    lines.append("-" * 70)
    for key, value in report["posts_prepared_checks"].items():
        if key != "duplicate_examples":
            lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("SCOPE-CHECKS POSTS")
    lines.append("-" * 70)
    for key, value in report["posts_scope_checks"].items():
        lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("SCOPE-CHECKS POSTS_PREPARED")
    lines.append("-" * 70)
    for key, value in report["posts_prepared_scope_checks"].items():
        lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("FILTERWIRKUNG")
    lines.append("-" * 70)
    for key, value in report["exclusion_breakdown"].items():
        lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("FLAGS POSTS")
    lines.append("-" * 70)
    for key, value in report["posts_flags"].items():
        lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("FLAGS POSTS_PREPARED")
    lines.append("-" * 70)
    for key, value in report["posts_prepared_flags"].items():
        lines.append(f"{key}: {value}")

    lines.append("")
    lines.append("ACCOUNTS POSTS")
    lines.append("-" * 70)
    for row in report["posts_accounts"]:
        lines.append(f"{row['account_name']}: {row['post_count']}")

    lines.append("")
    lines.append("ACCOUNTS POSTS_PREPARED")
    lines.append("-" * 70)
    for row in report["posts_prepared_accounts"]:
        lines.append(f"{row['account_name']}: {row['post_count']}")

    lines.append("")
    lines.append("PHASENVERTEILUNG POSTS_PREPARED")
    lines.append("-" * 70)
    for row in report["phase_distribution"]:
        lines.append(f"{row['phase']}: {row['post_count']}")

    lines.append("")
    lines.append("ABGLEICH GEGEN ERWARTETE ZIELACCOUNTS")
    lines.append("-" * 70)
    lines.append(f"Erwartete Anzahl: {report['expected_accounts_check']['expected_count']}")
    lines.append(f"Beobachtete Anzahl: {report['expected_accounts_check']['observed_count']}")
    lines.append(
        "Fehlende erwartete Accounts: "
        + (
            ", ".join(report["expected_accounts_check"]["missing_expected_accounts"])
            if report["expected_accounts_check"]["missing_expected_accounts"]
            else "keine"
        )
    )
    lines.append(
        "Unerwartete beobachtete Accounts: "
        + (
            ", ".join(report["expected_accounts_check"]["unexpected_observed_accounts"])
            if report["expected_accounts_check"]["unexpected_observed_accounts"]
            else "keine"
        )
    )

    return "\n".join(lines)


def print_console_summary(report: dict) -> None:
    print("\nVALIDIERUNG DATENBASIS")
    print("=" * 70)
    print(f"Datenbank: {report['database_path']}")
    print(f"Bestanden: {'JA' if report['validation_status']['passed'] else 'NEIN'}")

    if report["validation_status"]["fatal_errors"]:
        print("\nFatale Fehler:")
        for item in report["validation_status"]["fatal_errors"]:
            print(f"- {item}")

    if report["validation_status"]["warnings"]:
        print("\nWarnungen:")
        for item in report["validation_status"]["warnings"]:
            print(f"- {item}")

    print("\nKernzahlen:")
    print(f"- posts: {report['posts_checks']['row_count']}")
    print(f"- posts_prepared: {report['posts_prepared_checks']['row_count']}")
    print(f"- Differenz: {report['exclusion_breakdown']['difference']}")
    print(f"- Ausgeschlossene Replies: {report['exclusion_breakdown']['excluded_replies']}")
    print(f"- Ausgeschlossene Retweets: {report['exclusion_breakdown']['excluded_retweets']}")
    print(f"- Außerhalb Scope: {report['exclusion_breakdown']['excluded_outside_scope']}")

    print("\nReports gespeichert:")
    print(f"- {OUTPUT_JSON_PATH}")
    print(f"- {OUTPUT_TXT_PATH}")


def main() -> None:
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(f"Datenbank nicht gefunden: {DATABASE_PATH}")

    ensure_output_directory()

    with sqlite3.connect(DATABASE_PATH) as connection:
        cursor = connection.cursor()

        tables = {
            table_name: table_exists(cursor, table_name)
            for table_name in EXPECTED_TABLES
        }

        missing_tables = [name for name, exists in tables.items() if not exists]
        if missing_tables:
            report = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "database_path": str(DATABASE_PATH),
                "tables": tables,
                "validation_status": {
                    "passed": False,
                    "fatal_errors": [f"Tabelle fehlt: {name}" for name in missing_tables],
                    "warnings": [],
                },
            }

            json_text = json.dumps(report, indent=2, ensure_ascii=False)
            OUTPUT_JSON_PATH.write_text(json_text, encoding="utf-8")

            txt_text = build_text_summary(
                {
                    **report,
                    "posts_checks": {},
                    "posts_prepared_checks": {},
                    "posts_scope_checks": {},
                    "posts_prepared_scope_checks": {},
                    "posts_flags": {},
                    "posts_prepared_flags": {},
                    "exclusion_breakdown": {},
                    "posts_accounts": [],
                    "posts_prepared_accounts": [],
                    "phase_distribution": [],
                    "expected_accounts_check": {
                        "expected_count": len(EXPECTED_ACCOUNTS),
                        "observed_count": 0,
                        "missing_expected_accounts": EXPECTED_ACCOUNTS,
                        "unexpected_observed_accounts": [],
                    },
                }
            )
            OUTPUT_TXT_PATH.write_text(txt_text, encoding="utf-8")

            print_console_summary(
                {
                    **report,
                    "posts_checks": {"row_count": 0},
                    "posts_prepared_checks": {"row_count": 0},
                    "exclusion_breakdown": {
                        "difference": 0,
                        "excluded_replies": 0,
                        "excluded_retweets": 0,
                        "excluded_outside_scope": 0,
                    },
                }
            )
            return

        posts_schema = get_table_schema(cursor, "posts")
        posts_prepared_schema = get_table_schema(cursor, "posts_prepared")

        posts_checks = get_basic_checks(cursor, "posts")
        posts_prepared_checks = get_basic_checks(cursor, "posts_prepared")

        posts_scope_checks = get_scope_checks(cursor, "posts")
        posts_prepared_scope_checks = get_scope_checks(cursor, "posts_prepared")

        posts_flags = get_posts_flag_checks(cursor)
        posts_prepared_flags = get_prepared_flag_checks(cursor)

        exclusion_breakdown = get_exclusion_breakdown(cursor)

        posts_accounts = get_account_distribution(cursor, "posts")
        posts_prepared_accounts = get_account_distribution(cursor, "posts_prepared")

        posts_handles = get_handle_distribution(cursor, "posts")
        posts_prepared_handles = get_handle_distribution(cursor, "posts_prepared")

        phase_distribution = get_phase_distribution(cursor)

        observed_accounts = [row["account_name"] for row in posts_accounts]
        expected_accounts_check = get_expected_accounts_check(observed_accounts)

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "database_path": str(DATABASE_PATH),
            "tables": tables,
            "posts_schema": posts_schema,
            "posts_prepared_schema": posts_prepared_schema,
            "posts_checks": posts_checks,
            "posts_prepared_checks": posts_prepared_checks,
            "posts_scope_checks": posts_scope_checks,
            "posts_prepared_scope_checks": posts_prepared_scope_checks,
            "posts_flags": posts_flags,
            "posts_prepared_flags": posts_prepared_flags,
            "exclusion_breakdown": exclusion_breakdown,
            "posts_accounts": posts_accounts,
            "posts_prepared_accounts": posts_prepared_accounts,
            "posts_handles": posts_handles,
            "posts_prepared_handles": posts_prepared_handles,
            "phase_distribution": phase_distribution,
            "expected_accounts_check": expected_accounts_check,
        }

        report["validation_status"] = derive_validation_status(report)

    json_text = json.dumps(report, indent=2, ensure_ascii=False)
    OUTPUT_JSON_PATH.write_text(json_text, encoding="utf-8")

    txt_text = build_text_summary(report)
    OUTPUT_TXT_PATH.write_text(txt_text, encoding="utf-8")

    print_console_summary(report)


if __name__ == "__main__":
    main()