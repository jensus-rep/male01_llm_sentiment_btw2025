"""
05_sanitize_dataset.py

Bereinigt die SQLite-Datenbank für die finale Analyse.

Funktionen:
- entfernt Testdaten
- vereinheitlicht Parteinamen
- entfernt Zeilen mit leerem Text
- gibt Vorher/Nachher-Statistiken aus

Hinweis:
Das Skript arbeitet direkt auf der SQLite-Datenbank.
Vor dem Ausführen am besten ein Backup anlegen.
"""

from pathlib import Path
import sqlite3


DATABASE_PATH = Path("database") / "election_posts.db"


PARTY_NORMALIZATION = {
    "CDU": "CDU/CSU",
    "CSU": "CDU/CSU",
    "Grüne": "Bündnis 90/Die Grünen",
    "Bündnis90/Die Grünen": "Bündnis 90/Die Grünen",
    "Bündnis 90 Die Grünen": "Bündnis 90/Die Grünen",
}


def fetch_scalar(cursor: sqlite3.Cursor, query: str) -> int:
    cursor.execute(query)
    result = cursor.fetchone()
    return int(result[0]) if result and result[0] is not None else 0


def print_table_stats(cursor: sqlite3.Cursor, table_name: str) -> None:
    total_rows = fetch_scalar(cursor, f"SELECT COUNT(*) FROM {table_name};")
    empty_text_rows = fetch_scalar(
        cursor,
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE text IS NULL OR TRIM(text) = '';
        """
    )

    print(f"\nTabelle: {table_name}")
    print(f"- Zeilen gesamt: {total_rows}")
    print(f"- Leere Texte: {empty_text_rows}")

    cursor.execute(
        f"""
        SELECT party, COUNT(*) AS row_count
        FROM {table_name}
        GROUP BY party
        ORDER BY row_count DESC, party ASC;
        """
    )
    party_rows = cursor.fetchall()

    print("- Parteienverteilung:")
    for party, row_count in party_rows:
        print(f"  {party!r}: {row_count}")


def delete_test_data(cursor: sqlite3.Cursor, table_name: str) -> int:
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE LOWER(COALESCE(source, '')) IN ('test', 'testdata', 'manual_test');
        """
    )
    return cursor.rowcount


def delete_empty_text_rows(cursor: sqlite3.Cursor, table_name: str) -> int:
    cursor.execute(
        f"""
        DELETE FROM {table_name}
        WHERE text IS NULL OR TRIM(text) = '';
        """
    )
    return cursor.rowcount


def normalize_party_names(cursor: sqlite3.Cursor, table_name: str) -> int:
    updated_total = 0

    for old_value, new_value in PARTY_NORMALIZATION.items():
        cursor.execute(
            f"""
            UPDATE {table_name}
            SET party = ?
            WHERE party = ?;
            """,
            (new_value, old_value),
        )
        updated_total += cursor.rowcount

    return updated_total


def sanitize_table(cursor: sqlite3.Cursor, table_name: str) -> None:
    print(f"\nBereinige {table_name} ...")

    deleted_test = delete_test_data(cursor, table_name)
    print(f"- Entfernte Testdaten: {deleted_test}")

    normalized_parties = normalize_party_names(cursor, table_name)
    print(f"- Normalisierte Parteien: {normalized_parties}")

    deleted_empty = delete_empty_text_rows(cursor, table_name)
    print(f"- Entfernte Zeilen mit leerem Text: {deleted_empty}")


def main() -> None:
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(f"Datenbank nicht gefunden: {DATABASE_PATH}")

    with sqlite3.connect(DATABASE_PATH) as connection:
        cursor = connection.cursor()

        print("Vorher-Statistik")
        print_table_stats(cursor, "posts")

        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'posts_prepared';
            """
        )
        prepared_exists = cursor.fetchone() is not None

        if prepared_exists:
            print_table_stats(cursor, "posts_prepared")

        sanitize_table(cursor, "posts")

        if prepared_exists:
            sanitize_table(cursor, "posts_prepared")

        connection.commit()

        print("\nNachher-Statistik")
        print_table_stats(cursor, "posts")
        if prepared_exists:
            print_table_stats(cursor, "posts_prepared")

    print("\nSanitizing abgeschlossen.")


if __name__ == "__main__":
    main()