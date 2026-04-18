"""
03_fetch_user_ids.py

Lädt für eine definierte Liste politischer X-Accounts die
zugehörigen User-IDs über die offizielle X API und speichert
das Ergebnis als CSV-Datei.

Ziel:
Eine stabile Grundlage für den späteren Post-Abruf schaffen.

Wichtige Verbesserung:
Das Skript validiert den Lookup jetzt strikt und bricht ab,
wenn nicht alle Ziel-Accounts erfolgreich aufgelöst wurden.
So wird verhindert, dass die Pipeline später stillschweigend
mit unvollständigen Accounts weiterläuft.
"""

from pathlib import Path
import os
import sys
from typing import Any

import requests
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

OUTPUT_DIR = Path("data") / "raw"
OUTPUT_PATH = OUTPUT_DIR / "account_lookup.csv"

BASE_URL = "https://api.x.com/2/users/by/username"
EXPECTED_ACCOUNT_COUNT = 10
REQUEST_TIMEOUT_SECONDS = 30


ACCOUNTS = [
    {"account_name": "Friedrich Merz", "handle": "_FriedrichMerz", "party": "CDU/CSU"},
    {"account_name": "Markus Söder", "handle": "Markus_Soeder", "party": "CDU/CSU"},
    {"account_name": "Olaf Scholz", "handle": "OlafScholz", "party": "SPD"},
    {"account_name": "Lars Klingbeil", "handle": "larsklingbeil", "party": "SPD"},
    {"account_name": "Robert Habeck", "handle": "roberthabeck", "party": "Bündnis 90/Die Grünen"},
    {"account_name": "Christian Lindner", "handle": "c_lindner", "party": "FDP"},
    {"account_name": "Alice Weidel", "handle": "Alice_Weidel", "party": "AfD"},
    {"account_name": "Sahra Wagenknecht", "handle": "SWagenknecht", "party": "BSW"},
    {"account_name": "Jan van Aken", "handle": "jan_vanaken", "party": "Die Linke"},
    {"account_name": "Heidi Reichinnek", "handle": "HeidiReichinnek", "party": "Die Linke"},
]


def ensure_output_directory() -> None:
    """
    Stellt sicher, dass der Ausgabeordner existiert.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_bearer_token() -> str:
    """
    Liest den Bearer Token aus der .env-Datei.
    """
    token = os.getenv("X_BEARER_TOKEN")

    if not token:
        raise ValueError(
            "Kein Bearer Token gefunden. Bitte X_BEARER_TOKEN in der .env setzen."
        )

    return token


def build_headers(bearer_token: str) -> dict[str, str]:
    """
    Baut den Authorization Header für die X API.
    """
    return {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "election-x-analysis"
    }


def validate_accounts_config() -> None:
    """
    Prüft die lokale Account-Konfiguration vor dem API-Lauf.
    """
    if len(ACCOUNTS) != EXPECTED_ACCOUNT_COUNT:
        raise ValueError(
            f"Es sind {len(ACCOUNTS)} Accounts konfiguriert, erwartet werden aber "
            f"{EXPECTED_ACCOUNT_COUNT}."
        )

    handles = [account["handle"].strip() for account in ACCOUNTS]
    duplicate_handles = sorted({handle for handle in handles if handles.count(handle) > 1})

    if duplicate_handles:
        raise ValueError(
            "Doppelte Handles in ACCOUNTS gefunden: "
            + ", ".join(f"@{handle}" for handle in duplicate_handles)
        )


def extract_error_text(response: requests.Response) -> str:
    """
    Extrahiert eine möglichst brauchbare Fehlermeldung aus der API-Antwort.
    """
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


def fetch_user_id(handle: str, headers: dict[str, str]) -> dict[str, Any]:
    """
    Ruft die User-ID eines Handles über die X API ab.
    """
    url = f"{BASE_URL}/{handle}"

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": None,
            "name_from_api": None,
            "status_code": None,
            "error": f"RequestException: {exc}",
            "lookup_ok": False,
        }

    if response.status_code != 200:
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": None,
            "name_from_api": None,
            "status_code": response.status_code,
            "error": extract_error_text(response),
            "lookup_ok": False,
        }

    try:
        payload = response.json()
    except ValueError:
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": None,
            "name_from_api": None,
            "status_code": response.status_code,
            "error": "Antwort konnte nicht als JSON geparst werden.",
            "lookup_ok": False,
        }

    data = payload.get("data")
    if not isinstance(data, dict):
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": None,
            "name_from_api": None,
            "status_code": response.status_code,
            "error": "API-Antwort enthält kein gültiges 'data'-Objekt.",
            "lookup_ok": False,
        }

    user_id = data.get("id")
    username_from_api = data.get("username")
    name_from_api = data.get("name")

    if not user_id:
        return {
            "handle": handle,
            "user_id": None,
            "username_from_api": username_from_api,
            "name_from_api": name_from_api,
            "status_code": response.status_code,
            "error": "API-Antwort enthält keine User-ID.",
            "lookup_ok": False,
        }

    return {
        "handle": handle,
        "user_id": str(user_id),
        "username_from_api": username_from_api,
        "name_from_api": name_from_api,
        "status_code": response.status_code,
        "error": None,
        "lookup_ok": True,
    }


def collect_account_lookup() -> pd.DataFrame:
    """
    Holt User-IDs für alle definierten Accounts.
    """
    validate_accounts_config()
    bearer_token = get_bearer_token()
    headers = build_headers(bearer_token)

    rows: list[dict[str, Any]] = []

    for account in ACCOUNTS:
        print(f"Rufe User-ID ab für @{account['handle']} ...")
        result = fetch_user_id(account["handle"], headers)

        row = {
            "account_name": account["account_name"],
            "handle": account["handle"],
            "party": account["party"],
            "user_id": result["user_id"],
            "username_from_api": result["username_from_api"],
            "name_from_api": result["name_from_api"],
            "status_code": result["status_code"],
            "error": result["error"],
            "lookup_ok": result["lookup_ok"],
        }

        rows.append(row)

        if result["lookup_ok"]:
            print(
                f"OK: @{account['handle']} -> user_id={result['user_id']} "
                f"(API-Username: {result['username_from_api']})"
            )
        else:
            print(
                f"FEHLER: @{account['handle']} konnte nicht aufgelöst werden. "
                f"status_code={result['status_code']} | error={result['error']}"
            )

    return pd.DataFrame(rows)


def validate_lookup_result(df: pd.DataFrame) -> tuple[bool, pd.DataFrame]:
    """
    Prüft, ob alle Ziel-Accounts erfolgreich aufgelöst wurden.
    """
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
            "Im Lookup-Ergebnis fehlen Spalten: " + ", ".join(sorted(missing_columns))
        )

    invalid_mask = (
        df["lookup_ok"].fillna(False) != True
    ) | (
        df["user_id"].isna()
    ) | (
        df["user_id"].astype(str).str.strip().isin(["", "nan", "None"])
    )

    failed_df = df.loc[invalid_mask].copy()
    success = failed_df.empty and len(df) == EXPECTED_ACCOUNT_COUNT

    return success, failed_df


def print_summary(df: pd.DataFrame, failed_df: pd.DataFrame) -> None:
    """
    Gibt eine kompakte Qualitätsübersicht zum Lookup aus.
    """
    successful_count = int(df["lookup_ok"].fillna(False).sum())
    failed_count = len(df) - successful_count

    print("\nLookup abgeschlossen.")
    print(f"Datei gespeichert unter: {OUTPUT_PATH}")
    print(
        f"Erfolgreich aufgelöste Accounts: {successful_count}/{EXPECTED_ACCOUNT_COUNT}"
    )
    print(f"Fehlgeschlagene Accounts: {failed_count}")

    print("\nVollständige Übersicht:")
    print(
        df[
            [
                "account_name",
                "handle",
                "user_id",
                "username_from_api",
                "status_code",
                "lookup_ok",
            ]
        ].to_string(index=False)
    )

    if not failed_df.empty:
        print("\nNicht erfolgreich aufgelöste Accounts:")
        print(
            failed_df[
                [
                    "account_name",
                    "handle",
                    "status_code",
                    "error",
                ]
            ].to_string(index=False)
        )


def main() -> None:
    """
    Führt den User-Lookup vollständig aus.
    """
    ensure_output_directory()

    print("Starte Account-Lookup über die X API ...")
    df = collect_account_lookup()

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    success, failed_df = validate_lookup_result(df)
    print_summary(df, failed_df)

    if not success:
        print(
            "\nABBRUCH: Nicht alle Ziel-Accounts konnten erfolgreich aufgelöst werden. "
            "Bitte account_lookup.csv prüfen, bevor der Post-Abruf gestartet wird."
        )
        sys.exit(1)

    print("\nAlle Ziel-Accounts wurden erfolgreich aufgelöst. Der nächste Schritt kann starten.")


if __name__ == "__main__":
    main()