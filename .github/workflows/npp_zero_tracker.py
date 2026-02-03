#!/usr/bin/env python3
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from typing import Iterable

import requests
from bs4 import BeautifulSoup

NRC_STATUS_URL = "https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/ps.html"

THRESHOLD_DAYS = int(os.environ.get("NPP_THRESHOLD_DAYS", "40"))
DB_PATH = os.environ.get("NPP_DB", "data/npp_power.sqlite")
OUT_JSON = os.environ.get("NPP_OUT_JSON", "out/flagged.json")


@dataclass(frozen=True)
class UnitStatus:
    unit: str
    power_pct: int


def utc_today() -> dt.date:
    return dt.datetime.utcnow().date()


def fetch_status_html(timeout_s: int = 30) -> str:
    r = requests.get(NRC_STATUS_URL, timeout=timeout_s, headers={"User-Agent": "npp-zero-tracker/gha"})
    r.raise_for_status()
    return r.text


def parse_units_from_html(html: str) -> tuple[dt.date, list[UnitStatus]]:
    soup = BeautifulSoup(html, "html.parser")
    report_date = utc_today()  # best-effort

    units: list[UnitStatus] = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            power_raw = cells[-1].replace("%", "").strip()
            if not power_raw.isdigit():
                continue
            power = int(power_raw)
            if power < 0 or power > 100:
                continue

            unit_name = " ".join(cells[:-1]).strip()
            if unit_name.lower().startswith(("region", "unit power", "plant", "unit")):
                continue

            units.append(UnitStatus(unit=unit_name, power_pct=power))

    # de-dup
    seen = set()
    deduped: list[UnitStatus] = []
    for u in units:
        key = (u.unit, u.power_pct)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(u)

    if not deduped:
        raise RuntimeError("Parsed zero unit rows from NRC page—page structure may have changed.")

    return report_date, deduped


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_power (
                d DATE NOT NULL,
                unit TEXT NOT NULL,
                power_pct INTEGER NOT NULL,
                PRIMARY KEY (d, unit)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_daily_power_unit_d ON daily_power(unit, d)")


def upsert_day(db_path: str, d: dt.date, units: Iterable[UnitStatus]) -> None:
    rows = [(d.isoformat(), u.unit, int(u.power_pct)) for u in units]
    with sqlite3.connect(db_path) as con:
        con.executemany(
            "INSERT OR REPLACE INTO daily_power(d, unit, power_pct) VALUES (?, ?, ?)",
            rows,
        )


def list_units_for_date(db_path: str, d: dt.date) -> list[str]:
    with sqlite3.connect(db_path) as con:
        rows = con.execute("SELECT unit FROM daily_power WHERE d = ? ORDER BY unit", (d.isoformat(),)).fetchall()
    return [r[0] for r in rows]


def zero_streak_days(db_path: str, unit: str, asof: dt.date) -> int:
    """Consecutive days at 0% ending on asof. Missing data breaks streak (conservative)."""
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT power_pct FROM daily_power WHERE unit = ? AND d = ?",
            (unit, asof.isoformat()),
        ).fetchone()
        if row is None or int(row["power_pct"]) != 0:
            return 0

        streak = 0
        cursor_date = asof
        while True:
            r = con.execute(
                "SELECT power_pct FROM daily_power WHERE unit = ? AND d = ?",
                (unit, cursor_date.isoformat()),
            ).fetchone()
            if r is None or int(r["power_pct"]) != 0:
                break
            streak += 1
            cursor_date -= dt.timedelta(days=1)
        return streak


PLANT_CLEAN_RE = re.compile(r"(?:\s+\d+)$|(?:\s*Unit\s*\d+)$", re.IGNORECASE)


def plant_name_from_unit(unit_name: str) -> str:
    return PLANT_CLEAN_RE.sub("", unit_name).strip()


def main() -> int:
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    init_db(DB_PATH)

    html = fetch_status_html()
    report_date, units = parse_units_from_html(html)
    upsert_day(DB_PATH, report_date, units)

    todays_units = list_units_for_date(DB_PATH, report_date)

    # unit streaks
    unit_streaks = {u: zero_streak_days(DB_PATH, u, report_date) for u in todays_units}

    # plant rollup: max unit streak
    plant_units: dict[str, list[tuple[str, int]]] = {}
    plant_max: dict[str, int] = {}
    for unit_name, streak in unit_streaks.items():
        plant = plant_name_from_unit(unit_name)
        plant_units.setdefault(plant, []).append((unit_name, streak))
        plant_max[plant] = max(plant_max.get(plant, 0), streak)

    flagged = [
        {
            "plant": plant,
            "max_zero_days": plant_max[plant],
            "units": sorted(
                [{"unit": u, "zero_days": s} for (u, s) in plant_units[plant] if s > 0],
                key=lambda x: (-x["zero_days"], x["unit"]),
            ),
        }
        for plant in plant_max
        if plant_max[plant] > THRESHOLD_DAYS
    ]
    flagged.sort(key=lambda x: (-x["max_zero_days"], x["plant"]))

    payload = {
        "report_date": report_date.isoformat(),
        "threshold_days": THRESHOLD_DAYS,
        "flagged_count": len(flagged),
        "flagged_plants": flagged,
    }

    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # Human-readable output for logs
    print(f"Report date: {payload['report_date']}")
    print(f"Threshold: > {THRESHOLD_DAYS} days at 0%")
    if not flagged:
        print("FLAGGED: none")
    else:
        print("FLAGGED:")
        for p in flagged:
            print(f"- {p['plant']}: {p['max_zero_days']} day(s)")
            for u in p["units"]:
                print(f"    • {u['unit']}: {u['zero_days']} day(s)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
