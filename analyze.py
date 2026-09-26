#!/usr/bin/env python3
"""Reproducible descriptive analysis of the 2014 Taobao sample.

Uses only the Python standard library for data processing. Matplotlib is
optional and used only for charts. No sampled rows are discarded.
"""

import argparse
import csv
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import date, timedelta
from pathlib import Path


EXPECTED_HEADER = ["user_id", "item_id", "behavior_type", "item_category", "time"]
BEHAVIORS = {1: "browse", 2: "favorite", 3: "cart", 4: "purchase"}


def export_query(conn, sql, path, params=()):
    cur = conn.execute(sql, params)
    with path.open("w", newline="", encoding="utf-8-sig") as out:
        writer = csv.writer(out)
        writer.writerow([x[0] for x in cur.description])
        writer.writerows(cur)


def all_rows(conn, sql, params=()):
    cur = conn.execute(sql, params)
    return [dict(zip([x[0] for x in cur.description], row)) for row in cur]


def load_events(conn, source):
    conn.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        PRAGMA temp_store=MEMORY;
        PRAGMA cache_size=-200000;
        CREATE TABLE events (
            user_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            behavior INTEGER NOT NULL,
            category INTEGER NOT NULL,
            ts TEXT NOT NULL,
            day TEXT NOT NULL
        );
    """)
    counts = Counter()
    invalid = Counter()
    batch = []
    with source.open(newline="", encoding="utf-8-sig") as inp:
        reader = csv.DictReader(inp)
        if reader.fieldnames != EXPECTED_HEADER:
            raise ValueError(f"Unexpected CSV header: {reader.fieldnames}")
        for line, row in enumerate(reader, start=2):
            try:
                uid = int(row["user_id"])
                iid = int(row["item_id"])
                typ = int(row["behavior_type"])
                cat = int(row["item_category"])
                ts = row["time"]
                if typ not in BEHAVIORS or len(ts) != 13 or ts[10] != " ":
                    raise ValueError("invalid behavior or timestamp")
                date.fromisoformat(ts[:10])
                hour = int(ts[11:13])
                if not 0 <= hour <= 23:
                    raise ValueError("invalid hour")
            except (TypeError, ValueError, KeyError) as exc:
                invalid[type(exc).__name__] += 1
                if sum(invalid.values()) <= 3:
                    print(f"Invalid source row {line}: {exc}")
                continue
            batch.append((uid, iid, typ, cat, ts, ts[:10]))
            counts[typ] += 1
            if len(batch) >= 100000:
                conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,?)", batch)
                conn.commit()
                batch.clear()
                if sum(counts.values()) % 1000000 == 0:
                    print(f"Loaded {sum(counts.values()):,} records", flush=True)
        if batch:
            conn.executemany("INSERT INTO events VALUES (?,?,?,?,?,?)", batch)
            conn.commit()
    if invalid:
        raise ValueError(f"Source contains {sum(invalid.values())} invalid rows: {dict(invalid)}")
    return counts


def analyze(conn, outdir, counts):
    span = conn.execute("SELECT MIN(day), MAX(day), COUNT(DISTINCT user_id), COUNT(DISTINCT item_id), COUNT(DISTINCT category) FROM events").fetchone()
    first_day, last_day, users, items, categories = span
    if (first_day, last_day) != ("2014-11-18", "2014-12-18"):
        raise ValueError(f"Unexpected date range: {first_day} to {last_day}")
    summary = {
        "rows": sum(counts.values()), "users": users, "items": items,
        "categories": categories, "first_day": first_day, "last_day": last_day,
        "behavior_events": {BEHAVIORS[k]: counts[k] for k in sorted(BEHAVIORS)},
        "data_limits": ["sampled users", "hourly time precision", "no order value", "no traffic exposure", "no experimental assignment"],
    }

    print("Daily metrics", flush=True)
    conn.executescript("""
        CREATE TABLE daily AS
        SELECT day, COUNT(*) AS behavior_events,
               SUM(behavior=1) AS browse_events,
               SUM(behavior=2) AS favorite_events,
               SUM(behavior=3) AS cart_events,
               SUM(behavior=4) AS purchase_events,
               COUNT(DISTINCT user_id) AS active_users,
               COUNT(DISTINCT CASE WHEN behavior=4 THEN user_id END) AS purchase_users
        FROM events GROUP BY day;
        CREATE UNIQUE INDEX daily_day ON daily(day);
        CREATE TABLE user_day AS SELECT DISTINCT user_id, day FROM events;
        CREATE UNIQUE INDEX user_day_key ON user_day(user_id,day);
        CREATE INDEX user_day_date ON user_day(day);
    """)
    export_query(conn, """SELECT day,behavior_events,browse_events,favorite_events,cart_events,
        purchase_events,active_users,purchase_users,
        ROUND(1.0*purchase_users/active_users,6) AS purchase_user_rate
        FROM daily ORDER BY day""", outdir / "daily_metrics.csv")

    print("Return and repeat behavior", flush=True)
    export_query(conn, """SELECT a.day AS cohort_day, COUNT(*) AS active_users,
        SUM(b.user_id IS NOT NULL) AS day1_return_users,
        ROUND(1.0*SUM(b.user_id IS NOT NULL)/COUNT(*),6) AS day1_return_rate,
        CASE WHEN a.day <= date(?,'-7 day') THEN SUM(c.user_id IS NOT NULL) END AS day7_return_users,
        CASE WHEN a.day <= date(?,'-7 day') THEN ROUND(1.0*SUM(c.user_id IS NOT NULL)/COUNT(*),6) END AS day7_return_rate
        FROM user_day a
        LEFT JOIN user_day b ON b.user_id=a.user_id AND b.day=date(a.day,'+1 day')
        LEFT JOIN user_day c ON c.user_id=a.user_id AND c.day=date(a.day,'+7 day')
        WHERE a.day < ? GROUP BY a.day ORDER BY a.day""",
        outdir / "return_rates.csv", (last_day,last_day,last_day))
    repeat = all_rows(conn, """WITH buyers AS (
        SELECT user_id, COUNT(*) AS purchase_events, COUNT(DISTINCT day) AS purchase_days
        FROM events WHERE behavior=4 GROUP BY user_id)
        SELECT COUNT(*) AS purchase_users,
               SUM(purchase_events>=2) AS users_with_2plus_purchase_events,
               SUM(purchase_days>=2) AS users_with_2plus_purchase_days
        FROM buyers""")[0]
    repeat["distinct_purchase_user_item_days"] = conn.execute("""SELECT COUNT(*) FROM (
        SELECT user_id,item_id,day FROM events WHERE behavior=4
        GROUP BY user_id,item_id,day)""").fetchone()[0]
    summary["repeat_within_observation_window"] = repeat

    print("Purchaser item paths", flush=True)
    conn.executescript("""
        CREATE INDEX events_user_item_time ON events(user_id,item_id,ts);
        CREATE TABLE purchase_pairs AS
        SELECT user_id,item_id,MIN(ts) AS first_purchase_ts
        FROM events WHERE behavior=4 GROUP BY user_id,item_id;
        CREATE TABLE purchase_paths AS
        SELECT p.user_id,p.item_id,p.first_purchase_ts,
               MAX(CASE WHEN e.behavior=1 AND e.ts<p.first_purchase_ts THEN 1 ELSE 0 END) AS prior_browse,
               MAX(CASE WHEN e.behavior=2 AND e.ts<p.first_purchase_ts THEN 1 ELSE 0 END) AS prior_favorite,
               MAX(CASE WHEN e.behavior=3 AND e.ts<p.first_purchase_ts THEN 1 ELSE 0 END) AS prior_cart,
               MAX(CASE WHEN e.behavior IN (1,2,3) AND e.ts=p.first_purchase_ts THEN 1 ELSE 0 END) AS same_hour_intent
        FROM purchase_pairs p
        LEFT JOIN events e ON e.user_id=p.user_id AND e.item_id=p.item_id
             AND e.ts<=p.first_purchase_ts
        GROUP BY p.user_id,p.item_id,p.first_purchase_ts;
    """)
    duplicate_by_behavior = dict(conn.execute("""SELECT behavior, SUM(n-1) FROM (
        SELECT behavior, COUNT(*) AS n FROM events
        GROUP BY user_id,item_id,behavior,category,ts HAVING COUNT(*)>1)
        GROUP BY behavior""").fetchall())
    summary["same_five_fields_extra_rows_preserved"] = {
        "total": sum(duplicate_by_behavior.values()),
        "by_behavior": {BEHAVIORS[k]: duplicate_by_behavior.get(k, 0) for k in sorted(BEHAVIORS)},
        "interpretation": "Hourly timestamps cannot distinguish repeated actions from duplicate extraction. Rows were preserved."
    }
    export_query(conn, """SELECT prior_browse,prior_favorite,prior_cart,same_hour_intent,
        COUNT(*) AS purchase_user_item_pairs
        FROM purchase_paths GROUP BY 1,2,3,4 ORDER BY purchase_user_item_pairs DESC""",
        outdir / "purchase_paths.csv")
    path_total = conn.execute("SELECT COUNT(*) FROM purchase_paths").fetchone()[0]
    summary["purchase_user_item_pairs"] = path_total

    print("Early behavior segments", flush=True)
    cutoff = "2014-12-03"
    conn.executescript(f"""
        CREATE TABLE early_users AS
        SELECT user_id,
               MAX(behavior=3) AS had_cart,
               MAX(behavior=4) AS had_purchase,
               SUM(behavior=1) AS browse_events
        FROM events WHERE day <= '{cutoff}' GROUP BY user_id;
        CREATE UNIQUE INDEX early_user_key ON early_users(user_id);
        CREATE TABLE late_buyers AS
        SELECT DISTINCT user_id FROM events WHERE day > '{cutoff}' AND behavior=4;
        CREATE UNIQUE INDEX late_buyer_key ON late_buyers(user_id);
    """)
    export_query(conn, """SELECT CASE WHEN e.had_purchase=1 THEN 'already_bought'
             WHEN e.had_cart=1 THEN 'carted_no_purchase'
             WHEN e.browse_events>=20 THEN 'browsed_20plus_no_purchase'
             ELSE 'light_activity_no_purchase' END AS early_segment,
        COUNT(*) AS users, SUM(l.user_id IS NOT NULL) AS late_purchase_users,
        ROUND(1.0*SUM(l.user_id IS NOT NULL)/COUNT(*),6) AS late_purchase_user_rate
        FROM early_users e LEFT JOIN late_buyers l ON e.user_id=l.user_id
        GROUP BY 1 ORDER BY users DESC""", outdir / "early_segments.csv")

    print("Category metrics", flush=True)
    export_query(conn, """SELECT category, COUNT(*) AS behavior_events,
        SUM(behavior=4) AS purchase_events,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(DISTINCT CASE WHEN behavior=4 THEN user_id END) AS purchase_users
        FROM events GROUP BY category HAVING COUNT(*)>=1000
        ORDER BY purchase_events DESC""", outdir / "category_metrics.csv")

    print("Cart follow-up", flush=True)
    conn.executescript("""
        CREATE TABLE cart_pairs AS
        SELECT user_id,item_id,MIN(ts) AS first_cart_ts, MIN(day) AS first_cart_day
        FROM events WHERE behavior=3 AND day<='2014-12-11'
        GROUP BY user_id,item_id;
    """)
    cart = all_rows(conn, """WITH flagged AS (
        SELECT EXISTS(SELECT 1 FROM events b WHERE b.user_id=c.user_id AND b.item_id=c.item_id
            AND b.behavior=4 AND b.ts>c.first_cart_ts
            AND b.day<=date(c.first_cart_day,'+7 day')) AS later,
               EXISTS(SELECT 1 FROM events b WHERE b.user_id=c.user_id AND b.item_id=c.item_id
            AND b.behavior=4 AND b.ts=c.first_cart_ts) AS same_hour
        FROM cart_pairs c)
        SELECT COUNT(*) AS cart_user_item_pairs,
               SUM(later) AS purchased_later_by_day7,
               SUM(same_hour) AS same_hour_purchase_unknown_order,
               SUM(later OR same_hour) AS later_or_same_hour_purchase
        FROM flagged""")[0]
    summary["cart_pair_followup"] = cart
    summary["segment_cutoff"] = cutoff

    assert conn.execute("SELECT SUM(behavior_events) FROM daily").fetchone()[0] == summary["rows"]
    assert conn.execute("SELECT SUM(purchase_events) FROM daily").fetchone()[0] == counts[4]
    assert path_total <= counts[4]
    assert cart["purchased_later_by_day7"] <= cart["later_or_same_hour_purchase"] <= cart["cart_user_item_pairs"]

    with (outdir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    return summary


def charts(outdir):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("Matplotlib missing; CSV and JSON results are complete.")
        return
    with (outdir / "daily_metrics.csv").open(encoding="utf-8-sig") as f:
        data = list(csv.DictReader(f))
    xs = list(range(len(data)))
    fig, ax = plt.subplots(2, 1, figsize=(11, 6), sharex=True)
    ax[0].plot(xs, [int(r["active_users"]) for r in data], label="Active users")
    ax[0].plot(xs, [int(r["purchase_users"]) for r in data], label="Purchase users")
    ax[0].legend()
    ax[0].set_ylabel("Users")
    ax[1].plot(xs, [100*float(r["purchase_user_rate"]) for r in data], color="#c54c28")
    ax[1].set_ylabel("Purchase users / active users (%)")
    ax[1].set_xticks(xs[::3], [r["day"][5:] for r in data][::3], rotation=45)
    for a in ax:
        a.axvline(data.index(next(r for r in data if r["day"]=="2014-12-12")), color="gray", ls="--", lw=1)
        a.grid(alpha=.25)
    fig.tight_layout()
    fig.savefig(outdir / "daily_trends.png", dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Original user_action.csv")
    parser.add_argument("--output", type=Path, default=Path(__file__).resolve().parent / "results")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="taobao_sqlite_") as tmp:
        conn = sqlite3.connect(str(Path(tmp) / "events.sqlite"))
        try:
            counts = load_events(conn, args.input)
            summary = analyze(conn, args.output, counts)
        finally:
            conn.close()
    charts(args.output)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
