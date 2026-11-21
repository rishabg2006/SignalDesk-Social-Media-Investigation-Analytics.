"""Local API for the Bluesky Parquet dashboard. Run with: python server.py"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "bluesky_madoc.parquet"
VENDORED = ROOT / ".parquet-deps"
if VENDORED.exists():
    sys.path.insert(0, str(VENDORED))

try:
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit("Install the Parquet reader with: python -m pip install pyarrow") from exc

PARQUET = pq.ParquetFile(DATA)
TOTAL = PARQUET.metadata.num_rows
COL_NAMES = PARQUET.schema.names
VIEW_COLUMN = next((name for name in ("views", "view_count", "views_count", "impressions") if name in COL_NAMES), None)
DATE_COLUMN_INDEX = COL_NAMES.index("publish_date")
LATEST_STAMP = max(
    PARQUET.metadata.row_group(group).column(DATE_COLUMN_INDEX).statistics.max
    for group in range(PARQUET.metadata.num_row_groups)
)
WINDOW_SECONDS = 14 * 24 * 60 * 60
STOP = set("the and for with this that from have here about your next will are was has you our its into what would like who how any they them then than there their been were have when where while which some more most much very just only also into over under after before because into onto upon out off she him her his hers its not but can could should would may might must does did doing done such each every both few many other another same too own per via use used using says said post posts repost reply replies user users bluesky https http www com org net air bsky author likes because through however therefore".split())
LATEST = sorted(("created_at", "publish_date", "published_at"))


def day_for(value: int | None) -> str:
    if value is None:
        return ""
    # This dataset uses Unix seconds; accept milliseconds in other Parquet variants too.
    stamp = value / 1000 if value > 100_000_000_000 else value
    return datetime.fromtimestamp(stamp, timezone.utc).date().isoformat()


def analyze(filters: dict[str, str]) -> dict:
    user_filter = filters.get("user", "").casefold()
    platform_filter = filters.get("platform", "").casefold()
    keyword = filters.get("keyword", "").casefold()
    start, end = filters.get("from", ""), filters.get("to", "")
    daily: Counter[str] = Counter()
    users: Counter[str] = Counter()
    terms: Counter[str] = Counter()
    recent_terms: Counter[str] = Counter()
    previous_terms: Counter[str] = Counter()
    repost_texts: Counter[str] = Counter()
    repost_samples: dict[str, dict] = {}
    links: Counter[tuple[str, str]] = Counter()
    languages: Counter[str] = Counter()
    total = 0
    reply_count = 0
    top_viewed: dict | None = None
    records: list[tuple[int, dict]] = []
    cols = ["post_id", "publish_date", "user_id", "parent_id", "parent_user_id", "content", "platform", "interaction_type", "language"]
    if VIEW_COLUMN:
        cols.append(VIEW_COLUMN)
    for batch in PARQUET.iter_batches(batch_size=65536, columns=cols):
        data = batch.to_pydict()
        for i, raw_text in enumerate(data["content"]):
            text = raw_text or ""
            platform = (data["platform"][i] or "unknown").casefold()
            user = data["user_id"][i] or "unknown"
            stamp = data["publish_date"][i]
            day = day_for(stamp)
            if user_filter and user.casefold() != user_filter:
                continue
            if platform_filter and platform != platform_filter:
                continue
            if start and day < start or end and day > end:
                continue
            if keyword and keyword not in text.casefold():
                continue
            total += 1
            daily[day] += 1
            users[user] += 1
            languages[data["language"][i] or "unknown"] += 1
            parent_user = data["parent_user_id"][i]
            if parent_user:
                reply_count += 1
                if parent_user != user:
                    links[(user, parent_user)] += 1
            post_terms = re.findall(r"#[\w]+|@[\w.]+|[\w']{3,}", text, re.UNICODE)
            for term in post_terms:
                if term[0] not in "#@":
                    term = term.casefold()
                    if term in STOP:
                        continue
                terms[term] += 1
                if stamp and stamp >= LATEST_STAMP - WINDOW_SECONDS:
                    recent_terms[term] += 1
                elif stamp and stamp >= LATEST_STAMP - 2 * WINDOW_SECONDS:
                    previous_terms[term] += 1
            interaction = data["interaction_type"][i] or ""
            if interaction.upper() == "REPOST":
                key = " ".join(text.casefold().split())
                if key:
                    repost_texts[key] += 1
                    repost_samples[key] = {"text": text, "user": user}
            record = {
                "id": data["post_id"][i] or "",
                "timestamp": datetime.fromtimestamp((stamp or 0), timezone.utc).isoformat().replace("+00:00", "Z"),
                "platform": platform,
                "user": user,
                "text": text,
                "reply_to": parent_user,
                "likes": 0,
                "shares": 0,
                "interaction_type": interaction,
            }
            if VIEW_COLUMN:
                record["views"] = data[VIEW_COLUMN][i] or 0
                if top_viewed is None or record["views"] > top_viewed["views"]:
                    top_viewed = record.copy()
            records.append((stamp or 0, record))
            if len(records) > 48:
                records.sort(key=lambda item: item[0])
                del records[:-24]

    values = list(daily.values())
    mean = sum(values) / len(values) if values else 0
    sd = (sum((v - mean) ** 2 for v in values) / len(values)) ** .5 if values else 0
    signals = sum(v >= mean + max(2, sd * 1.5) for v in values)
    top_users = users.most_common(100)
    top_edges = [{"source": a, "target": b, "count": n} for (a, b), n in links.most_common(50)]
    recent_total, previous_total = sum(recent_terms.values()), sum(previous_terms.values())
    trending = []
    for term, count in recent_terms.items():
        old = previous_terms.get(term, 0)
        if count < 3:
            continue
        recent_share = count / max(1, recent_total)
        previous_share = old / max(1, previous_total)
        change = ((recent_share - previous_share) / previous_share * 100) if previous_share else None
        trending.append({"term": term, "recent": count, "previous": old, "change_pct": round(change, 1) if change is not None else None})
    trending.sort(key=lambda row: (row["change_pct"] if row["change_pct"] is not None else 1_000_000, row["recent"]), reverse=True)
    if VIEW_COLUMN:
        top_content = [top_viewed] if top_viewed else []
        performance_metric = "views"
    else:
        top_content = [{**repost_samples[key], "reposts": count} for key, count in repost_texts.most_common(3)]
        performance_metric = "reposts"
    return {
        "total": total,
        "users": len(users),
        "links": reply_count,
        "signals": signals,
        "daily": dict(sorted(daily.items())),
        "mean": mean,
        "top_users": [{"user": name, "count": count} for name, count in top_users],
        "terms": [{"term": term, "count": count} for term, count in terms.most_common(12)],
        "trends": trending[:8],
        "trend_window": {
            "recent_from": day_for(LATEST_STAMP - WINDOW_SECONDS),
            "recent_to": day_for(LATEST_STAMP),
            "previous_from": day_for(LATEST_STAMP - 2 * WINDOW_SECONDS),
            "previous_to": day_for(LATEST_STAMP - WINDOW_SECONDS - 1),
        },
        "views_available": bool(VIEW_COLUMN),
        "performance_metric": performance_metric,
        "top_content": top_content,
        "edges": top_edges,
        "posts": [row for _, row in sorted(records, key=lambda item: item[0], reverse=True)[:12]],
        "languages": [{"language": name, "count": count} for name, count in languages.most_common(8)],
        "data_rows": TOTAL,
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/meta":
            payload = {"rows": TOTAL, "platforms": ["bluesky"], "dataset": DATA.name}
        elif parsed.path == "/api/analyze":
            q = parse_qs(parsed.query)
            payload = analyze({key: values[0] for key, values in q.items() if values})
        else:
            return super().do_GET()
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print(f"Signal Desk: http://127.0.0.1:8765/  ({TOTAL:,} Parquet rows)", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 8765), Handler).serve_forever()
