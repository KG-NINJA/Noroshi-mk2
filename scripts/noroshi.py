#!/usr/bin/env python3

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
import sys
import urllib.request
import xml.etree.ElementTree as ET

HANDLE = "KG-NINJA"
GITHUB_PROFILE_URL = "https://github.com/KG-NINJA"
KAGGLE_PROFILE_URL = "https://www.kaggle.com/kgninja"

DEFAULT_PUBLIC_EVENT_TYPES: set[str] = {
    "PushEvent",
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "IssuesEvent",
    "IssueCommentEvent",
    "ReleaseEvent",
    "CreateEvent",
}


def load_config(repo_root: pathlib.Path) -> dict:
    config_path = repo_root / "noroshi" / "config.json"
    if not config_path.exists():
        return {}

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def normalize_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []

    out: list[str] = []
    for item in value:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
    return out


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat_z(timestamp: dt.datetime) -> str:
    return (
        timestamp.astimezone(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_github_timestamp(value: str) -> dt.datetime:
    # Example: 2026-01-13T12:34:56Z
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_site_url() -> str:
    explicit = os.getenv("SITE_URL")
    if explicit:
        return explicit.rstrip("/")

    repo = os.getenv("GITHUB_REPOSITORY")
    owner = os.getenv("GITHUB_REPOSITORY_OWNER")
    if not repo or not owner:
        return ""

    repo_name = repo.split("/", 1)[1]
    if repo_name.lower() == f"{owner.lower()}.github.io":
        return f"https://{owner}.github.io"

    return f"https://{owner}.github.io/{repo_name}"


def http_get_json(url: str) -> object:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "kg-noroshi/1.0",
    }

    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        data = response.read()

    return json.loads(data.decode("utf-8"))


def fetch_public_events(handle: str) -> list[dict]:
    url = f"https://api.github.com/users/{handle}/events/public?per_page=100"
    payload = http_get_json(url)
    if not isinstance(payload, list):
        raise ValueError("Unexpected GitHub API response")

    events: list[dict] = []
    for item in payload:
        if isinstance(item, dict):
            events.append(item)

    return events


def summarize_events(
    events: list[dict],
    window_from: dt.datetime,
    window_to: dt.datetime,
    *,
    include_repos: set[str],
    exclude_repos: set[str],
    include_event_types: set[str],
    exclude_event_types: set[str],
) -> tuple[dict, list[dict]]:
    counts_by_type: dict[str, int] = {}
    counts_by_repo: dict[str, int] = {}

    for event in events:
        created_at = event.get("created_at")
        if not isinstance(created_at, str):
            continue

        try:
            created = parse_github_timestamp(created_at)
        except ValueError:
            continue

        if not (window_from <= created <= window_to):
            continue

        event_type = event.get("type")
        if not isinstance(event_type, str):
            continue

        if exclude_event_types and event_type in exclude_event_types:
            continue

        if include_event_types:
            if event_type not in include_event_types:
                continue
        else:
            if event_type not in DEFAULT_PUBLIC_EVENT_TYPES:
                continue

        repo = event.get("repo")
        repo_name = None
        if isinstance(repo, dict):
            repo_name = repo.get("name")
        if not isinstance(repo_name, str) or not repo_name:
            continue

        if repo_name in exclude_repos:
            continue

        if include_repos and repo_name not in include_repos:
            continue

        counts_by_type[event_type] = counts_by_type.get(event_type, 0) + 1
        counts_by_repo[repo_name] = counts_by_repo.get(repo_name, 0) + 1

    top_repos = sorted(
        (
            {
                "repo": repo,
                "events": count,
                "url": f"https://github.com/{repo}",
            }
            for repo, count in counts_by_repo.items()
        ),
        key=lambda item: (-item["events"], item["repo"].lower()),
    )

    return counts_by_type, top_repos


def upsert_json_feed_item(feed: dict, item: dict, max_items: int = 30) -> dict:
    items = feed.get("items")
    if not isinstance(items, list):
        items = []

    item_id = item.get("id")
    if not isinstance(item_id, str) or not item_id:
        raise ValueError("Feed item must include a non-empty id")

    filtered: list[dict] = []
    for existing in items:
        if isinstance(existing, dict) and existing.get("id") == item_id:
            continue
        if isinstance(existing, dict):
            filtered.append(existing)

    filtered.insert(0, item)
    feed["items"] = filtered[:max_items]
    return feed


def build_rss_from_json_feed(feed: dict) -> str:
    channel_title = str(feed.get("title") or "NOROSHI")
    channel_link = str(feed.get("home_page_url") or "./")
    channel_description = str(feed.get("description") or "")

    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")

    ET.SubElement(channel, "title").text = channel_title
    ET.SubElement(channel, "link").text = channel_link
    ET.SubElement(channel, "description").text = channel_description

    items = feed.get("items")
    if not isinstance(items, list):
        items = []

    for entry in items:
        if not isinstance(entry, dict):
            continue

        item = ET.SubElement(channel, "item")
        guid = str(entry.get("id") or "")
        title = str(entry.get("title") or guid)
        link = str(entry.get("url") or "")
        description = str(entry.get("content_text") or "")
        published = str(entry.get("date_published") or "")

        ET.SubElement(item, "guid").text = guid
        ET.SubElement(item, "title").text = title
        if link:
            ET.SubElement(item, "link").text = link

        ET.SubElement(item, "description").text = description

        if published:
            try:
                when = dt.datetime.fromisoformat(published.replace("Z", "+00:00"))
                pub_date = when.astimezone(dt.timezone.utc).strftime(
                    "%a, %d %b %Y %H:%M:%S GMT"
                )
                ET.SubElement(item, "pubDate").text = pub_date
            except ValueError:
                pass

    return (
        ET.tostring(rss, encoding="utf-8", xml_declaration=True).decode("utf-8") + "\n"
    )


def write_text(path: pathlib.Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: pathlib.Path, payload: object) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    write_text(path, content)


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parent.parent

    config = load_config(repo_root)

    github_handle = str(config.get("github_handle") or HANDLE)

    window_hours = config.get("window_hours")
    if not isinstance(window_hours, int) or window_hours <= 0 or window_hours > 168:
        window_hours = 24

    max_top_repos = config.get("max_top_repos")
    if not isinstance(max_top_repos, int) or max_top_repos <= 0 or max_top_repos > 50:
        max_top_repos = 12

    include_repos = set(normalize_str_list(config.get("include_repos")))
    exclude_repos = set(normalize_str_list(config.get("exclude_repos")))
    include_event_types = set(normalize_str_list(config.get("include_event_types")))
    exclude_event_types = set(normalize_str_list(config.get("exclude_event_types")))

    now = utc_now()
    window_to = now
    window_from = now - dt.timedelta(hours=window_hours)
    run_date = now.date().isoformat()

    site_url = get_site_url()

    warnings: list[str] = []
    try:
        events = fetch_public_events(github_handle)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"GitHub events fetch failed: {type(exc).__name__}")
        events = []

    counts_by_type, top_repos = summarize_events(
        events,
        window_from,
        window_to,
        include_repos=include_repos,
        exclude_repos=exclude_repos,
        include_event_types=include_event_types,
        exclude_event_types=exclude_event_types,
    )

    total_events = sum(counts_by_type.values())
    repo_count = len(top_repos)
    summary = (
        f"Public GitHub activity (last 24h): {total_events} events across {repo_count} repos."
        if total_events
        else "No qualifying public GitHub activity in the last 24h. Pulse kept alive."
    )

    daily_json_rel = f"noroshi/daily/{run_date}.json"
    daily_md_rel = f"noroshi/daily/{run_date}.md"

    daily_json_url = f"{site_url}/{daily_json_rel}" if site_url else daily_json_rel
    daily_md_url = f"{site_url}/{daily_md_rel}" if site_url else daily_md_rel

    pulse = {
        "schema_version": "1.0",
        "kind": "noroshi.pulse",
        "generated_at": isoformat_z(now),
        "window": {
            "hours": window_hours,
            "from": isoformat_z(window_from),
            "to": isoformat_z(window_to),
        },
        "summary": summary,
        "counts": counts_by_type,
        "repos": top_repos[:max_top_repos],
        "evidence": [
            {"label": "GitHub profile", "url": GITHUB_PROFILE_URL},
            {"label": "Kaggle profile", "url": KAGGLE_PROFILE_URL},
        ],
        "privacy": {
            "public_only": True,
            "no_face_photo": True,
            "no_age": True,
            "no_education_history": True,
        },
        "provenance": {
            "generator": "scripts/noroshi.py",
            "github_sha": os.getenv("GITHUB_SHA") or "",
        },
        "warnings": warnings,
    }

    noroshi_dir = repo_root / "noroshi"
    daily_dir = noroshi_dir / "daily"

    write_json(daily_dir / f"{run_date}.json", pulse)
    write_json(noroshi_dir / "latest.json", pulse)

    daily_md = "\n".join(
        [
            f"# NOROSHI — {run_date}",
            "",
            summary,
            "",
            f"Window: {pulse['window']['from']} → {pulse['window']['to']}",
            "",
            "## Evidence",
            f"- {GITHUB_PROFILE_URL}",
            f"- {KAGGLE_PROFILE_URL}",
            "",
            "## Top repos (by public activity)",
        ]
        + [f"- {r['repo']} ({r['events']})" for r in top_repos[:max_top_repos]]
        + (["", "## Warnings"] + [f"- {w}" for w in warnings] if warnings else [])
        + [
            "",
            "Privacy-by-design: no face photo, age, or education history is published.",
            "",
        ]
    )

    write_text(daily_dir / f"{run_date}.md", daily_md)
    write_text(noroshi_dir / "latest.md", daily_md)

    # JSON Feed
    feed_path = repo_root / "feed.json"
    if feed_path.exists():
        try:
            feed = json.loads(feed_path.read_text(encoding="utf-8"))
            if not isinstance(feed, dict):
                raise ValueError("feed.json is not an object")
        except Exception:  # noqa: BLE001
            feed = {}
    else:
        feed = {}

    home_url = f"{site_url}/" if site_url else "./"
    feed_url = f"{site_url}/feed.json" if site_url else "feed.json"

    feed.setdefault("version", "https://jsonfeed.org/version/1.1")
    feed.setdefault("title", "KGNINJA — NOROSHI")
    feed["home_page_url"] = home_url
    feed["feed_url"] = feed_url
    feed.setdefault(
        "description",
        "Daily public-only pulse (NOROSHI) for KGNINJA. Evidence-first, hiring-oriented, and crawl-friendly.",
    )

    item = {
        "id": f"noroshi-{run_date}",
        "url": daily_md_url,
        "title": f"NOROSHI pulse — {run_date}",
        "content_text": summary,
        "date_published": isoformat_z(now),
    }

    feed = upsert_json_feed_item(feed, item)
    write_json(feed_path, feed)

    # RSS
    rss_path = repo_root / "rss.xml"
    rss = build_rss_from_json_feed(feed)
    write_text(rss_path, rss)

    # AIEO summary bump (updated_at)
    aieo_path = repo_root / ".well-known" / "aieo.json"
    if aieo_path.exists():
        try:
            aieo = json.loads(aieo_path.read_text(encoding="utf-8"))
            if not isinstance(aieo, dict):
                raise ValueError("aieo.json is not an object")
        except Exception:  # noqa: BLE001
            aieo = {}
    else:
        aieo = {}

    aieo.setdefault("schema_version", "1.0")
    aieo.setdefault("type", "aieo.profile")
    aieo["handle"] = "KGNINJA"
    aieo["updated_at"] = isoformat_z(now)
    aieo.setdefault("evidence", {})
    if isinstance(aieo["evidence"], dict):
        aieo["evidence"].setdefault("github", GITHUB_PROFILE_URL)
        aieo["evidence"].setdefault("kaggle", KAGGLE_PROFILE_URL)

    aieo.setdefault("noroshi", {})
    if isinstance(aieo["noroshi"], dict):
        aieo["noroshi"]["latest"] = "noroshi/latest.json"
        aieo["noroshi"]["json_feed"] = "feed.json"
        aieo["noroshi"]["rss"] = "rss.xml"
        aieo["noroshi"]["pulse"] = "daily"

    if site_url:
        aieo["site_url"] = site_url
        aieo["resolved_urls"] = {
            "daily_json": daily_json_url,
            "daily_md": daily_md_url,
            "latest_json": f"{site_url}/noroshi/latest.json",
            "facts": f"{site_url}/facts/",
            "clarifications": f"{site_url}/clarifications/",
        }

    write_json(aieo_path, aieo)

    # Sitemap + robots (only meaningful with absolute base URL)
    if site_url:
        sitemap_urls = [
            f"{site_url}/",
            f"{site_url}/facts/",
            f"{site_url}/evidence/",
            f"{site_url}/noroshi/",
            f"{site_url}/clarifications/",
            f"{site_url}/noroshi/latest.json",
            f"{site_url}/noroshi/latest.md",
            f"{site_url}/.well-known/aieo.json",
            f"{site_url}/feed.json",
            f"{site_url}/rss.xml",
            f"{site_url}/clarifications/clarifications.json",
            daily_json_url,
            daily_md_url,
        ]

        urlset = ET.Element(
            "urlset", {"xmlns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        )
        for loc in sitemap_urls:
            url = ET.SubElement(urlset, "url")
            ET.SubElement(url, "loc").text = loc
            ET.SubElement(url, "lastmod").text = isoformat_z(now)

        sitemap_xml = (
            ET.tostring(urlset, encoding="utf-8", xml_declaration=True).decode("utf-8")
            + "\n"
        )
        write_text(repo_root / "sitemap.xml", sitemap_xml)

        robots = "\n".join(
            [
                "User-agent: *",
                "Allow: /",
                "",
                f"Sitemap: {site_url}/sitemap.xml",
                "",
            ]
        )
        write_text(repo_root / "robots.txt", robots)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
