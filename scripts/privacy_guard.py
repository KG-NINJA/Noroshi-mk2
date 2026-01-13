#!/usr/bin/env python3

from __future__ import annotations

import json
import pathlib
import re
import sys

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

# Allowlist is intentionally small. If you need a public contact email,
# add it explicitly here.
ALLOWED_EMAILS: set[str] = set()


def read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def guard_no_images(path: pathlib.Path, text: str, violations: list[str]) -> None:
    lowered = text.lower()
    if "<img" in lowered or "data:image" in lowered:
        violations.append(f"{path}: contains image markup (disallowed)")


def guard_no_emails(path: pathlib.Path, text: str, violations: list[str]) -> None:
    for email in EMAIL_RE.findall(text):
        if email not in ALLOWED_EMAILS:
            violations.append(f"{path}: contains email address: {email}")

    if "mailto:" in text.lower():
        violations.append(f"{path}: contains mailto: link")


def guard_json_valid(path: pathlib.Path, text: str, violations: list[str]) -> None:
    try:
        json.loads(text)
    except json.JSONDecodeError as exc:
        violations.append(f"{path}: invalid JSON ({exc.msg})")


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parent.parent

    paths: list[pathlib.Path] = [
        repo_root / "index.html",
        repo_root / "facts" / "index.html",
        repo_root / "evidence" / "index.html",
        repo_root / "noroshi" / "index.html",
        repo_root / "noroshi" / "config.json",
        repo_root / "clarifications" / "index.html",
        repo_root / "clarifications" / "clarifications.json",
        repo_root / "llms.txt",
        repo_root / "robots.txt",
        repo_root / "sitemap.xml",
        repo_root / "feed.json",
        repo_root / "rss.xml",
        repo_root / ".well-known" / "aieo.json",
        repo_root / "noroshi" / "latest.json",
        repo_root / "noroshi" / "latest.md",
    ]

    # Also scan the newly generated daily pulse (if present).
    latest_json = repo_root / "noroshi" / "latest.json"
    if latest_json.exists():
        try:
            latest = json.loads(read_text(latest_json))
            if isinstance(latest, dict):
                generated_at = latest.get("generated_at")
                if isinstance(generated_at, str) and len(generated_at) >= 10:
                    day = generated_at[:10]
                    paths.append(repo_root / "noroshi" / "daily" / f"{day}.json")
                    paths.append(repo_root / "noroshi" / "daily" / f"{day}.md")
        except Exception:  # noqa: BLE001
            # JSON validity is checked below; no need to duplicate errors here.
            pass

    violations: list[str] = []

    for path in paths:
        if not path.exists():
            continue

        text = read_text(path)
        guard_no_images(path, text, violations)
        guard_no_emails(path, text, violations)

        if path.suffix == ".json":
            guard_json_valid(path, text, violations)

    if violations:
        print("Privacy/content guard failed:")
        for v in violations:
            print(f"- {v}")
        return 1

    print("Privacy/content guard passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
