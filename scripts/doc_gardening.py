#!/usr/bin/env python3
"""Generate a doc-gardening report for freshness and broken links."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import cast

from doc_lint import POLICY_FILE, ROOT, LintError, check_docs_links, read_policy

REPORT_PATH = ROOT / "docs" / "reports" / "doc-gardening-report.md"
StaleEntry = tuple[Path, str, int]


def parse_last_reviewed(text: str) -> str | None:
    """Extract the last reviewed date from a doc body.

    Args:
        text: Document contents.

    Returns:
        The date string or None if missing.
    """
    for line in text.splitlines():
        if line.startswith("Last reviewed:"):
            return line.split(":", 1)[1].strip()
    return None


def collect_freshness_findings() -> tuple[date, list[StaleEntry], list[Path]]:
    """Return freshness findings for tracked docs.

    Returns:
        Tuple of (today, stale entries, missing marker docs).
    """
    policy = read_policy(POLICY_FILE)
    today = date.today()
    stale: list[StaleEntry] = []
    missing: list[Path] = []
    for doc in sorted(policy.freshness_docs):
        path = ROOT / doc
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        last_reviewed = parse_last_reviewed(text)
        if not last_reviewed:
            missing.append(doc)
            continue
        try:
            reviewed_date = datetime.strptime(last_reviewed, "%Y-%m-%d").date()
        except ValueError:
            missing.append(doc)
            continue
        age_days = (today - reviewed_date).days
        if age_days > policy.stale_days:
            stale.append((doc, last_reviewed, age_days))
    return today, stale, missing


def collect_broken_links() -> list[LintError]:
    """Return any broken link findings from docs.

    Returns:
        List of broken link lint errors.
    """
    return cast(list[LintError], check_docs_links())


def build_report(
    today: date,
    stale: list[StaleEntry],
    missing: list[Path],
    broken_links: list[LintError],
) -> str:
    """Render the markdown report for doc-gardening findings.

    Args:
        today: Date of the report.
        stale: Docs with stale freshness markers.
        missing: Docs missing freshness markers.
        broken_links: Broken link lint errors.

    Returns:
        Markdown report text.
    """
    lines = ["# Doc Gardening Report", "", f"Last updated: {today}", ""]

    lines.append("## Stale docs")
    if stale:
        for doc, reviewed, age in stale:
            lines.append(f"- `{doc}` (Last reviewed: {reviewed}, {age} days old)")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Missing freshness markers")
    if missing:
        for doc in missing:
            lines.append(f"- `{doc}`")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Broken links")
    if broken_links:
        for error in broken_links:
            rel = error.file.relative_to(ROOT)
            lines.append(f"- `{rel}`: {error.message}")
    else:
        lines.append("- None")

    lines.append("")
    return "\n".join(lines)


def main() -> int:
    """Write the doc-gardening report to disk.

    Returns:
        Exit code.
    """
    today, stale, missing = collect_freshness_findings()
    broken_links = collect_broken_links()
    report = build_report(today, stale, missing, broken_links)
    REPORT_PATH.write_text(report, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
