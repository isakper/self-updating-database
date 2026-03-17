#!/usr/bin/env python3
"""Doc integrity linter for docs/ structure and links."""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
POLICY_FILE = DOCS_DIR / "references" / "doc-integrity.md"

LINK_PATTERN = re.compile(r"!??\[[^\]]*\]\(([^)]+)\)")
DATE_PATTERN = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


@dataclass(frozen=True)
class LintError:
    """Represents a doc-lint error with file context."""

    file: Path
    message: str


@dataclass(frozen=True)
class Policy:
    """Parsed policy settings for doc integrity checks."""

    required_docs: set[Path]
    freshness_docs: set[Path]
    stale_days: int


def read_policy(policy_path: Path) -> Policy:
    """Parse the doc integrity policy file into a structured Policy.

    Args:
        policy_path: Path to the policy markdown file.

    Returns:
        Parsed Policy settings.
    """
    text = policy_path.read_text(encoding="utf-8")
    required_docs = _parse_required_docs(text)
    freshness_docs = _parse_freshness_docs(text)
    stale_days = _parse_stale_days(text)
    return Policy(
        required_docs=required_docs,
        freshness_docs=freshness_docs,
        stale_days=stale_days,
    )


def _parse_required_docs(text: str) -> set[Path]:
    """Extract required doc paths from the policy section.

    Args:
        text: Policy file contents.

    Returns:
        Set of required doc paths.
    """
    required_docs: set[Path] = set()
    capture = False
    for line in text.splitlines():
        if line.strip().lower() == "## required docs (v1)":
            capture = True
            continue
        if capture and line.startswith("## "):
            break
        if capture and line.strip().startswith("-"):
            value = line.split("`", 2)
            if len(value) >= 3:
                required_docs.add(Path(value[1]))
    return required_docs


def _parse_freshness_docs(text: str) -> set[Path]:
    """Extract freshness-tracked doc paths from the policy section.

    Args:
        text: Policy file contents.

    Returns:
        Set of freshness-tracked doc paths.
    """
    freshness_docs: set[Path] = set()
    capture = False
    for line in text.splitlines():
        if line.strip().lower() == "tracked docs:":
            capture = True
            continue
        if capture and line.strip().startswith("stale after:"):
            break
        if capture and line.strip().startswith("-"):
            value = line.split("`", 2)
            if len(value) >= 3:
                freshness_docs.add(Path(value[1]))
    return freshness_docs


def _parse_stale_days(text: str) -> int:
    """Read the staleness threshold (days) from the policy.

    Args:
        text: Policy file contents.

    Returns:
        Staleness threshold in days.
    """
    for line in text.splitlines():
        if line.strip().lower().startswith("stale after:"):
            value = line.split(":", 1)[1].strip().split()[0]
            return int(value)
    return 180


def iter_markdown_files(root: Path) -> list[Path]:
    """Return all markdown files under a root directory.

    Args:
        root: Root directory to scan.

    Returns:
        Sorted list of markdown file paths.
    """
    return sorted(path for path in root.rglob("*.md") if path.is_file())


def normalize_link_target(target: str) -> str:
    """Normalize a markdown link target by stripping wrappers and titles.

    Args:
        target: Raw markdown link target.

    Returns:
        Normalized link target.
    """
    target = target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    if " " in target:
        target = target.split()[0]
    return target


def is_external_link(target: str) -> bool:
    """Return True when the link target should be treated as external.

    Args:
        target: Markdown link target.

    Returns:
        True if the target is external.
    """
    return (
        target.startswith("http://")
        or target.startswith("https://")
        or target.startswith("mailto:")
        or target.startswith("tel:")
        or target.startswith("#")
    )


def resolve_link(base_file: Path, target: str) -> Path:
    """Resolve a markdown link target to an absolute repo path.

    Args:
        base_file: File containing the link.
        target: Markdown link target.

    Returns:
        Resolved absolute path.
    """
    if target.startswith("/"):
        return ROOT / target.lstrip("/")
    return (base_file.parent / target).resolve()


def iter_links(text: str) -> list[str]:
    """Extract all markdown link targets from a document.

    Args:
        text: Document contents.

    Returns:
        List of normalized link targets.
    """
    return [normalize_link_target(match) for match in LINK_PATTERN.findall(text)]


def check_required_docs(policy: Policy) -> list[LintError]:
    """Validate required docs exist on disk.

    Args:
        policy: Parsed policy settings.

    Returns:
        List of missing required doc errors.
    """
    errors: list[LintError] = []
    for doc in sorted(policy.required_docs):
        if not (ROOT / doc).exists():
            errors.append(LintError(ROOT, f"Required doc missing: {doc}"))
    return errors


def check_agents_links() -> list[LintError]:
    """Validate AGENTS.md internal links resolve.

    Returns:
        List of dangling link errors.
    """
    errors: list[LintError] = []
    agents_path = ROOT / "AGENTS.md"
    text = agents_path.read_text(encoding="utf-8")
    for target in iter_links(text):
        if is_external_link(target):
            continue
        path_part = target.split("#", 1)[0]
        if not path_part:
            continue
        resolved = resolve_link(agents_path, path_part)
        if not resolved.exists():
            errors.append(LintError(agents_path, f"Dangling link: {target}"))
    return errors


def check_index(index_path: Path, directory: Path) -> list[LintError]:
    """Ensure index files include all markdown files in a directory.

    Args:
        index_path: Index file path.
        directory: Directory containing markdown docs.

    Returns:
        List of index missing entry errors.
    """
    errors: list[LintError] = []
    index_text = index_path.read_text(encoding="utf-8")
    linked = set()
    for target in iter_links(index_text):
        if is_external_link(target):
            continue
        path_part = target.split("#", 1)[0]
        if not path_part:
            continue
        resolved = resolve_link(index_path, path_part)
        if directory in resolved.parents and resolved.suffix == ".md":
            linked.add(resolved.name)
    expected = {
        path.name for path in directory.glob("*.md") if path.name != index_path.name
    }
    missing = sorted(expected - linked)
    for name in missing:
        errors.append(LintError(index_path, f"Index missing entry for {name}"))
    return errors


def check_docs_links() -> list[LintError]:
    """Validate docs/ markdown links resolve to files in the repo.

    Returns:
        List of broken link errors.
    """
    errors: list[LintError] = []
    for path in iter_markdown_files(DOCS_DIR):
        text = path.read_text(encoding="utf-8")
        for target in iter_links(text):
            if is_external_link(target):
                continue
            path_part = target.split("#", 1)[0]
            if not path_part:
                continue
            resolved = resolve_link(path, path_part)
            if not resolved.exists():
                errors.append(LintError(path, f"Broken link: {target}"))
    return errors


def check_generated_docs() -> list[LintError]:
    """Validate generated docs include required metadata markers.

    Returns:
        List of generated doc metadata errors.
    """
    errors: list[LintError] = []
    for path in iter_markdown_files(DOCS_DIR / "generated"):
        text = path.read_text(encoding="utf-8")
        if "Generated by:" not in text:
            errors.append(LintError(path, "Missing 'Generated by:' marker"))
        if not re.search(r"Last generated:\s*\d{4}-\d{2}-\d{2}", text):
            errors.append(
                LintError(path, "Missing 'Last generated: YYYY-MM-DD' marker")
            )
    return errors


def check_freshness_markers(policy: Policy) -> list[LintError]:
    """Validate tracked docs include a parsable Last reviewed marker.

    Args:
        policy: Parsed policy settings.

    Returns:
        List of freshness marker errors.
    """
    errors: list[LintError] = []
    for doc in sorted(policy.freshness_docs):
        path = ROOT / doc
        if not path.exists():
            errors.append(LintError(path, f"Freshness doc missing: {doc}"))
            continue
        text = path.read_text(encoding="utf-8")
        match = re.search(r"Last reviewed:\s*(\d{4}-\d{2}-\d{2})", text)
        if not match:
            errors.append(LintError(path, "Missing 'Last reviewed: YYYY-MM-DD' marker"))
            continue
        try:
            datetime.strptime(match.group(1), "%Y-%m-%d")
        except ValueError:
            errors.append(LintError(path, "Invalid 'Last reviewed' date"))
    return errors


def main() -> int:
    """Run all lint checks and print a summary.

    Returns:
        Exit code.
    """
    policy = read_policy(POLICY_FILE)
    errors: list[LintError] = []
    errors.extend(check_required_docs(policy))
    errors.extend(check_agents_links())
    errors.extend(
        check_index(DOCS_DIR / "design-docs" / "index.md", DOCS_DIR / "design-docs")
    )
    errors.extend(
        check_index(DOCS_DIR / "product-specs" / "index.md", DOCS_DIR / "product-specs")
    )
    errors.extend(check_docs_links())
    errors.extend(check_generated_docs())
    errors.extend(check_freshness_markers(policy))

    if errors:
        for error in errors:
            rel = error.file.relative_to(ROOT)
            print(f"{rel}: {error.message}")
        return 1
    print("doc-lint: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
