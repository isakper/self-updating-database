from pathlib import Path

from scripts import doc_lint


def test_normalize_link_target_strips_title() -> None:
    """Strips optional markdown link titles from targets."""
    target = 'docs/PLANS.md "Plans"'
    assert doc_lint.normalize_link_target(target) == "docs/PLANS.md"


def test_external_link_detection() -> None:
    """Treats external URLs and hash anchors as external links."""
    assert doc_lint.is_external_link("https://example.com")
    assert doc_lint.is_external_link("mailto:test@example.com")
    assert doc_lint.is_external_link("#section")
    assert not doc_lint.is_external_link("docs/PLANS.md")


def test_policy_parsing_has_required_docs() -> None:
    """Ensures policy parsing finds required docs and defaults."""
    policy = doc_lint.read_policy(doc_lint.POLICY_FILE)
    required = {Path("AGENTS.md"), Path("ARCHITECTURE.md")}
    assert required.issubset(policy.required_docs)
    assert policy.freshness_docs
    assert policy.stale_days == 180
