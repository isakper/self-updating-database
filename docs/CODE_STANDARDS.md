# Code Standards

Last reviewed: 2026-02-23


## Principles
- Optimize for legibility: clear names, small functions, explicit control flow.
- Keep boundaries crisp: avoid cross-layer imports and “spooky action at a distance”.
- Prefer boring, dependency-light solutions.

## Change hygiene
- Keep diffs scoped to the problem.
- Avoid drive-by refactors unless they unblock the change.
- Do not reformat unrelated files.

## Error handling
- Make failure modes explicit and actionable.
- Include enough context for debugging (without leaking secrets).

## Security and reliability
- Follow `docs/SECURITY.md` and `docs/RELIABILITY.md` for baseline expectations.
