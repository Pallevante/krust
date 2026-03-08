# Contributing to krust

Thanks for contributing.

For the full engineering standards and maintainer workflow, read:
- [Contributor Guide](docs/contributor-guide.md)

## Quick Start for Contributors

1. Fork and clone the repo.
2. Create a focused branch.
3. Make changes with tests/docs updates.
4. Run required validation.
5. Open a PR with clear behavior and performance notes.

## Required Validation

```bash
cargo fmt --all
cargo check --locked
cargo test --locked
cargo build --release --locked
```

## PR Expectations

- preserve keybinding and command behavior unless explicitly changing compatibility
- explain performance impact for hot-path changes
- include tests for changed behavior
- update docs for user-visible behavior changes

## Security

Do not commit kubeconfigs, tokens, certificates, or sensitive cluster data.
