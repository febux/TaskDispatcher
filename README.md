# taskmanager

Reliable cron-to-notification dispatcher. See the full documentation in the
[`docs/`](./docs) directory:

- **[docs/PLAN.md](docs/PLAN.md)** — current roadmap, shipped phases, and what remains.
- **[docs/DESIGN.md](docs/DESIGN.md)** — architecture, trade-offs, and design rationale.
- **[docs/README.md](docs/README.md)** — getting started, API surface, verification commands.

Quick links:
- `docker compose up -d --build` — run the full stack.
- `cargo test` — run tests (integration suite auto-skips without `DATABASE_URL`/`REDIS_URL`).
- `cargo clippy --all-targets -- -D warnings` — lint.
