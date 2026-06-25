# Repository Guidelines

## Project Structure & Module Organization

The application is a small Flask/Socket.IO service with Python modules at the repository root. `app.py` creates the web app and event batcher; `mqtt_client.py` handles broker connectivity; `dashboard_state.py`, `system_metrics.py`, and `message_store.py` own aggregation, telemetry normalization, and SQLite persistence. Runtime settings live in `config.py`.

Browser code is split between `templates/index.html`, `static/js/dashboard.js`, and `static/css/dashboard.css`. Tests are in `tests/` and mirror the Python module names (`tests/test_message_store.py`, for example). Design notes belong under `docs/`. `mqtt_tx64.py` is a Digi-device companion script and depends on device-only modules unavailable in a normal development environment.

## Build, Test, and Development Commands

Create an isolated environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the dashboard with `python app.py`, then open `http://localhost:5000`. Set configuration through environment variables; copy values from `.env.example` as a starting point. Run all tests with:

```bash
python -m unittest discover -s tests
```

Build the container with `docker build -t digi-mqtt-dashboard .`.

## Coding Style & Naming Conventions

Use four-space indentation in Python, type hints, module docstrings, and `snake_case` for functions and variables; use `PascalCase` for classes. Keep modules focused and preserve the existing separation between ingestion, state, persistence, and presentation. JavaScript uses two-space indentation, `camelCase`, `const`/`let`, and semicolons. CSS classes use lowercase kebab-case.

No formatter or linter is configured. Match surrounding style and keep lines readable.

## Testing Guidelines

Tests use `unittest`; name files `test_<module>.py`, classes `*Tests`, and methods `test_<behavior>`. Add focused unit tests for state and persistence changes, plus integration coverage in `test_app.py` for routes or Socket.IO events. Tests must not require a live MQTT broker; construct `Config(MQTT_ENABLED=False)` when needed.

## Commit & Pull Request Guidelines

History is currently too small to establish a strict convention. Use concise, imperative commit subjects such as `Add retention test for message store`. Keep each commit scoped to one logical change.

Pull requests should explain behavior changes, list verification commands, and call out configuration or schema effects. Link related issues and include screenshots for visible dashboard changes. Never commit `.env`, credentials, or generated SQLite files.
