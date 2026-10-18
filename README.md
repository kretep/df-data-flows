

This package contains the ETL pipelines for a number of different use cases for personal use, ranging from tracking electricity usage to birthday notifications to weather data retrieval for a weather display.

## Toolchain

- All scripts are in Python.
- The workflows use Prefect annotation and can be deployed and scheduled on a Prefect server. This allows for automatic retries, caching of data and has a nice dashboard.
- Database is postgres with the sqlalchemy library in Python.
- For notifications, ntfy is used running on a private server.
- Some of the workflows use OpenCV for image processing, but you can opt out of those.


## Related repositories

- df-timeseries-queries - (pg)sql queries for timeseries data.



## Run Prefect server and worker

Prefect uses SQLite by default, which is fine for a single-machine setup. The warning about `PREFECT_API_DATABASE_CONNECTION_URL` can be ignored — it just means PostgreSQL LISTEN/NOTIFY is unavailable, but Prefect works fine without it.

Set the API URL once (persisted to `~/.prefect/profiles.toml`):

```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Then start the server and worker. Make sure the arguments for the server (host and port) match the PREFECT_API_URL. Pass `--env-file .env` to the worker so it picks up environment variables from `.env`:

```bash
uv run -- prefect server start --host 0.0.0.0 --port 4200
uv run --env-file .env -- prefect worker start --pool "default"
```

Flow scripts pick up `.env` automatically via `load_dotenv()` at runtime.
