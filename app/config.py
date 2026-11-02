"""Application settings. Every tunable value comes from the environment (or .env)."""

from functools import lru_cache

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- General ---
    app_name: str = "hospital-bulk-processor"
    log_level: str = "INFO"
    log_json: bool = True

    # --- Redis (queue + progress store) ---
    redis_url: str = "redis://localhost:6379/0"
    job_ttl_seconds: int = Field(default=86_400, gt=0, description="TTL for all bulk:{batch_id}:* keys")
    idempotency_ttl_seconds: int = Field(default=86_400, gt=0, description="How long a file hash maps to its batch")

    # --- Upstream Hospital Directory API ---
    hospital_api_base_url: HttpUrl = HttpUrl("https://hospital-directory.onrender.com")
    # Render free tier cold-starts in 30s+, so the per-request timeout must be generous.
    hospital_api_timeout_seconds: float = Field(default=60.0, gt=0)
    hospital_api_max_retries: int = Field(default=3, ge=0)
    hospital_api_backoff_base_seconds: float = Field(default=0.5, gt=0)
    hospital_api_backoff_max_seconds: float = Field(default=8.0, gt=0)

    # --- CSV upload limits ---
    max_csv_rows: int = Field(default=20, gt=0)
    max_upload_bytes: int = Field(default=1_048_576, gt=0)

    # --- Concurrency control ---
    per_job_concurrency: int = Field(default=5, gt=0, description="Parallel upstream calls within one job")
    upstream_max_in_flight: int = Field(
        default=10, gt=0, description="Global cap on in-flight upstream calls across all jobs/workers"
    )
    upstream_slot_ttl_seconds: int = Field(
        default=120, gt=0, description="Lease TTL on a global slot so a crashed worker can't leak it"
    )

    # --- Worker ---
    worker_max_jobs: int = Field(default=10, gt=0)
    worker_job_timeout_seconds: int = Field(
        default=1800, gt=0, description="Worst case: rows/concurrency * attempts * timeout"
    )
    worker_max_tries: int = Field(default=3, gt=0)
    worker_drain_seconds: float = Field(
        default=75.0, gt=0, description="On shutdown, how long in-flight upstream requests may finish"
    )

    # --- Synchronous upload (POST /hospitals/bulk?wait=true) ---
    sync_wait_timeout_seconds: float = Field(
        default=120.0, gt=0, description="Max time the upload request waits for the job before falling back to 202"
    )
    sync_poll_interval_seconds: float = Field(default=0.5, gt=0)

    # --- Server-Sent Events ---
    sse_poll_interval_seconds: float = Field(default=1.0, gt=0)
    sse_heartbeat_seconds: float = Field(default=15.0, gt=0, description="Comment line to keep proxies from timing out")

    # --- Business policy ---
    # False: a batch with any failed row stays inactive (status=partial_failure).
    # True: activate whatever was created successfully.
    activate_on_partial_failure: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
