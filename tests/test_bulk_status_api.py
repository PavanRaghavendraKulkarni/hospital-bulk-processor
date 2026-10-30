from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.clients.hospital_api import HospitalApiClient
from app.models import HospitalRow, JobMeta, JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore
from app.services.bulk_processor import BulkJobProcessor
from tests.upstream import BASE_URL, FakeUpstream

BATCH_ID = "3f2b8a1e-6c1d-4f7a-9b2e-0d5c8e7f1a23"


async def _seed(store: ProgressStore, total: int = 4) -> None:
    rows = [HospitalRow(row_number=i, name=f"H{i}", address=f"{i} Main St") for i in range(1, total + 1)]
    meta = JobMeta(
        batch_id=BATCH_ID,
        filename="h.csv",
        file_hash="x",
        total_rows=total,
        created_at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    await store.create_job(meta, rows)


async def test_unknown_batch_returns_404(api) -> None:
    resp = await api.get("/hospitals/bulk/does-not-exist/status")

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


async def test_queued_job_reports_zero_progress(api, store: ProgressStore) -> None:
    await _seed(store)

    body = (await api.get(f"/hospitals/bulk/{BATCH_ID}/status")).json()

    assert body["status"] == "queued"
    assert (body["total_rows"], body["processed"], body["succeeded"], body["failed"]) == (4, 0, 0, 0)
    assert body["percent_complete"] == 0.0
    assert body["batch_activated"] is False
    assert body["results"] == []
    assert body["started_at"] is None


async def test_in_progress_job_reports_percent_and_sorted_results(api, store: ProgressStore) -> None:
    await _seed(store, total=3)
    await store.update_meta(BATCH_ID, status=JobStatus.PROCESSING, started_at=datetime(2026, 10, 30, 0, 1, tzinfo=UTC))
    await store.record_result(BATCH_ID, RowResult(row=2, name="H2", status=RowStatus.FAILED, error="bad"))
    await store.record_result(BATCH_ID, RowResult(row=1, name="H1", status=RowStatus.CREATED, hospital_id=7))

    body = (await api.get(f"/hospitals/bulk/{BATCH_ID}/status")).json()

    assert body["status"] == "processing"
    assert (body["processed"], body["succeeded"], body["failed"]) == (2, 1, 1)
    assert body["percent_complete"] == 66.7
    assert body["results"] == [
        {"row": 1, "name": "H1", "status": "created", "hospital_id": 7, "error": None},
        {"row": 2, "name": "H2", "status": "failed", "hospital_id": None, "error": "bad"},
    ]


@pytest.fixture
def upstream() -> AsyncIterator[FakeUpstream]:
    with respx.mock(assert_all_called=False) as router:
        yield FakeUpstream(router)


async def test_upload_process_status_end_to_end(api, store: ProgressStore, settings, upstream: FakeUpstream) -> None:
    csv = b"name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\n"
    batch_id = (await api.post("/hospitals/bulk", files={"file": ("h.csv", csv, "text/csv")})).json()["batch_id"]

    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        await BulkJobProcessor(store, HospitalApiClient(http), settings).run(batch_id)

    body = (await api.get(f"/hospitals/bulk/{batch_id}/status")).json()
    assert body["status"] == "completed"
    assert body["percent_complete"] == 100.0
    assert body["batch_activated"] is True
    assert body["finished_at"] is not None
    assert body["processing_time_seconds"] >= 0
    assert [r["status"] for r in body["results"]] == ["created", "created"]
