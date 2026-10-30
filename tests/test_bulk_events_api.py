import asyncio
import json
from itertools import pairwise

from app.models import JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore

CSV = b"name,address,phone\nCity Hospital,1 Main St,\nGeneral,2 Oak Ave,\n"


def _parse_sse(text: str) -> list[tuple[str, dict]]:
    events = []
    for block in text.strip().split("\n\n"):
        lines = dict(line.split(": ", 1) for line in block.splitlines() if not line.startswith(":"))
        if lines:
            events.append((lines["event"], json.loads(lines["data"])))
    return events


async def _upload(api) -> str:
    return (await api.post("/hospitals/bulk", files={"file": ("h.csv", CSV, "text/csv")})).json()["batch_id"]


async def test_events_for_unknown_batch_returns_404(api) -> None:
    resp = await api.get("/hospitals/bulk/nope/events")

    assert resp.status_code == 404


async def test_finished_job_emits_single_complete_event(api, store: ProgressStore) -> None:
    batch_id = await _upload(api)
    await store.update_meta(batch_id, status=JobStatus.COMPLETED, batch_activated=True)

    resp = await asyncio.wait_for(api.get(f"/hospitals/bulk/{batch_id}/events"), timeout=2)

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/event-stream")
    events = _parse_sse(resp.text)
    assert [name for name, _ in events] == ["complete"]
    assert events[0][1]["status"] == "completed"
    assert events[0][1]["batch_activated"] is True


async def test_streams_progress_until_complete(api, store: ProgressStore) -> None:
    batch_id = await _upload(api)

    async def simulate_worker() -> None:
        await asyncio.sleep(0.05)
        await store.update_meta(batch_id, status=JobStatus.PROCESSING)
        await asyncio.sleep(0.05)
        await store.record_result(
            batch_id, RowResult(row=1, name="City Hospital", status=RowStatus.CREATED, hospital_id=1)
        )
        await asyncio.sleep(0.05)
        await store.record_result(batch_id, RowResult(row=2, name="General", status=RowStatus.CREATED, hospital_id=2))
        await store.update_meta(batch_id, status=JobStatus.COMPLETED, batch_activated=True)

    worker = asyncio.create_task(simulate_worker())
    resp = await asyncio.wait_for(api.get(f"/hospitals/bulk/{batch_id}/events"), timeout=3)
    await worker

    events = _parse_sse(resp.text)
    names = [name for name, _ in events]
    assert names[0] == "progress" and names[-1] == "complete"
    assert names.count("complete") == 1
    percents = [data["percent_complete"] for _, data in events]
    assert percents == sorted(percents), "progress never goes backwards"
    assert 50.0 in percents
    assert events[-1][1]["percent_complete"] == 100.0
    # Only changes are emitted: no two consecutive identical payloads.
    payloads = [json.dumps(data, sort_keys=True) for _, data in events]
    assert all(a != b for a, b in pairwise(payloads))
