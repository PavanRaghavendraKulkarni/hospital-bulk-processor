import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest
import respx

from app.clients.hospital_api import (
    HospitalApiClient,
    UpstreamClientError,
    UpstreamUnavailableError,
)

BASE_URL = "https://upstream.test"
BATCH_ID = "3f2b8a1e-6c1d-4f7a-9b2e-0d5c8e7f1a23"
MAX_RETRIES = 3

HOSPITAL_JSON = {
    "id": 42,
    "name": "City Hospital",
    "address": "1 Main St",
    "phone": "555-0100",
    "creation_batch_id": BATCH_ID,
    "active": False,
    "created_at": "2026-10-30T12:00:00",
}


class RecordingSleep:
    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


@pytest.fixture
def sleep() -> RecordingSleep:
    return RecordingSleep()


@pytest.fixture
async def client(sleep: RecordingSleep) -> AsyncIterator[HospitalApiClient]:
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        yield HospitalApiClient(
            http,
            max_retries=MAX_RETRIES,
            backoff_base_seconds=0.5,
            backoff_max_seconds=8.0,
            sleep=sleep,
        )


async def _create(client: HospitalApiClient):
    return await client.create_hospital(
        name="City Hospital", address="1 Main St", phone="555-0100", batch_id=BATCH_ID
    )


# --- create_hospital: happy path ---------------------------------------------


@respx.mock
async def test_create_hospital_sends_payload_and_returns_hospital(client, sleep) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(return_value=httpx.Response(200, json=HOSPITAL_JSON))

    hospital = await _create(client)

    assert hospital.id == 42
    assert route.call_count == 1
    assert json.loads(route.calls.last.request.content) == {
        "name": "City Hospital",
        "address": "1 Main St",
        "phone": "555-0100",
        "creation_batch_id": BATCH_ID,
    }
    assert sleep.calls == []


@respx.mock
async def test_create_hospital_sends_null_phone_when_missing(client) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(
        return_value=httpx.Response(200, json={**HOSPITAL_JSON, "phone": None})
    )

    await client.create_hospital(name="City Hospital", address="1 Main St", phone=None, batch_id=BATCH_ID)

    sent = json.loads(route.calls.last.request.content)
    assert sent["phone"] is None


# --- retryable failures -------------------------------------------------------


@respx.mock
@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_retries_5xx_then_succeeds(client, sleep, status: int) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(
        side_effect=[httpx.Response(status), httpx.Response(200, json=HOSPITAL_JSON)]
    )

    hospital = await _create(client)

    assert hospital.id == 42
    assert route.call_count == 2
    assert len(sleep.calls) == 1


@respx.mock
async def test_retries_429_and_honours_retry_after(client, sleep) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "3"}),
            httpx.Response(200, json=HOSPITAL_JSON),
        ]
    )

    await _create(client)

    assert route.call_count == 2
    assert sleep.calls == [3.0]


@respx.mock
@pytest.mark.parametrize(
    "exc",
    [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), httpx.RemoteProtocolError("reset")],
)
async def test_retries_network_errors_then_succeeds(client, sleep, exc: Exception) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(
        side_effect=[exc, httpx.Response(200, json=HOSPITAL_JSON)]
    )

    hospital = await _create(client)

    assert hospital.id == 42
    assert route.call_count == 2
    assert len(sleep.calls) == 1


@respx.mock
async def test_gives_up_after_max_retries(client, sleep) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(return_value=httpx.Response(503))

    with pytest.raises(UpstreamUnavailableError):
        await _create(client)

    assert route.call_count == MAX_RETRIES + 1
    assert len(sleep.calls) == MAX_RETRIES


@respx.mock
async def test_backoff_is_exponential_with_jitter_and_capped(sleep) -> None:
    respx.post(f"{BASE_URL}/hospitals/").mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        client = HospitalApiClient(
            http, max_retries=6, backoff_base_seconds=1.0, backoff_max_seconds=8.0, sleep=sleep
        )
        with pytest.raises(UpstreamUnavailableError):
            await _create(client)

    # Nominal delays: 1, 2, 4, 8, 8, 8 (capped). Jitter keeps each in [nominal/2, nominal].
    nominal = [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]
    assert len(sleep.calls) == len(nominal)
    for actual, expected in zip(sleep.calls, nominal, strict=True):
        assert expected / 2 <= actual <= expected


# --- non-retryable failures ---------------------------------------------------


@respx.mock
@pytest.mark.parametrize("status", [400, 404, 409, 422])
async def test_does_not_retry_4xx(client, sleep, status: int) -> None:
    route = respx.post(f"{BASE_URL}/hospitals/").mock(
        return_value=httpx.Response(status, json={"detail": "bad input"})
    )

    with pytest.raises(UpstreamClientError) as exc_info:
        await _create(client)

    assert exc_info.value.status_code == status
    assert "bad input" in exc_info.value.detail
    assert route.call_count == 1
    assert sleep.calls == []


# --- activate_batch -----------------------------------------------------------


@respx.mock
async def test_activate_batch_patches_batch_endpoint(client) -> None:
    route = respx.patch(f"{BASE_URL}/hospitals/batch/{BATCH_ID}/activate").mock(
        return_value=httpx.Response(200, json={"message": "activated"})
    )

    await client.activate_batch(BATCH_ID)

    assert route.call_count == 1


@respx.mock
async def test_activate_batch_retries_5xx(client, sleep) -> None:
    route = respx.patch(f"{BASE_URL}/hospitals/batch/{BATCH_ID}/activate").mock(
        side_effect=[httpx.Response(502), httpx.Response(200, json={})]
    )

    await client.activate_batch(BATCH_ID)

    assert route.call_count == 2
    assert len(sleep.calls) == 1


@respx.mock
async def test_activate_batch_does_not_retry_4xx(client, sleep) -> None:
    route = respx.patch(f"{BASE_URL}/hospitals/batch/{BATCH_ID}/activate").mock(
        return_value=httpx.Response(404, json={"detail": "batch not found"})
    )

    with pytest.raises(UpstreamClientError):
        await client.activate_batch(BATCH_ID)

    assert route.call_count == 1
    assert sleep.calls == []


# --- global rate limiter integration ------------------------------------------


@respx.mock
async def test_each_attempt_holds_a_limiter_slot_but_backoff_does_not() -> None:
    events: list[str] = []

    class RecordingLimiter:
        @asynccontextmanager
        async def slot(self):
            events.append("acquire")
            yield
            events.append("release")

    async def recording_sleep(seconds: float) -> None:
        events.append("sleep")

    respx.post(f"{BASE_URL}/hospitals/").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json=HOSPITAL_JSON)]
    )
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        client = HospitalApiClient(http, max_retries=1, sleep=recording_sleep, limiter=RecordingLimiter())
        await _create(client)

    assert events == ["acquire", "release", "sleep", "acquire", "release"]


# --- list_batch -----------------------------------------------------------------


@respx.mock
async def test_list_batch_returns_hospitals(client) -> None:
    respx.get(f"{BASE_URL}/hospitals/batch/{BATCH_ID}").mock(return_value=httpx.Response(200, json=[HOSPITAL_JSON]))

    hospitals = await client.list_batch(BATCH_ID)

    assert [h.id for h in hospitals] == [42]
