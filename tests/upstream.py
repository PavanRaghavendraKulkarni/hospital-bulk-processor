"""A scripted fake of the upstream Hospital Directory API, mounted with respx."""

import asyncio
import json
from collections.abc import Callable

import httpx
import respx

BASE_URL = "https://upstream.test"


class FakeUpstream:
    def __init__(self, router: respx.MockRouter) -> None:
        self.created: list[dict] = []
        self.activated: list[str] = []
        self.batches: dict[str, list[dict]] = {}
        # name -> list of responses to return in order before succeeding (e.g. [503] or [422])
        self.failures: dict[str, list[int]] = {}
        self.activate_failures: list[int] = []
        self.delay_seconds = 0.0
        self.in_flight = 0
        self.peak_in_flight = 0
        self.on_create: Callable[[dict], None] | None = None
        self._next_id = 1
        router.post(f"{BASE_URL}/hospitals/").mock(side_effect=self._create)
        router.patch(url__regex=rf"{BASE_URL}/hospitals/batch/(?P<batch_id>[^/]+)/activate").mock(
            side_effect=self._activate
        )
        router.get(url__regex=rf"{BASE_URL}/hospitals/batch/(?P<batch_id>[^/]+)$").mock(side_effect=self._list_batch)

    @property
    def created_names(self) -> list[str]:
        return [h["name"] for h in self.created]

    async def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            if self.delay_seconds:
                await asyncio.sleep(self.delay_seconds)
            pending = self.failures.get(body["name"])
            if pending:
                status = pending.pop(0)
                return httpx.Response(status, json={"detail": f"scripted {status}"})
            hospital = {**body, "id": self._next_id, "active": False}
            self._next_id += 1
            self.created.append(hospital)
            self.batches.setdefault(body["creation_batch_id"], []).append(hospital)
            if self.on_create:
                self.on_create(hospital)
            return httpx.Response(200, json=hospital)
        finally:
            self.in_flight -= 1

    async def _activate(self, request: httpx.Request, batch_id: str) -> httpx.Response:
        if self.activate_failures:
            return httpx.Response(self.activate_failures.pop(0))
        self.activated.append(batch_id)
        return httpx.Response(200, json={"activated": len(self.batches.get(batch_id, []))})

    async def _list_batch(self, request: httpx.Request, batch_id: str) -> httpx.Response:
        return httpx.Response(200, json=self.batches.get(batch_id, []))
