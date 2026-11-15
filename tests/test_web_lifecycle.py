"""Проверки ограничения запросов и отмены через настоящий локальный TCP."""

import asyncio
import socket
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from threading import Event, Thread
from time import monotonic

import httpx
import pytest
import uvicorn
from pydantic import TypeAdapter

from web_app import create_app

TEST_URL = "https://example.test/file.bin"


class _PendingBody(httpx.AsyncByteStream):
    """Отдать первую часть ответа и ждать разрыва входящего соединения."""

    def __init__(self) -> None:
        """Подготовить сигналы для обмена между серверным и тестовым потоками."""
        self.started = Event()
        self.closed = Event()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Начать чтение и приостановиться до отмены серверной задачи."""
        yield b"partial body"
        self.started.set()
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        """Сообщить, что сервер освободил незавершённый ответ."""
        self.closed.set()


class _CompleteBody(httpx.AsyncByteStream):
    """Передать небольшой полный ответ для проверки повторного запуска."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Отдать тело с известным размером."""
        yield b"complete body"


class _ClientFactory:
    """Управлять исходящим ответом и считать обращения к тестовому серверу."""

    def __init__(self) -> None:
        """Первую серию оставить незавершённой до явного переключения режима."""
        self.pending = _PendingBody()
        self.complete_responses = Event()
        self.clients: list[httpx.AsyncClient] = []
        self.requests = 0

    def __call__(self, *, timeout_seconds: float) -> httpx.AsyncClient:
        """Создать отдельный клиент, сохранив штатное владение его ресурсами."""
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._respond), timeout=timeout_seconds
        )
        self.clients.append(client)
        return client

    def _respond(self, request: httpx.Request) -> httpx.Response:
        """Выдать ожидающий либо завершённый ответ без внешнего соединения."""
        self.requests += 1
        stream = _CompleteBody() if self.complete_responses.is_set() else self.pending
        return httpx.Response(200, request=request, stream=stream)


@dataclass(frozen=True, slots=True)
class _LiveServer:
    """Адрес сервера и наблюдаемые исходящие ресурсы одной проверки."""

    base_url: str
    port: int
    factory: _ClientFactory


def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 2.0) -> None:
    """Дождаться наблюдаемого состояния, ограничив время ожидания."""
    deadline = monotonic() + timeout_seconds
    pause = Event()
    while not predicate():
        if monotonic() >= deadline:
            pytest.fail("Сервер не достиг ожидаемого состояния вовремя.")
        pause.wait(0.01)


@pytest.fixture
def live_server() -> Iterator[_LiveServer]:
    """Запустить Uvicorn на свободном локальном порту и гарантировать остановку.

    Yields:
        Сервер в том же процессе, что и TCP-клиент теста.
    """
    factory = _ClientFactory()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        port = int(listener.getsockname()[1])
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(client_factory=factory),
                host="127.0.0.1",
                port=port,
                loop="asyncio",
                http="h11",
                access_log=False,
                proxy_headers=False,
                log_level="error",
                timeout_graceful_shutdown=2,
            )
        )
        worker = Thread(target=lambda: server.run(sockets=[listener]), daemon=True)
        worker.start()
        try:
            _wait_until(lambda: server.started)
            yield _LiveServer(f"http://127.0.0.1:{port}", port, factory)
        finally:
            server.should_exit = True
            worker.join(timeout=3.0)
            if worker.is_alive():
                server.force_exit = True
                worker.join(timeout=2.0)
            assert not worker.is_alive(), "Поток Uvicorn не завершился."


def test_disconnect_closes_download_and_releases_measurement_slot(
    live_server: _LiveServer,
) -> None:
    """429 сохраняет одну серию; разрыв TCP отменяет загрузку и освобождает слот."""
    factory = live_server.factory

    async def exercise_lifecycle() -> None:
        """Пройти занятость, отключение браузера и новый успешный замер."""
        adapter = TypeAdapter(dict[str, object])
        async with httpx.AsyncClient(
            base_url=live_server.base_url, timeout=5.0, trust_env=False
        ) as browser:
            async with browser.stream(
                "POST", "/api/measure", json={"url": TEST_URL, "timeout_seconds": 120}
            ) as first:
                assert first.status_code == 200
                lines = first.aiter_lines()
                first_line = await anext(lines)
                assert adapter.validate_json(first_line)["type"] == "started"
                assert await asyncio.to_thread(factory.pending.started.wait, 2.0)

                busy = await browser.post("/api/measure", json={"url": TEST_URL})
                assert busy.status_code == 429
                assert busy.headers["Retry-After"] == "1"
                assert factory.requests == 1
                assert len(factory.clients) == 1

            assert await asyncio.to_thread(factory.pending.closed.wait, 2.0)
            await asyncio.to_thread(_wait_until, lambda: factory.clients[0].is_closed)
            factory.complete_responses.set()

            following = await browser.post("/api/measure", json={"url": TEST_URL})
            assert following.status_code == 200
            events = [adapter.validate_json(line) for line in following.iter_lines()]
            assert [event["type"] for event in events] == [
                "started",
                *(["sample"] * 10),
                "complete",
            ]
            assert events[-1]["total_bytes"] == len(b"complete body") * 10
            assert factory.requests == 11
            await asyncio.to_thread(
                _wait_until, lambda: all(client.is_closed for client in factory.clients)
            )

    asyncio.run(exercise_lifecycle())


def test_slow_request_body_times_out_and_releases_slot(
    live_server: _LiveServer,
) -> None:
    """Незаконченное входящее тело получает 408 и не удерживает слот замера."""
    headers = (
        "POST /api/measure HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{live_server.port}\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: 100\r\n"
        "Connection: close\r\n\r\n"
    )
    response = bytearray()
    with socket.create_connection(
        ("127.0.0.1", live_server.port), timeout=5.0
    ) as client:
        client.sendall(headers.encode("ascii") + b"{")
        while chunk := client.recv(4096):
            response.extend(chunk)

    assert response.startswith(b"HTTP/1.1 408 ")
    assert live_server.factory.clients == []
    live_server.factory.complete_responses.set()
    with httpx.Client(
        base_url=live_server.base_url, timeout=5.0, trust_env=False
    ) as browser:
        following = browser.post("/api/measure", json={"url": TEST_URL})
    assert following.status_code == 200
    events = [
        TypeAdapter(dict[str, object]).validate_json(line)
        for line in following.iter_lines()
    ]
    assert events[-1]["type"] == "complete"
