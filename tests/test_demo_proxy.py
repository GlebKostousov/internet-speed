"""Проверка настоящего демо при настроенном прокси без исключений localhost."""

import asyncio
import socket
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Queue
from threading import Event, Thread
from time import monotonic

import httpx
import pytest
import uvicorn
from pydantic import TypeAdapter

from measurement import build_http_client
from web_app import create_app

PROXY_BODY = b"response from the test proxy".ljust(195, b" ")
REMOTE_URL = "http://localhost.external.example.test/file.bin"


class _RecordingProxy(ThreadingHTTPServer):
    """Локальный HTTP-прокси с отдельным журналом адресов каждого теста."""

    def __init__(self) -> None:
        """Открыть свободный loopback-порт и подготовить очередь обращений."""
        super().__init__(("127.0.0.1", 0), _ProxyHandler)
        self.targets: Queue[str] = Queue()


class _ProxyHandler(BaseHTTPRequestHandler):
    """Подменить только ответ прокси, не выполняя внешних соединений."""

    def do_GET(self) -> None:
        """Записать адрес запроса и вернуть отличимые от демо байты."""
        server = self.server
        assert isinstance(server, _RecordingProxy)
        server.targets.put(self.path)
        self.send_response(200)
        self.send_header("Content-Length", str(len(PROXY_BODY)))
        self.send_header("Content-Type", "application/octet-stream")
        self.end_headers()
        self.wfile.write(PROXY_BODY)

    def log_message(self, format: str, *args: object) -> None:
        """Не выводить сетевые адреса в журнал тестового сервера."""


@dataclass(frozen=True, slots=True)
class _LocalServers:
    """Адрес приложения, адрес прокси и наблюдаемый журнал маршрутизации."""

    app_url: str
    proxy_url: str
    proxied_targets: Queue[str]


def _wait_for_start(server: uvicorn.Server) -> None:
    """Дождаться запуска Uvicorn с ограничением времени проверки."""
    deadline = monotonic() + 2.0
    pause = Event()
    while not server.started:
        if monotonic() >= deadline:
            pytest.fail("Локальный сервер не запустился вовремя.")
        pause.wait(0.01)


@pytest.fixture
def local_servers() -> Iterator[_LocalServers]:
    """Запустить приложение и тестовый прокси в одном процессе.

    Yields:
        Адреса серверов с независимой остановкой обоих потоков.
    """
    with socket.socket() as listener, _RecordingProxy() as proxy:
        listener.bind(("127.0.0.1", 0))
        port = int(listener.getsockname()[1])
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(),
                host="127.0.0.1",
                port=port,
                loop="asyncio",
                http="h11",
                log_level="error",
                proxy_headers=False,
                access_log=False,
                timeout_graceful_shutdown=2,
            )
        )
        app_thread = Thread(target=lambda: server.run(sockets=[listener]), daemon=True)
        proxy_thread = Thread(target=proxy.serve_forever, daemon=True)
        proxy_thread.start()
        app_thread.start()
        try:
            _wait_for_start(server)
            yield _LocalServers(
                app_url=f"http://127.0.0.1:{port}",
                proxy_url=f"http://127.0.0.1:{proxy.server_port}",
                proxied_targets=proxy.targets,
            )
        finally:
            server.should_exit = True
            app_thread.join(timeout=3.0)
            proxy.shutdown()
            proxy_thread.join(timeout=2.0)
            assert not app_thread.is_alive()
            assert not proxy_thread.is_alive()


@pytest.mark.parametrize("hostname", ["127.0.0.1", "localhost"])
def test_demo_downloads_ten_mib_while_remote_requests_keep_proxy(
    local_servers: _LocalServers,
    monkeypatch: pytest.MonkeyPatch,
    hostname: str,
) -> None:
    """Штатный клиент получает 10 MiB напрямую, сохраняя прокси для внешних имён."""
    # Имитируем окружение с прокси без NO_PROXY, не обращаясь за пределы loopback.
    monkeypatch.setenv("HTTP_PROXY", local_servers.proxy_url)
    monkeypatch.setenv("http_proxy", local_servers.proxy_url)
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    demo_url = httpx.URL(local_servers.app_url).copy_with(
        host=hostname, path="/demo.bin"
    )

    with httpx.Client(
        base_url=local_servers.app_url, trust_env=False, timeout=10.0
    ) as browser:
        response = browser.post("/api/measure", json={"url": str(demo_url)})

    assert response.status_code == 200
    adapter = TypeAdapter(dict[str, object])
    events = [adapter.validate_json(line) for line in response.iter_lines()]
    assert len(events) == 12
    assert events[-1]["type"] == "complete"
    assert events[-1]["total_bytes"] == 10 * 1024**2
    assert [sample["bytes"] for sample in events[1:-1]] == [1024**2] * 10
    assert local_servers.proxied_targets.empty()

    async def check_remote_proxy() -> None:
        """Подтвердить, что внешнее имя с префиксом localhost остаётся за прокси."""
        async with build_http_client(timeout_seconds=3.0) as client:
            remote = await client.get(REMOTE_URL)
        assert remote.content == PROXY_BODY

    asyncio.run(check_remote_proxy())
    assert local_servers.proxied_targets.get_nowait() == REMOTE_URL
    assert local_servers.proxied_targets.empty()
