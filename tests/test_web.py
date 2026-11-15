"""Проверки потокового API и его входных границ без внешней сети."""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import TypeAdapter

from web_app import create_app

TEST_URL = "https://example.test/file.bin"
BODY_PARTS = (b"first", b"second")
BODY_SIZE = sum(map(len, BODY_PARTS))


class _BodyStream(httpx.AsyncByteStream):
    """Передать тело частями и сохранить факт закрытия ответа."""

    def __init__(self) -> None:
        """Подготовить состояние незавершённого потока."""
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Отдать обе части с возможностью переключения задач."""
        for chunk in BODY_PARTS:
            await asyncio.sleep(0)
            yield chunk

    async def aclose(self) -> None:
        """Зафиксировать освобождение ответа."""
        self.closed = True


class _ClientFactory:
    """Создать клиент с управляемым ответом и учётом закрытых ресурсов."""

    def __init__(self, *, failure_at: int | None = None) -> None:
        """Сохранить номер обращения, на котором сервер вернёт ошибку."""
        self.failure_at = failure_at
        self.clients: list[httpx.AsyncClient] = []
        self.requests: list[httpx.Request] = []
        self.streams: list[_BodyStream] = []

    def __call__(self, *, timeout_seconds: float = 30.0) -> httpx.AsyncClient:
        """Создать клиент с подменой только сетевого транспорта."""
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(self._respond),
            timeout=timeout_seconds,
            follow_redirects=False,
        )
        self.clients.append(client)
        return client

    def _respond(self, request: httpx.Request) -> httpx.Response:
        """Выдать ответ после закрытия предыдущего потока."""
        assert all(stream.closed for stream in self.streams)
        self.requests.append(request)
        stream = _BodyStream()
        self.streams.append(stream)
        return httpx.Response(
            500 if len(self.requests) == self.failure_at else 200,
            request=request,
            stream=stream,
        )


def _events(response: httpx.Response) -> list[dict[str, object]]:
    """Разобрать NDJSON, проверив, что каждая строка содержит объект."""
    adapter = TypeAdapter(dict[str, object])
    return [adapter.validate_json(line) for line in response.iter_lines()]


@pytest.mark.parametrize("origin", [None, "http://localhost"])
def test_success_streams_ten_samples_and_closes_client(origin: str | None) -> None:
    """API выдаёт полную серию и закрывает HTTP-клиент до окончания ответа."""
    factory = _ClientFactory()
    headers = {} if origin is None else {"Origin": origin}
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post("/api/measure", json={"url": TEST_URL}, headers=headers)
        assert all(client.is_closed for client in factory.clients)

    assert response.status_code == 200
    assert "ndjson" in response.headers["content-type"]
    events = _events(response)
    assert events[0] == {"type": "started", "request_count": 10}
    assert len(events) == 12
    for index, sample in enumerate(events[1:-1], start=1):
        assert sample["type"] == "sample"
        assert sample["index"] == index
        assert sample["bytes"] == BODY_SIZE
    complete = events[-1]
    assert complete["type"] == "complete"
    assert complete["request_count"] == 10
    assert complete["total_bytes"] == BODY_SIZE * 10
    total_seconds = complete["total_seconds"]
    assert isinstance(total_seconds, (int, float))
    assert total_seconds > 0
    assert complete["average_seconds"] == pytest.approx(total_seconds / 10)
    assert complete["speed_mib_s"] == pytest.approx(
        BODY_SIZE * 10 / total_seconds / 1024**2
    )
    assert complete["samples"] == [
        {key: value for key, value in sample.items() if key != "type"}
        for sample in events[1:-1]
    ]
    assert len(factory.requests) == 10
    assert all(request.method == "GET" for request in factory.requests)
    assert all(str(request.url) == TEST_URL for request in factory.requests)
    assert all(stream.closed for stream in factory.streams)


def test_failed_download_has_no_completion_and_releases_slot() -> None:
    """Ошибка третьего запроса прерывает серию и разрешает следующий замер."""
    factory = _ClientFactory(failure_at=3)
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post("/api/measure", json={"url": TEST_URL})
        events = _events(response)
        assert [event["type"] for event in events] == [
            "started",
            "sample",
            "sample",
            "error",
        ]
        message = events[-1]["message"]
        assert isinstance(message, str)
        assert "Запрос 3/10" in message
        assert len(factory.requests) == 3
        assert all(client.is_closed for client in factory.clients)
        assert all(stream.closed for stream in factory.streams)

        repeated = api.post("/api/measure", json={"url": TEST_URL})
        assert repeated.status_code == 200
        assert _events(repeated)[-1]["type"] == "complete"


@pytest.mark.parametrize(
    "body",
    [
        '{"url":"file:///tmp/file.bin"}',
        '{"url":"http://"}',
        '{"url":"example.test/file.bin"}',
        '{"url":"https://example.test/file.bin","timeout_seconds":0}',
        '{"url":"https://example.test/file.bin","timeout_seconds":121}',
        '{"url":"https://example.test/file.bin","timeout_seconds":NaN}',
        '{"url":"https://example.test/file.bin","timeout_seconds":Infinity}',
        '{"url":"https://example.test/file.bin","extra":true}',
        "{",
    ],
)
def test_invalid_input_is_rejected_before_download(body: str) -> None:
    """Адрес, таймаут, неизвестные поля и неверный JSON проверяются до HTTP."""
    factory = _ClientFactory()
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post(
            "/api/measure",
            content=body,
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 422
    message = TypeAdapter(dict[str, object]).validate_json(response.content)["message"]
    assert isinstance(message, str)
    assert message
    assert factory.clients == []


@pytest.mark.parametrize(
    "origin", ["https://evil.test", "null", "http://localhost:8000"]
)
def test_foreign_origin_is_rejected(origin: str) -> None:
    """Origin с чужим сайтом, портом либо значением null не запускает замер."""
    factory = _ClientFactory()
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post(
            "/api/measure",
            json={"url": TEST_URL},
            headers={"Origin": origin},
        )

    assert response.status_code == 403
    assert factory.clients == []


def test_untrusted_host_is_rejected() -> None:
    """Запрос с внешним Host не достигает обработчика замера."""
    factory = _ClientFactory()
    with TestClient(
        create_app(client_factory=factory), base_url="http://evil.test"
    ) as api:
        response = api.post("/api/measure", json={"url": TEST_URL})

    assert response.status_code == 400
    assert factory.clients == []


def test_oversized_request_body_is_rejected_before_parsing() -> None:
    """Тело больше восьми KiB отклоняется до разбора JSON и загрузки файла."""
    factory = _ClientFactory()
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post(
            "/api/measure",
            content=" " * 8193,
            headers={"Content-Type": "application/json"},
        )

    assert response.status_code == 413
    assert factory.clients == []


@pytest.mark.parametrize(
    "ssrf_url",
    [
        "http://localhost:22/",
        "http://127.0.0.1:18095/",
        "http://192.168.1.1/admin",
        "http://10.0.0.1/",
        "http://169.254.169.254/latest/meta-data",
        "http://0.0.0.0/",
        "http://[::1]:8080/",
    ],
)
def test_ssrf_internal_targets_are_rejected(ssrf_url: str) -> None:
    """Обращение к служебным, облачным и локальным адресам сети блокируется."""
    factory = _ClientFactory()
    with TestClient(
        create_app(client_factory=factory), base_url="http://localhost"
    ) as api:
        response = api.post("/api/measure", json={"url": ssrf_url})

    assert response.status_code == 422
    data = TypeAdapter(dict[str, object]).validate_json(response.content)
    assert "запрещено" in str(data["message"])
    assert factory.clients == []


def test_security_headers_are_present_on_responses() -> None:
    """Ответы сервера содержат защитные заголовки CSP, no-sniff и Deny."""
    with TestClient(create_app(), base_url="http://localhost") as client:
        response = client.get("/")

    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "default-src 'self'" in response.headers["Content-Security-Policy"]
