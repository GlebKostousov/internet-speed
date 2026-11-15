"""Тесты на вектора атак: SSRF, бомбы сжатия, OOM, Open Redirect и DoS."""

import asyncio
import gzip
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from measurement import MeasurementError, _count_body, measure
from web_app import create_app
from web_support import MeasureRequest


@pytest.mark.parametrize(
    "forbidden_url",
    [
        "http://127.0.0.1/admin",
        "http://127.0.0.1:8000/private",
        "http://localhost/secret",
        "http://sub.localhost:3000/metrics",
        "http://[::1]/internal",
        "http://0.0.0.0:22/ssh",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.1/network",
        "http://10.255.255.254/data",
        "http://192.168.1.1/router",
        "http://172.16.0.1/internal",
        "http://172.31.255.255/internal",
        "http://224.0.0.1/multicast",
    ],
)
def test_ssrf_direct_ip_and_internal_ranges_blocked(forbidden_url: str) -> None:
    """Запретить прямые обращения к loopback, приватным диапазонам и метаданным."""
    with pytest.raises(ValidationError, match="запрещено"):
        MeasureRequest.model_validate({"url": forbidden_url})


@pytest.mark.parametrize(
    "embedded_url",
    [
        "http://127.0.0.1.nip.io/file",
        "http://app.10.0.0.1.sslip.io/data",
        "http://sub.192.168.1.1.nip.io/admin",
        "http://service.local/status",
        "http://router.lan/config",
        "http://cluster.internal/secret",
    ],
)
def test_ssrf_embedded_ips_and_local_domain_suffixes_blocked(
    embedded_url: str,
) -> None:
    """Запретить домены с приватными IP (nip.io) и служебные зоны."""
    with pytest.raises(ValidationError, match="запрещено"):
        MeasureRequest.model_validate({"url": embedded_url})


def test_demo_file_is_the_only_allowed_loopback_exception() -> None:
    """Разрешить обращение к локальной петле исключительно для внутреннего demo.bin."""
    allowed = MeasureRequest.model_validate({"url": "http://127.0.0.1:8000/demo.bin"})
    assert allowed.url.path == "/demo.bin"


def test_open_redirect_is_rejected_without_following() -> None:
    """Предотвратить обход SSRF через HTTP-перенаправления (301/302)."""

    def mock_transport(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=302,
            headers={"Location": "http://127.0.0.1/admin"},
            request=request,
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(mock_transport),
        follow_redirects=False,
    )
    with pytest.raises(MeasurementError, match="перенаправление HTTP 302"):
        asyncio.run(
            measure(client, httpx.URL("https://public-server.example/file.bin"))
        )


def test_decompression_bomb_is_safe_because_raw_stream_is_not_expanded() -> None:
    """Защита от Zip/Gzip-бомбы: чтение через aiter_raw не распаковывает сжатое тело."""
    compressed_payload = gzip.compress(b"A" * 1_000_000)
    fake_request = httpx.Request("GET", "https://example.com/bomb.gz")

    class CompressedStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield compressed_payload

    response = httpx.Response(
        status_code=200,
        headers={
            "Content-Encoding": "gzip",
            "Content-Length": str(len(compressed_payload)),
        },
        stream=CompressedStream(),
        request=fake_request,
    )
    total_bytes = asyncio.run(_count_body(response))
    assert total_bytes == len(compressed_payload)
    assert total_bytes < 1_000_000


def test_oom_protection_large_stream_processed_in_constant_memory() -> None:
    """Защита от OOM: чтение больших потоков данных без накопления памяти."""
    chunk = b"X" * 65536
    chunks_count = 100
    fake_request = httpx.Request("GET", "https://example.com/huge.bin")

    class HugeStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            for _ in range(chunks_count):
                yield chunk

    response = httpx.Response(
        status_code=200, stream=HugeStream(), request=fake_request
    )
    total_bytes = asyncio.run(_count_body(response))
    assert total_bytes == chunks_count * len(chunk)


def test_slowloris_timeout_aborts_hanging_stream() -> None:
    """Защита от Slowloris: зависающий сервер отсекается по таймауту."""

    class HangingStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"start"
            await asyncio.sleep(10.0)
            yield b"end"

    def mock_transport(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code=200, stream=HangingStream(), request=request)

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(mock_transport),
        timeout=0.05,
    )
    with pytest.raises(MeasurementError, match="таймаут"):
        asyncio.run(
            measure(
                client,
                httpx.URL("https://slow-server.example/file.bin"),
                timeout_seconds=0.05,
            )
        )


def test_hostile_json_payload_rejected_by_api() -> None:
    """Защита API: отклонение запросов с лишними полями или некорректными схемами."""
    app = create_app()
    with TestClient(app, base_url="http://localhost") as client:
        # Лишние неизвестные поля запрещены
        res_extra = client.post(
            "/api/measure",
            json={"url": "https://example.com/f", "unexpected_payload": "hack"},
        )
        assert res_extra.status_code == 422

        # Не-HTTP схемы (file, gopher, ftp) запрещены
        res_file = client.post("/api/measure", json={"url": "file:///etc/passwd"})
        assert res_file.status_code == 422

        # Отрицательный таймаут запрещён
        res_timeout = client.post(
            "/api/measure",
            json={"url": "https://example.com/f", "timeout_seconds": -5},
        )
        assert res_timeout.status_code == 422
