"""Локальное веб-приложение для измерения скорости с живым отчётом."""

import asyncio
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from dataclasses import dataclass
from pathlib import Path

import anyio
import httpx
import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.staticfiles import StaticFiles

from measurement import (
    Measurement,
    MeasurementError,
    RequestMeasurement,
    build_http_client,
    iter_samples,
)
from web_support import (
    CompleteEvent,
    DisconnectAwareResponse,
    ErrorEvent,
    HttpClientFactory,
    MeasurementLimitMiddleware,
    MeasureRequest,
    SampleData,
    SampleEvent,
    StartedEvent,
    parse_measure_request,
)

FRONTEND_DIRECTORY = Path(__file__).parent / "frontend" / "dist"
DEMO_CHUNK = bytes(range(256)) * 256


def _line(event: BaseModel) -> str:
    """Сериализовать одно типизированное событие протокола JSON Lines."""
    return event.model_dump_json() + "\n"


def _complete(result: Measurement) -> CompleteEvent:
    """Построить итог из единственного набора исходных измерений."""
    return CompleteEvent(
        total_bytes=result.total_bytes,
        total_seconds=result.total_seconds,
        average_seconds=result.average_seconds,
        speed_mib_s=result.mebibytes_per_second,
        samples=tuple(SampleData.from_measurement(sample) for sample in result.samples),
    )


async def _events(
    parameters: MeasureRequest, client_factory: HttpClientFactory
) -> AsyncGenerator[str]:
    """Отдавать события после полного запроса и закрывать клиент при отмене.

    Args:
        parameters: Проверенные параметры загрузки.
        client_factory: Создание собственного клиента для одной серии.

    Yields:
        Строки NDJSON с результатами, итогом либо безопасным описанием ошибки.
    """
    client = client_factory(timeout_seconds=parameters.timeout_seconds)
    try:
        async with aclosing(_measure_events(client, parameters)) as events:
            async for line in events:
                yield line
    finally:
        # Отмена ASGI-группы не должна прерывать освобождение пула соединений.
        with anyio.move_on_after(2.0, shield=True):
            await client.aclose()


async def _measure_events(
    client: httpx.AsyncClient, parameters: MeasureRequest
) -> AsyncGenerator[str]:
    """Измерить серию; сериализация выполняется за границами таймеров."""
    yield _line(StartedEvent())
    samples: list[RequestMeasurement] = []
    try:
        async with aclosing(
            iter_samples(
                client,
                httpx.URL(str(parameters.url)),
                timeout_seconds=parameters.timeout_seconds,
            )
        ) as measurements:
            async for sample in measurements:
                samples.append(sample)
                yield _line(
                    SampleEvent(
                        index=sample.index,
                        bytes=sample.total_bytes,
                        seconds=sample.elapsed_seconds,
                        speed_mib_s=sample.mebibytes_per_second,
                    )
                )
    except MeasurementError as error:
        yield _line(ErrorEvent(message=str(error)))
        return
    yield _line(_complete(Measurement(samples=tuple(samples))))


async def _demo_bytes() -> AsyncIterator[bytes]:
    """Передать ровно 1 MiB локальных данных с короткими паузами для примера."""
    for _ in range(16):
        await asyncio.sleep(0.008)
        yield DEMO_CHUNK


async def _http_error(request: Request, error: Exception) -> JSONResponse:
    """Вернуть безопасную ошибку в едином формате для браузера."""
    if not isinstance(error, HTTPException):
        raise error
    return JSONResponse(
        {"message": str(error.detail)},
        status_code=error.status_code,
        headers=error.headers,
    )


@dataclass(frozen=True, slots=True)
class WebHandlers:
    """HTTP-граница с явно переданной фабрикой сетевого клиента."""

    client_factory: HttpClientFactory

    async def start_measurement(self, request: Request) -> StreamingResponse:
        """Начать измерение и отдавать результаты после каждого полного ответа."""
        parameters = await parse_measure_request(request)
        return DisconnectAwareResponse(
            _events(parameters, self.client_factory),
            media_type="application/x-ndjson",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )


async def _demo_file() -> StreamingResponse:
    """Отдать локальный тестовый файл для проверки интерфейса."""
    return StreamingResponse(
        _demo_bytes(),
        media_type="application/octet-stream",
        headers={"Cache-Control": "no-store"},
    )


async def _index() -> FileResponse:
    """Открыть готовую сборку интерфейса."""
    path = FRONTEND_DIRECTORY / "index.html"
    if not path.is_file():
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Соберите frontend: npm ci && npm run build.",
        )
    return FileResponse(path)


def create_app(*, client_factory: HttpClientFactory = build_http_client) -> FastAPI:
    """Собрать локальное приложение с изолированными ресурсами измерений.

    Args:
        client_factory: Фабрика HTTP-клиентов; подменяется тестовым транспортом.

    Returns:
        Приложение с допуском одной активной серии на процесс.
    """
    app = FastAPI(
        title="Скорость загрузки", docs_url=None, redoc_url=None, openapi_url=None
    )
    app.add_middleware(MeasurementLimitMiddleware)
    raw_hosts = os.getenv(
        "ALLOWED_HOSTS", "127.0.0.1,localhost,[::1],reportflow.ru,*.reportflow.ru"
    )
    allowed_hosts = [host.strip() for host in raw_hosts.split(",") if host.strip()]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts)
    app.add_exception_handler(HTTPException, _http_error)

    handlers = WebHandlers(client_factory=client_factory)
    app.add_api_route("/api/measure", handlers.start_measurement, methods=["POST"])
    app.add_api_route("/demo.bin", _demo_file, methods=["GET", "HEAD"])
    app.add_api_route("/", _index, methods=["GET", "HEAD"])

    app.mount(
        "/assets",
        StaticFiles(directory=FRONTEND_DIRECTORY / "assets", check_dir=False),
        name="assets",
    )
    return app


def main() -> None:
    """Запустить один процесс на локальном интерфейсе компьютера."""
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "web_app:create_app",
        factory=True,
        host=host,
        port=port,
        workers=1,
        proxy_headers=True,
        forwarded_allow_ips="*",
        access_log=False,
    )


if __name__ == "__main__":
    main()
