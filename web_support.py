"""Контракты HTTP и владение ресурсами потокового измерения."""

import asyncio
import ipaddress
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from typing import Literal, Protocol, Self

import anyio
import httpx
from anyio.abc import TaskGroup
from fastapi import HTTPException, Request, status
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    ValidationError,
    model_validator,
)
from starlette.responses import JSONResponse, StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from measurement import DEFAULT_TIMEOUT_SECONDS, REQUEST_COUNT, RequestMeasurement

MAX_INPUT_BYTES = 8 * 1024
INPUT_TIMEOUT_SECONDS = 3.0


class HttpClientFactory(Protocol):
    """Фабрика отдельного HTTP-клиента для одной серии загрузок."""

    def __call__(self, *, timeout_seconds: float) -> httpx.AsyncClient:
        """Создать клиент с заданным сетевым таймаутом."""
        ...


def _is_ip_private(ip_str: str) -> bool:
    """Проверить, является ли строковый IP-адрес приватным или локальным.

    Args:
        ip_str: Строка с IP-адресом.

    Returns:
        True, если адрес принадлежит приватным, локальным или служебным диапазонам.
    """
    try:
        ip = ipaddress.ip_address(ip_str)
        return (
            ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_unspecified
            or ip.is_multicast
        )
    except ValueError:
        return False


def _is_ip_embedded_in_host(host: str) -> bool:
    """Проверить наличие приватного IPv4 в имени домена (например, nip.io).

    Args:
        host: Доменное имя хоста.

    Returns:
        True, если подстрока домена совпадает с приватным IP.
    """
    parts = host.split(".")
    return any(
        _is_ip_private(".".join(parts[i : i + 4])) for i in range(len(parts) - 3)
    )


def _is_private_or_internal_host(host: str) -> bool:
    """Проверить, является ли хост приватным, локальным или служебным адресом.

    Args:
        host: Доменное имя или IP-адрес.

    Returns:
        True, если адрес указывает на внутреннюю или локальную сеть.
    """
    clean_host = host.strip("[]").lower()
    if clean_host in {
        "localhost",
        "127.0.0.1",
        "::1",
    } or clean_host.endswith((".localhost", ".local", ".internal", ".lan")):
        return True
    return _is_ip_private(clean_host) or _is_ip_embedded_in_host(clean_host)


class MeasureRequest(BaseModel):
    """Проверенные параметры измерения из браузера."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    url: HttpUrl = Field(max_length=2048)
    timeout_seconds: float = Field(
        default=DEFAULT_TIMEOUT_SECONDS, gt=0, le=120, strict=True
    )

    @model_validator(mode="after")
    def validate_target_safety(self) -> Self:
        """Запретить несанкционированные обращения к приватной инфраструктуре (SSRF).

        Returns:
            Проверенный объект запроса.

        Raises:
            ValueError: Целевой адрес находится в приватной или служебной сети.
        """
        host = self.url.host
        if not host:
            raise ValueError("В адресе не указан хост.")
        path = self.url.path or ""
        is_demo = host.lower() in {"localhost", "127.0.0.1", "[::1]"} and path.endswith(
            "demo.bin"
        )
        if _is_private_or_internal_host(host) and not is_demo:
            raise ValueError(
                "Обращение к локальным или приватным адресам сети запрещено."
            )
        return self


class SampleData(BaseModel):
    """Числовые данные завершённого запроса для графика и выгрузки."""

    index: int
    bytes: int
    seconds: float
    speed_mib_s: float

    @classmethod
    def from_measurement(cls, sample: RequestMeasurement) -> Self:
        """Преобразовать внутреннее измерение без округления.

        Args:
            sample: Завершённая загрузка.

        Returns:
            Сериализуемые числовые данные.
        """
        return cls(
            index=sample.index,
            bytes=sample.total_bytes,
            seconds=sample.elapsed_seconds,
            speed_mib_s=sample.mebibytes_per_second,
        )


class StartedEvent(BaseModel):
    """Подтверждение начала серии."""

    type: Literal["started"] = "started"
    request_count: int = REQUEST_COUNT


class SampleEvent(SampleData):
    """Промежуточное событие после полного ответа."""

    type: Literal["sample"] = "sample"


class CompleteEvent(BaseModel):
    """Итог успешной серии; единственное подтверждение её завершения."""

    type: Literal["complete"] = "complete"
    request_count: int = REQUEST_COUNT
    total_bytes: int
    total_seconds: float
    average_seconds: float
    speed_mib_s: float
    samples: tuple[SampleData, ...]


class ErrorEvent(BaseModel):
    """Ошибка серии без ложного итогового результата."""

    type: Literal["error"] = "error"
    message: str


class MeasurementLimitMiddleware:
    """Допуск одной серии на весь цикл ASGI, включая отмену и отправку тела."""

    def __init__(self, app: ASGIApp) -> None:
        """Создать ограничитель для экземпляра приложения."""
        self._app = app
        self._limiter = anyio.CapacityLimiter(1)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Пропустить обычные запросы и ограничить только запуск измерения."""
        if (
            scope["type"] != "http"
            or scope["path"] != "/api/measure"
            or scope["method"] != "POST"
        ):
            await self._app(scope, receive, send)
            return
        await self._run_limited(scope, receive, send)

    async def _run_limited(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Захватить слот без очереди и обязательно освободить после ответа."""
        try:
            self._limiter.acquire_nowait()
        except anyio.WouldBlock:
            response = JSONResponse(
                {"message": "Уже идёт другой замер. Дождитесь его завершения."},
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                headers={"Retry-After": "1"},
            )
            await response(scope, receive, send)
            return
        try:
            await self._app(scope, receive, send)
        finally:
            self._limiter.release()


class DisconnectAwareResponse(StreamingResponse):
    """Отменять скачивание при закрытии вкладки даже между отправками событий."""

    def __init__(
        self,
        content: AsyncGenerator[str],
        *,
        media_type: str,
        headers: Mapping[str, str],
    ) -> None:
        """Принять владение генератором до завершения отправки ответа."""
        super().__init__(content, media_type=media_type, headers=headers)
        self._events = content

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Слушать disconnect независимо от версии HTTP-расширения ASGI."""
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(self._send_and_finish, send, tasks)
                await self.listen_for_disconnect(receive)
                tasks.cancel_scope.cancel()
        finally:
            with anyio.move_on_after(2.0, shield=True):
                await self._events.aclose()
        if self.background is not None:
            await self.background()

    async def _send_and_finish(self, send: Send, tasks: TaskGroup) -> None:
        """Остановить наблюдатель после конца отправки или разрыва соединения."""
        try:
            await self.stream_response(send)
        except OSError:
            # Получатель закрыл соединение; повторять отправку бессмысленно.
            pass
        finally:
            tasks.cancel_scope.cancel()


async def parse_measure_request(request: Request) -> MeasureRequest:
    """Проверить origin, размер, длительность чтения и схему запроса.

    Args:
        request: Входящий запрос локального браузера.

    Returns:
        Проверенные параметры измерения.

    Raises:
        HTTPException: Запрос нарушает контракт или лимиты.
    """
    _check_origin(request)
    body = await _read_body(request)
    try:
        return MeasureRequest.model_validate_json(body)
    except ValidationError as error:
        detail = error.errors()[0]["msg"] if error.errors() else "Некорректный запрос."
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"Некорректные параметры: {detail}",
        ) from error


def _check_origin(request: Request) -> None:
    """Разрешить собственный браузерный origin и прямые JSON-запросы."""
    origin = request.headers.get("origin")
    expected = f"{request.url.scheme}://{request.url.netloc}"
    if origin is not None and origin != expected:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Недопустимый источник запроса.")
    if request.headers.get("content-type", "").split(";", 1)[0] != "application/json":
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, "Нужен JSON-запрос."
        )


async def _read_body(request: Request) -> bytes:
    """Читать тело с пределом размера и времени, не доверяя Content-Length."""
    try:
        async with asyncio.timeout(INPUT_TIMEOUT_SECONDS):
            return await _collect_body(request.stream())
    except TimeoutError as error:
        raise HTTPException(
            status.HTTP_408_REQUEST_TIMEOUT, "Истекло время получения запроса."
        ) from error


async def _collect_body(stream: AsyncIterator[bytes]) -> bytes:
    """Собрать только небольшой JSON, остановив слишком большой запрос."""
    body = bytearray()
    async for chunk in stream:
        if len(body) + len(chunk) > MAX_INPUT_BYTES:
            raise HTTPException(
                status.HTTP_413_CONTENT_TOO_LARGE, "Слишком длинный запрос."
            )
        body.extend(chunk)
    return bytes(body)
