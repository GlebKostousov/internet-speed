"""Контракты HTTP и владение ресурсами потокового измерения."""

import asyncio
import ipaddress
import random
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from time import monotonic
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


@dataclass(slots=True)
class _ClientRateRecord:
    """Состояние запросов и блокировки одного IP-адреса."""

    timestamps: list[float] = field(default_factory=list)
    violations: int = 0
    blocked_until: float = 0.0


def calculate_exponential_jitter_delay(
    streak: int,
    *,
    base_delay: float = 10.0,
    max_delay: float = 600.0,
    jitter_ratio: float = 0.25,
    random_factor: float = 0.5,
) -> int:
    """Вычислить экспоненциальную задержку с джиттером для Retry-After.

    Args:
        streak: Количество нарушений подряд (от 1).
        base_delay: Базовое время задержки в секундах.
        max_delay: Максимальный потолок задержки.
        jitter_ratio: Доля случайного разброса.
        random_factor: Случайный фактор от 0.0 до 1.0.

    Returns:
        Целое число секунд ожидания для заголовка Retry-After.
    """
    exponent = min(max(0, streak - 1), 10)
    delay = min(base_delay * (2**exponent), max_delay)
    jitter = delay * jitter_ratio * random_factor
    return max(1, int(round(delay + jitter)))


def _forwarded_ip_or_none(header_value: str | None) -> str | None:
    """Извлечь первый IP-адрес из цепочки X-Forwarded-For."""
    if not header_value:
        return None
    candidate = header_value.split(",")[0].strip()
    return candidate or None


def extract_client_ip(request: Request) -> str:
    """Извлечь IP-адрес клиента с учётом прокси или сокета.

    Args:
        request: Входящий HTTP-запрос.

    Returns:
        Строка с IP-адресом клиента.
    """
    forwarded = _forwarded_ip_or_none(request.headers.get("x-forwarded-for"))
    if forwarded:
        return forwarded
    if request.client and request.client.host:
        return request.client.host
    return "127.0.0.1"


class IpRateLimiter:
    """Частотный ограничитель запусков по IP со скользящим окном и джиттером."""

    def __init__(
        self,
        *,
        max_requests: int = 5,
        window_seconds: float = 600.0,
        base_delay: float = 10.0,
        max_delay: float = 600.0,
        jitter_ratio: float = 0.25,
        clock: Callable[[], float] = monotonic,
        random_factor: Callable[[], float] = random.random,
    ) -> None:
        """Инициализировать параметры окна и задержки."""
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter_ratio = jitter_ratio
        self.clock = clock
        self.random_factor = random_factor
        self._records: dict[str, _ClientRateRecord] = {}

    def _get_record(self, ip: str) -> _ClientRateRecord:
        """Получить или создать запись для IP."""
        record = self._records.get(ip)
        if record is None:
            record = _ClientRateRecord()
            self._records[ip] = record
        return record

    def _check_active_block(self, record: _ClientRateRecord, now: float) -> None:
        """Проверить, не действует ли ещё текущая блокировка."""
        if now < record.blocked_until:
            remaining = max(1, int(round(record.blocked_until - now)))
            window_minutes = int(self.window_seconds // 60)
            message = (
                f"Превышен лимит запусков с вашего IP "
                f"(максимум {self.max_requests} за {window_minutes} мин). "
                f"Попробуйте через {remaining} с."
            )
            raise HTTPException(
                status.HTTP_429_TOO_MANY_REQUESTS,
                detail=message,
                headers={"Retry-After": str(remaining)},
            )

    def _enforce_limit(self, record: _ClientRateRecord, now: float) -> None:
        """Проверить скользящее окно и применить экспоненциальный бэкофф с джиттером."""
        cutoff = now - self.window_seconds
        record.timestamps = [t for t in record.timestamps if t > cutoff]
        if len(record.timestamps) >= self.max_requests:
            record.violations += 1
            delay = calculate_exponential_jitter_delay(
                record.violations,
                base_delay=self.base_delay,
                max_delay=self.max_delay,
                jitter_ratio=self.jitter_ratio,
                random_factor=self.random_factor(),
            )
            record.blocked_until = now + delay
            window_minutes = int(self.window_seconds // 60)
            message = (
                f"Превышен лимит запусков с вашего IP "
                f"(максимум {self.max_requests} за {window_minutes} мин). "
                f"Повторите попытку через {delay} с."
            )
            raise HTTPException(
                status.HTTP_429_TOO_MANY_REQUESTS,
                detail=message,
                headers={"Retry-After": str(delay)},
            )
        record.timestamps.append(now)

    def check(self, request: Request) -> None:
        """Проверить запрос на превышение лимита частоты.

        Args:
            request: Входящий HTTP-запрос.

        Raises:
            HTTPException: 429 Too Many Requests с понятным сообщением и Retry-After.
        """
        ip = extract_client_ip(request)
        now = self.clock()
        record = self._get_record(ip)
        self._check_active_block(record, now)
        self._enforce_limit(record, now)


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
