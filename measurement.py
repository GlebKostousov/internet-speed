"""Потоковый замер последовательных загрузок без зависимости от интерфейса."""

import asyncio
import math
from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from dataclasses import dataclass
from time import perf_counter

import httpx

REQUEST_COUNT = 10
DEFAULT_TIMEOUT_SECONDS = 30.0
BYTES_PER_MEBIBYTE = 1024**2


class MeasurementError(Exception):
    """Серия не завершена и не имеет итогового результата."""


@dataclass(frozen=True, slots=True)
class RequestMeasurement:
    """Полностью прочитанный ответ и его измеренная длительность.

    Attributes:
        index: Номер загрузки в серии, начиная с единицы.
        total_bytes: Размер переданного тела до распаковки.
        elapsed_seconds: Время до полного чтения и закрытия ответа.
    """

    index: int
    total_bytes: int
    elapsed_seconds: float

    def __post_init__(self) -> None:
        """Проверить инварианты измерения.

        Raises:
            ValueError: Некорректные номер, размер или длительность.
        """
        if not 1 <= self.index <= REQUEST_COUNT or self.total_bytes < 0:
            message = "Некорректный номер запроса или объём ответа."
            raise ValueError(message)
        if not math.isfinite(self.elapsed_seconds) or self.elapsed_seconds <= 0:
            message = "Длительность должна быть конечным положительным числом."
            raise ValueError(message)

    @property
    def mebibytes_per_second(self) -> float:
        """Вернуть скорость одной полной загрузки в MiB/s."""
        return self.total_bytes / self.elapsed_seconds / BYTES_PER_MEBIBYTE


@dataclass(frozen=True, slots=True)
class Measurement:
    """Полная серия; все итоговые значения вычисляются из исходных измерений.

    Attributes:
        samples: Ровно десять результатов в порядке выполнения.
    """

    samples: tuple[RequestMeasurement, ...]

    def __post_init__(self) -> None:
        """Запретить выдавать неполную или переставленную серию за итог.

        Raises:
            ValueError: Отсутствует запрос или нарушен порядок серии.
        """
        if tuple(item.index for item in self.samples) != tuple(
            range(1, REQUEST_COUNT + 1)
        ):
            message = "Итог требует десяти завершённых запросов по порядку."
            raise ValueError(message)

    @property
    def total_bytes(self) -> int:
        """Вернуть суммарный размер переданных тел."""
        return sum(item.total_bytes for item in self.samples)

    @property
    def total_seconds(self) -> float:
        """Вернуть сумму длительностей без времени интерфейса между запросами."""
        return math.fsum(item.elapsed_seconds for item in self.samples)

    @property
    def average_seconds(self) -> float:
        """Вернуть среднюю длительность полного запроса в секундах."""
        return self.total_seconds / REQUEST_COUNT

    @property
    def mebibytes_per_second(self) -> float:
        """Вернуть скорость по общему объёму и суммарному времени."""
        return self.total_bytes / self.total_seconds / BYTES_PER_MEBIBYTE


def build_http_client(
    *, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
) -> httpx.AsyncClient:
    """Создать клиент; закрывает его вызывающий код через async with.

    Args:
        timeout_seconds: Предельное ожидание каждой сетевой операции.

    Returns:
        Клиент с проверкой TLS, без редиректов и автоматических повторов.
        Прокси и доверенные сертификаты берутся из окружения HTTPX;
        три точных loopback-адреса соединяются напрямую.
    """
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=False,
        trust_env=True,
        # Локальное демо работает и без NO_PROXY; внешние адреса сохраняют прокси.
        mounts={"all://localhost": None, "all://127.0.0.1": None, "all://[::1]": None},
        headers={"Accept-Encoding": "identity", "Cache-Control": "no-cache"},
    )


async def _count_body(response: httpx.Response) -> int:
    """Дочитать успешный ответ до EOF и посчитать переданные байты тела.

    Args:
        response: Открытый поток ответа; его жизненным циклом владеет вызывающий.

    Returns:
        Размер без HTTP-заголовков и служебных данных TCP/TLS.

    Raises:
        httpx.HTTPError: Неуспешный статус или ошибка при чтении тела.
    """
    response.raise_for_status()
    total_bytes = 0
    # aiter_bytes распаковывает gzip; для скорости нужны байты до распаковки.
    async for chunk in response.aiter_raw():
        total_bytes += len(chunk)
    return total_bytes


def _request_error_message(error: httpx.HTTPError | TimeoutError) -> str:
    """Описать сбой без URL, токенов и тела ответа.

    Args:
        error: Ошибка HTTPX или полного таймаута запроса.

    Returns:
        Сообщение для stderr.
    """
    if isinstance(error, (TimeoutError, httpx.TimeoutException)):
        return "превышен таймаут. При необходимости увеличьте --timeout."
    if isinstance(error, httpx.HTTPStatusError):
        return (
            f"сервер вернул HTTP {error.response.status_code}. "
            "Нужен прямой адрес с успешным ответом 2xx."
        )
    return f"ошибка сети или передачи данных ({type(error).__name__})."


async def _download(
    client: httpx.AsyncClient,
    url: httpx.URL,
    *,
    request_number: int,
    timeout_seconds: float,
) -> int:
    """Загрузить одно тело с ограничением общей длительности запроса.

    Args:
        client: Открытый HTTP-клиент.
        url: Прямой HTTP(S)-адрес ресурса.
        request_number: Номер запроса для сообщения об ошибке.
        timeout_seconds: Предел времени от отправки запроса до конца чтения.

    Returns:
        Количество полностью прочитанных байтов тела.

    Raises:
        MeasurementError: Сеть, HTTP-статус, оборванный ответ или таймаут.
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            async with client.stream("GET", url, follow_redirects=False) as response:
                return await _count_body(response)
    except (httpx.HTTPError, TimeoutError) as error:
        message = (
            f"Запрос {request_number}/{REQUEST_COUNT}: "
            f"{_request_error_message(error)} Замер не завершён."
        )
        raise MeasurementError(message) from error


async def iter_samples(
    client: httpx.AsyncClient,
    url: httpx.URL,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    clock: Callable[[], float] = perf_counter,
) -> AsyncGenerator[RequestMeasurement]:
    """Отдавать результат после каждой из десяти последовательных загрузок.

    Args:
        client: Открытый клиент, которым владеет вызывающий код.
        url: Проверенный прямой HTTP(S)-адрес.
        timeout_seconds: Максимальная длительность полной загрузки.
        clock: Монотонные часы; подменяются в тестах.

    Yields:
        Неизменяемый результат; таймер остановлен до передачи в интерфейс.

    Raises:
        MeasurementError: Загрузка не завершилась успешно.
    """
    for index in range(1, REQUEST_COUNT + 1):
        started = clock()
        downloaded_bytes = await _download(
            client,
            url,
            request_number=index,
            timeout_seconds=timeout_seconds,
        )
        elapsed = clock() - started
        yield RequestMeasurement(
            index=index, total_bytes=downloaded_bytes, elapsed_seconds=elapsed
        )


async def measure(
    client: httpx.AsyncClient,
    url: httpx.URL,
    *,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    clock: Callable[[], float] = perf_counter,
) -> Measurement:
    """Собрать полную серию с тем же алгоритмом, что использует веб-интерфейс.

    Args:
        client: Клиент, создаваемый и закрываемый вызывающим кодом.
        url: Проверенный URL ресурса.
        timeout_seconds: Максимальная длительность одного запроса.
        clock: Источник монотонного времени.

    Returns:
        Результат десяти завершённых запросов.

    Raises:
        MeasurementError: Один из запросов не завершён.
    """
    samples: list[RequestMeasurement] = []
    async with aclosing(
        iter_samples(client, url, timeout_seconds=timeout_seconds, clock=clock)
    ) as stream:
        async for sample in stream:
            samples.append(sample)
    return Measurement(samples=tuple(samples))
