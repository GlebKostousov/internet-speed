"""Проверки расчёта, полного чтения ответа и отказов без внешней сети."""

import asyncio
import gzip
from collections.abc import AsyncIterator, Callable
from time import perf_counter

import httpx
import pytest

import speedtest

TEST_URL = httpx.URL("https://example.test/file.bin")


class _ChunkStream(httpx.AsyncByteStream):
    """Отдать несколько частей тела и сохранить факт закрытия потока."""

    def __init__(
        self,
        chunks: tuple[bytes, ...],
        *,
        error: httpx.RequestError | None = None,
    ) -> None:
        """Сохранить части ответа и необязательную ошибку после них."""
        self.chunks = chunks
        self.error = error
        self.exhausted = False
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Отдать части с переключением задач и завершить чтение либо ошибкой."""
        for chunk in self.chunks:
            await asyncio.sleep(0)
            yield chunk
        if self.error is not None:
            raise self.error
        self.exhausted = True

    async def aclose(self) -> None:
        """Зафиксировать освобождение потока."""
        self.closed = True


class _WaitingStream(httpx.AsyncByteStream):
    """Начать передачу тела и ждать таймаута либо внешней отмены."""

    def __init__(self) -> None:
        """Подготовить сигнал начала чтения и состояние закрытия."""
        self.started = asyncio.Event()
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Отдать первую часть и приостановиться до отмены задачи."""
        yield b"partial body"
        self.started.set()
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        """Зафиксировать закрытие незавершённого ответа."""
        self.closed = True


async def _measure(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    clock: Callable[[], float] = perf_counter,
    timeout_seconds: float = 30.0,
) -> speedtest.Measurement:
    """Запустить замер с тестовым HTTP-транспортом и закрыть клиент.

    Args:
        handler: функция ответа без обращения к сети.
        clock: источник времени для точной проверки формулы.
        timeout_seconds: ограничение длительности одного запроса.

    Returns:
        Результат завершённой серии запросов.
    """
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await speedtest.measure(
            client,
            TEST_URL,
            clock=clock,
            timeout_seconds=timeout_seconds,
        )


def test_ten_complete_responses_are_sequential_and_speed_uses_total_time() -> None:
    """Следующий GET начинается после EOF; скорость считается по сумме времён."""
    streams: list[_ChunkStream] = []
    ticks: list[float] = []
    cursor = 0.0
    for duration in range(1, 11):
        ticks.extend((cursor, cursor + duration))
        cursor += duration + 7.0
    clock = iter(ticks)

    def handler(request: httpx.Request) -> httpx.Response:
        """Проверить порядок запросов и выдать тело без Content-Length."""
        assert request.method == "GET"
        assert request.url == TEST_URL
        assert all(stream.exhausted and stream.closed for stream in streams)
        stream = _ChunkStream((b"a" * 16_384, b"b" * 8_192))
        streams.append(stream)
        return httpx.Response(200, stream=stream)

    result = asyncio.run(_measure(handler, clock=clock.__next__))

    assert len(streams) == 10
    assert all(stream.exhausted and stream.closed for stream in streams)
    assert result.total_bytes == 245_760
    assert result.total_seconds == pytest.approx(55.0)
    assert result.average_seconds == pytest.approx(5.5)
    assert result.mebibytes_per_second == pytest.approx(245_760 / 55 / 1024**2)


def test_compressed_response_counts_transferred_bytes() -> None:
    """Объём gzip берётся до распаковки, даже если сервер прислал сжатие."""
    compressed = gzip.compress(b"repeated content" * 1000)
    clock = iter(float(value) for value in range(20))

    def handler(request: httpx.Request) -> httpx.Response:
        """Вернуть сжатый поток вместо заранее прочитанного тела."""
        return httpx.Response(
            200,
            request=request,
            headers={"Content-Encoding": "gzip"},
            stream=_ChunkStream((compressed[:10], compressed[10:])),
        )

    result = asyncio.run(_measure(handler, clock=clock.__next__))

    assert result.total_bytes == len(compressed) * 10
    assert result.mebibytes_per_second == pytest.approx(len(compressed) / 1024**2)


def test_empty_responses_have_zero_volume_and_speed() -> None:
    """Пустое успешное тело даёт нулевой объём при измеренном времени."""

    def handler(request: httpx.Request) -> httpx.Response:
        """Вернуть пустой ответ с корректным завершением потока."""
        return httpx.Response(200, request=request, stream=_ChunkStream(()))

    result = asyncio.run(_measure(handler))

    assert result.total_bytes == 0
    assert result.mebibytes_per_second == 0.0


@pytest.mark.parametrize("status_code", [302, 404, 500])
def test_bad_status_stops_on_third_request(status_code: int) -> None:
    """Редирект и HTTP-ошибки прерывают серию без повторов и утечки ответа."""
    streams: list[_ChunkStream] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """Вернуть ошибку на третьем обращении."""
        stream = _ChunkStream((b"response body",))
        streams.append(stream)
        return httpx.Response(
            status_code if len(streams) == 3 else 200,
            request=request,
            headers={"Location": "https://example.test/other.bin"},
            stream=stream,
        )

    with pytest.raises(speedtest.MeasurementError, match="Запрос 3/10"):
        asyncio.run(_measure(handler))

    assert len(streams) == 3
    assert all(stream.closed for stream in streams)


def test_broken_body_stops_series_and_closes_stream() -> None:
    """Частично прочитанный ответ не превращается в успешный замер."""
    streams: list[_ChunkStream] = []

    def handler(request: httpx.Request) -> httpx.Response:
        """Оборвать тело третьего ответа после первой части."""
        error = httpx.ReadError("connection closed") if len(streams) == 2 else None
        stream = _ChunkStream((b"partial body",), error=error)
        streams.append(stream)
        return httpx.Response(200, request=request, stream=stream)

    with pytest.raises(speedtest.MeasurementError, match="Запрос 3/10"):
        asyncio.run(_measure(handler))

    assert len(streams) == 3
    assert all(stream.closed for stream in streams)
    assert not streams[-1].exhausted


def test_whole_request_timeout_closes_unfinished_response() -> None:
    """Общий таймаут действует и на ожидание продолжения тела ответа."""
    stream = _WaitingStream()

    def handler(request: httpx.Request) -> httpx.Response:
        """Начать ответ, который сам не завершится."""
        return httpx.Response(200, request=request, stream=stream)

    with pytest.raises(speedtest.MeasurementError, match="Запрос 1/10"):
        asyncio.run(_measure(handler, timeout_seconds=0.05))

    assert stream.started.is_set()
    assert stream.closed


def test_external_cancellation_propagates_and_closes_response() -> None:
    """Отмена задачи сохраняет CancelledError и освобождает сетевой ресурс."""
    stream = _WaitingStream()

    def handler(request: httpx.Request) -> httpx.Response:
        """Вернуть ответ, ожидающий дальнейших байтов."""
        return httpx.Response(200, request=request, stream=stream)

    async def cancel_measurement() -> None:
        """Отменить замер после начала чтения тела."""
        task = asyncio.create_task(_measure(handler))
        await asyncio.wait_for(stream.started.wait(), timeout=1.0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel_measurement())

    assert stream.closed


@pytest.mark.parametrize(
    "arguments",
    [
        ["file:///tmp/file.bin"],
        ["http://"],
        ["example.test/file.bin"],
        ["http://127.0.0.1:0/file.bin"],
        ["http://127.0.0.1:-1/file.bin"],
        ["http://127.0.0.1:65536/file.bin"],
        ["http://127.0.0.1:9999999999999999999999/file.bin"],
        [str(TEST_URL), "--timeout", "0"],
        [str(TEST_URL), "--timeout", "-1"],
        [str(TEST_URL), "--timeout", "nan"],
        [str(TEST_URL), "--timeout", "inf"],
        [str(TEST_URL), "--timeout", "invalid"],
    ],
)
def test_invalid_cli_arguments_exit_before_network(arguments: list[str]) -> None:
    """Некорректные адреса и таймауты отклоняются на границе CLI."""
    with pytest.raises(SystemExit) as caught:
        speedtest.main(arguments)

    assert caught.value.code == 2


def test_cli_failure_has_nonzero_exit_without_success_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """При сетевой ошибке CLI печатает только ошибку и возвращает код 1."""

    def handler(request: httpx.Request) -> httpx.Response:
        """Вернуть HTTP-ошибку без обращения к внешнему серверу."""
        return httpx.Response(500, request=request, stream=_ChunkStream((b"error",)))

    def build_client(*, timeout_seconds: float = 30.0) -> httpx.AsyncClient:
        """Подставить тестовый транспорт в штатную сборку CLI."""
        return httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=timeout_seconds,
        )

    monkeypatch.setattr(speedtest, "build_http_client", build_client)

    assert speedtest.main([str(TEST_URL)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Запрос 1/10" in captured.err


@pytest.mark.parametrize(
    ("status_code", "expected_phrase"),
    [
        (404, r"файл не найден \(HTTP 404\)"),
        (403, "доступ к файлу запрещён"),
        (301, "перенаправление HTTP 301"),
    ],
)
def test_specific_http_status_error_messages(
    status_code: int, expected_phrase: str
) -> None:
    """Ошибки 404, 403 и редиректы 301 дают понятное описание причины."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, request=request, stream=_ChunkStream(()))

    with pytest.raises(speedtest.MeasurementError, match=expected_phrase):
        asyncio.run(_measure(handler))


def _frozen_clock() -> float:
    return 100.0


def _frozen_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, request=request, stream=_ChunkStream((b"bytes",)))


def test_zero_duration_clock_is_safe_against_division_by_zero() -> None:
    """Одинаковое значение часов не вызывает деления на ноль."""
    result = asyncio.run(_measure(_frozen_handler, clock=_frozen_clock))
    assert result.total_bytes == 50
    assert result.total_seconds > 0
    assert result.mebibytes_per_second > 0
