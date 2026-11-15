"""Консольный запуск измерителя; веб-интерфейс запускается через web_app.py."""

import argparse
import asyncio
import math
import sys
from collections.abc import Sequence

import httpx

from measurement import (
    DEFAULT_TIMEOUT_SECONDS,
    REQUEST_COUNT,
    Measurement,
    MeasurementError,
    build_http_client,
    measure,
)

__all__ = [
    "Measurement",
    "MeasurementError",
    "REQUEST_COUNT",
    "build_http_client",
    "measure",
    "main",
]


def _http_url(value: str) -> httpx.URL:
    """Проверить адрес из аргумента командной строки.

    Args:
        value: Пользовательский URL.

    Returns:
        Абсолютный HTTP(S)-адрес с непустым хостом.

    Raises:
        argparse.ArgumentTypeError: Адрес некорректен или использует другую схему.
    """
    try:
        url = httpx.URL(value)
    except httpx.InvalidURL as error:
        message = "некорректный URL."
        raise argparse.ArgumentTypeError(message) from error
    if (
        url.scheme not in {"http", "https"}
        or not url.host
        or (url.port is not None and not 1 <= url.port <= 65535)
    ):
        message = "укажите полный HTTP(S)-адрес с хостом и портом от 1 до 65535."
        raise argparse.ArgumentTypeError(message)
    return url


def _positive_seconds(value: str) -> float:
    """Проверить конечный положительный таймаут.

    Args:
        value: Число секунд из командной строки.

    Returns:
        Конечное значение больше нуля.

    Raises:
        argparse.ArgumentTypeError: Значение не подходит для таймаута.
    """
    message = "таймаут должен быть конечным числом больше нуля."
    try:
        seconds = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(message) from error
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError(message)
    return seconds


class _Arguments(argparse.Namespace):
    """Типизированные аргументы, которые заполняет argparse."""

    url: httpx.URL
    timeout: float


def _parse_args(argv: Sequence[str] | None) -> _Arguments:
    """Разобрать и проверить аргументы CLI.

    Args:
        argv: Аргументы без имени скрипта; None читает sys.argv.

    Returns:
        Проверенные URL и таймаут.
    """
    parser = argparse.ArgumentParser(
        description="Скорость десяти последовательных загрузок. Единица: MiB/s.",
    )
    parser.add_argument(
        "url", metavar="URL", type=_http_url, help="прямой HTTP(S)-адрес"
    )
    parser.add_argument(
        "--timeout",
        type=_positive_seconds,
        default=DEFAULT_TIMEOUT_SECONDS,
        metavar="SECONDS",
        help="таймаут одной полной загрузки в секундах (по умолчанию: %(default)s)",
    )
    arguments = _Arguments()
    parser.parse_args(argv, namespace=arguments)
    return arguments


async def _run(url: httpx.URL, *, timeout_seconds: float) -> Measurement:
    """Создать клиент, выполнить замер и закрыть ресурсы.

    Args:
        url: Проверенный URL ресурса.
        timeout_seconds: Ограничение длительности одной загрузки.

    Returns:
        Полный результат замера.

    Raises:
        MeasurementError: Загрузка не завершилась успешно.
    """
    async with build_http_client(timeout_seconds=timeout_seconds) as client:
        return await measure(client, url, timeout_seconds=timeout_seconds)


def main(argv: Sequence[str] | None = None) -> int:
    """Выполнить CLI и вернуть код завершения.

    Args:
        argv: Аргументы без имени скрипта; None читает sys.argv.

    Returns:
        0 при успехе, 1 при сбое замера, 130 при Ctrl+C.

    Raises:
        SystemExit: argparse завершает процесс с кодом 2 при неверных аргументах
            или с кодом 0 при --help.
    """
    arguments = _parse_args(argv)
    try:
        result = asyncio.run(_run(arguments.url, timeout_seconds=arguments.timeout))
    except MeasurementError as error:
        print(f"Ошибка: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Замер прерван пользователем.", file=sys.stderr)
        return 130
    print(
        f"Завершено запросов: {REQUEST_COUNT}\n"
        f"Среднее время запроса: {result.average_seconds:.6f} с\n"
        f"Суммарное время запросов: {result.total_seconds:.6f} с\n"
        f"Скачано: {result.total_bytes} байт\n"
        f"Скорость скачивания: {result.mebibytes_per_second:.6f} MiB/s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
