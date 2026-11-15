/** Форматировать только представление; исходные числа сохраняются для выгрузки. */
export function number(value: number, digits = 2): string {
  return new Intl.NumberFormat('ru-RU', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
}

export function duration(seconds: number): string {
  if (seconds < 1) return `${number(seconds * 1000, 0)} мс`;
  return `${number(seconds, 2)} с`;
}

export function volume(bytes: number): string {
  if (bytes < 1024) return `${number(bytes, 0)} Б`;
  if (bytes < 1024 ** 2) return `${number(bytes / 1024, 1)} KiB`;
  return `${number(bytes / 1024 ** 2, 2)} MiB`;
}
