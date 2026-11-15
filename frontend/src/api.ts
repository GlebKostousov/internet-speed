/** Проверяемый контракт потокового API; данные JSON остаются unknown до разбора. */
export type Sample = Readonly<{
  index: number;
  bytes: number;
  seconds: number;
  speed_mib_s: number;
}>;

export type Result = Readonly<{
  type: 'complete';
  request_count: number;
  total_bytes: number;
  total_seconds: number;
  average_seconds: number;
  speed_mib_s: number;
  samples: readonly Sample[];
}>;

export type MeasurementEvent =
  | Readonly<{ type: 'started'; request_count: number }>
  | (Sample & Readonly<{ type: 'sample' }>)
  | Result
  | Readonly<{ type: 'error'; message: string }>;

const REQUEST_COUNT = 10;
const MAX_LINE_LENGTH = 64 * 1024;
const CONTRACT_ERROR = 'Сервер вернул неполные или некорректные данные замера.';

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(CONTRACT_ERROR);
  }
  return value as Record<string, unknown>;
}

function finite(value: unknown, minimum = 0): number {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < minimum) {
    throw new Error(CONTRACT_ERROR);
  }
  return value;
}

function integer(value: unknown, minimum = 0): number {
  const result = finite(value, minimum);
  if (!Number.isSafeInteger(result)) throw new Error(CONTRACT_ERROR);
  return result;
}

function sample(value: unknown): Sample {
  const item = record(value);
  const index = integer(item.index, 1);
  const seconds = finite(item.seconds, Number.MIN_VALUE);
  if (index > REQUEST_COUNT) throw new Error(CONTRACT_ERROR);
  return {
    index,
    bytes: integer(item.bytes),
    seconds,
    speed_mib_s: finite(item.speed_mib_s),
  };
}

function parseEvent(line: string): MeasurementEvent {
  const raw: unknown = JSON.parse(line);
  const item = record(raw);
  if (item.type === 'error' && typeof item.message === 'string') {
    return { type: 'error', message: item.message.slice(0, 2000) };
  }
  if (item.type === 'sample') return { type: 'sample', ...sample(item) };
  if (item.type === 'started' && item.request_count === REQUEST_COUNT) {
    return { type: 'started', request_count: REQUEST_COUNT };
  }
  if (item.type !== 'complete' || item.request_count !== REQUEST_COUNT) {
    throw new Error(CONTRACT_ERROR);
  }
  if (!Array.isArray(item.samples) || item.samples.length !== REQUEST_COUNT) {
    throw new Error(CONTRACT_ERROR);
  }
  const samples = item.samples.map(sample);
  if (samples.some((entry, index) => entry.index !== index + 1)) {
    throw new Error(CONTRACT_ERROR);
  }
  return {
    type: 'complete',
    request_count: REQUEST_COUNT,
    total_bytes: integer(item.total_bytes),
    total_seconds: finite(item.total_seconds, Number.MIN_VALUE),
    average_seconds: finite(item.average_seconds, Number.MIN_VALUE),
    speed_mib_s: finite(item.speed_mib_s),
    samples,
  };
}

async function responseError(response: Response): Promise<string> {
  try {
    const raw: unknown = await response.json();
    const value = record(raw);
    if (typeof value.message === 'string') return value.message.slice(0, 2000);
  } catch {
    // Ответ прокси или другого сервера может не иметь JSON-тела.
  }
  return response.status === 429
    ? 'Другой замер уже выполняется. Дождитесь его завершения и попробуйте ещё раз.'
    : `Не удалось начать замер (HTTP ${response.status}).`;
}

/** Дочитать серию до EOF; complete публикуется только после проверки всего потока. */
export async function runMeasurement(
  url: string,
  timeoutSeconds: number,
  signal: AbortSignal,
  onEvent: (event: MeasurementEvent) => void,
): Promise<void> {
  const endpoint =
    typeof window !== 'undefined'
      ? new URL('api/measure', window.location.href).href
      : '/api/measure';
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ url, timeout_seconds: timeoutSeconds }),
    signal,
  });
  if (!response.ok) throw new Error(await responseError(response));
  if (response.body === null) throw new Error('Сервер не передал результаты замера.');

  const reader = response.body.getReader();
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let buffer = '';
  let started = false;
  let completedSamples = 0;
  let completedResult: Result | null = null;
  let reachedEof = false;

  function consume(line: string): void {
    if (!line.trim()) return;
    if (line.length > MAX_LINE_LENGTH || completedResult !== null) throw new Error(CONTRACT_ERROR);
    const event = parseEvent(line);
    if (event.type === 'error') throw new Error(event.message);
    if (event.type === 'started') {
      if (started || completedSamples !== 0) throw new Error(CONTRACT_ERROR);
      started = true;
    } else if (event.type === 'sample') {
      if (!started || event.index !== completedSamples + 1) throw new Error(CONTRACT_ERROR);
      completedSamples += 1;
    } else {
      if (!started || completedSamples !== REQUEST_COUNT) throw new Error(CONTRACT_ERROR);
      completedResult = event;
      return;
    }
    onEvent(event);
  }

  try {
    while (!reachedEof) {
      const chunk = await reader.read();
      reachedEof = chunk.done;
      buffer += decoder.decode(chunk.value, { stream: !chunk.done });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) consume(line);
      if (buffer.length > MAX_LINE_LENGTH) throw new Error(CONTRACT_ERROR);
      if (chunk.done) {
        if (buffer.trim()) consume(buffer);
      }
    }
    if (completedResult === null) {
      throw new Error('Соединение прервалось до завершения десяти загрузок. Повторите замер.');
    }
    onEvent(completedResult);
  } finally {
    if (!reachedEof) {
      try {
        await reader.cancel();
      } catch {
        // abort мог уже закрыть поток; важен исходный результат операции.
      }
    }
    reader.releaseLock();
  }
}
