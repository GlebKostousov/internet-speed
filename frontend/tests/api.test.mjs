import assert from 'node:assert/strict';
import test from 'node:test';
import { runMeasurement } from '../src/api.ts';

const samples = Array.from({ length: 10 }, (_, index) => ({
  index: index + 1,
  bytes: 1024,
  seconds: (index + 1) / 10,
  speed_mib_s: 1024 / ((index + 1) / 10) / 1024 ** 2,
}));
const result = {
  type: 'complete',
  request_count: 10,
  total_bytes: 10 * 1024,
  total_seconds: 5.5,
  average_seconds: 0.55,
  speed_mib_s: 10 * 1024 / 5.5 / 1024 ** 2,
  samples,
};

function ndjson(events) {
  return events.map((event) => JSON.stringify(event)).join('\n') + '\n';
}

function streamResponse(text, chunkSize = 7) {
  const encoded = new TextEncoder().encode(text);
  return new Response(new ReadableStream({
    start(controller) {
      for (let offset = 0; offset < encoded.length; offset += chunkSize) {
        controller.enqueue(encoded.slice(offset, offset + chunkSize));
      }
      controller.close();
    },
  }), { headers: { 'Content-Type': 'application/x-ndjson' } });
}

test('полная серия разбирается при произвольных границах сетевых частей', async (context) => {
  const events = [
    { type: 'started', request_count: 10 },
    ...samples.map((sample) => ({ type: 'sample', ...sample })),
    result,
  ];
  context.mock.method(globalThis, 'fetch', async (path, request) => {
    assert.equal(path, '/api/measure');
    assert.equal(request.method, 'POST');
    assert.deepEqual(JSON.parse(request.body), {
      url: 'https://example.test/file', timeout_seconds: 30,
    });
    return streamResponse(ndjson(events));
  });
  const observed = [];
  await runMeasurement('https://example.test/file', 30, new AbortController().signal, (event) => observed.push(event));
  assert.equal(observed.length, 12);
  assert.deepEqual(observed.at(-1), result);
});

test('разрыв после частичного результата не становится успешным итогом', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => streamResponse(ndjson([
    { type: 'started', request_count: 10 },
    { type: 'sample', ...samples[0] },
  ])));
  const observed = [];
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, (event) => observed.push(event)),
    /Соединение прервалось/,
  );
  assert.equal(observed.some((event) => event.type === 'complete'), false);
});

test('нарушенный порядок ответов отклоняется', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => streamResponse(ndjson([
    { type: 'started', request_count: 10 },
    { type: 'sample', ...samples[2] },
  ])));
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, () => {}),
    /некорректные данные/,
  );
});

test('ошибка 429 передаётся пользователю без успешного результата', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => Response.json(
    { message: 'Другой замер уже выполняется.' }, { status: 429 },
  ));
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, () => assert.fail('unexpected event')),
    /Другой замер уже выполняется/,
  );
});

test('ошибка потока корректно декодируется даже по одному UTF-8 байту', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => streamResponse(
    ndjson([{ type: 'error', message: 'Запрос 1/10: превышен таймаут.' }]), 1,
  ));
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, () => {}),
    /Запрос 1\/10: превышен таймаут/,
  );
});

test('размер одной строки ограничен до JSON-разбора', async (context) => {
  context.mock.method(globalThis, 'fetch', async () => streamResponse('x'.repeat(65_537) + '\n', 4096));
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, () => {}),
    /некорректные данные/,
  );
});

test('успех ждёт EOF после complete и не отменяет успешно прочитанный ответ', async (context) => {
  const sawLastSample = Promise.withResolvers();
  const observed = [];
  let producer;
  let cancellations = 0;
  const response = new Response(new ReadableStream({
    start(controller) {
      producer = controller;
      controller.enqueue(new TextEncoder().encode(ndjson([
        { type: 'started', request_count: 10 },
        ...samples.map((sample) => ({ type: 'sample', ...sample })),
        result,
      ])));
      // Сервер ещё завершает ответ: complete уже передан, EOF пока нет.
    },
    cancel() { cancellations += 1; },
  }));
  context.mock.method(globalThis, 'fetch', async () => response);
  let settled = false;
  const measurement = runMeasurement(
    'https://example.test/file', 30, new AbortController().signal,
    (event) => {
      observed.push(event);
      if (event.type === 'sample' && event.index === 10) sawLastSample.resolve();
    },
  ).then(() => { settled = true; });

  await sawLastSample.promise;
  assert.equal(settled, false);
  assert.equal(cancellations, 0);
  assert.equal(observed.some((event) => event.type === 'complete'), false);

  producer.close();
  await measurement;
  assert.equal(settled, true);
  assert.equal(cancellations, 0);
  assert.equal(response.body.locked, false);
  assert.deepEqual(observed.at(-1), result);
});

test('лишнее событие после complete проверяется и в следующих сетевых частях', async (context) => {
  const parts = [
    ndjson([
      { type: 'started', request_count: 10 },
      ...samples.map((sample) => ({ type: 'sample', ...sample })),
      result,
    ]),
    '{"type":"sta',
    'rted","request_count":10}\n',
  ];
  let nextPart = 0;
  context.mock.method(globalThis, 'fetch', async () => new Response(new ReadableStream({
    pull(controller) {
      if (nextPart === parts.length) {
        controller.close();
        return;
      }
      controller.enqueue(new TextEncoder().encode(parts[nextPart]));
      nextPart += 1;
    },
  })));
  const observed = [];
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, (event) => observed.push(event)),
    /некорректные данные/,
  );
  assert.equal(observed.some((event) => event.type === 'complete'), false);
});

test('ранняя ошибка отменяет открытый поток и освобождает reader', async (context) => {
  let cancellations = 0;
  const response = new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(ndjson([
        { type: 'started', request_count: 10 },
        { type: 'sample', ...samples[2] },
      ])));
    },
    cancel() { cancellations += 1; },
  }));
  context.mock.method(globalThis, 'fetch', async () => response);
  await assert.rejects(
    runMeasurement('https://example.test/file', 30, new AbortController().signal, () => {}),
    /некорректные данные/,
  );
  assert.equal(cancellations, 1);
  assert.equal(response.body.locked, false);
});
