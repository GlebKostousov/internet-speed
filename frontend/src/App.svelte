<script lang="ts">
  import { onDestroy } from 'svelte';
  import { runMeasurement } from './api';
  import type { MeasurementEvent, Result, Sample } from './api';
  import { duration, number, volume } from './format';

  type Phase = 'idle' | 'running' | 'stopping' | 'done' | 'error' | 'cancelled';
  const REQUEST_COUNT = 10;
  const positions = Array.from({ length: REQUEST_COUNT }, (_, index) => index);
  const labels: Record<Phase, string> = {
    idle: 'Готов к замеру',
    running: 'Идёт измерение',
    stopping: 'Останавливаем',
    done: 'Замер завершён',
    error: 'Замер не завершён',
    cancelled: 'Замер остановлен',
  };

  const STORAGE_KEY_URL = 'internet_speed_url';
  const STORAGE_KEY_TIMEOUT = 'internet_speed_timeout';

  function getStoredUrl(): string {
    if (typeof window === 'undefined') return '';
    try {
      return window.localStorage.getItem(STORAGE_KEY_URL) ?? '';
    } catch {
      return '';
    }
  }

  function getStoredTimeout(): number {
    if (typeof window === 'undefined') return 30;
    try {
      const saved = Number(window.localStorage.getItem(STORAGE_KEY_TIMEOUT));
      return Number.isFinite(saved) && saved >= 1 && saved <= 120 ? saved : 30;
    } catch {
      return 30;
    }
  }

  let url = $state(getStoredUrl());
  let timeoutSeconds = $state<number>(getStoredTimeout());

  $effect(() => {
    if (typeof window !== 'undefined') {
      try {
        window.localStorage.setItem(STORAGE_KEY_URL, url);
        window.localStorage.setItem(STORAGE_KEY_TIMEOUT, String(timeoutSeconds));
      } catch {
        // Игнорируем недоступность хранилища в приватном режиме
      }
    }
  });
  let phase = $state<Phase>('idle');
  let samples = $state<Sample[]>([]);
  let result = $state.raw<Result | null>(null);
  let errorMessage = $state('');
  let runHost = $state('');
  let runIsDemo = $state(false);
  let focusedSample = $state<number | null>(null);
  let controller: AbortController | null = null;
  let activeTask: Promise<void> | null = null;
  let generation = 0;

  const busy = $derived(phase === 'running' || phase === 'stopping');
  const totalBytes = $derived(samples.reduce((sum, entry) => sum + entry.bytes, 0));
  const totalSeconds = $derived(samples.reduce((sum, entry) => sum + entry.seconds, 0));
  const showNumbers = $derived(phase === 'done' || phase === 'running');
  const speed = $derived(
    showNumbers && totalSeconds > 0
      ? (result?.speed_mib_s ?? totalBytes / totalSeconds / 1024 ** 2)
      : null,
  );
  const averageSeconds = $derived(
    samples.length > 0 ? (result?.average_seconds ?? totalSeconds / samples.length) : null,
  );
  const chartMaximum = $derived(Math.max(...samples.map((entry) => entry.seconds), 0.001));
  const selectedSample = $derived(
    focusedSample === null ? samples[samples.length - 1] : samples[focusedSample],
  );
  const nextRequest = $derived(Math.min(samples.length + 1, REQUEST_COUNT));

  function acceptEvent(event: MeasurementEvent, currentGeneration: number): void {
    if (currentGeneration !== generation) return;
    if (event.type === 'sample') samples = [...samples, event];
    if (event.type === 'complete') result = event;
  }

  async function execute(
    address: string,
    timeout: number,
    currentGeneration: number,
    abort: AbortController,
  ): Promise<void> {
    try {
      await runMeasurement(address, timeout, abort.signal, (event) => {
        acceptEvent(event, currentGeneration);
      });
      if (currentGeneration === generation) phase = 'done';
    } catch (error: unknown) {
      if (currentGeneration !== generation) return;
      if (abort.signal.aborted) {
        phase = 'cancelled';
      } else {
        phase = 'error';
        errorMessage = error instanceof Error ? error.message : 'Не удалось выполнить замер.';
      }
      result = null;
    } finally {
      if (currentGeneration === generation) {
        controller = null;
        activeTask = null;
      }
    }
  }

  function launch(address: string): void {
    if (busy) return;
    let target: URL;
    try {
      target = new URL(address.trim());
      if (!['http:', 'https:'].includes(target.protocol)) throw new Error('scheme');
    } catch {
      phase = 'error';
      errorMessage = 'Укажите полную ссылку на файл, которая начинается с http:// или https://.';
      return;
    }
    if (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 1 || timeoutSeconds > 120) {
      phase = 'error';
      errorMessage = 'Таймаут должен быть от 1 до 120 секунд.';
      return;
    }
    generation += 1;
    const abort = new AbortController();
    controller = abort;
    samples = [];
    result = null;
    errorMessage = '';
    focusedSample = null;
    runHost = target.host;
    runIsDemo = target.href === new URL('demo.bin', window.location.href).href;
    phase = 'running';
    activeTask = execute(target.href, timeoutSeconds, generation, abort);
  }

  function startDemo(): void {
    if (busy) return;
    url = new URL('demo.bin', window.location.href).href;
    launch(url);
  }

  async function stop(): Promise<void> {
    if (controller === null || phase !== 'running') return;
    const task = activeTask;
    generation += 1;
    phase = 'stopping';
    controller.abort();
    await task;
    controller = null;
    activeTask = null;
    result = null;
    phase = 'cancelled';
  }

  function downloadResult(): void {
    if (result === null || phase !== 'done') return;
    const document = JSON.stringify(result, null, 2);
    const download = URL.createObjectURL(new Blob([document], { type: 'application/json' }));
    const anchor = window.document.createElement('a');
    anchor.href = download;
    anchor.download = 'internet-speed-result.json';
    anchor.click();
    URL.revokeObjectURL(download);
  }

  onDestroy(() => {
    generation += 1;
    controller?.abort();
  });
</script>

<svelte:head><title>Замер — скорость загрузки</title></svelte:head>

<div class="app-shell">
  <header class="topbar">
    <div class="wordmark" aria-label="Замер">
      <span class="brand-mark" aria-hidden="true">
        <svg viewBox="0 0 28 28" fill="none"><path d="M14 4v15m-6-6 6 6 6-6M7 24h14" /></svg>
      </span>
      <span>замер<span class="brand-period">.</span></span>
    </div>
    <div class="header-note"><span class="tiny-line" aria-hidden="true"></span>10 запросов. Последовательно.</div>
  </header>

  <main>
    <section class="intro" aria-labelledby="page-title">
      <div>
        <p class="eyebrow">ПРОВЕРКА ЗАГРУЗКИ</p>
        <h1 id="page-title">Скорость загрузки — в деталях.</h1>
        <p class="lead">Укажите файл. Мы скачаем его десять раз и покажем каждый замер.</p>
      </div>
      <div class="measurement-origin">
        <svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><rect x="3" y="4" width="18" height="12" rx="2" /><path d="M8 20h8m-4-4v4" /></svg>
        <span>С компьютера, <br />где запущен Python</span>
      </div>
    </section>

    <section class="input-section" aria-label="Параметры замера">
      <form onsubmit={(event) => { event.preventDefault(); launch(url); }}>
        <label class="field-label" for="download-url">Прямая ссылка на файл</label>
        <div class="input-line">
          <div class="url-field">
            <svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="m10 13 4-4m-6.5 7.5-1 1a3.54 3.54 0 0 1-5-5l4-4a3.54 3.54 0 0 1 5 0m3 1 1-1a3.54 3.54 0 0 1 5 5l-4 4a3.54 3.54 0 0 1-5 0" transform="translate(1 -1)" /></svg>
            <input id="download-url" name="url" type="url" bind:value={url} required disabled={busy} spellcheck="false" autocomplete="off" placeholder="https://example.com/big-file.bin" aria-describedby="url-help" />
          </div>
          {#if busy}
            <button class="primary-button stop-button" type="button" onclick={stop} disabled={phase === 'stopping'}>
              <span class="stop-square" aria-hidden="true"></span>{phase === 'stopping' ? 'Останавливаем' : 'Остановить'}
            </button>
          {:else}
            <button class="primary-button" type="submit">
              {phase === 'done' ? 'Измерить ещё раз' : 'Измерить'}
              <svg viewBox="0 0 20 20" fill="none" aria-hidden="true"><path d="M4 10h12m-5-5 5 5-5 5" /></svg>
            </button>
          {/if}
        </div>
        <div class="form-bottom">
          <p id="url-help">Нет ссылки под рукой? <button class="text-button" type="button" disabled={busy} onclick={startDemo}>Тестовый файл <svg viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="M4 12 12 4M4 4h8v8" /></svg></button></p>
          <label class="timeout-control" for="timeout">Таймаут <span class="timeout-input"><input id="timeout" type="number" min="1" max="120" step="1" bind:value={timeoutSeconds} disabled={busy} required /><span>с</span></span><span class="timeout-caption">на запрос</span></label>
        </div>
      </form>
    </section>

    {#if runIsDemo && phase !== 'idle'}
      <div class="demo-note"><span class="note-tag">ПРИМЕР</span>Локальный пример — не скорость интернета<span class="demo-detail">Данные приходят с этого же компьютера.</span></div>
    {/if}

    {#if phase === 'error'}
      <div class="notice error-notice" role="alert"><strong>Не получилось завершить замер.</strong><span>{errorMessage}</span></div>
    {:else if phase === 'cancelled'}
      <div class="notice" role="status"><strong>Вы остановили измерение.</strong><span>Завершено {samples.length} из 10 запросов. Для итоговой скорости нужна полная серия.</span></div>
    {/if}

    <section class="results-section" aria-label="Результаты измерения">
      <div class="results-heading">
        <p class:running={busy} class:complete={phase === 'done'} class="status-label"><span class="status-dot" aria-hidden="true"></span><span aria-live="polite">{labels[phase]}</span></p>
        {#if phase === 'done'}
          <button class="export-button" type="button" onclick={downloadResult}><svg viewBox="0 0 20 20" fill="none" aria-hidden="true"><path d="M10 2v10m-4-4 4 4 4-4M3 13v4h14v-4" /></svg>Скачать JSON</button>
        {:else}
          <span class="series-count">Серия <strong>{String(samples.length).padStart(2, '0')}</strong><span>/ 10</span></span>
        {/if}
      </div>

      <div class="result-workspace">
        <div class="speed-result">
          <h2>Скорость скачивания</h2>
          <div class="speed-number" class:empty={speed === null} data-testid="speed-value">{speed === null ? '—' : number(speed)}</div>
          <div class="speed-units"><strong>MiB/s</strong><span>{speed === null ? 'мебибайт в секунду' : `${number(speed * 8.388608)} Мбит/с`}</span></div>
          <div class="result-context">
            {#if phase === 'done'}
              <svg class="check-mark" viewBox="0 0 16 16" fill="none" aria-hidden="true"><path d="m3 8 3.5 3.5L13 5" /></svg>По всем 10 полным ответам
            {:else if busy}
              <span class="waiting-mark" aria-hidden="true"></span>Ждём полный ответ {nextRequest} из 10
            {:else if phase === 'idle'}
              <span class="context-line" aria-hidden="true"></span>Начните с прямой ссылки на файл
            {:else}
              <span class="context-line" aria-hidden="true"></span>Итог появится после полной серии
            {/if}
          </div>
          <div class="segments" role="progressbar" aria-label="Завершённые загрузки" aria-valuemin="0" aria-valuemax="10" aria-valuenow={samples.length} aria-valuetext={`${samples.length} из 10 загрузок завершено`}>
            {#each positions as index}<span class:filled={index < samples.length} class:current={busy && index === samples.length}></span>{/each}
          </div>
          {#if phase === 'running' && samples.length > 0}<p class="partial-note">Пока по {samples.length} завершённым запросам</p>{/if}
        </div>

        <figure class="timing-figure">
          <figcaption><h2>Время каждого ответа</h2><span>Меньше — быстрее</span></figcaption>
          <div class="chart-area">
            <div class="chart-grid" aria-hidden="true"><span></span><span></span><span></span><span></span></div>
            {#if samples.length === 0}
              <div class="chart-empty"><svg viewBox="0 0 40 28" fill="none" aria-hidden="true"><path d="M2 24h36M7 17v7m9-15v15m8-10v10m9-22v22" /></svg><span>{busy ? 'Скачиваем первый файл…' : 'Здесь появятся десять замеров'}</span></div>
            {/if}
            <div class="columns">
              {#each positions as index}
                {@const entry = samples[index]}
                <div class="chart-column">
                  <button class="bar-hit-area" class:has-data={Boolean(entry)} class:highlighted={focusedSample === index} type="button" disabled={!entry} aria-label={entry ? `Запрос ${entry.index}: ${duration(entry.seconds)}, ${volume(entry.bytes)}, ${number(entry.speed_mib_s)} MiB/s` : `Запрос ${index + 1}: пока нет данных`} title={entry ? `№ ${entry.index} · ${duration(entry.seconds)} · ${volume(entry.bytes)}` : undefined} onmouseenter={() => { focusedSample = index; }} onmouseleave={() => { focusedSample = null; }} onfocus={() => { focusedSample = index; }} onblur={() => { focusedSample = null; }}>
                    <span class="bar" style:height={entry ? `${Math.max(entry.seconds / chartMaximum * 100, 1)}%` : '0%'}></span>
                    {#if entry}<span class="bar-value">{duration(entry.seconds)}</span>{/if}
                  </button>
                  <span class="column-label" class:has-data={Boolean(entry)}>{String(index + 1).padStart(2, '0')}</span>
                </div>
              {/each}
            </div>
          </div>
          <div class="chart-description" aria-live="off">
            {#if selectedSample}<span>Запрос <strong>{String(selectedSample.index).padStart(2, '0')}</strong></span><span>{duration(selectedSample.seconds)}</span><span>{volume(selectedSample.bytes)}</span><span>{number(selectedSample.speed_mib_s)} MiB/s</span>
            {:else}<span>От отправки запроса до последнего байта</span>{/if}
          </div>
        </figure>
      </div>

      <dl class="summary-metrics">
        <div><dt>Среднее время</dt><dd>{showNumbers && averageSeconds !== null ? duration(averageSeconds) : '—'}<span>на полный ответ</span></dd></div>
        <div><dt>Скачано всего</dt><dd title={samples.length ? `${number(totalBytes, 0)} байт` : undefined}>{samples.length ? volume(totalBytes) : '—'}<span>{samples.length ? `${number(totalBytes, 0)} байт` : 'только тела ответов'}</span></dd></div>
        <div><dt>Завершено запросов</dt><dd>{String(samples.length).padStart(2, '0')}<span class="count-denominator">/ 10</span><span>без параллельных загрузок</span></dd></div>
      </dl>
      {#if runHost}<p class="target-host">Сервер назначения <span>{runHost}</span></p>{/if}
    </section>

    <details class="method-note">
      <summary><span>Что именно мы измеряем</span><span class="details-plus" aria-hidden="true">+</span></summary>
      <div class="method-content"><p>Общий объём десяти ответов делится на сумму времени их полной загрузки. Время обновления интерфейса в расчёт не входит.</p><p>Результат зависит от выбранного сервера, размера файла и соединения компьютера, на котором запущен Python. Первый запрос включает установление соединения. Нужна прямая ссылка: перенаправления не выполняются.</p><p><strong>1 MiB = 1 048 576 байт.</strong> Считаются полученные байты тела до распаковки, без HTTP-заголовков и служебного трафика.</p></div>
    </details>
  </main>

  <footer><span>10 последовательных загрузок</span><span>1 MiB = 1 048 576 байт</span></footer>
</div>
