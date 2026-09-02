import {performance} from 'node:perf_hooks';
import {createInterface} from 'node:readline';
import WebSocket from 'ws';
import {Zero} from '@rocicorp/zero';
import {queries} from './queries.ts';
import {schema} from './schema.ts';

if (!('WebSocket' in globalThis)) {
  (globalThis as any).WebSocket = WebSocket;
}

type QueryClass =
  | 'filter'
  | 'sorted'
  | 'window'
  | 'window_join'
  | 'window_search'
  | 'composite_and'
  | 'composite_or'
  | 'offset_probe'
  | 'count_probe'
  | 'max_probe';

type Options = {
  clientId: string;
  count: number;
  startIndex: number;
  topics: number;
  queryClass: QueryClass;
  limit: number;
  offset: number;
  needle: string;
  cacheURL: string;
  lagIntervalMs: number;
  emitRows: boolean;
  barrier: boolean;
  connectTimeoutMs: number;
};

function arg(
  name: string,
  fallback?: string,
): string | undefined {
  const i = process.argv.indexOf(name);

  if (i >= 0 && i + 1 < process.argv.length) {
    return process.argv[i + 1];
  }

  return fallback;
}

function numArg(
  name: string,
  fallback: number,
): number {
  return Number(
    arg(name, String(fallback)),
  );
}

if (process.argv.includes('--help')) {
  console.log(
    'subscriber ' +
      '--client-id C ' +
      '--queries N ' +
      '--start-index I ' +
      '--query-class ' +
      'filter|sorted|window|window_join|window_search|' +
      'composite_and|composite_or|' +
      'offset_probe|count_probe|max_probe ' +
      '--topics T ' +
      '--limit 10 ' +
      '--offset 5 ' +
      '--needle gamma ' +
      '--lag-interval-ms 5000 ' +
      '--connect-timeout-ms 120000 ' +
      '[--emit-rows] ' +
      '[--barrier]',
  );

  process.exit(0);
}

const opts: Options = {
  clientId:
    arg(
      '--client-id',
      'zero-client-0',
    )!,

  count:
    numArg(
      '--queries',
      1,
    ),

  startIndex:
    numArg(
      '--start-index',
      0,
    ),

  topics:
    numArg(
      '--topics',
      30000,
    ),

  queryClass:
    arg(
      '--query-class',
      'window',
    ) as QueryClass,

  limit:
    numArg(
      '--limit',
      10,
    ),

  offset:
    numArg(
      '--offset',
      5,
    ),

  needle:
    arg(
      '--needle',
      'gamma',
    )!,

  cacheURL:
    arg(
      '--cache-url',
      process.env.ZERO_CACHE_URL ??
        'http://127.0.0.1:4848',
    )!,

  lagIntervalMs:
    numArg(
      '--lag-interval-ms',
      5000,
    ),

  emitRows:
    process.argv.includes(
      '--emit-rows',
    ),

  barrier:
    process.argv.includes(
      '--barrier',
    ),

  connectTimeoutMs:
    numArg(
      '--connect-timeout-ms',
      120000,
    ),
};

function epochNs(): string {
  return Math.round(
    (
      performance.timeOrigin +
      performance.now()
    ) * 1e6,
  ).toString();
}

function wallNs(): string {
  return (
    BigInt(Date.now()) *
    1000000n
  ).toString();
}

function emit(
  o: Record<string, unknown>,
) {
  process.stdout.write(
    JSON.stringify(o) + '\n',
  );
}

// ---------------------------------------------------------------------------
// Verzoegerung der Ereignisschleife
//
// Im Barrier-Modus wird die Messung erst beim GO aktiviert.
// Damit enthaelt max_lag_ms nur den gemessenen Initial-Sync-Pfad und nicht
// Containerstart oder Verbindungsaufbau.
// ---------------------------------------------------------------------------

const TICK_MS = 100;

let maxLagMs = 0;
let lastTick = performance.now();

let loopLagMeasurementEnabled =
  !opts.barrier;

function resetLoopLagMeasurement() {
  maxLagMs = 0;
  lastTick = performance.now();
}

setInterval(
  () => {
    const now =
      performance.now();

    const lag =
      now -
      lastTick -
      TICK_MS;

    lastTick = now;

    if (
      loopLagMeasurementEnabled &&
      lag > maxLagMs
    ) {
      maxLagMs = lag;
    }
  },
  TICK_MS,
);

function emitLoopLag() {
  if (
    !loopLagMeasurementEnabled
  ) {
    return;
  }

  emit({
    type:
      'loop-lag',

    client_id:
      opts.clientId,

    max_lag_ms:
      Number(
        maxLagMs.toFixed(3),
      ),

    t_epoch_ns:
      epochNs(),
  });

  maxLagMs = 0;
  lastTick = performance.now();
}

setInterval(
  () => {
    emitLoopLag();
  },
  opts.lagIntervalMs,
);

// ---------------------------------------------------------------------------
// Zero-Client
// ---------------------------------------------------------------------------

const zero = new Zero({
  cacheURL:
    opts.cacheURL,

  schema,

  queries,

  kvStore:
    'mem',
});

const views: any[] = [];

const seenProbeTokens =
  new Set<string>();

let ready = 0;

const readyViews =
  new Set<number>();

let allReadyEmitted =
  false;

// ---------------------------------------------------------------------------
// Lebenszyklus
// ---------------------------------------------------------------------------

emit({
  type:
    'started',

  client_id:
    opts.clientId,

  queries:
    opts.count,

  start_index:
    opts.startIndex,

  query_class:
    opts.queryClass,

  needle:
    opts.needle,

  limit:
    opts.limit,

  offset:
    opts.offset,

  barrier:
    opts.barrier,

  t_epoch_ns:
    epochNs(),
});

// ---------------------------------------------------------------------------
// Anfrageaufbau
// ---------------------------------------------------------------------------

function queryFor(
  globalIndex: number,
) {
  if (
    globalIndex >=
    opts.topics
  ) {
    throw new Error(
      `query index ${globalIndex} exceeds distinct topic count ` +
        `${opts.topics}; increase ZERO_TOPICS or use fewer ` +
        'distinct queries',
    );
  }

  const topicId =
    globalIndex + 1;

  switch (
    opts.queryClass
  ) {
    case 'filter':
      return queries.bench.byTopic({
        topicId,
      });

    case 'sorted':
      return queries.bench.byTopicSorted({
        topicId,
      });

    case 'window':
      return queries.bench.byTopicWindow({
        topicId,
        limit:
          opts.limit,
      });

    case 'window_join':
      return queries.bench.byTopicWindowJoin({
        topicId,
        limit:
          opts.limit,
      });

    case 'window_search':
      return queries.bench.byTopicWindowSearch({
        topicId,
        needle:
          opts.needle,
        limit:
          opts.limit,
      });

    case 'composite_and':
      return queries.bench.compositeAnd({
        topicId,
        userId:
          (
            globalIndex %
            1000
          ) + 1,
      });

    case 'composite_or':
      return queries.bench.compositeOr({
        topicId,
        userA:
          (
            globalIndex %
            1000
          ) + 1,
        userB:
          (
            (
              globalIndex +
              1
            ) %
            1000
          ) + 1,
      });

    case 'offset_probe':
      return queries.bench.offsetProbe({
        topicId,
        limit:
          opts.limit,
        offset:
          opts.offset,
      });

    case 'count_probe':
      return queries.bench.countProbe({
        topicId,
      });

    case 'max_probe':
      return queries.bench.maxProbe({
        topicId,
      });

    default:
      throw new Error(
        `unknown query class ${opts.queryClass}`,
      );
  }
}

// ---------------------------------------------------------------------------
// Sondenerkennung
// ---------------------------------------------------------------------------

const highestSeenId =
  new Map<number, number>();

function scanForProbe(
  rows: readonly any[],
  globalIndex: number,
) {
  const floor =
    highestSeenId.get(
      globalIndex,
    ) ?? 0;

  let maxId =
    floor;

  for (
    const row
    of rows
  ) {
    const id =
      Number(
        row?.id ?? 0,
      );

    if (
      id > maxId
    ) {
      maxId = id;
    }

    if (
      id <= floor
    ) {
      continue;
    }

    const content =
      String(
        row?.content ?? '',
      );

    const p =
      content.indexOf(
        '__probe__:',
      );

    if (
      p < 0
    ) {
      continue;
    }

    const token =
      content
        .slice(p)
        .split(/\s+/)[0];

    if (
      seenProbeTokens.has(
        token,
      )
    ) {
      continue;
    }

    seenProbeTokens.add(
      token,
    );

    emit({
      type:
        'probe',

      client_id:
        opts.clientId,

      query_index:
        globalIndex,

      token,

      t_receive_epoch_ns:
        epochNs(),

      t_receive_wall_ns:
        wallNs(),
    });
  }

  highestSeenId.set(
    globalIndex,
    maxId,
  );
}

// ---------------------------------------------------------------------------
// Aufbau der Zero-Verbindung
// ---------------------------------------------------------------------------

async function waitForZeroConnected():
  Promise<void> {

  if (
    !opts.barrier
  ) {
    return;
  }

  const current =
    zero.connection.state.current;

  if (
    current.name ===
    'connected'
  ) {
    return;
  }

  await new Promise<void>(
    (
      resolve,
      reject,
    ) => {
      let finished =
        false;

      let unsubscribe:
        (() => void) |
        undefined;

      let timer:
        ReturnType<typeof setTimeout>;

      const finish = (
        error?: Error,
      ) => {
        if (
          finished
        ) {
          return;
        }

        finished =
          true;

        clearTimeout(
          timer
        );

        if (
          unsubscribe
        ) {
          unsubscribe();
        }

        if (
          error
        ) {
          reject(
            error
          );
        } else {
          resolve();
        }
      };

      const handleState = (
        state: any,
      ) => {
        if (
          state.name ===
          'connected'
        ) {
          finish();
          return;
        }

        if (
          state.name ===
            'error' ||
          state.name ===
            'needs-auth' ||
          state.name ===
            'closed'
        ) {
          const reason =
            typeof state.reason ===
              'string'
              ? state.reason
              : 'no reason supplied';

          finish(
            new Error(
              `Zero connection entered state ${state.name}: ${reason}`,
            ),
          );
        }
      };

      timer =
        setTimeout(
          () => {
            const state =
              zero.connection.state.current;

            finish(
              new Error(
                `Zero connection timeout after ` +
                  `${opts.connectTimeoutMs} ms; ` +
                  `current state=${state.name}`,
              ),
            );
          },
          opts.connectTimeoutMs,
        );

      unsubscribe =
        zero.connection.state.subscribe(
          handleState,
        );

      handleState(
        zero.connection.state.current,
      );
    },
  );
}

// ---------------------------------------------------------------------------
// Synchronisierte Initialisierungsbarriere
// ---------------------------------------------------------------------------

async function waitForBarrierRelease():
  Promise<void> {

  if (
    !opts.barrier
  ) {
    return;
  }

  await waitForZeroConnected();

  emit({
    type:
      'connected',

    client_id:
      opts.clientId,

    t_epoch_ns:
      epochNs(),
  });

  const input =
    createInterface({
      input:
        process.stdin,

      crlfDelay:
        Infinity,
    });

  emit({
    type:
      'armed',

    client_id:
      opts.clientId,

    queries:
      opts.count,

    start_index:
      opts.startIndex,

    t_epoch_ns:
      epochNs(),
  });

  try {
    for await (
      const rawLine
      of input
    ) {
      const line =
        rawLine
          .trim()
          .toLowerCase();

      if (
        line ===
        'go'
      ) {
        // Nur das Intervall nach GO gehoert zur gemessenen Initialisierung.
        resetLoopLagMeasurement();

        loopLagMeasurementEnabled =
          true;

        emit({
          type:
            'released',

          client_id:
            opts.clientId,

          queries:
            opts.count,

          start_index:
            opts.startIndex,

          t_epoch_ns:
            epochNs(),
        });

        return;
      }
    }

    throw new Error(
      'stdin closed before barrier release',
    );

  } finally {
    input.close();
  }
}

await waitForBarrierRelease();

// ---------------------------------------------------------------------------
// Materialisierung
// ---------------------------------------------------------------------------

for (
  let local = 0;
  local < opts.count;
  local++
) {
  const globalIndex =
    opts.startIndex +
    local;

  emit({
    type:
      'query-start',

    client_id:
      opts.clientId,

    query_index:
      globalIndex,

    t_epoch_ns:
      epochNs(),
  });

  const view =
    zero.materialize(
      queryFor(
        globalIndex,
      ),
      {
        ttl:
          'none',
      },
    );

  views.push(
    view,
  );

  view.addListener(
    (
      rowsRaw:
        readonly any[],

      resultType:
        string,

      error?:
        unknown,
    ) => {
      if (
        resultType ===
        'error'
      ) {
        let errorText:
          string;

        if (
          error instanceof
          Error
        ) {
          errorText =
            error.stack ??
            error.message;

        } else {
          try {
            errorText =
              JSON.stringify(
                error,
              );

          } catch {
            errorText =
              String(
                error ??
                  'unknown query error',
              );
          }
        }

        emit({
          type:
            'error',

          client_id:
            opts.clientId,

          query_index:
            globalIndex,

          error:
            errorText,

          t_epoch_ns:
            epochNs(),
        });

        return;
      }

      const rows:
        readonly any[] =
        Array.isArray(
          rowsRaw,
        )
          ? rowsRaw
          : [];

      if (
        resultType ===
          'complete' &&
        !readyViews.has(
          globalIndex,
        )
      ) {
        emit({
          type:
            'query-ready',

          client_id:
            opts.clientId,

          query_index:
            globalIndex,

          t_epoch_ns:
            epochNs(),
        });

        readyViews.add(
          globalIndex,
        );

        ready++;

        if (
          ready % 250 === 0 &&
          ready < opts.count
        ) {
          emit({
            type:
              'progress',

            client_id:
              opts.clientId,

            ready,

            total:
              opts.count,

            t_epoch_ns:
              epochNs(),
          });
        }

        if (
          ready ===
            opts.count &&
          !allReadyEmitted
        ) {
          allReadyEmitted =
            true;

          // Kurze Initialisierungslaeufe enden vor dem regulaeren 5-s-Intervall.
          emitLoopLag();

          emit({
            type:
              'ready',

            client_id:
              opts.clientId,

            queries:
              opts.count,

            start_index:
              opts.startIndex,

            t_epoch_ns:
              epochNs(),
          });
        }
      }

      scanForProbe(
        rows,
        globalIndex,
      );

      if (
        opts.emitRows &&
        resultType ===
          'complete'
      ) {
        const snapshot =
          rows.map(
            (
              r: any,
            ) => ({
              id:
                Number(
                  r?.id ?? 0,
                ),

              topicId:
                Number(
                  r?.topicId ?? 0,
                ),

              userId:
                Number(
                  r?.userId ?? 0,
                ),

              content:
                String(
                  r?.content ?? '',
                ),

              createdAt:
                String(
                  r?.createdAt ?? '',
                ),

              value:
                typeof r?.value ===
                  'number'
                  ? Number(
                      r.value,
                    )
                  : undefined,

              author:
                r?.author
                  ? {
                      id:
                        Number(
                          r.author?.id ??
                            0,
                        ),

                      name:
                        String(
                          r.author?.name ??
                            '',
                        ),
                    }
                  : null,
            }),
          );

        emit({
          type:
            'rows',

          client_id:
            opts.clientId,

          query_index:
            globalIndex,

          ids:
            snapshot.map(
              (
                r: any,
              ) => r.id,
            ),

          rows:
            snapshot,

          t_epoch_ns:
            epochNs(),
        });
      }
    },
  );

  if (
    (local + 1) %
      250 ===
    0
  ) {
    await new Promise<void>(
      resolve =>
        setImmediate(
          resolve,
        ),
    );
  }
}

// ---------------------------------------------------------------------------
// Beenden
// ---------------------------------------------------------------------------

async function shutdown() {
  for (
    const view
    of views
  ) {
    try {
      view.destroy();
    } catch {
      // Fehler beim Beenden werden nicht weitergegeben.
    }
  }

  try {
    await zero.close();
  } catch {
    // Fehler beim Beenden werden nicht weitergegeben.
  }

  process.exit(0);
}

process.on(
  'SIGTERM',
  () =>
    void shutdown(),
);

process.on(
  'SIGINT',
  () =>
    void shutdown(),
);
