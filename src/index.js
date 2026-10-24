/**
 * Morbin is a provider-neutral, demand-driven cache and monitoring service.
 * Providers can represent managed APIs or ordinary fetched websites; both use
 * the same fetch/query/subscribe contract.
 */

const DEFAULT_FRESHNESS = Object.freeze({
  pull: [1_000, 2_000],
  upstream: [4_000, 6_000],
  push: [4_000, 6_000],
});

function stable(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stable).join(",")}]`;
  return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stable(value[key])}`).join(",")}}`;
}

export function canonicalResourceId(provider, resource) {
  if (!provider || !resource) throw new TypeError("provider and resource are required");
  return `${provider}:resource:${typeof resource === "string" ? resource : stable(resource)}`;
}

export function canonicalQueryId(provider, query) {
  if (!provider || query === undefined) throw new TypeError("provider and query are required");
  return `${provider}:query:${typeof query === "string" ? query : stable(query)}`;
}

export function randomizedTtl(range, random = Math.random) {
  const [minimum, maximum] = range;
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum) || minimum < 0 || maximum < minimum) {
    throw new TypeError("freshness ranges must be finite [minimum, maximum] millisecond pairs");
  }
  return minimum + Math.floor(random() * (maximum - minimum + 1));
}

/** Consumers may use this to reject a configuration that violates the cross-service contract. */
export function assertFreshnessSeparation({ morbrMaximum, butchrdMinimum }) {
  if (!(butchrdMinimum > morbrMaximum)) {
    throw new RangeError("ButchrD minimum poll TTL must be strictly greater than Morbr maximum TTL");
  }
}

function normalizeError(error) {
  const code = error?.code || error?.status;
  if (code === 401 || code === 403 || code === "UNAUTHORIZED" || code === "FORBIDDEN") {
    const terminal = new Error(code === 403 || code === "FORBIDDEN" ? "FORBIDDEN" : "UNAUTHORIZED");
    terminal.code = terminal.message;
    terminal.retryable = false;
    return terminal;
  }
  return error instanceof Error ? error : new Error(String(error));
}

function itemId(value) {
  if (value && typeof value === "object" && value.id !== undefined) return String(value.id);
  return stable(value);
}

export function createMorbin({ providers = {}, freshness = {}, now = () => Date.now(), random = Math.random } = {}) {
  const ranges = { ...DEFAULT_FRESHNESS, ...freshness };
  for (const source of ["pull", "upstream", "push"]) randomizedTtl(ranges[source], random);

  const cache = new Map();
  const watches = new Map();
  const seenCursors = new Set();
  let nextSubscription = 1;

  function providerFor(name) {
    const provider = providers[name];
    if (!provider) throw new Error(`Unknown provider: ${name}`);
    return provider;
  }

  function cacheKey(provider, resource) {
    return canonicalResourceId(provider, resource);
  }

  function store(provider, resource, value, source = "pull") {
    const key = cacheKey(provider, resource);
    const entry = {
      value,
      source,
      updatedAt: now(),
      expiresAt: now() + randomizedTtl(ranges[source], random),
    };
    cache.set(key, entry);
    return entry;
  }

  function fresh(entry) {
    return entry && entry.expiresAt > now();
  }

  async function read(providerName, resource, { force = false } = {}) {
    const key = cacheKey(providerName, resource);
    const entry = cache.get(key);
    if (!force && fresh(entry)) return entry.value;
    const provider = providerFor(providerName);
    if (typeof provider.fetch !== "function") throw new TypeError(`Provider ${providerName} does not implement fetch`);
    const value = await provider.fetch(resource);
    return store(providerName, resource, value, "pull").value;
  }

  function emit(watch, event) {
    if (!watch.closed) watch.listener({ subscriptionId: watch.id, epoch: watch.epoch, ...event });
  }

  async function emitResourceSnapshot(watch) {
    const value = await read(watch.provider, watch.resource);
    emit(watch, { type: "snapshot", resource: watch.resource, value });
  }

  async function querySnapshot(watch) {
    const provider = providerFor(watch.provider);
    if (typeof provider.query !== "function") throw new TypeError(`Provider ${watch.provider} does not implement query`);
    const items = await provider.query(watch.query);
    watch.items = new Map(items.map((value) => [itemId(value), value]));
    emit(watch, { type: "snapshot", query: watch.query, mode: watch.mode, value: items });
  }

  async function startWatch(watch) {
    try {
      if (watch.kind === "resource") await emitResourceSnapshot(watch);
      else await querySnapshot(watch);
      const provider = providerFor(watch.provider);
      if (typeof provider.subscribe === "function") {
        const cancel = await provider.subscribe({
          resource: watch.resource,
          query: watch.query,
          cursor: watch.cursor,
          onEvent: (event) => receive({ provider: watch.provider, ...event }),
          onError: (error) => failWatch(watch, error),
        });
        watch.cancelTransport = typeof cancel === "function" ? cancel : cancel?.close;
      }
    } catch (error) {
      failWatch(watch, error);
    }
  }

  function closeWatch(watch) {
    if (watch.closed) return;
    watch.closed = true;
    if (typeof watch.cancelTransport === "function") watch.cancelTransport();
    watches.delete(watch.id);
  }

  function failWatch(watch, cause) {
    const error = normalizeError(cause);
    // Authorization failures terminate without emitting cached/protected data.
    emit(watch, { type: "error", error });
    if (error.retryable === false) closeWatch(watch);
  }

  function watchResource(provider, resource, listener) {
    const watch = { id: `sub-${nextSubscription++}`, epoch: 1, kind: "resource", provider, resource, listener, closed: false };
    watches.set(watch.id, watch);
    void startWatch(watch);
    return { id: watch.id, close: () => closeWatch(watch) };
  }

  function watchQuery(provider, query, mode, listener) {
    if (!new Set(["membership", "content"]).has(mode)) throw new TypeError("query watch mode must be membership or content");
    const watch = { id: `sub-${nextSubscription++}`, epoch: 1, kind: "query", provider, query, mode, listener, items: new Map(), closed: false };
    watches.set(watch.id, watch);
    void startWatch(watch);
    return { id: watch.id, close: () => closeWatch(watch) };
  }

  async function updateQueryWatch(watch) {
    const provider = providerFor(watch.provider);
    const nextItems = await provider.query(watch.query);
    const next = new Map(nextItems.map((value) => [itemId(value), value]));
    const added = [...next.keys()].filter((id) => !watch.items.has(id));
    const removed = [...watch.items.keys()].filter((id) => !next.has(id));
    const changed = [...next.keys()].filter((id) => watch.items.has(id) && stable(next.get(id)) !== stable(watch.items.get(id)));
    watch.items = next;
    if (watch.mode === "membership" && (added.length || removed.length)) {
      emit(watch, { type: "change", query: watch.query, mode: watch.mode, added, removed });
    }
    if (watch.mode === "content" && (added.length || removed.length || changed.length)) {
      emit(watch, { type: "change", query: watch.query, mode: watch.mode, added, removed, changed, value: nextItems });
    }
  }

  async function receive({ provider, resource, value, cursor, source = "upstream" }) {
    if (!provider || resource === undefined) throw new TypeError("provider and resource are required for an upstream event");
    const cursorKey = cursor === undefined ? null : `${provider}:${cursor}`;
    if (cursorKey && seenCursors.has(cursorKey)) return false;
    if (cursorKey) seenCursors.add(cursorKey);
    if (value !== undefined) store(provider, resource, value, source === "push" ? "push" : "upstream");
    for (const watch of watches.values()) {
      if (watch.provider !== provider) continue;
      if (watch.kind === "resource" && stable(watch.resource) === stable(resource)) {
        emit(watch, { type: "change", resource, value, cursor });
      } else if (watch.kind === "query") {
        await updateQueryWatch(watch);
      }
    }
    return true;
  }

  /** Replaces a stream while retaining the logical subscription id and advancing its epoch. */
  async function reconnect(subscriptionId, { cursorAvailable = true, error } = {}) {
    const watch = watches.get(subscriptionId);
    if (!watch || watch.closed) return false;
    if (error) return failWatch(watch, error);
    if (typeof watch.cancelTransport === "function") watch.cancelTransport();
    watch.cancelTransport = undefined;
    watch.epoch += 1;
    if (!cursorAvailable) {
      emit(watch, { type: "reset", reason: "cursor-unavailable" });
    }
    await startWatch(watch);
    return true;
  }

  /** Poll only creates upstream demand when a subscription exists and data is stale. */
  async function poll() {
    const attempted = new Set();
    for (const watch of watches.values()) {
      if (watch.kind !== "resource") continue;
      const key = cacheKey(watch.provider, watch.resource);
      if (!attempted.has(key) && !fresh(cache.get(key))) {
        attempted.add(key);
        try {
          const value = await read(watch.provider, watch.resource, { force: true });
          emit(watch, { type: "change", resource: watch.resource, value, source: "poll" });
        } catch (error) { failWatch(watch, error); }
      }
    }
  }

  return Object.freeze({
    read,
    watchResource,
    watchQuery,
    receive,
    reconnect,
    poll,
    cacheEntry: (provider, resource) => cache.get(cacheKey(provider, resource)),
  });
}
