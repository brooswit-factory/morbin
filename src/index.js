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

function isCursorGap(error) {
  return error?.code === "CURSOR_EXPIRED" || error?.code === "CURSOR_UNAVAILABLE" || error?.code === "CURSOR_GAP";
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

  function subjectFor(watch) {
    return watch.kind === "resource"
      ? canonicalResourceId(watch.provider, watch.resource)
      : canonicalQueryId(watch.provider, watch.query);
  }

  function emit(watch, event) {
    if (watch.closed) return;
    if (!watch.ready && event.kind === "change") {
      watch.pending.push(event);
      return;
    }
    watch.listener({
      contractVersion: "1.0",
      kind: event.kind,
      subscriptionId: watch.id,
      streamEpoch: watch.epoch,
      sequence: ++watch.sequence,
      cursor: event.cursor ?? null,
      subject: { kind: watch.kind, key: subjectFor(watch) },
      data: event.data,
      meta: { emittedAt: now(), source: event.source ?? "local" },
      ...(event.operation ? { operation: event.operation } : {}),
      ...(event.error ? { error: event.error } : {}),
    });
  }

  function ready(watch) {
    watch.ready = true;
    for (const event of watch.pending.splice(0)) emit(watch, event);
  }

  async function emitResourceSnapshot(watch) {
    const value = await read(watch.provider, watch.resource);
    emit(watch, { kind: "snapshot", data: value, source: "pull" });
  }

  function setQueryExpiry(watch, source = "pull") {
    watch.queryExpiresAt = now() + randomizedTtl(ranges[source], random);
  }

  function applyQuery(watch, items, { initial = false, source = "pull" } = {}) {
    watch.items = new Map(items.map((value) => [itemId(value), value]));
    setQueryExpiry(watch, source);
    if (initial) emit(watch, { kind: "snapshot", data: items, source });
  }

  async function querySnapshot(watch) {
    const provider = providerFor(watch.provider);
    if (typeof provider.query !== "function") throw new TypeError(`Provider ${watch.provider} does not implement query`);
    applyQuery(watch, await provider.query(watch.query), { initial: true });
  }

  async function startWatch(watch, { snapshot = true } = {}) {
    try {
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
      // A transport authorization failure must occur before protected cache data is delivered.
      if (snapshot) {
        if (watch.kind === "resource") await emitResourceSnapshot(watch);
        else await querySnapshot(watch);
      }
      if (snapshot) watch.initialized = true;
      ready(watch);
    } catch (error) {
      if (watch.reconnecting && isCursorGap(error)) {
        watch.cursor = undefined;
        watch.cursors.clear();
        watch.reconnecting = false;
        emit(watch, { kind: "reset", operation: "reset", data: null, source: "replay" });
        await startWatch(watch, { snapshot: true });
      } else {
        failWatch(watch, error);
      }
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
    emit(watch, { kind: "error", data: null, error, source: "provider" });
    if (error.retryable === false) closeWatch(watch);
    else if (watch.initialized) ready(watch);
  }

  function watchResource(provider, resource, listener) {
    const watch = { id: `sub-${nextSubscription++}`, epoch: 1, sequence: 0, kind: "resource", provider, resource, listener, cursors: new Set(), pending: [], ready: false, initialized: false, closed: false };
    watches.set(watch.id, watch);
    void startWatch(watch);
    return { id: watch.id, close: () => closeWatch(watch) };
  }

  function watchQuery(provider, query, mode, listener) {
    if (!new Set(["membership", "content"]).has(mode)) throw new TypeError("query watch mode must be membership or content");
    const watch = { id: `sub-${nextSubscription++}`, epoch: 1, sequence: 0, kind: "query", provider, query, mode, listener, items: new Map(), queryExpiresAt: 0, cursors: new Set(), pending: [], ready: false, initialized: false, closed: false };
    watches.set(watch.id, watch);
    void startWatch(watch);
    return { id: watch.id, close: () => closeWatch(watch) };
  }

  function updateQueryWatch(watch, nextItems, source = "upstream") {
    const next = new Map(nextItems.map((value) => [itemId(value), value]));
    const added = [...next.keys()].filter((id) => !watch.items.has(id));
    const removed = [...watch.items.keys()].filter((id) => !next.has(id));
    const changed = [...next.keys()].filter((id) => watch.items.has(id) && stable(next.get(id)) !== stable(watch.items.get(id)));
    watch.items = next;
    setQueryExpiry(watch, source);
    if (watch.mode === "membership" && (added.length || removed.length)) {
      emit(watch, { kind: "change", operation: "upsert", data: { added, removed }, source });
    }
    if (watch.mode === "content" && (added.length || removed.length || changed.length)) {
      emit(watch, { kind: "change", operation: "upsert", data: nextItems, source });
    }
  }

  async function receive({ provider, resource, value, cursor, source = "upstream" }) {
    if (!provider || resource === undefined) throw new TypeError("provider and resource are required for an upstream event");
    const affected = [...watches.values()].filter((watch) => watch.provider === provider);
    const matchingResources = affected.filter((watch) => watch.kind === "resource" && stable(watch.resource) === stable(resource));
    const hasQueryWatch = affected.some((watch) => watch.kind === "query");
    if (cursor != null && matchingResources.length && !hasQueryWatch && matchingResources.every((watch) => watch.cursors.has(cursor))) {
      return false;
    }
    let delivered = false;
    const queries = new Map();
    for (const watch of affected) {
      if (watch.kind === "resource" && stable(watch.resource) === stable(resource)) {
        if (cursor != null && watch.cursors.has(cursor)) continue;
        if (cursor != null) {
          watch.cursors.add(cursor);
          watch.cursor = cursor;
        }
        emit(watch, { kind: "change", operation: "upsert", data: value, cursor, source });
        delivered = true;
      } else if (watch.kind === "query") {
        const key = canonicalQueryId(watch.provider, watch.query);
        if (!queries.has(key)) queries.set(key, { provider: providerFor(watch.provider), query: watch.query, watches: [] });
        queries.get(key).watches.push(watch);
      }
    }
    for (const group of queries.values()) {
      const items = await group.provider.query(group.query);
      for (const watch of group.watches) {
        if (cursor != null && watch.cursors.has(cursor)) continue;
        if (cursor != null) { watch.cursors.add(cursor); watch.cursor = cursor; }
        updateQueryWatch(watch, items, source === "push" ? "push" : "upstream");
        delivered = true;
      }
    }
    if (value !== undefined && (delivered || affected.length === 0)) store(provider, resource, value, source === "push" ? "push" : "upstream");
    return delivered;
  }

  /** Replaces a stream while retaining the logical subscription id and advancing its epoch. */
  async function reconnect(subscriptionId, { cursorAvailable = true, error } = {}) {
    const watch = watches.get(subscriptionId);
    if (!watch || watch.closed) return false;
    if (error) return failWatch(watch, error);
    if (typeof watch.cancelTransport === "function") watch.cancelTransport();
    watch.cancelTransport = undefined;
    watch.epoch += 1;
    watch.sequence = 0;
    watch.ready = false;
    watch.reconnecting = cursorAvailable;
    if (!cursorAvailable) {
      emit(watch, { kind: "reset", operation: "reset", data: null, source: "replay" });
    }
    await startWatch(watch, { snapshot: !cursorAvailable });
    return true;
  }

  /** Poll only creates upstream demand when a subscription exists and data is stale. */
  async function poll() {
    const resources = new Map();
    const queries = new Map();
    for (const watch of watches.values()) {
      if (watch.kind === "resource") {
        const key = cacheKey(watch.provider, watch.resource);
        if (!resources.has(key)) resources.set(key, []);
        resources.get(key).push(watch);
      } else {
        const key = canonicalQueryId(watch.provider, watch.query);
        if (!queries.has(key)) queries.set(key, []);
        queries.get(key).push(watch);
      }
    }
    for (const group of resources.values()) {
      const [first] = group;
      if (fresh(cache.get(cacheKey(first.provider, first.resource)))) continue;
      try {
        const value = await read(first.provider, first.resource, { force: true });
        for (const watch of group) {
          if (!watch.initialized) {
            emit(watch, { kind: "snapshot", data: value, source: "poll" });
            watch.initialized = true;
            ready(watch);
          } else {
            emit(watch, { kind: "change", operation: "upsert", data: value, source: "poll" });
          }
        }
      } catch (error) { for (const watch of group) failWatch(watch, error); }
    }
    for (const group of queries.values()) {
      if (!group.some((watch) => watch.queryExpiresAt <= now())) continue;
      const [first] = group;
      try {
        const provider = providerFor(first.provider);
        if (typeof provider.query !== "function") throw new TypeError(`Provider ${first.provider} does not implement query`);
        const items = await provider.query(first.query);
        for (const watch of group) updateQueryWatch(watch, items, "pull");
      } catch (error) { for (const watch of group) failWatch(watch, error); }
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
