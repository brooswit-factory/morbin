import test from "node:test";
import assert from "node:assert/strict";
import {
  assertFreshnessSeparation,
  canonicalQueryId,
  canonicalResourceId,
  createMorbin,
  randomizedTtl,
} from "../src/index.js";

const flush = () => new Promise((resolve) => setImmediate(resolve));

function fakeProvider() {
  const values = new Map([["a", { id: "a", version: 1 }], ["b", { id: "b", version: 1 }]]);
  const subscribers = [];
  let fetches = 0;
  return {
    values,
    get fetches() { return fetches; },
    async fetch(key) { fetches += 1; return structuredClone(values.get(key)); },
    async query() { return [...values.values()].map((value) => structuredClone(value)); },
    async subscribe(subscriber) { subscribers.push(subscriber); return () => subscribers.splice(subscribers.indexOf(subscriber), 1); },
    async push(event) { await Promise.all(subscribers.map((subscriber) => subscriber.onEvent(event))); },
  };
}

test("canonical identities are stable across object key ordering", () => {
  assert.equal(canonicalResourceId("site", { url: "https://example.test", params: { b: 2, a: 1 } }), canonicalResourceId("site", { params: { a: 1, b: 2 }, url: "https://example.test" }));
  assert.equal(canonicalQueryId("api", { z: 1, a: 2 }), canonicalQueryId("api", { a: 2, z: 1 }));
});

test("lazy reads fetch once while fresh and TTL itself never fetches", async () => {
  let clock = 0;
  const api = fakeProvider();
  const morbin = createMorbin({ providers: { api }, now: () => clock, random: () => 0, freshness: { pull: [10, 10] } });
  assert.deepEqual(await morbin.read("api", "a"), { id: "a", version: 1 });
  assert.deepEqual(await morbin.read("api", "a"), { id: "a", version: 1 });
  assert.equal(api.fetches, 1);
  clock = 20;
  await morbin.poll();
  assert.equal(api.fetches, 1, "a stale cache without a subscription has no provider demand");
  await morbin.read("api", "a");
  assert.equal(api.fetches, 2);
});

test("resource watches snapshot before pushed changes and pushed data refreshes cache", async () => {
  const api = fakeProvider();
  const events = [];
  const morbin = createMorbin({ providers: { api }, random: () => 0 });
  morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  api.values.set("a", { id: "a", version: 2 });
  await api.push({ resource: "a", value: { id: "a", version: 2 }, cursor: "1", source: "push" });
  assert.deepEqual(events.map((event) => event.type), ["snapshot", "change"]);
  assert.equal(morbin.cacheEntry("api", "a").source, "push");
  assert.deepEqual(events.map(({ version, subject, operation, sequence }) => ({ version, subject, operation, sequence })), [
    { version: 1, subject: canonicalResourceId("api", "a"), operation: "snapshot", sequence: 1 },
    { version: 1, subject: canonicalResourceId("api", "a"), operation: "change", sequence: 2 },
  ]);
});

test("query membership and content watches distinguish their changes", async () => {
  const api = fakeProvider();
  const membership = [];
  const content = [];
  const morbin = createMorbin({ providers: { api } });
  morbin.watchQuery("api", { all: true }, "membership", (event) => membership.push(event));
  morbin.watchQuery("api", { all: true }, "content", (event) => content.push(event));
  await flush();
  api.values.set("a", { id: "a", version: 2 });
  await api.push({ resource: "a", value: { id: "a", version: 2 }, cursor: "2" });
  assert.deepEqual(membership.map((event) => event.type), ["snapshot"]);
  assert.deepEqual(content.map((event) => event.type), ["snapshot", "change"]);
  api.values.set("c", { id: "c", version: 1 });
  await api.push({ resource: "c", value: { id: "c", version: 1 }, cursor: "3" });
  assert.equal(membership.at(-1).type, "change");
  assert.deepEqual(membership.at(-1).added, ["c"]);
});

test("replay de-duplicates immutable cursors and recovery is reset then snapshot", async () => {
  const api = fakeProvider();
  const events = [];
  const morbin = createMorbin({ providers: { api } });
  const subscription = morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 2 }, cursor: "cursor-1" }), true);
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 3 }, cursor: "cursor-1" }), false);
  await morbin.reconnect(subscription.id, { cursorAvailable: false });
  assert.deepEqual(events.map((event) => event.type), ["snapshot", "change", "reset", "snapshot"]);
  assert.equal(events.at(-1).epoch, 2);
});

test("reconnect authorizes its stream before emitting a cached protected snapshot", async () => {
  const api = fakeProvider();
  const subscribe = api.subscribe;
  let revoked = false;
  api.subscribe = async (request) => {
    if (revoked) throw { status: 403 };
    return subscribe(request);
  };
  const events = [];
  const morbin = createMorbin({ providers: { api } });
  const subscription = morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  revoked = true;
  await morbin.reconnect(subscription.id);
  assert.deepEqual(events.map((event) => event.type), ["snapshot", "error"]);
  assert.equal(events.at(-1).error.code, "FORBIDDEN");
});

test("reconnect carries a logical watch cursor, de-duplicates replay, and permits null cursors", async () => {
  const api = fakeProvider();
  const subscribe = api.subscribe;
  const requestedCursors = [];
  api.subscribe = async (request) => {
    requestedCursors.push(request.cursor);
    return subscribe(request);
  };
  const events = [];
  const morbin = createMorbin({ providers: { api } });
  const subscription = morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 2 }, cursor: "c12" });
  await morbin.reconnect(subscription.id);
  assert.deepEqual(requestedCursors, [undefined, "c12"]);
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 2 }, cursor: "c12" }), false);
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 3 }, cursor: "c13" }), true);
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 2 }, cursor: "c12" }), false);
  assert.equal(morbin.cacheEntry("api", "a").value.version, 3, "a duplicate replay cannot roll cache state backward");
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 4 }, cursor: null }), true);
  assert.equal(await morbin.receive({ provider: "api", resource: "a", value: { id: "a", version: 5 }, cursor: null }), true);
  assert.deepEqual(events.map((event) => event.type), ["snapshot", "change", "change", "change", "change"]);
});

test("unauthorized reconnect terminates without a protected replacement snapshot", async () => {
  const api = fakeProvider();
  const events = [];
  const morbin = createMorbin({ providers: { api } });
  const subscription = morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  await morbin.reconnect(subscription.id, { error: { status: 401 } });
  assert.deepEqual(events.map((event) => event.type), ["snapshot", "error"]);
  assert.equal(events.at(-1).error.code, "UNAUTHORIZED");
  assert.equal(events.at(-1).error.retryable, false);
});

test("a transient provider failure remains recoverable through subscribed polling", async () => {
  let attempts = 0;
  const api = fakeProvider();
  api.fetch = async (key) => {
    attempts += 1;
    if (attempts === 1) throw new Error("temporary upstream failure");
    return structuredClone(api.values.get(key));
  };
  const events = [];
  const morbin = createMorbin({ providers: { api } });
  morbin.watchResource("api", "a", (event) => events.push(event));
  await flush();
  assert.deepEqual(events.map((event) => event.type), ["error"]);
  await morbin.poll();
  assert.deepEqual(events.map((event) => event.type), ["error", "change"]);
  assert.equal(attempts, 2);
});

test("polling only refreshes stale subscribed resources and freshness ranges are ordered", async () => {
  let clock = 0;
  const api = fakeProvider();
  const morbin = createMorbin({ providers: { api }, now: () => clock, random: () => 0, freshness: { pull: [10, 10], upstream: [20, 20], push: [30, 30] } });
  morbin.watchResource("api", "a", () => {});
  await flush();
  clock = 11;
  await morbin.poll();
  assert.equal(api.fetches, 2);
  assert.equal(randomizedTtl([20, 30], () => 0), 20);
  assert.equal(randomizedTtl([20, 30], () => 0.999), 30);
  assert.doesNotThrow(() => assertFreshnessSeparation({ morbrMaximum: 30, butchrdMinimum: 31 }));
  assert.throws(() => assertFreshnessSeparation({ morbrMaximum: 30, butchrdMinimum: 30 }));
});

test("poll coalesces stale resource fetches and fans the result to every watcher", async () => {
  let clock = 0;
  const api = fakeProvider();
  const first = [];
  const second = [];
  const morbin = createMorbin({ providers: { api }, now: () => clock, random: () => 0, freshness: { pull: [10, 10] } });
  morbin.watchResource("api", "a", (event) => first.push(event));
  morbin.watchResource("api", "a", (event) => second.push(event));
  await flush();
  const before = api.fetches;
  clock = 11;
  await morbin.poll();
  assert.equal(api.fetches, before + 1);
  assert.equal(first.at(-1).source, "poll");
  assert.equal(second.at(-1).source, "poll");
});

test("poll recovers stale active query watches with one fetch and ignores inactive queries", async () => {
  let clock = 0;
  const api = fakeProvider();
  let queries = 0;
  const query = api.query;
  api.query = async (...args) => { queries += 1; return query(...args); };
  const membership = [];
  const content = [];
  const morbin = createMorbin({ providers: { api }, now: () => clock, random: () => 0, freshness: { pull: [10, 10] } });
  const first = morbin.watchQuery("api", { all: true }, "membership", (event) => membership.push(event));
  const second = morbin.watchQuery("api", { all: true }, "content", (event) => content.push(event));
  await flush();
  const before = queries;
  api.values.set("c", { id: "c", version: 1 });
  clock = 11;
  await morbin.poll();
  assert.equal(queries, before + 1, "matching active query watches share one fallback fetch");
  assert.equal(membership.at(-1).type, "change");
  assert.equal(content.at(-1).type, "change");
  first.close();
  second.close();
  clock = 30;
  await morbin.poll();
  assert.equal(queries, before + 1, "inactive query watches create no demand");
});
