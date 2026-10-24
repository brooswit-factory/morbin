# Morbin

Morbin is the isolated domain/service package for the Morbin architecture.

## Build and verify

Requires Node.js 20 or newer. This repository deliberately has no production
dependencies, so a clean checkout is reproducible with:

```sh
npm ci
npm run build
npm test
```

## Consumption

Until publishing is explicitly approved, consumers should depend on a pinned
Git commit or a workspace checkout rather than assuming an npm release:

```json
{ "dependencies": { "@brooswit-factory/morbin": "github:brooswit-factory/morbin#<commit>" } }
```

Import `createMorbin` from `@brooswit-factory/morbin`. Providers implement
`fetch(resource)`, optionally `query(query)`, and optionally
`subscribe({ resource, query, cursor, onEvent, onError })`. This makes managed
APIs and fetched websites first-class without baking in Jira-specific logic.

`watchResource` emits a snapshot followed by changes. `watchQuery` supports
`membership` and `content` modes. Provider pushes refresh the cache; `poll()`
only asks a provider for stale resources with an active resource watch. TTL
expiration alone never causes a fetch. `reconnect()` preserves a logical
subscription id while advancing its epoch; a missing cursor emits `reset` then
a replacement snapshot, while authorization failures terminate safely.

No legacy `butchr` code or repository is a dependency of this package.
