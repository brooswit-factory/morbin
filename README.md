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

Import `createMorbin` from `@brooswit-factory/morbin`. No legacy `butchr`
code or repository is a dependency of this package.
