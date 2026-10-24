import test from "node:test";
import assert from "node:assert/strict";
import { createMorbin } from "../src/index.js";

test("creates the Morbin service marker", () => {
  assert.deepEqual(createMorbin(), { service: "morbin" });
});
