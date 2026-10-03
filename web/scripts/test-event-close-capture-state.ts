import assert from "node:assert/strict";
import { captureStageBlocksDispatch } from "../src/lib/event-close-capture-state";

assert.equal(captureStageBlocksDispatch([]), true, "unknown capture job blocks a second paid request");
assert.equal(captureStageBlocksDispatch([{ name: "capture", status: "in_progress" }]), true);
assert.equal(captureStageBlocksDispatch([
  { name: "capture", status: "completed" }, { name: "process", status: "in_progress" },
]), false, "later analysis must not block the next short checkpoint window");
console.log("Event close capture-stage checks passed");
