// Only the standalone action integration test uses this: preserve real database
// writes while replacing framework cache invalidation outside a Next request.
require("./server-only-stub.cjs");
const Module = require("module");
const originalLoad = Module._load;
Module._load = function (request, parent, isMain) {
  if (request === "next/cache") return { revalidatePath() {} };
  return originalLoad.call(this, request, parent, isMain);
};
