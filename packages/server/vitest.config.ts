import * as path from "node:path";
import { mergeConfig } from "vitest/config";
import shared from "../../vitest.shared.js";

export default mergeConfig(shared, {
  test: {
    alias: {
      "@test/": path.join(__dirname, "test") + "/",
    },
  },
});
