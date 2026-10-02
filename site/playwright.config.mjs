import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./test/browser",
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  timeout: 30000,
  reporter: "list",
  use: {
    baseURL: process.env.BASE_URL || "http://127.0.0.1:4173",
    browserName: "chromium",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: process.env.BASE_URL
    ? undefined
    : {
        command: "node serve.mjs",
        url: "http://127.0.0.1:4173",
        reuseExistingServer: !process.env.CI,
      },
});
