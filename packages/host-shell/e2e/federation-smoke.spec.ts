import { expect, test } from "@playwright/test";

test("cross-remote publish and request/reply", async ({ page }) => {
  await page.goto("/");

  await page.getByTestId("publish-btn").click();
  await expect(page.getByTestId("invoice-count")).toContainText("1");

  await page.getByTestId("request-btn").click();
  await expect(page.getByTestId("request-state")).toContainText("13 + 8 = 21");
});

