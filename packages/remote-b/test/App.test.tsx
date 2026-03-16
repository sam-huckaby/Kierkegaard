import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

const unsubscribeMock = vi.fn(() => undefined);
const stopResponderMock = vi.fn(() => undefined);

vi.mock("../src/feature", () => ({
  ensureBrokerReady: vi.fn(async () => undefined),
  startInvoiceSubscriber: vi.fn(() => unsubscribeMock),
  startMathResponder: vi.fn(() => stopResponderMock)
}));

describe("Remote B App", () => {
  it("shows responder state and invoice count", async () => {
    render(<App />);

    expect(await screen.findByTestId("responder-state")).toHaveTextContent("ready");
    expect(screen.getByTestId("invoice-count")).toHaveTextContent("Received invoices: 0");
  });
});

