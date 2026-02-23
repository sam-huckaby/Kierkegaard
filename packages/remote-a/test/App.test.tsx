import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as feature from "../src/feature";
import { App } from "../src/App";

vi.mock("../src/feature", () => ({
  publishInvoicePaid: vi.fn(async () => undefined),
  requestMathAdd: vi.fn(async () => 21)
}));

describe("Remote A App", () => {
  it("renders publish and request controls", async () => {
    render(<App />);
    expect(screen.getByTestId("publish-btn")).toBeInTheDocument();
    expect(screen.getByTestId("request-btn")).toBeInTheDocument();
  });

  it("invokes broker calls on actions", async () => {
    const publishSpy = vi.spyOn(feature, "publishInvoicePaid");
    const requestSpy = vi.spyOn(feature, "requestMathAdd");
    render(<App />);
    fireEvent.click(screen.getByTestId("publish-btn"));
    fireEvent.click(screen.getByTestId("request-btn"));

    expect(publishSpy).toHaveBeenCalled();
    expect(requestSpy).toHaveBeenCalled();
  });
});

