import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

const publishMock = vi.fn(async () => undefined);
const requestMock = vi.fn(async () => ({ result: 21 }));

vi.mock("broker/client", () => ({
  publish: publishMock,
  request: requestMock
}));

describe("Remote A App", () => {
  it("renders publish and request controls", async () => {
    render(<App />);
    expect(screen.getByTestId("publish-btn")).toBeInTheDocument();
    expect(screen.getByTestId("request-btn")).toBeInTheDocument();
  });

  it("invokes broker calls on actions", async () => {
    render(<App />);
    fireEvent.click(screen.getByTestId("publish-btn"));
    fireEvent.click(screen.getByTestId("request-btn"));

    expect(publishMock).toHaveBeenCalled();
    expect(requestMock).toHaveBeenCalled();
  });
});

