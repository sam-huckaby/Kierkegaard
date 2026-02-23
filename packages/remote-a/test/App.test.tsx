import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as brokerClient from "broker/client";
import { App } from "../src/App";

describe("Remote A App", () => {
  it("renders publish and request controls", async () => {
    render(<App />);
    expect(screen.getByTestId("publish-btn")).toBeInTheDocument();
    expect(screen.getByTestId("request-btn")).toBeInTheDocument();
  });

  it("invokes broker calls on actions", async () => {
    const publishSpy = vi.spyOn(brokerClient, "publish");
    const requestSpy = vi.spyOn(brokerClient, "request");
    render(<App />);
    fireEvent.click(screen.getByTestId("publish-btn"));
    fireEvent.click(screen.getByTestId("request-btn"));

    expect(publishSpy).toHaveBeenCalled();
    expect(requestSpy).toHaveBeenCalled();
  });
});

