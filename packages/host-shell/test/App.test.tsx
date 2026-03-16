import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import * as brokerClient from "broker/client";
import { App } from "../src/App";

describe("Host App", () => {
  it("renders federation containers and configures broker driver", async () => {
    const setDriverSpy = vi.spyOn(brokerClient, "setDriver");
    render(<App />);

    expect(await screen.findByText("Federated Kafka Host Shell")).toBeInTheDocument();
    expect(await screen.findByTestId("remote-a")).toBeInTheDocument();
    expect(await screen.findByTestId("remote-b")).toBeInTheDocument();
    expect(setDriverSpy).toHaveBeenCalled();
  });
});

