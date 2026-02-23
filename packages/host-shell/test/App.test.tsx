import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { App } from "../src/App";

const setDriverMock = vi.fn(async () => undefined);

vi.mock("broker/client", () => ({
  setDriver: setDriverMock
}));

vi.mock("remoteA/App", () => ({
  default: () => <div data-testid="remote-a">Remote A</div>
}));

vi.mock("remoteB/App", () => ({
  default: () => <div data-testid="remote-b">Remote B</div>
}));

vi.mock("broker/devtools", () => ({
  default: () => <div data-testid="broker-devtools">Broker Devtools</div>
}));

describe("Host App", () => {
  it("renders federation containers and configures broker driver", async () => {
    render(<App />);

    expect(await screen.findByText("Federated Kafka Host Shell")).toBeInTheDocument();
    expect(await screen.findByTestId("remote-a")).toBeInTheDocument();
    expect(await screen.findByTestId("remote-b")).toBeInTheDocument();
    expect(setDriverMock).toHaveBeenCalled();
  });
});

