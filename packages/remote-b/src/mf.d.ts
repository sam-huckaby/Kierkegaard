declare module "broker/client" {
  import type { Envelope } from "@federated-kafka/contracts";

  export const subscribe: (
    pattern: string,
    handler: (envelope: Envelope) => void
  ) => () => void;
  export const respond: (
    pattern: string,
    handler: (payload: unknown, envelope: Envelope) => unknown | Promise<unknown>
  ) => () => void;
}

