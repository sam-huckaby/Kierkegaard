import { describe, expect, it } from "vitest";
import { matchTopicPattern, TopicPatternError, validateTopicPattern } from "../src/topics";

describe("validateTopicPattern", () => {
  it("accepts exact pattern", () => {
    expect(() => validateTopicPattern("billing.invoice_paid")).not.toThrow();
  });

  it("accepts suffix wildcard pattern", () => {
    expect(() => validateTopicPattern("billing.*")).not.toThrow();
  });

  it("accepts global wildcard", () => {
    expect(() => validateTopicPattern("*")).not.toThrow();
  });

  it("rejects infix wildcard usage", () => {
    expect(() => validateTopicPattern("billing.*.created")).toThrow(TopicPatternError);
  });

  it("rejects wildcard as partial segment", () => {
    expect(() => validateTopicPattern("billing.in*voice")).toThrow(TopicPatternError);
  });

  it("rejects empty patterns", () => {
    expect(() => validateTopicPattern("")).toThrow(TopicPatternError);
  });
});

describe("matchTopicPattern", () => {
  it("matches exact pattern", () => {
    expect(matchTopicPattern("billing.invoice_paid", "billing.invoice_paid")).toBe(true);
    expect(matchTopicPattern("billing.invoice_paid", "billing.invoice_created")).toBe(false);
  });

  it("matches prefix wildcard", () => {
    expect(matchTopicPattern("billing.*", "billing.invoice_paid")).toBe(true);
    expect(matchTopicPattern("billing.*", "billing.subsystem.invoice_paid")).toBe(true);
    expect(matchTopicPattern("billing.*", "billing")).toBe(false);
    expect(matchTopicPattern("billing.*", "orders.invoice_paid")).toBe(false);
  });

  it("matches global wildcard", () => {
    expect(matchTopicPattern("*", "billing.invoice_paid")).toBe(true);
    expect(matchTopicPattern("*", "anything.goes.here")).toBe(true);
  });

  it("does not match invalid topics", () => {
    expect(matchTopicPattern("*", "billing..invoice_paid")).toBe(false);
  });
});

