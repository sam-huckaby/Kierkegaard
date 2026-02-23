export class TopicPatternError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TopicPatternError";
  }
}

const TOPIC_SEGMENT_REGEX = /^[a-zA-Z0-9_-]+$/;

export const Topics = {
  BillingInvoicePaid: "billing.invoice_paid",
  MathAdd: "math.add",
  UiLog: "ui.log"
} as const;

const split = (value: string): string[] => value.split(".");

export const isValidTopic = (topic: string): boolean => {
  if (!topic.trim()) {
    return false;
  }
  const segments = split(topic);
  return segments.length > 0 && segments.every((segment) => TOPIC_SEGMENT_REGEX.test(segment));
};

export const validateTopicPattern = (pattern: string): void => {
  if (!pattern.trim()) {
    throw new TopicPatternError("Pattern cannot be empty.");
  }

  if (pattern === "*") {
    return;
  }

  const segments = split(pattern);
  if (segments.some((segment) => segment.length === 0)) {
    throw new TopicPatternError("Pattern cannot contain empty segments.");
  }

  segments.forEach((segment, index) => {
    if (segment === "*") {
      if (index !== segments.length - 1) {
        throw new TopicPatternError("Wildcard '*' is only supported as the suffix segment.");
      }
      return;
    }

    if (segment.includes("*")) {
      throw new TopicPatternError("Wildcard must be a full segment.");
    }

    if (!TOPIC_SEGMENT_REGEX.test(segment)) {
      throw new TopicPatternError(`Invalid topic segment: '${segment}'.`);
    }
  });
};

export const matchTopicPattern = (pattern: string, topic: string): boolean => {
  validateTopicPattern(pattern);
  if (!isValidTopic(topic)) {
    return false;
  }

  if (pattern === "*") {
    return true;
  }

  const patternSegments = split(pattern);
  const topicSegments = split(topic);

  const wildcardIndex = patternSegments.indexOf("*");
  if (wildcardIndex === -1) {
    return pattern === topic;
  }

  if (topicSegments.length < wildcardIndex + 1) {
    return false;
  }

  for (let i = 0; i < wildcardIndex; i += 1) {
    if (patternSegments[i] !== topicSegments[i]) {
      return false;
    }
  }

  return true;
};

