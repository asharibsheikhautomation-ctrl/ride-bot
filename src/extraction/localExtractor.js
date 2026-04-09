const { createEmptyRideObject } = require("./schemas");
const { normalizeLineEndings, safeTrim } = require("../utils/text");
const { generateRefer } = require("../utils/reference");

const DATE_TIME_PATTERN =
  /\b((?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+\s+\d{4})\s*,\s*([0-2]?\d:[0-5]\d\s*(?:am|pm))\b/i;
const FARE_PATTERN = /\u00A3\s*\d+(?:,\d{3})*(?:\.\d{1,2})?/;

const LABEL_PATTERNS = {
  pickup: [/^pick(?:\s*|-)?up\b\s*:?\s*(.*)$/i],
  drop_off: [/^drop(?:\s*|-)?off\b\s*:?\s*(.*)$/i, /^dropoff\b\s*:?\s*(.*)$/i],
  head_passenger: [/^head\s*passenger\b\s*:?\s*(.*)$/i],
  mobile_number: [/^mobile\s*number\b\s*:?\s*(.*)$/i],
  flight_number: [/^flight\b\s*:?\s*(.*)$/i],
  arriving_from: [/^arriving\s*from\b\s*:?\s*(.*)$/i]
};

const STOP_LABELS = [
  /^landing\b/i,
  /^route\b/i,
  /^pick(?:\s*|-)?up\b/i,
  /^drop(?:\s*|-)?off\b/i,
  /^dropoff\b/i,
  /^head\s*passenger\b/i,
  /^mobile\s*number\b/i,
  /^flight\b/i,
  /^arriving\s*from\b/i,
  /^expires?\b/i
];

function stripBulletPrefix(line) {
  return safeTrim(String(line || "").replace(/^[^\p{L}\p{N}+\u00A3$]+/u, ""));
}

function splitLines(rawMessage) {
  return normalizeLineEndings(rawMessage)
    .split("\n")
    .map((line) => line.replace(/\t/g, " ").trimEnd());
}

function collectFirstNonEmptyLine(lines) {
  for (const line of lines) {
    const trimmed = stripBulletPrefix(line);
    if (trimmed) return trimmed;
  }
  return "";
}

function extractRequiredVehicle(lines) {
  const firstLine = collectFirstNonEmptyLine(lines);
  if (!firstLine) return "";

  return safeTrim(firstLine.replace(/\s*\([^)]*\)\s*$/, ""));
}

function extractDayDateAndStarting(lines) {
  for (const line of lines) {
    const candidate = stripBulletPrefix(line);
    if (!candidate) continue;

    const match = candidate.match(DATE_TIME_PATTERN);
    if (match) {
      return {
        day_date: safeTrim(match[1]),
        starting: safeTrim(match[2])
      };
    }
  }

  return {
    day_date: "",
    starting: ""
  };
}

function isStopLine(line) {
  if (!line) return false;
  if (FARE_PATTERN.test(line)) return true;
  return STOP_LABELS.some((pattern) => pattern.test(line));
}

function compressMultiline(parts) {
  return parts
    .map((part) => safeTrim(part))
    .filter(Boolean)
    .join(", ");
}

function extractLabeledMultiline(lines, patterns) {
  if (!Array.isArray(patterns) || patterns.length === 0) return "";

  for (let i = 0; i < lines.length; i += 1) {
    const source = stripBulletPrefix(lines[i]);
    if (!source) continue;

    let match = null;
    for (const pattern of patterns) {
      match = source.match(pattern);
      if (match) break;
    }
    if (!match) continue;

    const captured = [];
    const inlineValue = safeTrim(match[1] || "");
    if (inlineValue) {
      captured.push(inlineValue);
    }

    for (let j = i + 1; j < lines.length; j += 1) {
      const nextLine = stripBulletPrefix(lines[j]);
      if (!nextLine) continue;
      if (isStopLine(nextLine)) break;
      captured.push(nextLine);
    }

    return compressMultiline(captured);
  }

  return "";
}

function extractFare(rawMessage) {
  const match = String(rawMessage || "").match(FARE_PATTERN);
  return match ? safeTrim(match[0].replace(/\s+/g, " ")) : "";
}

function extractLocalRideFields(rawMessage, context = {}) {
  const raw = String(rawMessage || "");
  const lines = splitLines(raw);

  const output = createEmptyRideObject({
    raw_message: raw,
    source_group: context.source_group || context.sourceGroup || "",
    message_id: context.message_id || context.messageId || "",
    received_at: context.received_at || context.receivedAt || ""
  });

  output.required_vehicle = extractRequiredVehicle(lines);

  const dateTime = extractDayDateAndStarting(lines);
  output.day_date = dateTime.day_date;
  output.starting = dateTime.starting;

  output.pickup = extractLabeledMultiline(lines, LABEL_PATTERNS.pickup);
  output.drop_off = extractLabeledMultiline(lines, LABEL_PATTERNS.drop_off);
  output.fare = extractFare(raw);
  output.head_passenger = extractLabeledMultiline(lines, LABEL_PATTERNS.head_passenger);
  output.mobile_number = extractLabeledMultiline(lines, LABEL_PATTERNS.mobile_number);
  output.flight_number = extractLabeledMultiline(lines, LABEL_PATTERNS.flight_number);
  output.arriving_from = extractLabeledMultiline(lines, LABEL_PATTERNS.arriving_from);

  output.refer = generateRefer({
    messageId: output.message_id,
    rawMessage: output.raw_message,
    groupId: output.source_group,
    timestamp: output.received_at || context.timestamp || Date.now()
  });

  return output;
}

function createLocalExtractor({ logger } = {}) {
  const safeLogger = logger || { debug: () => {} };

  return {
    extract(rawMessage, context = {}) {
      const record = extractLocalRideFields(rawMessage, context);

      safeLogger.debug("Local extraction completed", {
        messageId: record.message_id,
        sourceGroup: record.source_group,
        pickupFound: Boolean(record.pickup),
        dropOffFound: Boolean(record.drop_off),
        fareFound: Boolean(record.fare)
      });

      return record;
    }
  };
}

/*
Sample usage:
const extractor = createLocalExtractor({ logger });
const result = extractor.extract(rawMessage, {
  source_group: "1203630xxxx@g.us",
  message_id: "ABCD1234",
  received_at: "2026-03-11T10:15:00.000Z"
});
*/

module.exports = {
  createLocalExtractor,
  extractLocalRideFields
};

