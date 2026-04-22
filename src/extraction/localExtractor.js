const { createEmptyRideObject } = require("./schemas");
const { normalizeLineEndings, safeTrim } = require("../utils/text");
const { generateRefer } = require("../utils/reference");
const { detectCurrencyCodeFromMoneyString } = require("../routing/fare");

const DATE_TIME_PATTERN =
  /\b((?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+\d{1,2}(?:st|nd|rd|th)?\s+[a-z]+\s+\d{4})\s*,\s*([0-2]?\d:[0-5]\d\s*(?:am|pm))\b/i;
const FARE_PATTERN =
  /(?:\u00A3|\$|\u20AC|PKR|Rs\.?)\s*\d+(?:,\d{3})*(?:\.\d{1,2})?/i;
const VEHICLE_COUNT_PATTERN = /\((\d+)\s*persons?\)/i;
const SHORT_CARD_TIME_PATTERN =
  /^(today|tomorrow|tonight|asap|now|mon|monday|tue|tues|tuesday|wed|wednesday|thu|thur|thurs|thursday|fri|friday|sat|saturday|sun|sunday)\b[\s,/-]*([0-2]?\d:\d{2}(?:\s*(?:am|pm))?)$/i;
const SHORT_CARD_ROUTE_PATTERN =
  /^(.+?)\s+(?:to|->|[-=]{1,2}>)\s+(.+)$/i;
const VEHICLE_ONLY_PATTERN = /^[A-Za-z][A-Za-z0-9\s/&-]{1,40}$/;
const SHORT_CARD_FARE_TYPE_PATTERN = /\b(net|cash|account)\b/i;
const LABELED_SECTION_PATTERN =
  /\b(pick(?:\s*|-)?up|drop(?:\s*|-)?off|dropoff|head\s*passenger|mobile\s*number|flight|arriving\s*from)\b/i;

const LABEL_PATTERNS = {
  pickup: [/^pick(?:\s*|-)?up\b\s*:?\s*(.*)$/i],
  via_1: [/^via(?:\s*1)?\b\s*:?\s*(.*)$/i, /^stop\s*1\b\s*:?\s*(.*)$/i],
  via_2: [/^via\s*2\b\s*:?\s*(.*)$/i, /^stop\s*2\b\s*:?\s*(.*)$/i],
  via_3: [/^via\s*3\b\s*:?\s*(.*)$/i, /^stop\s*3\b\s*:?\s*(.*)$/i],
  drop_off: [/^drop(?:\s*|-)?off\b\s*:?\s*(.*)$/i, /^dropoff\b\s*:?\s*(.*)$/i],
  expiry_utc: [/^expires?\s*utc\b\s*:?\s*(.*)$/i],
  expiry: [/^expires?\b\s*:?\s*(.*)$/i],
  head_passenger: [/^head\s*passenger\b\s*:?\s*(.*)$/i],
  mobile_number: [/^mobile\s*number\b\s*:?\s*(.*)$/i],
  flight_number: [/^flight\b\s*:?\s*(.*)$/i],
  arriving_from: [/^arriving\s*from\b\s*:?\s*(.*)$/i]
};

const STOP_LABELS = [
  /^landing\b/i,
  /^route\b/i,
  /^pick(?:\s*|-)?up\b/i,
  /^via(?:\s*[123])?\b/i,
  /^stop\s*[123]\b/i,
  /^drop(?:\s*|-)?off\b/i,
  /^dropoff\b/i,
  /^head\s*passenger\b/i,
  /^mobile\s*number\b/i,
  /^flight\b/i,
  /^arriving\s*from\b/i,
  /^expires?(?:\s*utc)?\b/i
];

const WEEKDAY_PATTERN =
  /^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b\s*(.*)$/i;

function stripBulletPrefix(line) {
  return safeTrim(String(line || "").replace(/^[^\p{L}\p{N}+\u00A3$\u20AC]+/u, ""));
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

function splitDayAndDate(value) {
  const text = safeTrim(value);
  if (!text) {
    return {
      day_label: "",
      pickup_date: ""
    };
  }

  const match = text.match(WEEKDAY_PATTERN);
  if (!match) {
    return {
      day_label: "",
      pickup_date: text
    };
  }

  return {
    day_label: safeTrim(match[1]),
    pickup_date: safeTrim(match[2] || "")
  };
}

function extractVehicleDetails(lines) {
  const firstLine = collectFirstNonEmptyLine(lines);
  if (!firstLine) {
    return {
      required_vehicle: "",
      seat_count: "",
      passenger_count: ""
    };
  }

  const countMatch = firstLine.match(VEHICLE_COUNT_PATTERN);
  const count = countMatch ? safeTrim(countMatch[1]) : "";

  return {
    required_vehicle: safeTrim(firstLine.replace(/\s*\([^)]*\)\s*$/, "")),
    seat_count: count,
    passenger_count: count
  };
}

function extractPickupSchedule(lines) {
  for (const line of lines) {
    const candidate = stripBulletPrefix(line);
    if (!candidate) continue;

    const match = candidate.match(DATE_TIME_PATTERN);
    if (match) {
      const dayDate = splitDayAndDate(match[1]);
      const pickupTime = safeTrim(match[2]);
      const dayDateText = [dayDate.day_label, dayDate.pickup_date].filter(Boolean).join(" ");

      return {
        day_label: dayDate.day_label,
        pickup_date: dayDate.pickup_date,
        pickup_time: pickupTime,
        pickup_datetime: [dayDateText, pickupTime].filter(Boolean).join(", ")
      };
    }
  }

  return {
    day_label: "",
    pickup_date: "",
    pickup_time: "",
    pickup_datetime: ""
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

function extractKeywordFlag(rawMessage, patterns = []) {
  const text = String(rawMessage || "");
  if (!text) return "";
  return patterns.some((pattern) => pattern.test(text)) ? "yes" : "";
}

function getNonEmptyLines(rawMessage) {
  return splitLines(rawMessage)
    .map((line) => stripBulletPrefix(line))
    .filter(Boolean);
}

function normalizeShortDateLabel(value) {
  const text = safeTrim(value);
  if (!text) {
    return {
      day_label: "",
      pickup_date: "",
      asap: ""
    };
  }

  const lower = text.toLowerCase();
  if (lower === "asap" || lower === "now") {
    return {
      day_label: lower.toUpperCase(),
      pickup_date: "",
      asap: "yes"
    };
  }

  if (lower === "today" || lower === "tomorrow" || lower === "tonight") {
    return {
      day_label: text.toUpperCase(),
      pickup_date: text.toUpperCase(),
      asap: ""
    };
  }

  return {
    day_label: text,
    pickup_date: text,
    asap: ""
  };
}

function normalizeRouteToken(value) {
  return safeTrim(String(value || "").replace(/\s+/g, " "));
}

function isLikelyVehicleLine(line) {
  const text = safeTrim(line);
  if (!text) return false;
  if (!VEHICLE_ONLY_PATTERN.test(text)) return false;
  if (SHORT_CARD_TIME_PATTERN.test(text)) return false;
  if (SHORT_CARD_ROUTE_PATTERN.test(text)) return false;
  if (FARE_PATTERN.test(text)) return false;
  return true;
}

function parseShortRideCard(rawMessage) {
  const lines = getNonEmptyLines(rawMessage);
  if (lines.length < 3 || lines.length > 8) {
    return null;
  }

  if (lines.some((line) => LABELED_SECTION_PATTERN.test(line))) {
    return null;
  }

  let vehicleIndex = -1;
  let scheduleIndex = -1;
  let routeIndex = -1;
  let fareIndex = -1;

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];

    if (vehicleIndex === -1 && isLikelyVehicleLine(line)) {
      vehicleIndex = index;
      continue;
    }

    if (scheduleIndex === -1 && SHORT_CARD_TIME_PATTERN.test(line)) {
      scheduleIndex = index;
      continue;
    }

    if (routeIndex === -1 && SHORT_CARD_ROUTE_PATTERN.test(line)) {
      routeIndex = index;
      continue;
    }

    if (fareIndex === -1 && FARE_PATTERN.test(line)) {
      fareIndex = index;
    }
  }

  if (vehicleIndex === -1 || scheduleIndex === -1 || routeIndex === -1 || fareIndex === -1) {
    return null;
  }

  const vehicleLine = lines[vehicleIndex];
  const scheduleLine = lines[scheduleIndex];
  const routeLine = lines[routeIndex];
  const fareLine = lines[fareIndex];

  const scheduleMatch = scheduleLine.match(SHORT_CARD_TIME_PATTERN);
  const routeMatch = routeLine.match(SHORT_CARD_ROUTE_PATTERN);
  const fareExtracted = extractFare(fareLine);

  if (!scheduleMatch || !routeMatch || !fareExtracted) {
    return null;
  }

  const schedule = normalizeShortDateLabel(scheduleMatch[1]);
  const pickupTime = safeTrim(scheduleMatch[2]);
  const pickup = normalizeRouteToken(routeMatch[1]);
  const dropOff = normalizeRouteToken(routeMatch[2]);
  const currency = detectCurrencyCodeFromMoneyString(fareExtracted);
  const fareTypeMatch = fareLine.match(SHORT_CARD_FARE_TYPE_PATTERN);
  const fareType = fareTypeMatch ? safeTrim(fareTypeMatch[1]).toLowerCase() : "quoted";

  const consumedIndexes = new Set([vehicleIndex, scheduleIndex, routeIndex, fareIndex]);
  const specialNotes = lines
    .filter((_line, index) => !consumedIndexes.has(index))
    .join("\n");

  return {
    required_vehicle: vehicleLine,
    day_label: schedule.day_label,
    pickup_date: schedule.pickup_date,
    pickup_time: pickupTime,
    pickup_datetime: [schedule.pickup_date || schedule.day_label, pickupTime].filter(Boolean).join(", "),
    asap: schedule.asap,
    pickup,
    drop_off: dropOff,
    route_summary: `${pickup} -> ${dropOff}`,
    fare_extracted: fareExtracted,
    currency,
    fare_type: fareType,
    final_fare: fareExtracted,
    special_notes: specialNotes
  };
}

function extractLocalRideFields(rawMessage, context = {}) {
  const raw = String(rawMessage || "");
  const lines = splitLines(raw);

  const output = createEmptyRideObject({
    raw_message: raw,
    source_name: context.source_name || context.sourceName || "whatsapp",
    group_name: context.group_name || context.groupName || context.source_group || "",
    source_group: context.source_group || context.sourceGroup || "",
    message_id: context.message_id || context.messageId || "",
    received_at: context.received_at || context.receivedAt || ""
  });

  const vehicleDetails = extractVehicleDetails(lines);
  output.required_vehicle = vehicleDetails.required_vehicle;
  output.seat_count = vehicleDetails.seat_count;
  output.passenger_count = vehicleDetails.passenger_count;

  const pickupSchedule = extractPickupSchedule(lines);
  output.day_label = pickupSchedule.day_label;
  output.pickup_date = pickupSchedule.pickup_date;
  output.pickup_time = pickupSchedule.pickup_time;
  output.pickup_datetime = pickupSchedule.pickup_datetime;

  output.pickup = extractLabeledMultiline(lines, LABEL_PATTERNS.pickup);
  output.via_1 = extractLabeledMultiline(lines, LABEL_PATTERNS.via_1);
  output.via_2 = extractLabeledMultiline(lines, LABEL_PATTERNS.via_2);
  output.via_3 = extractLabeledMultiline(lines, LABEL_PATTERNS.via_3);
  output.drop_off = extractLabeledMultiline(lines, LABEL_PATTERNS.drop_off);
  output.expiry_utc = extractLabeledMultiline(lines, LABEL_PATTERNS.expiry_utc);
  output.expiry = extractLabeledMultiline(lines, LABEL_PATTERNS.expiry);

  output.fare_extracted = extractFare(raw);
  output.currency = detectCurrencyCodeFromMoneyString(output.fare_extracted);
  output.fare_type = output.fare_extracted ? "quoted" : "";
  output.final_fare = output.fare_extracted;

  output.head_passenger = extractLabeledMultiline(lines, LABEL_PATTERNS.head_passenger);
  output.mobile_number = extractLabeledMultiline(lines, LABEL_PATTERNS.mobile_number);
  output.flight_number = extractLabeledMultiline(lines, LABEL_PATTERNS.flight_number);
  output.arriving_from = extractLabeledMultiline(lines, LABEL_PATTERNS.arriving_from);

  output.asap = extractKeywordFlag(raw, [/\basap\b/i, /\bimmediate\b/i, /\bnow\b/i]);
  output.child_seat = extractKeywordFlag(raw, [/\bchild\s*seat\b/i, /\bbooster\b/i]);
  output.wait_and_return = extractKeywordFlag(raw, [/\bwait\s*(?:&|and)\s*return\b/i]);
  output.pet_dog = extractKeywordFlag(raw, [/\bpet\s*dog\b/i, /\bdog\b/i]);
  output.payment_status = extractKeywordFlag(raw, [/\bpaid\b/i, /\baccount\b/i, /\bcash\b/i]);

  const shortCard = parseShortRideCard(raw);
  if (shortCard) {
    for (const [key, value] of Object.entries(shortCard)) {
      if (safeTrim(value)) {
        output[key] = value;
      }
    }
  }

  output.refer = generateRefer({
    messageId: output.message_id,
    rawMessage: output.raw_message,
    groupId: output.source_group || output.group_name,
    timestamp: output.received_at || context.timestamp || Date.now()
  });

  return createEmptyRideObject(output);
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
        fareFound: Boolean(record.fare_extracted)
      });

      return record;
    }
  };
}

/*
Sample usage:
const extractor = createLocalExtractor({ logger });
const result = extractor.extract(rawMessage, {
  source_name: "whatsapp",
  group_name: "Dispatch Group",
  source_group: "1203630xxxx@g.us",
  message_id: "ABCD1234",
  received_at: "2026-03-11T10:15:00.000Z"
});
*/

module.exports = {
  createLocalExtractor,
  extractLocalRideFields
};
