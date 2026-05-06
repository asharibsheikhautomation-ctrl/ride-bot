const { createEmptyRideObject } = require("./schemas");
const { normalizeLineEndings, safeTrim, collapseWhitespace } = require("../utils/text");
const { generateRefer } = require("../utils/reference");

const WEEKDAY_INDEX = Object.freeze({
  sunday: 0,
  sun: 0,
  monday: 1,
  mon: 1,
  tuesday: 2,
  tue: 2,
  tues: 2,
  wednesday: 3,
  wed: 3,
  thursday: 4,
  thu: 4,
  thur: 4,
  thurs: 4,
  friday: 5,
  fri: 5,
  saturday: 6,
  sat: 6
});

const MONTH_INDEX = Object.freeze({
  january: 0,
  jan: 0,
  february: 1,
  feb: 1,
  march: 2,
  mar: 2,
  april: 3,
  apr: 3,
  may: 4,
  june: 5,
  jun: 5,
  july: 6,
  jul: 6,
  august: 7,
  aug: 7,
  september: 8,
  sept: 8,
  sep: 8,
  october: 9,
  oct: 9,
  november: 10,
  nov: 10,
  december: 11,
  dec: 11
});

const PAYMENT_STATUS_RULES = Object.freeze([
  { value: "Same Day Payment", pattern: /\bsame(?:-|\s)day\s+payment\b/i },
  { value: "Same Day Payment", pattern: /\bpayment\s*:?\s*same(?:-|\s)day\b/i },
  { value: "Same Day Payment", pattern: /\bsame(?:-|\s)?day\b/i },
  { value: "Prepaid", pattern: /\bprepaid\b/i },
  { value: "Cash", pattern: /\bcash\b/i },
  { value: /\bunpaid\b/i, pattern: /\bunpaid\b/i, normalizedValue: "Unpaid" },
  { value: "Pending", pattern: /\bpending\b/i },
  { value: "Card", pattern: /\bcard\b/i },
  { value: "Account", pattern: /\baccount\b/i },
  { value: "Invoice", pattern: /\binvoice\b/i },
  { value: "Paid", pattern: /\bpaid\b/i }
]);

const VEHICLE_RULES = Object.freeze([
  { pattern: /\bany\s+estate\s+car\b/i, value: "Any estate car" },
  { pattern: /\bany\s+saloon\s+car\b/i, value: "Saloon Car" },
  { pattern: /\bany\s+car\b/i, value: "Any Car" },
  { pattern: /^any$/i, value: "Any Car" },
  { pattern: /\bsaloon\s+car\b/i, value: "Saloon Car" },
  { pattern: /\bsaloon\b/i, value: "Saloon" },
  { pattern: /\bestate\s+car\b/i, value: "Estate Car" },
  { pattern: /\bestate\b/i, value: "Estate" },
  { pattern: /\bexecutive\s+saloon\b/i, value: "Executive" },
  { pattern: /\bexecutive\b/i, value: "Executive" },
  { pattern: /\bexec\b/i, value: "Executive" },
  { pattern: /\bmpv[-\s]?8\b/i, value: "MPV-8" },
  { pattern: /\bmpv[-\s]?7\b/i, value: "MPV-7" },
  { pattern: /\bmpv[-\s]?\d\b/i, value: "MPV" },
  { pattern: /\bmpv\b/i, value: "MPV" },
  { pattern: /\b8\s*seater\b/i, value: "8 Seater" },
  { pattern: /\b6\s*seater\b/i, value: "6 Seater" },
  { pattern: /\bv\s*class\b/i, value: "V Class" },
  { pattern: /\bvito\b/i, value: "Vito" },
  { pattern: /\bany\s*[89]\s*\/\s*[89]\s*seater\b/i, value: "8 Seater" },
  { pattern: /\b[89]\s*\/\s*[89]\s*seater\b/i, value: "8 Seater" },
  { pattern: /\bminibus\b/i, value: "Minibus" },
  { pattern: /\bmercedes\b/i, value: "Mercedes" },
  { pattern: /\bprius\b/i, value: "Prius" },
  { pattern: /\bcorolla\b/i, value: "Corolla" }
]);

const ROUTE_HINT_PATTERN =
  /\b(airport|terminal|station|stn|heathrow|gatwick|stansted|luton|lhr|lgw|ltn|euston|paddington|victoria|road|rd|street|st|close|cl|lane|ln|avenue|ave|court|ct|drive|dr|square|sq|park|flat|suite|hotel)\b/i;
const UK_POSTCODE_PATTERN = /\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b/i;
const UK_POSTCODE_DISTRICT_PATTERN = /^[A-Z]{1,2}\d[A-Z\d]?$/i;
const KNOWN_AIRPORT_CODE_PATTERN = /^(?:LHR|LGW|LTN|STN|LCY|SEN)$/i;
const DATE_WORD_MONTH_YEAR_PATTERN =
  /\b(?:(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+)?(\d{1,2})(?:st|nd|rd|th)?[\s-]+([a-z]+)[\s,/-]+(\d{4})\b/i;
const DATE_NUMERIC_PATTERN =
  /\b(?:(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+)?(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})\b/i;
const DATE_DAY_MONTH_PATTERN =
  /\b(?:(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+)?(\d{1,2})(?:st|nd|rd|th)?[\s-]+([a-z]+)\b/i;
const DATE_ISO_PATTERN = /\b(\d{4})-(\d{1,2})-(\d{1,2})\b/;
const RELATIVE_DAY_PATTERN = /\b(today|tomorrow|tonight|now|asap)\b/i;
const WEEKDAY_ONLY_PATTERN =
  /\b(mon|monday|tue|tues|tuesday|wed|wednesday|thu|thur|thurs|thursday|fri|friday|sat|saturday|sun|sunday)\b/i;
const TIME_MERIDIEM_PATTERN = /\b(?:at\s*)?(\d{1,2})(?::(\d{1,2}))?\s*(am|pm)\b/i;
const TIME_24H_PATTERN = /\b(?:at\s*)?([01]?\d|2[0-3]):([0-5]?\d)\b/i;
const SHORT_CARD_TIME_PATTERN =
  /^(today|tomorrow|tonight|asap|now|mon|monday|tue|tues|tuesday|wed|wednesday|thu|thur|thurs|thursday|fri|friday|sat|saturday|sun|sunday)\b[\s,/@-]*([0-2]?\d(?::\d{1,2})?(?:\s*(?:am|pm))?)$/i;
const ROUTE_SPLIT_PATTERNS = Object.freeze([
  /\bpick(?:\s*|-)?up\b\s*:?\s*(.+?)\s+\b(?:drop(?:\s*|-)?off|drop)\b\s*:?\s*(.+)$/i,
  /^(.+?)\s+\b(?:drop(?:\s*|-)?off|drop)\b\s*:?\s*(.+)$/i,
  /^\s*from\s+(.+?)\s+\bto\b\s+(.+)$/i,
  /^(.+?)\s*(?:->|=>|[-=]{1,2}>)\s*(.+)$/i,
  /^(.+?)\s+\bto\b\s+(.+)$/i,
  /^(.+?)\s+-\s+(.+)$/i
]);
const ROUTE_CONTEXT_PREFIX_PATTERN =
  /^(?:(?:today|tomorrow|tonight)\b\s*@?\s*\d{1,2}(?::\d{1,2})?\s*(?:am|pm)?\s*|at\s*\d{1,2}(?::\d{1,2})?\s*(?:am|pm)?\s*)/i;
const ROUTE_INLINE_FARE_SUFFIX_PATTERN =
  /\s*=\s*(?:\u00A3|\$|\u20AC)?\s*\d+(?:[.,]\d{1,2})?(?:\s*net(?:\s+amount)?)?(?:\s+(?:same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid))?\s*$/i;
const STREET_START_PATTERN = /(?:,\s*|\s+)(\d{1,5}[A-Za-z]?\s+(?!terminal\b|term\b)[A-Za-z].*)/i;
const EXPLICIT_PICKUP_LABEL_PATTERN = /^(?:pick(?:\s*|-)?up(?:\s+location)?|pickup\s+location)\b(?!\s*time)\s*:?\s*(.*)$/i;
const EXPLICIT_DROP_LABEL_PATTERN = /^\d*\s*(?:drop(?:\s*|-)?off(?:\s+location)?|dropoff(?:\s+location)?)\b\s*:?\s*(.*)$/i;
const VEHICLE_LABEL_PATTERN = /^(?:vehicle|required\s+vehicle|vehcile\s+type|vehicle\s+type)\b\s*:?\s*(.*)$/i;
const FARE_LABEL_PATTERN = /^(?:best\s+price|net\s+fare|fare|price|cost|net)\b\s*:?\s*(.*)$/i;
const JOB_REFERENCE_PATTERN =
  /\b(?:job\s*alert|booking|ref(?:erence)?|ace\s*\d[\w-]*|[A-Z]{2,}\d{3,}(?:-\d+)?)\b/i;
const PASSENGER_PATTERN = /\b\d+\s*(?:persons?|pax|passengers?)\b/i;
const BAG_PATTERN = /\b\d+\s*bags?\b/i;
const FLIGHT_SECTION_PATTERN = /^(?:flight|arriving\s+from)\b/i;
const FLIGHT_CODE_PATTERN = /\b[A-Z]{1,3}\d{2,4}\b/i;
const FARE_EXCLUSION_PATTERN =
  /(?:\+?\d{7,}|flight\b|arriving\s+from\b|\bpax\b|\bpersons?\b|\bpassengers?\b|\bbags?\b|ref(?:erence)?\b)/i;
const PAYMENT_STATUS_NOISE_PATTERN =
  /\b(?:same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid)\b/gi;
const CURRENCY_PREFIX_AMOUNT_PATTERN =
  /((?:\u00A3|\$|\u20AC)\s*\d+(?:[.,]\d{1,2})?|\b(?:gbp|usd|eur|pkr|rs\.?)\s*\d+(?:[.,]\d{1,2})?)/i;
const CURRENCY_SUFFIX_AMOUNT_PATTERN = /(\d+(?:[.,]\d{1,2})?)\s*(\u00A3|\$|\u20AC)/i;
const INLINE_EQUALS_FARE_PATTERN = /=\s*(\d+(?:[.,]\d{1,2})?)\b/i;
const BARE_NET_FARE_PATTERN = /^(\d+(?:[.,]\d{1,2})?)\s*net(?:\s+amount)?\b/i;
const LABELED_BARE_FARE_PATTERN =
  /^(?:best\s+price|net\s+fare|fare|price|cost|net)\b\s*:?\s*(\d+(?:[.,]\d{1,2})?)\b/i;
const PURE_AMOUNT_PATTERN = /^\d+(?:[.,]\d{1,2})?$/;
const PURE_PHONE_PATTERN = /^\+?\d{7,}$/;
const PURE_TIME_PATTERN = /^\d{1,2}(?::\d{2})?\s*(?:am|pm)?$/i;
const LOCATION_BLOCKED_PATTERN =
  /\b(?:same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid|flight(?:\s+number)?|arriving\s+from|job\s*alert|price|fare|cost|net\s+fare|net\s+amount|required\s+vehicle|vehicle)\b/i;
const VEHICLE_BLOCKED_PATTERN =
  /\b(?:today|tomorrow|tonight|same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid|flight|arriving\s+from|job\s*alert|pick(?:\s*|-)?up|drop(?:\s*|-)?off|route|price|fare|cost)\b/i;
const BLOCKED_LOCATION_SECTIONS = new Set(["passenger", "mobile", "flight", "arriving"]);

function normalizeCurrencySymbols(value) {
  return String(value || "")
    .replace(/Ã‚Â£/g, "\u00A3")
    .replace(/Â£/g, "\u00A3")
    .replace(/\u00A0/g, " ");
}

function splitLines(rawMessage) {
  return normalizeLineEndings(normalizeCurrencySymbols(rawMessage))
    .split("\n")
    .map((line) => String(line || "").replace(/\t/g, " "));
}

function cleanLineText(line) {
  return collapseWhitespace(
    normalizeCurrencySymbols(line)
      .replace(/^[^\p{L}\p{N}]+/u, "")
      .replace(/^\d+\.\s*/u, "")
      .replace(/^(?:â€¢|â—|â—¦)+/g, " ")
      .replace(/^[\u2022\u00B7\u25AA\u25E6\u25CF\u25C6\u25BA]+/g, " ")
      .replace(/^\s*[-*]+\s*/, " ")
  );
}

function normalizeVehicleText(value) {
  let text = safeTrim(normalizeCurrencySymbols(value));
  if (!text) return "";

  text = safeTrim(text.replace(VEHICLE_LABEL_PATTERN, "$1"));
  text = safeTrim(text.replace(/\((?:same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid)\)/gi, ""));
  text = safeTrim(text.replace(/\((?:[^)]*\d+\s*(?:persons?|pax|bags?)[^)]*)\)/gi, ""));
  text = safeTrim(text.replace(PASSENGER_PATTERN, ""));
  text = safeTrim(text.replace(BAG_PATTERN, ""));
  text = collapseWhitespace(text.replace(/[|;,]+/g, " "));
  if (!text) return "";

  for (const rule of VEHICLE_RULES) {
    if (rule.pattern.test(text)) {
      return rule.value;
    }
  }

  return "";
}

function extractPaymentStatus(value) {
  const text = String(value || "");
  for (const rule of PAYMENT_STATUS_RULES) {
    if (rule.pattern.test(text)) {
      return rule.normalizedValue || rule.value;
    }
  }
  return "";
}

function normalizeFareText(value) {
  return collapseWhitespace(
    normalizeCurrencySymbols(value)
      .replace(/\?\s*(?=\d)/g, "")
      .replace(/(\d)\?/g, "$1")
  );
}

function normalizeFareAmount(amountValue) {
  const normalized = safeTrim(String(amountValue || "").replace(/,/g, ""));
  if (!normalized) return "";

  const numeric = Number(normalized);
  if (!Number.isFinite(numeric)) return normalized;
  return Number.isInteger(numeric) ? String(numeric) : String(numeric);
}

function extractNormalizedFareCandidate(value, options = {}) {
  const text = normalizeFareText(value);
  const allowBareNumber = Boolean(options.allowBareNumber);
  if (!text) return "";
  if (FARE_EXCLUSION_PATTERN.test(text)) return "";

  const sanitized = safeTrim(text.replace(PAYMENT_STATUS_NOISE_PATTERN, ""));
  if (!sanitized) return "";

  const prefixMatch = sanitized.match(CURRENCY_PREFIX_AMOUNT_PATTERN);
  if (prefixMatch) {
    const amountMatch = prefixMatch[1].match(/(\d+(?:[.,]\d{1,2})?)/);
    return normalizeFareAmount(amountMatch?.[1] || "");
  }

  const suffixMatch = sanitized.match(CURRENCY_SUFFIX_AMOUNT_PATTERN);
  if (suffixMatch) {
    return normalizeFareAmount(suffixMatch[1]);
  }

  const embeddedNetMatch = sanitized.match(/(?:^|[^\d])(\d+(?:[.,]\d{1,2})?)\s*net\b/i);
  if (embeddedNetMatch) {
    return normalizeFareAmount(embeddedNetMatch[1]);
  }

  const bareMatch =
    sanitized.match(INLINE_EQUALS_FARE_PATTERN) ||
    sanitized.match(BARE_NET_FARE_PATTERN) ||
    sanitized.match(LABELED_BARE_FARE_PATTERN);
  if (bareMatch) {
    return normalizeFareAmount(bareMatch[1]);
  }

  if (allowBareNumber && PURE_AMOUNT_PATTERN.test(sanitized)) {
    return normalizeFareAmount(sanitized);
  }

  return "";
}

function formatHourValue(hour24, minute) {
  const normalizedHour = Number(hour24);
  const normalizedMinute = Number(minute || 0);
  if (!Number.isFinite(normalizedHour) || !Number.isFinite(normalizedMinute)) return "";
  if (normalizedHour < 0 || normalizedHour > 23) return "";
  if (normalizedMinute < 0 || normalizedMinute > 59) return "";

  const meridiem = normalizedHour >= 12 ? "pm" : "am";
  const hour12 = normalizedHour % 12 || 12;
  return `${hour12}:${String(normalizedMinute).padStart(2, "0")} ${meridiem}`;
}

function extractTimeFromText(value) {
  const text = collapseWhitespace(String(value || "").replace(/@/g, " "));
  if (!text) return "";

  const meridiemMatch = text.match(TIME_MERIDIEM_PATTERN);
  if (meridiemMatch) {
    const rawHour = Number(meridiemMatch[1]);
    const minute = Number(meridiemMatch[2] || 0);
    const meridiem = meridiemMatch[3].toLowerCase();
    if (!Number.isFinite(rawHour) || rawHour > 23) return "";

    let hour24 = rawHour;
    if (rawHour <= 12) {
      hour24 = rawHour % 12;
      if (meridiem === "pm") hour24 += 12;
    }

    return formatHourValue(hour24, minute);
  }

  const time24Match = text.match(TIME_24H_PATTERN);
  if (time24Match) {
    return formatHourValue(Number(time24Match[1]), Number(time24Match[2]));
  }

  return "";
}

function resolveSectionLabel(text) {
  if (!text) return "";
  if (/^head\s*passenger\b/i.test(text)) return "passenger";
  if (/^mobile\s*number\b/i.test(text)) return "mobile";
  if (/^flight\b/i.test(text)) return "flight";
  if (/^arriving\s*from\b/i.test(text)) return "arriving";
  if (/^route\b/i.test(text)) return "route";
  if (/^landing\b/i.test(text)) return "landing";
  return "";
}

function getTimeZoneDateParts(input, timeZone) {
  const date = input instanceof Date ? input : new Date(input);
  if (Number.isNaN(date.getTime())) return null;

  const formatter = new Intl.DateTimeFormat("en-GB", {
    timeZone: timeZone || "Europe/London",
    year: "numeric",
    month: "numeric",
    day: "numeric",
    weekday: "long"
  });
  const parts = formatter.formatToParts(date);
  const mapped = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      mapped[part.type] = part.value;
    }
  }

  const weekdayKey = String(mapped.weekday || "").toLowerCase();
  return {
    year: Number(mapped.year),
    month: Number(mapped.month),
    day: Number(mapped.day),
    weekdayName: mapped.weekday || "",
    weekdayIndex: WEEKDAY_INDEX[weekdayKey]
  };
}

function buildUtcNoonDate(year, monthIndex, day) {
  return new Date(Date.UTC(year, monthIndex, day, 12, 0, 0));
}

function addDays(date, days) {
  return new Date(date.getTime() + days * 24 * 60 * 60 * 1000);
}

function ordinalDay(day) {
  const remainder100 = day % 100;
  if (remainder100 >= 11 && remainder100 <= 13) return `${day}th`;
  const remainder10 = day % 10;
  if (remainder10 === 1) return `${day}st`;
  if (remainder10 === 2) return `${day}nd`;
  if (remainder10 === 3) return `${day}rd`;
  return `${day}th`;
}

function formatPickupDayDate(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-GB", {
    weekday: "long",
    month: "long",
    year: "numeric",
    day: "numeric",
    timeZone: timeZone || "Europe/London"
  });
  const parts = formatter.formatToParts(date);
  const mapped = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      mapped[part.type] = part.value;
    }
  }

  const day = Number(mapped.day);
  if (!Number.isFinite(day)) return "";
  return `${mapped.weekday} ${ordinalDay(day)} ${mapped.month} ${mapped.year}`;
}

function resolveWeekdayDate(targetWeekdayIndex, receivedAt, timeZone) {
  const baseParts = getTimeZoneDateParts(receivedAt, timeZone);
  if (!baseParts || !Number.isFinite(targetWeekdayIndex)) return null;

  const baseDate = buildUtcNoonDate(baseParts.year, baseParts.month - 1, baseParts.day);
  const currentWeekday = Number(baseParts.weekdayIndex);
  if (!Number.isFinite(currentWeekday)) return null;

  let dayDelta = (targetWeekdayIndex - currentWeekday + 7) % 7;
  if (dayDelta < 0) dayDelta += 7;
  return addDays(baseDate, dayDelta);
}

function resolveRelativeDate(token, receivedAt, timeZone) {
  const baseParts = getTimeZoneDateParts(receivedAt, timeZone);
  if (!baseParts) return null;

  const baseDate = buildUtcNoonDate(baseParts.year, baseParts.month - 1, baseParts.day);
  const normalized = String(token || "").toLowerCase();
  if (normalized === "tomorrow") return addDays(baseDate, 1);
  return baseDate;
}

function parseYearValue(value, fallbackYear) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallbackYear;
  if (parsed < 100) return 2000 + parsed;
  return parsed;
}

function resolveAbsoluteDateFromText(text, receivedAt, timeZone) {
  const baseParts = getTimeZoneDateParts(receivedAt, timeZone);
  if (!baseParts) return null;

  let match = text.match(DATE_ISO_PATTERN);
  if (match) {
    return buildUtcNoonDate(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  }

  match = text.match(DATE_WORD_MONTH_YEAR_PATTERN);
  if (match) {
    const monthIndex = MONTH_INDEX[String(match[3] || "").toLowerCase()];
    if (Number.isFinite(monthIndex)) {
      return buildUtcNoonDate(Number(match[4]), monthIndex, Number(match[2]));
    }
  }

  match = text.match(DATE_NUMERIC_PATTERN);
  if (match) {
    const day = Number(match[2]);
    const month = Number(match[3]);
    const year = parseYearValue(match[4], baseParts.year);
    if (day >= 1 && day <= 31 && month >= 1 && month <= 12) {
      return buildUtcNoonDate(year, month - 1, day);
    }
  }

  match = text.match(DATE_DAY_MONTH_PATTERN);
  if (match) {
    const monthIndex = MONTH_INDEX[String(match[3] || "").toLowerCase()];
    if (Number.isFinite(monthIndex)) {
      return buildUtcNoonDate(baseParts.year, monthIndex, Number(match[2]));
    }
  }

  return null;
}

function extractResolvedDate(value, context = {}) {
  const text = collapseWhitespace(String(value || ""));
  if (!text) return { value: "", confidence: "none" };

  const timeZone = context.timeZone || "Europe/London";
  const receivedAt = context.receivedAt || context.received_at || new Date().toISOString();

  const absoluteDate = resolveAbsoluteDateFromText(text, receivedAt, timeZone);
  if (absoluteDate) {
    return {
      value: formatPickupDayDate(absoluteDate, timeZone),
      confidence: /(?:\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})/.test(text) ? "high" : "medium"
    };
  }

  const relativeMatch = text.match(RELATIVE_DAY_PATTERN);
  if (relativeMatch) {
    const resolved = resolveRelativeDate(relativeMatch[1], receivedAt, timeZone);
    if (resolved) {
      return {
        value: formatPickupDayDate(resolved, timeZone),
        confidence: "medium"
      };
    }
  }

  const weekdayMatch = text.match(WEEKDAY_ONLY_PATTERN);
  if (weekdayMatch) {
    const weekdayIndex = WEEKDAY_INDEX[String(weekdayMatch[1] || "").toLowerCase()];
    const resolved = resolveWeekdayDate(weekdayIndex, receivedAt, timeZone);
    if (resolved) {
      return {
        value: formatPickupDayDate(resolved, timeZone),
        confidence: "medium"
      };
    }
  }

  return { value: "", confidence: "none" };
}

function stripLocationLabels(text) {
  return safeTrim(
    String(text || "")
      .replace(/^(?:pick(?:\s*|-)?up|drop(?:\s*|-)?off|dropoff)\b\s*:?\s*/i, "")
      .replace(/^(?:route|landing)\b\s*:?\s*/i, "")
  );
}

function cleanLocationText(value) {
  const cleaned = collapseWhitespace(
    normalizeCurrencySymbols(value)
      .replace(ROUTE_CONTEXT_PREFIX_PATTERN, "")
      .replace(ROUTE_INLINE_FARE_SUFFIX_PATTERN, "")
      .replace(/\s*[.:-]?\s*\??\u00A3?\d+(?:[.,]\d{1,2})?\s*net\b.*$/i, "")
      .replace(/\s*[.:-]?\s*\u00A3?\??\d+(?:[.,]\d{1,2})?\s*(?:cash|card|account|invoice|paid|prepaid|pending|unpaid)\b.*$/i, "")
      .replace(/\b\d+(?:\.\d+)?\s*mile(?:s)?\s+job\b.*$/i, "")
      .replace(/[.]{2,}\s*(?:\d+(?:\.\d+)?\s*miles?\b.*|\u00A3?\d+(?:[.,]\d{1,2})?.*|\d+(?:[.,]\d{1,2})?\s*net\b.*)$/i, "")
      .replace(/\b\d+(?:\.\d+)?\s*miles?\b.*$/i, "")
      .replace(/\b(?:net|fare|price|cost)\b\s*:?\s*\u00A3?\d+(?:[.,]\d{1,2})?.*$/i, "")
      .replace(/^\s*[|:;,-]+\s*/, "")
      .replace(/\s*[|:;,-]+\s*$/, "")
      .replace(/[.]{2,}$/g, "")
  );
  return stripLocationLabels(cleaned);
}

function containsLocationContamination(value) {
  const text = collapseWhitespace(String(value || ""));
  if (!text) return false;
  if (LOCATION_BLOCKED_PATTERN.test(text)) return true;
  if (FLIGHT_CODE_PATTERN.test(text) && /flight/i.test(text)) return true;
  if (TIME_MERIDIEM_PATTERN.test(text) || TIME_24H_PATTERN.test(text)) return true;
  return false;
}

function isLocationCandidateValue(value) {
  const text = cleanLocationText(value);
  if (!text) return false;
  if (PURE_PHONE_PATTERN.test(text)) return false;
  if (PURE_AMOUNT_PATTERN.test(text)) return false;
  if (PURE_TIME_PATTERN.test(text)) return false;
  if (containsLocationContamination(text)) return false;
  if (normalizeVehicleText(text)) return false;
  if (JOB_REFERENCE_PATTERN.test(text)) return false;

  if (UK_POSTCODE_PATTERN.test(text)) return true;
  if (UK_POSTCODE_DISTRICT_PATTERN.test(text)) return true;
  if (KNOWN_AIRPORT_CODE_PATTERN.test(text)) return true;
  if (ROUTE_HINT_PATTERN.test(text)) return true;
  if (/^\d{1,5}[A-Za-z]?\s+[A-Za-z]/.test(text)) return true;
  if (/^[A-Z]{2,5}\s*T\d$/i.test(text)) return true;
  if (/[A-Za-z]/.test(text) && text.split(/\s+/).length >= 2) return true;

  return false;
}

function buildLocationCandidate(value, lineNumbers = [], source = "", options = {}) {
  const cleaned = cleanLocationText(value);
  if (!cleaned || !isLocationCandidateValue(cleaned)) return null;
  return {
    value: cleaned,
    lineNumbers,
    source,
    contaminated: Boolean(options.contaminated && containsLocationContamination(cleaned))
  };
}

function splitMergedPickupByAddressBoundary(value) {
  const text = cleanLocationText(value);
  if (!text) return null;

  const match = text.match(STREET_START_PATTERN);
  if (!match) return null;

  const destination = cleanLocationText(match[1]);
  const boundaryIndex = match.index + match[0].indexOf(match[1]);
  const pickup = cleanLocationText(text.slice(0, boundaryIndex));

  if (!pickup || !destination) return null;
  if (!isLocationCandidateValue(pickup) || !isLocationCandidateValue(destination)) return null;
  if (!ROUTE_HINT_PATTERN.test(pickup)) return null;
  if (!(destination.startsWith("0") || /^\d/.test(destination) || UK_POSTCODE_PATTERN.test(destination))) {
    return null;
  }

  return {
    pickup: buildLocationCandidate(pickup, [], "merged_route"),
    drop_off: buildLocationCandidate(destination, [], "merged_route")
  };
}

function splitRouteByPatterns(value) {
  const cleanedText = cleanLocationText(value);
  if (!cleanedText) return null;

  for (const pattern of ROUTE_SPLIT_PATTERNS) {
    const match = cleanedText.match(pattern);
    if (!match) continue;

    const pickup = buildLocationCandidate(match[1], [], "inline_route");
    const dropOff = buildLocationCandidate(match[2], [], "inline_route");
    if (pickup && dropOff) {
      return {
        pickup,
        drop_off: dropOff
      };
    }
  }

  return splitMergedPickupByAddressBoundary(cleanedText);
}

function buildLineRecord(rawLine, index, context = {}, section = "") {
  const raw = String(rawLine || "");
  const text = cleanLineText(raw);
  const lower = text.toLowerCase();
  const vehicleValue = normalizeVehicleText(text);
  const paymentStatus = extractPaymentStatus(text);
  const labeledFare = text.match(FARE_LABEL_PATTERN);
  const fareValue =
    (labeledFare &&
      extractNormalizedFareCandidate(labeledFare[1], {
        allowBareNumber: true
      })) ||
    extractNormalizedFareCandidate(text, {
      allowBareNumber:
        PURE_AMOUNT_PATTERN.test(text) ||
        /\bnet\b/i.test(text) ||
        INLINE_EQUALS_FARE_PATTERN.test(text)
    });
  const explicitPickupMatch = text.match(EXPLICIT_PICKUP_LABEL_PATTERN);
  const explicitDropMatch = text.match(EXPLICIT_DROP_LABEL_PATTERN);
  const explicitVehicleMatch = text.match(VEHICLE_LABEL_PATTERN);
  const routeSplit = splitRouteByPatterns(text);
  const resolvedDate = extractResolvedDate(text, context);
  const timeValue = extractTimeFromText(text);
  const hasResolvedDate = Boolean(resolvedDate.value);
  const hasRelativeOrWeekdayToken = RELATIVE_DAY_PATTERN.test(text) || WEEKDAY_ONLY_PATTERN.test(text);

  return {
    index,
    lineNumber: index + 1,
    raw,
    text,
    lower,
    isBlank: !text,
    explicitPickupMatch,
    explicitDropMatch,
    explicitVehicleMatch,
    routeSplit,
    section,
    paymentStatus,
    fareValue,
    vehicleValue,
    resolvedDate,
    timeValue,
    hasFareLike: Boolean(labeledFare || fareValue || CURRENCY_PREFIX_AMOUNT_PATTERN.test(text) || CURRENCY_SUFFIX_AMOUNT_PATTERN.test(text) || INLINE_EQUALS_FARE_PATTERN.test(text)),
    hasPaymentLike: Boolean(paymentStatus),
    hasFlightLike: FLIGHT_SECTION_PATTERN.test(text) || (/flight/i.test(text) && FLIGHT_CODE_PATTERN.test(text)),
    hasReferenceLike: JOB_REFERENCE_PATTERN.test(text),
    hasPassengerLike: PASSENGER_PATTERN.test(text) || BAG_PATTERN.test(text),
    hasVehicleLike: Boolean(vehicleValue || explicitVehicleMatch),
    hasDateLike: Boolean(hasResolvedDate || hasRelativeOrWeekdayToken),
    hasTimeLike: Boolean(timeValue || TIME_MERIDIEM_PATTERN.test(text) || TIME_24H_PATTERN.test(text)),
    hasRouteLike: Boolean(routeSplit || /\bfrom\b/i.test(text) || /\bto\b/i.test(text) || /->|=>/.test(text)),
    hasLocationLike: isLocationCandidateValue(text)
  };
}

function buildLineRecords(rawMessage, context = {}) {
  const rawLines = splitLines(rawMessage);
  const records = [];
  let activeSection = "";

  for (let index = 0; index < rawLines.length; index += 1) {
    const rawLine = rawLines[index];
    const text = cleanLineText(rawLine);
    const sectionLabel = resolveSectionLabel(text);
    const section = sectionLabel || activeSection;
    const record = buildLineRecord(rawLine, index, context, section);
    records.push(record);

    if (!record.text) {
      activeSection = "";
      continue;
    }

    if (sectionLabel) {
      activeSection = sectionLabel;
    }
  }

  return records;
}

function buildCandidateMap(lines) {
  return {
    pickup: lines.filter((line) => line.explicitPickupMatch).map((line) => `L${line.lineNumber}: ${line.text}`),
    drop_off: lines.filter((line) => line.explicitDropMatch).map((line) => `L${line.lineNumber}: ${line.text}`),
    route: lines.filter((line) => line.routeSplit).map((line) => `L${line.lineNumber}: ${line.text}`),
    date: lines.filter((line) => line.hasDateLike).map((line) => `L${line.lineNumber}: ${line.text}`),
    time: lines.filter((line) => line.hasTimeLike).map((line) => `L${line.lineNumber}: ${line.text}`),
    vehicle: lines.filter((line) => line.hasVehicleLike).map((line) => `L${line.lineNumber}: ${line.text}`),
    fare: lines.filter((line) => line.hasFareLike).map((line) => `L${line.lineNumber}: ${line.text}`),
    payment_status: lines.filter((line) => line.hasPaymentLike).map((line) => `L${line.lineNumber}: ${line.text}`),
    flight_reference: lines
      .filter((line) => line.hasFlightLike || line.hasReferenceLike)
      .map((line) => `L${line.lineNumber}: ${line.text}`)
  };
}

function shouldStopLocationSection(line) {
  if (!line || line.isBlank) return true;
  if (BLOCKED_LOCATION_SECTIONS.has(line.section)) return true;
  if (line.explicitPickupMatch || line.explicitDropMatch || line.explicitVehicleMatch) return true;
  if (line.hasFareLike || line.hasPaymentLike || line.hasFlightLike || line.hasReferenceLike) {
    return true;
  }
  if (line.hasDateLike || line.hasTimeLike || line.hasVehicleLike) return true;
  return false;
}

function extractLabeledSection(lines, field) {
  const matcher = field === "pickup" ? "explicitPickupMatch" : "explicitDropMatch";

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const match = line?.[matcher];
    if (!match) continue;

    const collected = [];
    const inline = safeTrim(match[1] || "");
    if (inline) {
      collected.push(inline);
    }

    const lineNumbers = [line.lineNumber];
    for (let nextIndex = index + 1; nextIndex < lines.length; nextIndex += 1) {
      const nextLine = lines[nextIndex];
      if (!nextLine || nextLine.isBlank) {
        if (collected.length > 0) break;
        continue;
      }
      if (shouldStopLocationSection(nextLine)) break;
      if (!nextLine.hasLocationLike) break;
      collected.push(nextLine.text);
      lineNumbers.push(nextLine.lineNumber);
    }

    const candidate = buildLocationCandidate(collected.join(", "), lineNumbers, `${field}_label`);
    if (candidate) {
      return candidate;
    }
  }

  return null;
}

function extractSchedule(lines, context = {}) {
  const timeZone = context.timeZone || context.appTimeZone || "Europe/London";
  const receivedAt = context.received_at || context.receivedAt || new Date().toISOString();
  let pickupDayDate = "";
  let startingTiming = "";
  let dateConfidence = "none";
  let timeConfidence = "none";
  let dateLineNumbers = [];
  let timeLineNumbers = [];
  let hadDateLikeText = false;
  let hadTimeLikeText = false;

  for (const line of lines) {
    if (line.hasDateLike) {
      hadDateLikeText = true;
      if (!pickupDayDate) {
        const resolved = extractResolvedDate(line.text, {
          receivedAt,
          timeZone
        });
        if (resolved.value) {
          pickupDayDate = resolved.value;
          dateConfidence = resolved.confidence;
          dateLineNumbers = [line.lineNumber];
        }
      }
    }

    if (line.hasTimeLike) {
      hadTimeLikeText = true;
      if (!startingTiming) {
        const resolvedTime = extractTimeFromText(line.text);
        if (resolvedTime) {
          startingTiming = resolvedTime;
          timeConfidence = "high";
          timeLineNumbers = [line.lineNumber];
        }
      }
    }
  }

  const shortCardLine = lines.find((line) => SHORT_CARD_TIME_PATTERN.test(line.text));
  if (shortCardLine) {
    const match = shortCardLine.text.match(SHORT_CARD_TIME_PATTERN);
    if (match) {
      hadDateLikeText = true;
      hadTimeLikeText = true;
      if (!pickupDayDate) {
        const resolved = extractResolvedDate(match[1], { receivedAt, timeZone });
        pickupDayDate = resolved.value || pickupDayDate;
        dateConfidence = resolved.confidence !== "none" ? resolved.confidence : dateConfidence;
        dateLineNumbers = [shortCardLine.lineNumber];
      }
      if (!startingTiming) {
        startingTiming = extractTimeFromText(match[2]) || startingTiming;
        if (startingTiming) {
          timeConfidence = "high";
          timeLineNumbers = [shortCardLine.lineNumber];
        }
      }
    }
  }

  return {
    pickup_day_date: pickupDayDate,
    starting_timing: startingTiming,
    hadDateLikeText,
    hadTimeLikeText,
    dateResolved: !hadDateLikeText || Boolean(pickupDayDate),
    timeResolved: !hadTimeLikeText || Boolean(startingTiming),
    selected: {
      pickup_day_date: {
        value: pickupDayDate,
        lineNumbers: dateLineNumbers,
        source: pickupDayDate ? "schedule" : "",
        confidence: dateConfidence
      },
      starting_timing: {
        value: startingTiming,
        lineNumbers: timeLineNumbers,
        source: startingTiming ? "schedule" : "",
        confidence: timeConfidence
      }
    }
  };
}

function extractVehicle(lines) {
  for (const line of lines) {
    if (!line.text) continue;
    if (line.explicitVehicleMatch) {
      const normalized = normalizeVehicleText(line.explicitVehicleMatch[1]);
      if (normalized) {
        return {
          value: normalized,
          lineNumbers: [line.lineNumber],
          source: "vehicle_label",
          confidence: "high"
        };
      }
    }
  }

  for (const line of lines) {
    if (!line.text || !line.vehicleValue) continue;
    if (BLOCKED_LOCATION_SECTIONS.has(line.section)) continue;
    if (line.hasFareLike || line.hasPaymentLike || line.hasFlightLike || line.hasReferenceLike) {
      continue;
    }
    if (line.hasDateLike || line.hasTimeLike || line.hasRouteLike) continue;
    if (line.hasPassengerLike && !/\(.*\d+\s*(?:persons?|pax).*\)/i.test(line.text)) continue;
    if (VEHICLE_BLOCKED_PATTERN.test(line.text) && !line.explicitVehicleMatch) continue;

    return {
      value: line.vehicleValue,
      lineNumbers: [line.lineNumber],
      source: "vehicle_line",
      confidence: "high"
    };
  }

  return {
    value: "",
    lineNumbers: [],
    source: "",
    confidence: "none"
  };
}

function extractFare(lines) {
  for (const line of lines) {
    if (BLOCKED_LOCATION_SECTIONS.has(line.section)) continue;
    const labeledMatch = line.text.match(FARE_LABEL_PATTERN);
    if (!labeledMatch) continue;
    const fare = extractNormalizedFareCandidate(labeledMatch[1], {
      allowBareNumber: true
    });
    if (fare) {
      return {
        value: fare,
        lineNumbers: [line.lineNumber],
        source: "fare_label",
        confidence: "high"
      };
    }
  }

  for (const line of lines) {
    if (BLOCKED_LOCATION_SECTIONS.has(line.section)) continue;
    if (!line.text || line.hasFlightLike || line.hasReferenceLike || line.hasPassengerLike) continue;
    const fare = extractNormalizedFareCandidate(line.text, {
      allowBareNumber:
        PURE_AMOUNT_PATTERN.test(line.text) ||
        /\bnet\b/i.test(line.text) ||
        INLINE_EQUALS_FARE_PATTERN.test(line.text)
    });
    if (fare) {
      return {
        value: fare,
        lineNumbers: [line.lineNumber],
        source: PURE_AMOUNT_PATTERN.test(line.text) ? "standalone_amount" : "fare_candidate",
        confidence: PURE_AMOUNT_PATTERN.test(line.text) ? "medium" : "high"
      };
    }
  }

  return {
    value: "",
    lineNumbers: [],
    source: "",
    confidence: "none"
  };
}

function buildRouteFromInlineLine(line, source) {
  if (!line?.text) return null;
  const split = splitRouteByPatterns(line.text);
  if (!split) return null;

  return {
    pickup: {
      ...split.pickup,
      lineNumbers: [line.lineNumber],
      source,
      confidence: "high"
    },
    drop_off: {
      ...split.drop_off,
      lineNumbers: [line.lineNumber],
      source,
      confidence: "high"
    }
  };
}

function extractRouteFromToSeparator(lines) {
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    if (!line?.text || !/^to$/i.test(line.text)) continue;

    const pickupParts = [];
    const pickupLineNumbers = [];
    for (let prev = index - 1; prev >= 0; prev -= 1) {
      const candidate = lines[prev];
      if (!candidate || candidate.isBlank) break;
      if (/\bjobs?\b/i.test(candidate.text)) break;
      if (BLOCKED_LOCATION_SECTIONS.has(candidate.section)) break;
      if (
        candidate.hasFareLike ||
        candidate.hasPaymentLike ||
        candidate.hasFlightLike ||
        candidate.hasReferenceLike ||
        candidate.hasVehicleLike
      ) {
        break;
      }
      if (!candidate.hasLocationLike) break;
      pickupParts.unshift(candidate.text);
      pickupLineNumbers.unshift(candidate.lineNumber);
    }

    const dropParts = [];
    const dropLineNumbers = [];
    for (let next = index + 1; next < lines.length; next += 1) {
      const candidate = lines[next];
      if (!candidate || candidate.isBlank) {
        if (dropParts.length > 0) break;
        continue;
      }
      if (
        BLOCKED_LOCATION_SECTIONS.has(candidate.section) ||
        candidate.hasPaymentLike ||
        candidate.hasFlightLike ||
        candidate.hasReferenceLike
      ) {
        break;
      }

      const cleaned = cleanLocationText(candidate.text);
      if (!cleaned) break;
      if (!candidate.hasLocationLike && dropParts.length > 0) break;
      dropParts.push(cleaned);
      dropLineNumbers.push(candidate.lineNumber);
      if (candidate.hasLocationLike) {
        continue;
      }
      break;
    }

    const pickupValue = cleanLocationText(pickupParts.join(", "));
    const dropValue = cleanLocationText(dropParts.join(", "));
    if (pickupValue && dropValue) {
      return {
        pickup: {
          value: pickupValue,
          lineNumbers: pickupLineNumbers,
          source: "to_separator",
          contaminated: false,
          confidence: "medium"
        },
        drop_off: {
          value: dropValue,
          lineNumbers: dropLineNumbers,
          source: "to_separator",
          contaminated: false,
          confidence: "medium"
        }
      };
    }
  }

  return null;
}

function extractRoute(lines) {
  const explicitPickup = extractLabeledSection(lines, "pickup");
  const explicitDrop = extractLabeledSection(lines, "drop_off");

  if (explicitPickup || explicitDrop) {
    let pickup = explicitPickup;
    let dropOff = explicitDrop;

    if (pickup && !dropOff) {
      const split = splitRouteByPatterns(pickup.value);
      if (split) {
        pickup = {
          ...split.pickup,
          lineNumbers: pickup.lineNumbers,
          source: "pickup_label_split",
          confidence: "high"
        };
        dropOff = {
          ...split.drop_off,
          lineNumbers: pickup.lineNumbers,
          source: "pickup_label_split",
          confidence: "high"
        };
      }
    }

    if (pickup || dropOff) {
      return {
        pickup: pickup || null,
        drop_off: dropOff || null
      };
    }
  }

  for (const line of lines) {
    if (BLOCKED_LOCATION_SECTIONS.has(line.section)) continue;
    const inlineRoute = buildRouteFromInlineLine(line, "inline_route");
    if (inlineRoute) {
      return inlineRoute;
    }
  }

  const separatorRoute = extractRouteFromToSeparator(lines);
  if (separatorRoute) {
    return separatorRoute;
  }

  const locationLines = lines.filter((line) => {
    if (!line.text) return false;
    if (BLOCKED_LOCATION_SECTIONS.has(line.section)) return false;
    if (line.hasFareLike || line.hasPaymentLike || line.hasFlightLike || line.hasReferenceLike) {
      return false;
    }
    if (line.hasDateLike || line.hasTimeLike || line.hasVehicleLike) return false;
    return line.hasLocationLike;
  });

  if (locationLines.length >= 2) {
    const pickup = buildLocationCandidate(
      locationLines[0].text,
      [locationLines[0].lineNumber],
      "location_pair"
    );
    const dropOff = buildLocationCandidate(
      locationLines[1].text,
      [locationLines[1].lineNumber],
      "location_pair"
    );
    if (pickup && dropOff) {
      return {
        pickup: {
          ...pickup,
          confidence: "medium"
        },
        drop_off: {
          ...dropOff,
          confidence: "medium"
        }
      };
    }
  }

  return {
    pickup: null,
    drop_off: null
  };
}

function computeLockedFields(selected) {
  const locked = [];
  for (const [field, meta] of Object.entries(selected || {})) {
    if (!meta?.value) continue;
    if (meta.confidence === "high" && !meta.contaminated) {
      locked.push(field);
    }
  }
  return locked;
}

function deriveConfidence(selected, schedule, issues) {
  let score = 0.15;
  if (selected.pickup?.value) score += 0.25;
  if (selected.drop_off?.value) score += 0.25;
  if (selected.required_vehicle?.value) score += 0.1;
  if (selected.fare?.value) score += 0.05;
  if (schedule.pickup_day_date) score += 0.1;
  if (schedule.starting_timing) score += 0.1;
  score -= (Array.isArray(issues) ? issues.length : 0) * 0.08;
  return Math.max(0, Math.min(1, Number(score.toFixed(2))));
}

function analyzeRideMessage(rawMessage, context = {}) {
  const raw = String(rawMessage || "");
  const lines = buildLineRecords(raw, context);
  const output = createEmptyRideObject({
    group_name: context.group_name || context.groupName || "",
    source_name: context.source_name || context.sourceName || "",
    source_time: context.source_time || context.sourceTime || ""
  });

  const schedule = extractSchedule(lines, context);
  const vehicle = extractVehicle(lines);
  const route = extractRoute(lines);
  const fare = extractFare(lines);
  const paymentStatus = extractPaymentStatus(raw);

  output.pickup_day_date = schedule.pickup_day_date;
  output.starting_timing = schedule.starting_timing;
  output.required_vehicle = vehicle.value;
  output.pickup = route.pickup?.value || "";
  output.drop_off = route.drop_off?.value || "";
  output.fare = fare.value;
  output.payment_status = paymentStatus;

  output.refer = generateRefer({
    messageId: context.message_id || context.messageId,
    rawMessage: raw,
    groupId: output.group_name,
    timestamp: context.received_at || context.receivedAt || context.timestamp || Date.now()
  });

  const issues = [];
  if ((schedule.hadDateLikeText && !schedule.dateResolved) || (schedule.hadTimeLikeText && !schedule.timeResolved)) {
    issues.push("schedule_unresolved");
  }
  if (!output.pickup) issues.push("pickup_missing");
  if (!output.drop_off) issues.push("drop_off_missing");
  if (route.pickup?.contaminated || route.drop_off?.contaminated) {
    issues.push("route_contaminated");
  }

  const selected = {
    pickup_day_date: schedule.selected.pickup_day_date,
    starting_timing: schedule.selected.starting_timing,
    pickup: route.pickup
      ? { ...route.pickup, contaminated: containsLocationContamination(route.pickup.value) }
      : { value: "", lineNumbers: [], source: "", confidence: "none", contaminated: false },
    drop_off: route.drop_off
      ? { ...route.drop_off, contaminated: containsLocationContamination(route.drop_off.value) }
      : { value: "", lineNumbers: [], source: "", confidence: "none", contaminated: false },
    required_vehicle: {
      value: vehicle.value,
      lineNumbers: vehicle.lineNumbers,
      source: vehicle.source,
      confidence: vehicle.confidence,
      contaminated: false
    },
    fare: {
      value: fare.value,
      lineNumbers: fare.lineNumbers,
      source: fare.source,
      confidence: fare.confidence,
      contaminated: false
    },
    payment_status: {
      value: paymentStatus,
      lineNumbers: lines.filter((line) => line.paymentStatus).map((line) => line.lineNumber),
      source: paymentStatus ? "payment_status" : "",
      confidence: paymentStatus ? "high" : "none",
      contaminated: false
    }
  };

  const analysis = {
    numberedLines: lines
      .filter((line) => line.text)
      .map((line) => `L${line.lineNumber}: ${line.text}`),
    deterministicCandidateMap: buildCandidateMap(lines),
    hadDateLikeText: schedule.hadDateLikeText,
    hadTimeLikeText: schedule.hadTimeLikeText,
    dateResolved: schedule.dateResolved,
    timeResolved: schedule.timeResolved,
    selected,
    lockedFields: computeLockedFields(selected),
    issues,
    confidence: deriveConfidence(selected, schedule, issues)
  };

  return {
    record: createEmptyRideObject(output),
    analysis
  };
}

function extractLocalRideFields(rawMessage, context = {}) {
  return analyzeRideMessage(rawMessage, context).record;
}

function createLocalExtractor({ logger } = {}) {
  const safeLogger = logger || { info: () => {} };

  return {
    extract(rawMessage, context = {}) {
      const { record } = analyzeRideMessage(rawMessage, context);
      safeLogger.info("Local extraction completed", {
        stage: "local_extraction",
        reason: record.refer
      });
      return record;
    },
    extractWithAnalysis(rawMessage, context = {}) {
      const result = analyzeRideMessage(rawMessage, context);
      safeLogger.info("Local extraction completed", {
        stage: "local_extraction",
        reason: result.record.refer
      });
      return result;
    }
  };
}

module.exports = {
  createLocalExtractor,
  extractLocalRideFields,
  analyzeRideMessage,
  containsLocationContamination,
  normalizeVehicleText
};
