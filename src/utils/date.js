function parseDateInput(input) {
  if (input instanceof Date) {
    return Number.isNaN(input.getTime()) ? null : input;
  }

  if (input === null || input === undefined || input === "") {
    return new Date();
  }

  const parsed = new Date(input);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toUtcIsoString(input) {
  const date = parseDateInput(input);
  return date ? date.toISOString() : "";
}

function formatInTimeZone(input, options = {}) {
  const date = parseDateInput(input);
  if (!date) return "";

  const locale = options.locale || "en-US";
  const timeZone = options.timeZone || "UTC";

  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone
  }).format(date);
}

function getReceivedAtFormats(receivedAt, options = {}) {
  const date = parseDateInput(receivedAt);
  if (!date) {
    return {
      utc: "",
      local: ""
    };
  }

  const localTimeZone =
    options.timeZone || Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";

  return {
    utc: date.toISOString(),
    local: formatInTimeZone(date, {
      locale: options.locale || "en-US",
      timeZone: localTimeZone
    })
  };
}

function formatDayAndDate(input, options = {}) {
  const date = parseDateInput(input);
  if (!date) return "";

  return new Intl.DateTimeFormat(options.locale || "en-US", {
    weekday: "short",
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone: options.timeZone || "UTC"
  }).format(date);
}

function nowUtcIso() {
  return toUtcIsoString(new Date());
}

module.exports = {
  parseDateInput,
  toUtcIsoString,
  formatInTimeZone,
  getReceivedAtFormats,
  formatDayAndDate,
  nowUtcIso
};
