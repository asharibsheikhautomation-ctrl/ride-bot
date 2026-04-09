const SECRET_KEY_PATTERN = /(api[_-]?key|token|secret|password|private[_-]?key|authorization|cookie)/i;
const RAW_TEXT_KEY_PATTERN = /(raw[_-]?message|body|messageText|content)/i;
const MESSAGE_ID_KEY_PATTERN = /(message[_-]?id|msg[_-]?id|chat[_-]?id)$/i;
const COMPACT_META_KEYS = [
  "stage",
  "sourceGroup",
  "group",
  "chatId",
  "refer",
  "messageId",
  "fallbackUsed",
  "attempt",
  "maxAttempts",
  "status",
  "reason"
];

function normalizeLevel(input) {
  const level = String(input || "info").toLowerCase();
  return ["error", "warn", "info", "debug"].includes(level) ? level : "info";
}

function normalizeMode(input, level) {
  const mode = String(input || "").toLowerCase();
  if (mode === "debug") return "debug";
  if (mode === "normal") return "normal";
  return level === "debug" ? "debug" : "normal";
}

function maskMessageId(value) {
  const text = String(value || "");
  if (!text) return "";
  if (text.length <= 8) return "***";
  return `${text.slice(0, 4)}...${text.slice(-4)}`;
}

function maskPhoneNumbers(value) {
  return String(value || "").replace(/(\+?\d[\d\s().-]{6,}\d)/g, (match) => {
    const digits = match.replace(/\D/g, "");
    if (digits.length <= 2) return "***";
    return `***${digits.slice(-2)}`;
  });
}

function truncateText(value, maxLength = 140) {
  const text = String(value || "");
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}...`;
}

function looksSensitiveString(value) {
  const text = String(value || "");
  if (!text) return false;
  if (text.includes("BEGIN PRIVATE KEY")) return true;
  if (/^sk-[a-z0-9]/i.test(text)) return true;
  if (text.length > 56 && !text.includes(" ")) return true;
  return false;
}

function sanitizeString(value, key = "") {
  if (SECRET_KEY_PATTERN.test(String(key))) return "[REDACTED]";
  if (looksSensitiveString(value)) return "[REDACTED]";

  const normalized = maskPhoneNumbers(String(value));
  if (RAW_TEXT_KEY_PATTERN.test(String(key))) {
    return truncateText(normalized.replace(/\s+/g, " ").trim(), 120);
  }

  if (MESSAGE_ID_KEY_PATTERN.test(String(key))) {
    return maskMessageId(normalized);
  }

  return normalized;
}

function sanitizeValue(value, key = "", seen = new WeakSet(), options = {}) {
  const mode = options.mode || "normal";

  if (SECRET_KEY_PATTERN.test(String(key))) return "[REDACTED]";
  if (value === null || value === undefined) return value;

  if (typeof value === "string") {
    return sanitizeString(value, key);
  }

  if (typeof value === "number" || typeof value === "boolean") {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (value instanceof Error) {
    const out = {
      name: value.name,
      message: sanitizeString(value.message || "", "error_message")
    };

    if (mode === "debug" && value.stack) {
      out.stack = sanitizeString(value.stack, "stack");
    }

    return out;
  }

  if (Array.isArray(value)) {
    return value.map((entry) => sanitizeValue(entry, key, seen, options));
  }

  if (typeof value === "object") {
    if (seen.has(value)) return "[Circular]";
    seen.add(value);

    const output = {};
    for (const [nestedKey, nestedValue] of Object.entries(value)) {
      const sanitized = sanitizeValue(nestedValue, nestedKey, seen, options);
      if (sanitized === undefined) continue;
      output[nestedKey] = sanitized;
    }
    return output;
  }

  return sanitizeString(String(value), key);
}

function toLineSafe(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (typeof value === "string") return truncateText(value, 80);
  try {
    return truncateText(JSON.stringify(value), 120);
  } catch (error) {
    return "[object]";
  }
}

function compactMeta(meta = {}) {
  const out = [];

  for (const key of COMPACT_META_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(meta, key)) continue;
    const value = meta[key];
    if (value === "" || value === null || value === undefined) continue;
    out.push(`${key}=${toLineSafe(value)}`);
  }

  return out.join(" ");
}

function simplifyErrorText(error, context = {}) {
  const message = String(error?.message || error || "");
  const lower = message.toLowerCase();
  const stage = String(context.stage || "").toLowerCase();
  const status = Number(error?.status || error?.response?.status || context.status);
  const code = String(error?.code || context.code || "");

  const result = {
    summary: context.defaultSummary || "Operation failed",
    likelyCause: context.likelyCause || "",
    status: Number.isFinite(status) ? status : null,
    code,
    fallbackUsed: Boolean(context.fallbackUsed)
  };

  if (code === "WHATSAPP_STARTUP_TIMEOUT") {
    result.summary = "WhatsApp startup failed: persisted session did not become ready";
    result.likelyCause =
      context.likelyCause ||
      error?.likelyCause ||
      "Saved login may be stale, linked device may be invalid, or browser startup is stuck";
    return result;
  }

  if (code === "WHATSAPP_AUTH_FAILED") {
    result.summary = "WhatsApp auth failure";
    result.likelyCause =
      context.likelyCause || error?.likelyCause || "Saved session is invalid or expired";
    return result;
  }

  if (code === "SHEETS_AUTH_FAILED") {
    result.summary = "Google Sheets authentication failed";
    result.likelyCause =
      context.likelyCause || "Service account credentials or private key are invalid";
    return result;
  }

  if (code === "SHEETS_PERMISSION_DENIED") {
    result.summary = "Google Sheets permission denied";
    result.likelyCause =
      context.likelyCause || "Share the spreadsheet with the service account email";
    return result;
  }

  if (code === "SHEETS_SPREADSHEET_NOT_FOUND") {
    result.summary = "Google Sheets spreadsheet not found";
    result.likelyCause = context.likelyCause || "Check GOOGLE_SHEETS_ID";
    return result;
  }

  if (code === "SHEETS_WORKSHEET_NOT_FOUND") {
    result.summary = "Google Sheets worksheet not found";
    result.likelyCause = context.likelyCause || "Check worksheet name/range";
    return result;
  }

  if (code === "SHEETS_NETWORK_TIMEOUT") {
    result.summary = "Google Sheets request failed: network timeout";
    result.likelyCause = context.likelyCause || "Network issue or temporary API outage";
    return result;
  }

  if (
    (stage.includes("openai") || context.provider === "openai") &&
    (status === 401 ||
      lower.includes("incorrect api key") ||
      lower.includes("invalid api key") ||
      lower.includes("unauthorized"))
  ) {
    result.summary = "OpenAI request failed: invalid API key";
    result.likelyCause = "Check OPENAI_API_KEY";
    return result;
  }

  if (
    stage.includes("geocod") &&
    (context.noResults || lower.includes("no results") || lower.includes("not found"))
  ) {
    result.summary = "Geocoding failed: no match found for address";
    result.likelyCause = "Address text may be incomplete or ambiguous";
    return result;
  }

  if (
    (stage.includes("sheets") || context.provider === "google-sheets") &&
    (lower.includes("private key") ||
      lower.includes("pem") ||
      lower.includes("no start line") ||
      lower.includes("asn1"))
  ) {
    result.summary = "Google Sheets authentication failed: invalid private key format";
    result.likelyCause =
      "Check private_key in GOOGLE_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS";
    return result;
  }

  if (
    stage.includes("sheets_append") &&
    (context.retryExhausted || code === "SHEETS_APPEND_FAILED")
  ) {
    result.summary = "Google Sheets append failed: retry limit reached";
    result.likelyCause = "Temporary API/network issue or sheet permission problem";
    return result;
  }

  if (
    stage.includes("whatsapp") &&
    (lower.includes("execution context was destroyed") ||
      lower.includes("target closed") ||
      lower.includes("session closed") ||
      lower.includes("protocol error") ||
      lower.includes("navigation failed"))
  ) {
    result.summary = "WhatsApp session error: browser page context lost";
    result.likelyCause = "Browser session restarted, crashed, or disconnected";
    return result;
  }

  if (message) {
    result.summary = context.defaultSummary || message;
  }

  return result;
}

function createLogger(level = "info", options = {}) {
  const levels = { error: 0, warn: 1, info: 2, debug: 3 };
  const activeLevel = normalizeLevel(level);
  const activePriority = levels[activeLevel];
  const mode = normalizeMode(options.mode || process.env.LOG_MODE, activeLevel);
  const baseMeta = sanitizeValue(options.baseMeta || {}, "baseMeta", new WeakSet(), { mode });

  function canLog(targetLevel) {
    const targetPriority = levels[targetLevel] ?? levels.info;
    return targetPriority <= activePriority;
  }

  function write(targetLevel, message, meta = {}) {
    if (!canLog(targetLevel)) return;

    const timestamp = new Date().toISOString();
    const label = targetLevel.toUpperCase();
    const safeMessage = sanitizeString(
      typeof message === "string" ? message : String(message),
      "message"
    );

    const combinedMeta = {
      ...baseMeta,
      ...meta
    };

    const sanitizedMeta = sanitizeValue(combinedMeta, "meta", new WeakSet(), { mode });

    if (mode === "debug") {
      const payload = {
        timestamp,
        level: targetLevel,
        mode,
        message: safeMessage
      };

      if (
        sanitizedMeta &&
        typeof sanitizedMeta === "object" &&
        !Array.isArray(sanitizedMeta) &&
        Object.keys(sanitizedMeta).length > 0
      ) {
        payload.meta = sanitizedMeta;
      }

      const line = JSON.stringify(payload);
      if (targetLevel === "error") {
        console.error(line);
        return;
      }
      if (targetLevel === "warn") {
        console.warn(line);
        return;
      }
      console.log(line);
      return;
    }

    const compact = compactMeta(sanitizedMeta);
    const line = compact
      ? `${timestamp} ${label} ${safeMessage} | ${compact}`
      : `${timestamp} ${label} ${safeMessage}`;

    if (targetLevel === "error") {
      console.error(line);
      return;
    }
    if (targetLevel === "warn") {
      console.warn(line);
      return;
    }
    console.log(line);
  }

  function child(childMeta = {}) {
    const nextBase = {
      ...baseMeta,
      ...sanitizeValue(childMeta, "childMeta", new WeakSet(), { mode })
    };

    return createLogger(activeLevel, {
      baseMeta: nextBase,
      mode
    });
  }

  return {
    error: (message, meta) => write("error", message, meta),
    warn: (message, meta) => write("warn", message, meta),
    info: (message, meta) => write("info", message, meta),
    debug: (message, meta) => write("debug", message, meta),
    child,
    mode
  };
}

module.exports = {
  createLogger,
  summarizeKnownError: simplifyErrorText,
  maskPhoneNumbers,
  maskMessageId
};
