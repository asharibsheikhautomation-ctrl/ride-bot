const os = require("node:os");
const path = require("node:path");

const PROJECT_ROOT = path.resolve(__dirname, "../..");
const RAILWAY_DATA_ROOT = "/data";

try {
  // dotenv is optional in environments that already inject variables.
  // Always resolve from project root so startup location does not change config source.
  // eslint-disable-next-line global-require
  require("dotenv").config({ path: path.resolve(PROJECT_ROOT, ".env") });
} catch (error) {
  // Ignore missing dotenv package so imports do not crash before install.
}

function safeString(value, fallback = "") {
  if (value === null || value === undefined) return fallback;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : fallback;
}

function isProductionNodeEnv() {
  return safeString(process.env.NODE_ENV, "development").toLowerCase() === "production";
}

function parseArray(value) {
  if (!value) return [];

  return String(value)
    .split(/[,\n;]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseNumber(value, fallback, options = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;

  let next = parsed;
  if (options.integer) next = Math.trunc(next);
  if (Number.isFinite(options.min) && next < options.min) return fallback;
  if (Number.isFinite(options.max) && next > options.max) return fallback;
  return next;
}

function parseBoolean(value, fallback = false) {
  if (value === null || value === undefined || value === "") return fallback;

  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function normalizePrivateKey(value) {
  const input = safeString(value);
  if (!input) return "";

  const hasWrappingQuotes =
    (input.startsWith('"') && input.endsWith('"')) ||
    (input.startsWith("'") && input.endsWith("'"));
  const unwrapped = hasWrappingQuotes ? input.slice(1, -1) : input;

  return unwrapped.replace(/\\n/g, "\n");
}

function resolveSessionPath(value) {
  const fromEnv = safeString(value);
  if (!fromEnv) {
    if (isProductionNodeEnv()) {
      return path.resolve(RAILWAY_DATA_ROOT, ".wwebjs_auth");
    }
    return path.resolve(PROJECT_ROOT, "data/.wwebjs_auth");
  }

  return path.isAbsolute(fromEnv) ? fromEnv : path.resolve(PROJECT_ROOT, fromEnv);
}

function resolveWhatsAppSessionPath() {
  return resolveSessionPath(process.env.WHATSAPP_SESSION_PATH || process.env.WHATSAPP_SESSION_DIR);
}

function resolveDataPath(value, fallbackRelativePath) {
  const fromEnv = safeString(value);
  if (!fromEnv) {
    return path.resolve(PROJECT_ROOT, fallbackRelativePath);
  }

  return path.isAbsolute(fromEnv) ? fromEnv : path.resolve(PROJECT_ROOT, fromEnv);
}

function resolveGoogleCredentialsPath() {
  const fallbackPath = isProductionNodeEnv()
    ? path.resolve(RAILWAY_DATA_ROOT, "credentials.json")
    : path.resolve(PROJECT_ROOT, "credentials.json");

  return resolveDataPath(process.env.GOOGLE_APPLICATION_CREDENTIALS, fallbackPath);
}

function parseGoogleCredentialsJson(rawValue) {
  const raw = safeString(rawValue);
  if (!raw) return null;

  try {
    return JSON.parse(raw);
  } catch (firstError) {
    try {
      const decoded = Buffer.from(raw, "base64").toString("utf8");
      return JSON.parse(decoded);
    } catch (secondError) {
      const error = new Error("GOOGLE_CREDENTIALS_JSON is not valid JSON or base64 JSON");
      error.code = "GOOGLE_CREDENTIALS_JSON_INVALID";
      throw error;
    }
  }
}

function resolveGoogleCredentialsState() {
  const rawCredentials = safeString(process.env.GOOGLE_CREDENTIALS_JSON);
  if (!rawCredentials) {
    return {
      json: null,
      error: "",
      source: "file_path"
    };
  }

  try {
    return {
      json: parseGoogleCredentialsJson(rawCredentials),
      error: "",
      source: "env_json"
    };
  } catch (error) {
    return {
      json: null,
      error: safeString(error?.message || "GOOGLE_CREDENTIALS_JSON could not be parsed"),
      source: "env_json"
    };
  }
}

function extractWorksheetNameFromRange(rangeValue) {
  const range = safeString(rangeValue);
  if (!range) return "";

  const separatorIndex = range.indexOf("!");
  const worksheetPart = separatorIndex >= 0 ? range.slice(0, separatorIndex) : range;
  return safeString(worksheetPart.replace(/^'(.+)'$/, "$1"));
}

function resolveGoogleSheetsRange() {
  const explicitRange = safeString(process.env.GOOGLE_SHEETS_RANGE);
  if (explicitRange) return explicitRange;

  const worksheetName = safeString(
    process.env.GOOGLE_SHEETS_WORKSHEET_NAME || process.env.GOOGLE_WORKSHEET_NAME,
    "Sheet1"
  );
  return worksheetName;
}

function resolveWorksheetRange(preferredRange, worksheetName, fallbackRange) {
  const explicitRange = safeString(preferredRange);
  if (explicitRange) return explicitRange;
  const explicitWorksheet = safeString(worksheetName);
  if (explicitWorksheet) return explicitWorksheet;
  return safeString(fallbackRange);
}

const resolvedGoogleSheetsRange = resolveGoogleSheetsRange();
const resolvedGoogleWorksheetName = safeString(
  process.env.GOOGLE_SHEETS_WORKSHEET_NAME || process.env.GOOGLE_WORKSHEET_NAME,
  extractWorksheetNameFromRange(resolvedGoogleSheetsRange) || "Sheet1"
);
const googleCredentialsState = resolveGoogleCredentialsState();
const defaultDedupeStorePath = isProductionNodeEnv()
  ? path.resolve(RAILWAY_DATA_ROOT, "dedupe-store.json")
  : path.resolve(PROJECT_ROOT, "data/dedupe-store.json");

const env = Object.freeze({
  nodeEnv: safeString(process.env.NODE_ENV, "development"),
  logLevel: safeString(process.env.LOG_LEVEL, "info").toLowerCase(),
  logMode: safeString(process.env.LOG_MODE).toLowerCase() || undefined,
  openaiApiKey: safeString(process.env.OPENAI_API_KEY),
  openaiModel: safeString(process.env.OPENAI_MODEL, "gpt-4.1-mini"),
  googleSheetsId: safeString(
    process.env.GOOGLE_SHEETS_ID || process.env.GOOGLE_SHEETS_SPREADSHEET_ID
  ),
  googleSheetsRange: resolvedGoogleSheetsRange,
  googleWorksheetName: resolvedGoogleWorksheetName,
  googleRidesWorksheetName: safeString(
    process.env.GOOGLE_SHEETS_RIDES_WORKSHEET_NAME,
    resolvedGoogleWorksheetName || "Rides"
  ),
  googleNeedsReviewWorksheetName: safeString(
    process.env.GOOGLE_SHEETS_NEEDS_REVIEW_WORKSHEET_NAME,
    "Needs Review"
  ),
  googleRidesRange: resolveWorksheetRange(
    process.env.GOOGLE_SHEETS_RIDES_RANGE,
    process.env.GOOGLE_SHEETS_RIDES_WORKSHEET_NAME,
    resolvedGoogleSheetsRange || resolvedGoogleWorksheetName || "Rides"
  ),
  googleNeedsReviewRange: resolveWorksheetRange(
    process.env.GOOGLE_SHEETS_NEEDS_REVIEW_RANGE,
    process.env.GOOGLE_SHEETS_NEEDS_REVIEW_WORKSHEET_NAME,
    "Needs Review"
  ),
  googleCredentialsPath: resolveGoogleCredentialsPath(),
  googleCredentialsJson: googleCredentialsState.json,
  googleCredentialsJsonError: googleCredentialsState.error,
  googleCredentialsSource: googleCredentialsState.source,
  whatsappClientId: safeString(process.env.WHATSAPP_CLIENT_ID),
  whatsappStartupTimeoutMs: parseNumber(process.env.WHATSAPP_STARTUP_TIMEOUT_MS, 90000, {
    integer: true,
    min: 15000,
    max: 300000
  }),
  whatsappSessionPath: resolveWhatsAppSessionPath(),
  // Backward-compatible alias used by existing modules.
  whatsappSessionDir: resolveWhatsAppSessionPath(),
  dedupeStorePath: resolveDataPath(process.env.DEDUPE_STORE_PATH, defaultDedupeStorePath),
  dedupeTtlMs: parseNumber(process.env.DEDUPE_TTL_MS, 6 * 60 * 60 * 1000, {
    integer: true,
    min: 60 * 1000,
    max: 7 * 24 * 60 * 60 * 1000
  }),
  dedupeMaxEntries: parseNumber(process.env.DEDUPE_MAX_ENTRIES, 20000, {
    integer: true,
    min: 1000,
    max: 500000
  }),
  allowedGroups: parseArray(process.env.ALLOWED_GROUPS || process.env.ALLOWED_GROUP_IDS),
  allowFromMeMessages: parseBoolean(
    process.env.ALLOW_FROM_ME_MESSAGES || process.env.WHATSAPP_ALLOW_FROM_ME_TEST_MESSAGES,
    false
  ),
  defaultCurrency: safeString(process.env.DEFAULT_CURRENCY, "PKR").toUpperCase(),
  appTimeZone: safeString(process.env.APP_TIME_ZONE, "Europe/London"),
  fareBase: parseNumber(process.env.FARE_BASE, 250, { min: 0 }),
  farePerKm: parseNumber(process.env.FARE_PER_KM, 95, { min: 0 }),
  geocodingProvider: safeString(process.env.GEOCODING_PROVIDER, "nominatim").toLowerCase(),
  geocodingBaseUrl: safeString(process.env.GEOCODING_BASE_URL),
  geocodingUserAgent: safeString(process.env.GEOCODING_USER_AGENT, "ride-bot/1.0 (geocode)"),
  geocodingTimeoutMs: parseNumber(process.env.GEOCODING_TIMEOUT_MS, 12000, {
    integer: true,
    min: 1000,
    max: 120000
  }),
  geocodingApiKey: safeString(process.env.GEOCODING_API_KEY),
  ocrTesseractPath: safeString(process.env.OCR_TESSERACT_PATH, "tesseract"),
  ocrTimeoutMs: parseNumber(process.env.OCR_TIMEOUT_MS, 20000, {
    integer: true,
    min: 1000,
    max: 120000
  }),
  ocrTempDir: resolveDataPath(process.env.OCR_TEMP_DIR, path.join(os.tmpdir(), "ride-bot-ocr")),
  puppeteerExecutablePath: safeString(process.env.PUPPETEER_EXECUTABLE_PATH),
  puppeteerNoSandbox: parseBoolean(process.env.PUPPETEER_NO_SANDBOX, true),
  port: parseNumber(process.env.PORT, 3000, { integer: true, min: 1, max: 65535 })
});

module.exports = {
  env,
  parseNumber,
  parseBoolean,
  parseArray,
  normalizePrivateKey,
  safeString,
  resolveSessionPath,
  resolveWhatsAppSessionPath,
  resolveDataPath,
  PROJECT_ROOT
};
