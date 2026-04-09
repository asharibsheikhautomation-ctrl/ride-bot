const fs = require("node:fs");
const path = require("node:path");
const { google } = require("googleapis");
const { env, PROJECT_ROOT } = require("../config/env");
const { createLogger, summarizeKnownError } = require("../utils/logger");
const { safeTrim } = require("../utils/text");

const SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets";

function normalizePrivateKey(value) {
  const text = safeTrim(value);
  if (!text) return "";

  const hasWrappingQuotes =
    (text.startsWith('"') && text.endsWith('"')) ||
    (text.startsWith("'") && text.endsWith("'"));

  const unwrapped = hasWrappingQuotes ? text.slice(1, -1) : text;
  return unwrapped.replace(/\\n/g, "\n");
}

function resolveCredentialsPath(credentialsPath) {
  const rawPath = safeTrim(credentialsPath);
  if (!rawPath) return "";

  return path.isAbsolute(rawPath) ? rawPath : path.resolve(PROJECT_ROOT, rawPath);
}

function loadServiceAccountCredentials(credentialsPath) {
  const resolvedPath = resolveCredentialsPath(credentialsPath);
  if (!resolvedPath) {
    return {
      ok: false,
      code: "GOOGLE_CREDENTIALS_PATH_MISSING",
      message: "Google credentials file missing",
      reason: "GOOGLE_APPLICATION_CREDENTIALS is not set",
      path: ""
    };
  }

  if (!fs.existsSync(resolvedPath)) {
    return {
      ok: false,
      code: "GOOGLE_CREDENTIALS_FILE_MISSING",
      message: "Google credentials file missing",
      reason: resolvedPath,
      path: resolvedPath
    };
  }

  let parsed;
  try {
    const raw = fs.readFileSync(resolvedPath, "utf8");
    parsed = JSON.parse(raw);
  } catch (error) {
    return {
      ok: false,
      code: "GOOGLE_CREDENTIALS_JSON_INVALID",
      message: "Google credentials JSON is invalid",
      reason: error?.message || "Invalid JSON syntax",
      path: resolvedPath
    };
  }

  const clientEmail = safeTrim(parsed?.client_email);
  const privateKey = normalizePrivateKey(parsed?.private_key);
  const accountType = safeTrim(parsed?.type);
  const missingFields = [];

  if (!clientEmail) missingFields.push("client_email");
  if (!privateKey) missingFields.push("private_key");
  if (accountType && accountType !== "service_account") {
    missingFields.push("type=service_account");
  }

  if (missingFields.length > 0) {
    return {
      ok: false,
      code: "GOOGLE_CREDENTIALS_FIELDS_MISSING",
      message: "Google credentials file is missing required fields",
      reason: missingFields.join(", "),
      path: resolvedPath
    };
  }

  return {
    ok: true,
    code: "GOOGLE_CREDENTIALS_READY",
    message: "Google credentials file found",
    path: resolvedPath,
    clientEmail,
    privateKey
  };
}

function resolveServiceAccountConfig(options = {}) {
  return {
    spreadsheetId: safeTrim(options.spreadsheetId ?? env.googleSheetsId),
    worksheetName: safeTrim(options.worksheetName ?? env.googleWorksheetName),
    range: safeTrim(options.range ?? env.googleSheetsRange),
    credentialsPath: safeTrim(options.credentialsPath ?? env.googleCredentialsPath)
  };
}

function validateSheetsConfig(config = {}) {
  const missing = [];

  if (!safeTrim(config.spreadsheetId)) missing.push("GOOGLE_SHEETS_ID");
  if (!safeTrim(config.worksheetName) && !safeTrim(config.range)) {
    missing.push("GOOGLE_SHEETS_WORKSHEET_NAME or GOOGLE_SHEETS_RANGE");
  }
  if (!safeTrim(config.credentialsPath)) {
    missing.push("GOOGLE_APPLICATION_CREDENTIALS");
  }

  const credentialsStatus = loadServiceAccountCredentials(config.credentialsPath);
  const valid = missing.length === 0 && credentialsStatus.ok;
  const reason =
    missing.length > 0
      ? missing.join(", ")
      : credentialsStatus.ok
        ? ""
        : [credentialsStatus.message, credentialsStatus.reason].filter(Boolean).join(": ");

  return {
    valid,
    missing,
    reason,
    credentialsStatus
  };
}

function createSheetsClient(options = {}) {
  const logger =
    options.logger ||
    createLogger(env.logLevel || "info", {
      mode: env.logMode,
      baseMeta: { component: "sheets-client" }
    });

  const config = resolveServiceAccountConfig(options);
  const validation = validateSheetsConfig(config);

  if (!validation.valid) {
    if (validation.credentialsStatus?.code === "GOOGLE_CREDENTIALS_JSON_INVALID") {
      logger.error("Google credentials JSON is invalid", {
        stage: "sheets_auth",
        fallbackUsed: true,
        reason: validation.credentialsStatus.reason
      });
      return null;
    }

    if (validation.credentialsStatus?.code === "GOOGLE_CREDENTIALS_FIELDS_MISSING") {
      logger.error("Google credentials file is missing required fields", {
        stage: "sheets_auth",
        fallbackUsed: true,
        reason: validation.credentialsStatus.reason
      });
      return null;
    }

    logger.error("Google credentials file missing", {
      stage: "sheets_auth",
      fallbackUsed: true,
      reason: validation.reason || validation.credentialsStatus?.reason || "Missing configuration"
    });
    return null;
  }

  const credentials = validation.credentialsStatus;

  logger.info("Google credentials file found", {
    stage: "sheets_auth",
    reason: credentials.path
  });

  try {
    const auth = new google.auth.JWT({
      email: credentials.clientEmail,
      key: credentials.privateKey,
      scopes: [SHEETS_SCOPE]
    });

    const sheets = google.sheets({
      version: "v4",
      auth
    });

    logger.info("Google Sheets auth ready", {
      stage: "sheets_auth",
      fallbackUsed: false
    });
    return sheets;
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "sheets_auth",
      defaultSummary: "Google Sheets authentication failed",
      fallbackUsed: true
    });

    logger.error(summary.summary, {
      stage: "sheets_auth",
      reason: summary.likelyCause || "Service account credentials were rejected",
      fallbackUsed: true,
      error
    });
    return null;
  }
}

module.exports = {
  createSheetsClient,
  normalizePrivateKey,
  resolveCredentialsPath,
  loadServiceAccountCredentials,
  resolveServiceAccountConfig,
  validateSheetsConfig
};
