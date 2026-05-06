const { env } = require("../config/env");
const { createLogger, summarizeKnownError } = require("../utils/logger");
const {
  STRICT_SHEET_HEADERS,
  buildRowFromRideObject
} = require("../extraction/schemas");
const { executeWithRetry } = require("../utils/retry");
const { safeTrim } = require("../utils/text");

const DEFAULT_RANGE = "Sheet1";
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_DELAY_MS = 500;
const DEFAULT_HEADER_CACHE_TTL_MS = 60 * 1000;
const NETWORK_ERROR_CODES = new Set([
  "ECONNRESET",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "ENOTFOUND",
  "ECONNABORTED"
]);

class SheetsAppendError extends Error {
  constructor(message, options = {}) {
    super(message);
    this.name = "SheetsAppendError";
    this.code = options.code || "SHEETS_APPEND_FAILED";
    this.attempts = options.attempts || 0;
    this.retryable = Boolean(options.retryable);
    this.details = options.details || {};
    if (options.cause) this.cause = options.cause;
  }
}

function buildSheetRow(ride = {}, headers = STRICT_SHEET_HEADERS) {
  return buildRowFromRideObject(ride, headers);
}

function sanitizeHeaders(headers = []) {
  return (Array.isArray(headers) ? headers : [])
    .map((value) => safeTrim(value))
    .filter(Boolean);
}

async function fetchSheetHeaders({
  sheetsClient,
  spreadsheetId,
  worksheetName,
  maxAttempts,
  retryDelayMs,
  logger
}) {
  try {
    const response = await executeWithRetry(
      async () =>
        sheetsClient.spreadsheets.values.get({
          spreadsheetId,
          range: buildHeaderRange(worksheetName),
          majorDimension: "ROWS"
        }),
      {
        maxAttempts,
        initialDelayMs: retryDelayMs,
        maxDelayMs: retryDelayMs * 8,
        shouldRetry: isTransientError
      }
    );

    const rawHeaders = Array.isArray(response?.data?.values?.[0]) ? response.data.values[0] : [];
    const headers = sanitizeHeaders(rawHeaders);

    if (headers.length === 0) {
      logger.warn("Google Sheets header row is empty; using strict defaults", {
        stage: "sheets_append",
        fallbackUsed: true,
        reason: worksheetName
      });
      return [...STRICT_SHEET_HEADERS];
    }

    return headers;
  } catch (error) {
    const classification = classifyAppendFailure(error);
    const summary = summarizeKnownError(error, {
      stage: "sheets_append",
      defaultSummary: "Google Sheets header lookup failed; using strict defaults",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "sheets_append",
      fallbackUsed: true,
      status: classification.status || summary.status,
      code: classification.errorCode || summary.code,
      reason: classification.detail || summary.likelyCause || "Unable to read worksheet header row",
      error
    });

    return [...STRICT_SHEET_HEADERS];
  }
}

function isTransientError(error) {
  const classification = classifyAppendFailure(error);
  if (classification.type === "network_timeout") return true;

  const status = Number(error?.response?.status);
  const code = String(error?.code || "");

  if ([408, 409, 425, 429, 500, 502, 503, 504].includes(status)) return true;
  if (NETWORK_ERROR_CODES.has(code)) return true;

  return false;
}

function extractWorksheetNameFromRange(range) {
  const value = safeTrim(range);
  if (!value) return "";

  const separatorIndex = value.indexOf("!");
  const worksheetPart = separatorIndex >= 0 ? value.slice(0, separatorIndex) : value;
  return safeTrim(worksheetPart.replace(/^'(.+)'$/, "$1"));
}

function normalizeWorksheetNameForRange(worksheetName) {
  const cleanName = safeTrim(worksheetName);
  if (!cleanName) return "";

  if (cleanName.includes(" ") || cleanName.includes("!")) {
    return `'${cleanName.replace(/'/g, "''")}'`;
  }

  return cleanName;
}

function buildAppendRange({ range, worksheetName }) {
  const explicitRange = safeTrim(range);
  if (explicitRange) return explicitRange;

  const safeWorksheetName = normalizeWorksheetNameForRange(worksheetName) || "Sheet1";
  return safeWorksheetName;
}

function buildHeaderRange(worksheetName) {
  const safeWorksheetName = normalizeWorksheetNameForRange(worksheetName) || "Sheet1";
  return `${safeWorksheetName}!1:1`;
}

function extractApiErrorMessage(error) {
  return safeTrim(error?.response?.data?.error?.message || error?.message || "");
}

function classifyAppendFailure(error) {
  const status = Number(error?.response?.status || error?.status);
  const code = String(error?.code || "");
  const apiMessage = extractApiErrorMessage(error);
  const messageLower = apiMessage.toLowerCase();

  if (
    NETWORK_ERROR_CODES.has(code) ||
    [408, 409, 425, 429, 500, 502, 503, 504].includes(status)
  ) {
    return {
      type: "network_timeout",
      errorCode: "SHEETS_NETWORK_TIMEOUT",
      summary: "Google Sheets request failed: network timeout or transient API error",
      detail: "Request timed out or transient network/API failure",
      status,
      code,
      apiMessage
    };
  }

  if (
    status === 401 ||
    messageLower.includes("invalid credentials") ||
    messageLower.includes("invalid_grant") ||
    messageLower.includes("unauthenticated")
  ) {
    return {
      type: "authentication_error",
      errorCode: "SHEETS_AUTH_FAILED",
      summary: "Google Sheets authentication failed",
      detail: apiMessage || "Service account credentials were rejected",
      status,
      code,
      apiMessage
    };
  }

  if (
    messageLower.includes("unable to parse range") ||
    (messageLower.includes("range") && messageLower.includes("not found"))
  ) {
    return {
      type: "worksheet_not_found",
      errorCode: "SHEETS_WORKSHEET_NOT_FOUND",
      summary: "Google Sheets worksheet not found",
      detail: "Worksheet name in append range is invalid",
      status,
      code,
      apiMessage
    };
  }

  if (
    status === 403 ||
    messageLower.includes("permission") ||
    messageLower.includes("forbidden") ||
    messageLower.includes("insufficient permissions")
  ) {
    return {
      type: "permission_denied",
      errorCode: "SHEETS_PERMISSION_DENIED",
      summary: "Google Sheets permission denied",
      detail: "Service account does not have access to the spreadsheet",
      status,
      code,
      apiMessage
    };
  }

  if (status === 404 || messageLower.includes("requested entity was not found")) {
    return {
      type: "spreadsheet_not_found",
      errorCode: "SHEETS_SPREADSHEET_NOT_FOUND",
      summary: "Google Sheets spreadsheet not found",
      detail: "Spreadsheet ID is invalid or inaccessible",
      status,
      code,
      apiMessage
    };
  }

  return {
    type: "unknown",
    errorCode: "SHEETS_APPEND_FAILED",
    summary: "Google Sheets append failed",
    detail: apiMessage || "Unknown Google Sheets error",
    status,
    code,
    apiMessage
  };
}

function assertSheetPreconditions({ sheetsClient, spreadsheetId, worksheetName }) {
  if (!sheetsClient) {
    throw new SheetsAppendError("Google Sheets client is not initialized", {
      code: "SHEETS_NOT_CONFIGURED"
    });
  }

  if (!spreadsheetId) {
    throw new SheetsAppendError("GOOGLE_SHEETS_ID is missing", {
      code: "SHEETS_NOT_CONFIGURED"
    });
  }

  if (!worksheetName) {
    throw new SheetsAppendError("Worksheet name is missing", {
      code: "SHEETS_NOT_CONFIGURED"
    });
  }
}

function createAppendRow(options = {}) {
  const sheetsClient = options.sheetsClient;
  const spreadsheetId = options.sheetId || options.spreadsheetId || env.googleSheetsId;
  const worksheetName = safeTrim(
    options.worksheetName ||
      env.googleRidesWorksheetName ||
      env.googleWorksheetName ||
      extractWorksheetNameFromRange(options.range || env.googleSheetsRange || DEFAULT_RANGE)
  );
  const range = buildAppendRange({
    range: options.range || env.googleSheetsRange || "",
    worksheetName
  });
  const maxAttempts =
    Number.isFinite(options.maxAttempts) && options.maxAttempts > 0
      ? Math.trunc(options.maxAttempts)
      : DEFAULT_MAX_ATTEMPTS;
  const retryDelayMs =
    Number.isFinite(options.retryDelayMs) && options.retryDelayMs > 0
      ? options.retryDelayMs
      : DEFAULT_RETRY_DELAY_MS;
  const headerCacheTtlMs =
    Number.isFinite(options.headerCacheTtlMs) && options.headerCacheTtlMs > 0
      ? options.headerCacheTtlMs
      : DEFAULT_HEADER_CACHE_TTL_MS;
  const logger =
    options.logger ||
    createLogger(env.logLevel || "info", {
      mode: env.logMode,
      baseMeta: { component: "sheets-append" }
    });
  let headerCache = null;
  let headerCacheUpdatedAt = 0;

  async function resolveHeaders(forceRefresh = false) {
    const now = Date.now();
    if (
      !forceRefresh &&
      Array.isArray(headerCache) &&
      headerCache.length > 0 &&
      now - headerCacheUpdatedAt < headerCacheTtlMs
    ) {
      return headerCache;
    }

    const headers = await fetchSheetHeaders({
      sheetsClient,
      spreadsheetId,
      worksheetName,
      maxAttempts,
      retryDelayMs,
      logger
    });

    headerCache = Array.isArray(headers) && headers.length > 0 ? headers : [...STRICT_SHEET_HEADERS];
    headerCacheUpdatedAt = now;
    return headerCache;
  }

  return async function appendRow(ride) {
    assertSheetPreconditions({ sheetsClient, spreadsheetId, worksheetName });

    const headers = await resolveHeaders();
    const row = buildRowFromRideObject(ride, headers);
    if (row.length !== headers.length) {
      throw new SheetsAppendError("Invalid sheet row shape", {
        code: "SHEETS_ROW_INVALID",
        details: { expected: headers.length, received: row.length }
      });
    }

    let lastAttempt = 1;

    try {
      const response = await executeWithRetry(
        async (attempt) => {
          lastAttempt = attempt;
          return sheetsClient.spreadsheets.values.append({
            spreadsheetId,
            range: range || DEFAULT_RANGE,
            valueInputOption: "RAW",
            insertDataOption: "INSERT_ROWS",
            requestBody: { values: [row] }
          });
        },
        {
          maxAttempts,
          initialDelayMs: retryDelayMs,
          maxDelayMs: retryDelayMs * 8,
          shouldRetry: isTransientError,
          onRetry: ({ attempt, maxAttempts: max, delayMs, error }) => {
            const classification = classifyAppendFailure(error);
            const summary = summarizeKnownError(error, {
              stage: "sheets_append",
              defaultSummary: `${classification.summary}, retrying`,
              fallbackUsed: true
            });

            logger.warn(summary.summary, {
              stage: "sheets_append",
              fallbackUsed: true,
              attempt,
              maxAttempts: max,
              delayMs,
              status: classification.status || summary.status,
              code: classification.errorCode || summary.code,
              reason:
                classification.detail || summary.likelyCause || "Temporary API/network issue",
              error
            });
          }
        }
      );

      logger.info("Row appended to Google Sheet", {
        stage: "sheets_append",
        fallbackUsed: false,
        attempt: lastAttempt,
        reason: response?.data?.updates?.updatedRange || ""
      });

      return {
        updatedRange: response?.data?.updates?.updatedRange || "",
        updatedRows: response?.data?.updates?.updatedRows || 0,
        headers
      };
    } catch (error) {
      const attempts = Number(error?.attempts) || maxAttempts;
      const classification = classifyAppendFailure(error);
      const summary = summarizeKnownError(error, {
        stage: "sheets_append",
        defaultSummary: classification.summary,
        retryExhausted: attempts >= maxAttempts,
        fallbackUsed: true
      });

      logger.error(summary.summary, {
        stage: "sheets_append",
        fallbackUsed: true,
        reason: classification.detail || summary.likelyCause || "Retry limit reached",
        attempts,
        maxAttempts,
        status: classification.status || summary.status,
        code: classification.errorCode || summary.code,
        error
      });
      throw new SheetsAppendError(classification.summary, {
        code: classification.errorCode || "SHEETS_APPEND_FAILED",
        attempts,
        retryable: isTransientError(error),
        cause: error,
        details: {
          range: range || DEFAULT_RANGE,
          worksheetName: worksheetName || "",
          columns: headers.length,
          headers,
          failureType: classification.type || "unknown",
          providerMessage: classification.apiMessage || ""
        }
      });
    }
  };
}

module.exports = {
  createAppendRow,
  buildSheetRow,
  fetchSheetHeaders,
  buildHeaderRange,
  SheetsAppendError,
  classifyAppendFailure,
  buildAppendRange
};
