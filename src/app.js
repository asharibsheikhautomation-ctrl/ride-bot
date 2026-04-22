const fs = require("node:fs");
const path = require("node:path");
const express = require("express");
const { env } = require("./config/env");
const { createLogger, summarizeKnownError } = require("./utils/logger");
const { DedupeStore } = require("./utils/dedupe");
const { createLocalExtractor } = require("./extraction/localExtractor");
const { createOpenAiNormalizer } = require("./extraction/openaiNormalizer");
const { createTesseractOcr } = require("./extraction/tesseractOcr");
const { normalizeHeaderName } = require("./extraction/schemas");
const { createGeocoder } = require("./routing/geocode");
const { createOsrmClient } = require("./routing/osrm");
const { createSheetsClient, validateSheetsConfig } = require("./sheets/sheetsClient");
const { createAppendRow } = require("./sheets/appendRow");
const { createMessageHandler } = require("./whatsapp/messageHandler");
const { initializeWhatsAppClient } = require("./whatsapp/client");
const { validateAndPrepareSessionStorage } = require("./whatsapp/session");

const RAILWAY_PUPPETEER_ARGS = [
  "--no-sandbox",
  "--disable-setuid-sandbox",
  "--disable-dev-shm-usage",
  "--disable-gpu",
  "--no-zygote",
  "--single-process"
];

function safeString(value, fallback = "") {
  if (value === null || value === undefined) return fallback;
  const normalized = String(value).trim();
  return normalized.length > 0 ? normalized : fallback;
}

function resolveQrImagePath() {
  const preferredDir = "/data";

  try {
    if (fs.existsSync(preferredDir) && fs.statSync(preferredDir).isDirectory()) {
      return path.join(preferredDir, "qr.png");
    }
  } catch (error) {
    // Fall back to project-local writable storage when /data is unavailable.
  }

  return path.resolve(__dirname, "../data/qr.png");
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function describeBotState(state) {
  switch (state) {
    case "qr_required":
      return "Scan this QR with WhatsApp Linked Devices.";
    case "authenticated":
      return "Authentication complete. Waiting for ready state.";
    case "ready":
      return "Bot is ready, QR not required.";
    case "auth_failed":
      return "Authentication failed. Check logs and wait for a fresh QR.";
    case "starting":
    default:
      return "Bot is starting.";
  }
}

function createBotRuntimeStatus() {
  const snapshot = {
    state: "starting",
    qrImagePath: resolveQrImagePath(),
    qrAvailable: false,
    qrUpdatedAt: "",
    lastError: "",
    startedAt: new Date().toISOString()
  };

  return {
    update(state, details = {}) {
      const nextState = safeString(state);
      if (nextState) snapshot.state = nextState;

      if (typeof details.qrAvailable === "boolean") {
        snapshot.qrAvailable = details.qrAvailable;
      }

      if (safeString(details.qrUpdatedAt)) {
        snapshot.qrUpdatedAt = safeString(details.qrUpdatedAt);
      }

      if (safeString(details.qrImagePath)) {
        snapshot.qrImagePath = safeString(details.qrImagePath);
      }

      if (Object.prototype.hasOwnProperty.call(details, "error")) {
        snapshot.lastError = safeString(details.error);
      } else if (nextState !== "auth_failed") {
        snapshot.lastError = "";
      }
    },
    getSnapshot() {
      const qrAvailable = Boolean(
        snapshot.qrAvailable &&
          safeString(snapshot.qrImagePath) &&
          fs.existsSync(snapshot.qrImagePath)
      );

      return {
        ok: true,
        state: snapshot.state,
        qrAvailable,
        qrImagePath: snapshot.qrImagePath,
        qrUpdatedAt: snapshot.qrUpdatedAt,
        lastError: snapshot.lastError,
        startedAt: snapshot.startedAt
      };
    }
  };
}

function renderQrPage(status) {
  const title = "Ride Bot QR";
  const stateLabel = escapeHtml(status.state);
  const statusMessage = escapeHtml(describeBotState(status.state));
  const lastUpdated = escapeHtml(status.qrUpdatedAt || "not generated yet");
  const imageBlock = status.qrAvailable
    ? `<img src="/qr.png?ts=${encodeURIComponent(status.qrUpdatedAt || Date.now())}" alt="WhatsApp QR" style="max-width:320px;width:100%;height:auto;border:1px solid #d0d7de;border-radius:12px;background:#fff;padding:12px;" />`
    : `<p style="margin:0;color:#57606a;">No QR image is currently available.</p>`;
  const extraMessage =
    status.state === "ready"
      ? `<p style="margin:0;color:#1a7f37;">Bot is ready, QR not required.</p>`
      : status.lastError
        ? `<p style="margin:0;color:#d1242f;">${escapeHtml(status.lastError)}</p>`
        : "";

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>${title}</title>
  </head>
  <body style="font-family:Segoe UI,Arial,sans-serif;background:#f6f8fa;color:#24292f;margin:0;padding:24px;">
    <main style="max-width:480px;margin:0 auto;background:#ffffff;border:1px solid #d0d7de;border-radius:16px;padding:24px;box-shadow:0 10px 30px rgba(31,35,40,.08);">
      <h1 style="margin-top:0;">${title}</h1>
      <p style="margin:0 0 8px;"><strong>State:</strong> ${stateLabel}</p>
      <p style="margin:0 0 16px;">${statusMessage}</p>
      ${extraMessage}
      <div style="display:flex;justify-content:center;align-items:center;min-height:180px;margin:16px 0;">
        ${imageBlock}
      </div>
      <p style="margin:0;color:#57606a;"><strong>QR updated:</strong> ${lastUpdated}</p>
      <p style="margin:12px 0 0;color:#57606a;">Refresh this page if the state changes.</p>
    </main>
  </body>
</html>`;
}

function startHttpServer({ logger, runtimeStatus, port }) {
  const app = express();

  app.get("/health", (_request, response) => {
    const status = runtimeStatus.getSnapshot();
    response.json({
      ok: true,
      state: status.state
    });
  });

  app.get("/qr.png", (_request, response) => {
    const status = runtimeStatus.getSnapshot();

    if (!status.qrAvailable) {
      response.status(404).json({
        ok: false,
        state: status.state,
        message: "QR image not available"
      });
      return;
    }

    response.setHeader("Cache-Control", "no-store");
    response.sendFile(status.qrImagePath);
  });

  app.get("/qr", (_request, response) => {
    const status = runtimeStatus.getSnapshot();
    response.setHeader("Cache-Control", "no-store");
    response.type("html").send(renderQrPage(status));
  });

  return new Promise((resolve, reject) => {
    const server = app.listen(port, () => {
      logger.info("QR route available", {
        stage: "http_server",
        reason: `port=${port} /health /qr /qr.png`
      });
      resolve(server);
    });

    server.on("error", (error) => {
      reject(error);
    });
  });
}

function startupHealthSnapshot() {
  const memory = process.memoryUsage();
  return {
    pid: process.pid,
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
    rssMb: Math.round(memory.rss / (1024 * 1024)),
    heapUsedMb: Math.round(memory.heapUsed / (1024 * 1024))
  };
}

function resolvePuppeteerOptions() {
  if (env.nodeEnv !== "production") {
    return {};
  }

  if (process.platform === "win32") {
    return {
      headless: true
    };
  }

  return {
    headless: true,
    args: RAILWAY_PUPPETEER_ARGS
  };
}

async function verifyWorksheetTargetsReady({
  sheetsClient,
  spreadsheetId,
  worksheetTargets,
  logger
}) {
  if (!sheetsClient || !spreadsheetId) {
    throw new Error("Google Sheets client is not ready for worksheet verification");
  }

  const response = await sheetsClient.spreadsheets.get({
    spreadsheetId,
    fields: "sheets(properties(title))"
  });
  const existingTitles = new Set(
    (Array.isArray(response?.data?.sheets) ? response.data.sheets : [])
      .map((sheet) => safeString(sheet?.properties?.title))
      .filter(Boolean)
  );

  const targets = Array.isArray(worksheetTargets) ? worksheetTargets : [];
  const missing = targets
    .map((target) => safeString(target?.worksheetName))
    .filter(
      (worksheetName) => safeString(worksheetName) && !existingTitles.has(safeString(worksheetName))
    );

  if (missing.length > 0) {
    const error = new Error(`Missing Google Sheets worksheets: ${missing.join(", ")}`);
    error.code = "SHEETS_WORKSHEETS_MISSING";
    error.details = {
      missing,
      existing: [...existingTitles]
    };
    throw error;
  }

  for (const target of targets) {
    const worksheetName = safeString(target?.worksheetName);
    const minimumHeaders = Array.isArray(target?.minimumHeaders) ? target.minimumHeaders : [];
    const headerResponse = await sheetsClient.spreadsheets.values.get({
      spreadsheetId,
      range: `'${worksheetName.replace(/'/g, "''")}'!1:1`
    });
    const rawHeaders = Array.isArray(headerResponse?.data?.values?.[0])
      ? headerResponse.data.values[0]
      : [];
    const normalizedHeaders = new Set(rawHeaders.map((header) => normalizeHeaderName(header)));
    const missingHeaders = minimumHeaders.filter(
      (header) => !normalizedHeaders.has(normalizeHeaderName(header))
    );

    if (missingHeaders.length > 0) {
      const error = new Error(
        `Worksheet ${worksheetName} is missing required headers: ${missingHeaders.join(", ")}`
      );
      error.code = "SHEETS_HEADERS_MISSING";
      error.details = {
        worksheetName,
        missingHeaders,
        headers: rawHeaders
      };
      throw error;
    }
  }

  logger.info("Google Sheets worksheet targets verified", {
    stage: "sheets_startup",
    fallbackUsed: false,
    reason: targets.map((target) => target.worksheetName).join(", ")
  });
}

function registerProcessHandlers(logger) {
  process.on("uncaughtException", (error) => {
    const summary = summarizeKnownError(error, {
      stage: "process",
      defaultSummary: "Service error: uncaught exception"
    });

    logger.error(summary.summary, {
      stage: "process",
      reason: summary.likelyCause || "Check debug logs for stack trace",
      fallbackUsed: false,
      status: summary.status,
      code: summary.code,
      error
    });
  });

  process.on("unhandledRejection", (reason) => {
    const error = reason instanceof Error ? reason : new Error(String(reason));
    const summary = summarizeKnownError(error, {
      stage: "process",
      defaultSummary: "Service error: unhandled promise rejection"
    });

    logger.error(summary.summary, {
      stage: "process",
      reason: summary.likelyCause || "Unhandled async failure",
      fallbackUsed: false,
      status: summary.status,
      code: summary.code,
      error
    });
  });
}

function registerShutdownHooks({ logger, getClient, getDedupe, getServer, sessionPath }) {
  let shuttingDown = false;

  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;

    logger.warn("Shutdown signal received", {
      stage: "shutdown",
      reason: signal
    });

    const client = typeof getClient === "function" ? getClient() : null;
    if (client && typeof client.destroy === "function") {
      try {
        await Promise.race([
          client.destroy(),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error("WhatsApp client shutdown timeout")), 10000)
          )
        ]);
        logger.info("WhatsApp client closed", {
          stage: "shutdown",
          reason: "session data preserved"
        });
      } catch (error) {
        const summary = summarizeKnownError(error, {
          stage: "whatsapp_shutdown",
          defaultSummary: "WhatsApp close issue during shutdown"
        });

        logger.warn(summary.summary, {
          stage: "shutdown",
          reason: summary.likelyCause || "Timeout or browser disconnect",
          fallbackUsed: true,
          error
        });
      }
    }

    const dedupeStore = typeof getDedupe === "function" ? getDedupe() : null;
    if (dedupeStore && typeof dedupeStore.flush === "function") {
      try {
        const flushed = dedupeStore.flush();
        if (flushed) {
          logger.info("Dedupe state flushed", {
            stage: "shutdown"
          });
        }
      } catch (error) {
        logger.warn("Unable to flush dedupe state", {
          stage: "shutdown",
          fallbackUsed: true
        });
      }
    }

    const server = typeof getServer === "function" ? getServer() : null;
    if (server && typeof server.close === "function" && server.listening) {
      await new Promise((resolve) => {
        server.close((error) => {
          if (error) {
            logger.warn("HTTP server close issue during shutdown", {
              stage: "shutdown",
              fallbackUsed: true,
              error
            });
          } else {
            logger.info("HTTP server closed", {
              stage: "shutdown"
            });
          }
          resolve();
        });
      });
    }

    logger.info("Service stopped", {
      stage: "shutdown",
      reason: sessionPath || ""
    });
    process.exit(0);
  }

  process.on("SIGINT", () => {
    shutdown("SIGINT");
  });

  process.on("SIGTERM", () => {
    shutdown("SIGTERM");
  });
}

async function bootstrap() {
  const logger = createLogger(env.logLevel || "info", {
    mode: env.logMode,
    baseMeta: { service: "ride-bot", component: "app" }
  });

  registerProcessHandlers(logger);
  let clientRef = null;
  let dedupeRef = null;
  let serverRef = null;
  const runtimeStatus = createBotRuntimeStatus();
  registerShutdownHooks({
    logger,
    getClient: () => clientRef,
    getDedupe: () => dedupeRef,
    getServer: () => serverRef,
    sessionPath: env.whatsappSessionPath
  });

  logger.info("Ride bot starting", {
    stage: "startup",
    reason: `env=${env.nodeEnv}`
  });

  logger.debug("Startup diagnostics", {
    stage: "startup",
    ...startupHealthSnapshot()
  });

  serverRef = await startHttpServer({
    logger: logger.child({ component: "http-server" }),
    runtimeStatus,
    port: env.port
  });

  if (env.googleCredentialsJsonError) {
    const startupError = new Error(
      `Google credentials env parsing failed: ${env.googleCredentialsJsonError}`
    );
    startupError.code = "GOOGLE_CREDENTIALS_JSON_INVALID";
    throw startupError;
  }

  if (env.googleCredentialsSource === "env_json" && env.googleCredentialsJson) {
    logger.info("Google credentials loaded from env", {
      stage: "sheets_startup",
      reason: "GOOGLE_CREDENTIALS_JSON"
    });
  }

  if (!env.whatsappClientId) {
    throw new Error("WHATSAPP_CLIENT_ID is required for stable LocalAuth persistence");
  }

  const sessionState = validateAndPrepareSessionStorage({
    sessionPath: env.whatsappSessionPath,
    clientId: env.whatsappClientId
  });

  logger.info("WhatsApp session path resolved", {
    stage: "whatsapp_auth",
    reason: sessionState.sessionPath
  });

  logger.info(
    sessionState.sessionFolderHasData
      ? "Saved session found before startup"
      : "No saved session found before startup",
    {
      stage: "whatsapp_auth",
      reason: sessionState.sessionFolderPath
    }
  );

  const startupSheetTargets = [
    {
      worksheetName: env.googleRidesWorksheetName,
      range: env.googleRidesRange
    },
    {
      worksheetName: env.googleNeedsReviewWorksheetName,
      range: env.googleNeedsReviewRange
    }
  ];

  for (const target of startupSheetTargets) {
    const sheetsStartupValidation = validateSheetsConfig({
      spreadsheetId: env.googleSheetsId,
      worksheetName: target.worksheetName,
      range: target.range,
      credentialsJson: env.googleCredentialsJson,
      credentialsPath: env.googleCredentialsPath
    });
    if (!sheetsStartupValidation.valid) {
      const sheetsReason =
        sheetsStartupValidation.reason ||
        sheetsStartupValidation.credentialsStatus?.message ||
        sheetsStartupValidation.missing.join(", ");
      const startupError = new Error(
        `Google Sheets startup validation failed: ${sheetsReason}`
      );
      startupError.code = "SHEETS_STARTUP_CONFIG_MISSING";

      logger.error("Google Sheets startup validation failed", {
        stage: "sheets_startup",
        fallbackUsed: true,
        reason: `${target.worksheetName}: ${sheetsReason}`
      });
      throw startupError;
    }
  }

  logger.info("Google Sheets startup validation passed", {
    stage: "sheets_startup",
    fallbackUsed: false,
    reason: `worksheets=${env.googleRidesWorksheetName},${env.googleNeedsReviewWorksheetName}`
  });

  const dedupe = new DedupeStore({
    ttlMs: env.dedupeTtlMs,
    maxEntries: env.dedupeMaxEntries,
    filePath: env.dedupeStorePath,
    logger: logger.child({ component: "dedupe" })
  });
  dedupeRef = dedupe;

  const localExtractor = createLocalExtractor({
    logger: logger.child({ component: "local-extractor" })
  });

  const openaiNormalizer = createOpenAiNormalizer({
    apiKey: env.openaiApiKey,
    model: env.openaiModel,
    logger: logger.child({ component: "openai-normalizer" })
  });

  const ocrExtractor = createTesseractOcr({
    tesseractPath: env.ocrTesseractPath,
    timeoutMs: env.ocrTimeoutMs,
    tempDir: env.ocrTempDir,
    logger: logger.child({ component: "ocr" })
  });

  const geocoder = createGeocoder({
    provider: env.geocodingProvider,
    apiKey: env.geocodingApiKey,
    baseUrl: env.geocodingBaseUrl,
    userAgent: env.geocodingUserAgent,
    timeoutMs: env.geocodingTimeoutMs,
    logger: logger.child({ component: "geocoder" })
  });

  const osrmClient = createOsrmClient({
    logger: logger.child({ component: "osrm" })
  });

  const sheetsClient = createSheetsClient({
    spreadsheetId: env.googleSheetsId,
    worksheetName: env.googleRidesWorksheetName,
    range: env.googleRidesRange,
    credentialsJson: env.googleCredentialsJson,
    credentialsPath: env.googleCredentialsPath,
    logger: logger.child({ component: "sheets-client" })
  });

  await verifyWorksheetTargetsReady({
    sheetsClient,
    spreadsheetId: env.googleSheetsId,
    worksheetTargets: [
      {
        worksheetName: env.googleRidesWorksheetName,
        minimumHeaders: ["Refer", "Pickup", "Drop Off", "Raw Message"]
      },
      {
        worksheetName: env.googleNeedsReviewWorksheetName,
        minimumHeaders: ["Raw Message"]
      }
    ],
    logger: logger.child({ component: "sheets-startup" })
  });

  const appendRideRow = createAppendRow({
    sheetsClient,
    spreadsheetId: env.googleSheetsId,
    worksheetName: env.googleRidesWorksheetName,
    range: env.googleRidesRange,
    logger: logger.child({ component: "sheets-append-rides" })
  });

  const appendReviewRow = createAppendRow({
    sheetsClient,
    spreadsheetId: env.googleSheetsId,
    worksheetName: env.googleNeedsReviewWorksheetName,
    range: env.googleNeedsReviewRange,
    logger: logger.child({ component: "sheets-append-review" })
  });

  const onMessage = createMessageHandler({
    env,
    logger: logger.child({ component: "message-handler" }),
    dedupe,
    localExtractor,
    openaiNormalizer,
    ocrExtractor,
    geocoder,
    osrmClient,
    appendRideRow,
    appendReviewRow
  });

  const bootSummary = {
    allowedGroups: env.allowedGroups,
    whatsappClientId: env.whatsappClientId,
    sessionDir: sessionState.sessionPath,
    worksheetName: env.googleWorksheetName,
    ridesWorksheetName: env.googleRidesWorksheetName,
    needsReviewWorksheetName: env.googleNeedsReviewWorksheetName,
    geocodingProvider: env.geocodingProvider || "",
    sheetsConfigured: Boolean(sheetsClient && env.googleSheetsId),
    googleCredentialsSource: env.googleCredentialsSource,
    googleCredentialsPath:
      env.googleCredentialsSource === "file_path" ? env.googleCredentialsPath : "",
    openaiConfigured: Boolean(env.openaiApiKey),
    dedupePersistence: env.dedupeStorePath
  };

  logger.info("Startup summary", {
    stage: "startup",
    reason: `allowedGroups=${bootSummary.allowedGroups.length}, clientId=${bootSummary.whatsappClientId}, sessionDir=${bootSummary.sessionDir}, geocoder=${bootSummary.geocodingProvider}, sheetsConfigured=${bootSummary.sheetsConfigured}, sheetsCredentials=${bootSummary.googleCredentialsSource || "unknown"}, openaiConfigured=${bootSummary.openaiConfigured}`
  });
  logger.debug("Startup details", {
    stage: "startup",
    ...bootSummary
  });

  if (env.allowedGroups.length === 0) {
    logger.warn("No allowed groups configured; messages will be ignored", {
      stage: "startup",
      fallbackUsed: true
    });
  }

  if (!bootSummary.openaiConfigured) {
    logger.warn("OpenAI key missing; local extraction only", {
      stage: "openai_normalization",
      fallbackUsed: true
    });
  }

  if (!bootSummary.sheetsConfigured) {
    logger.warn("Google Sheets not fully configured; row append will fail", {
      stage: "sheets_append",
      fallbackUsed: false
    });
  }

  const client = await initializeWhatsAppClient({
    sessionPath: sessionState.sessionPath,
    clientId: sessionState.clientId,
    startupTimeoutMs: env.whatsappStartupTimeoutMs,
    persistedSessionDetected: sessionState.sessionFolderHasData,
    qrImagePath: runtimeStatus.getSnapshot().qrImagePath,
    onStateChange: (state, details = {}) => {
      runtimeStatus.update(state, details);
    },
    logger: logger.child({ component: "whatsapp-client" }),
    puppeteer: resolvePuppeteerOptions(),
    onMessage
  });
  clientRef = client;

  logger.info("WhatsApp startup completed", {
    stage: "whatsapp_connect"
  });

  return { client, server: serverRef };
}

function formatStartupError(error) {
  if (error instanceof Error) {
    return {
      message: String(error.message || error.name || "Unknown startup error"),
      code: error.code ? String(error.code) : "",
      stack: typeof error.stack === "string" ? error.stack : ""
    };
  }

  if (error && typeof error === "object") {
    const candidateMessage = error.message || error.reason || error.error;
    const candidateStack = error.stack;

    return {
      message: candidateMessage ? String(candidateMessage) : String(error),
      code: error.code ? String(error.code) : "",
      stack: typeof candidateStack === "string" ? candidateStack : ""
    };
  }

  return {
    message: String(error || "Unknown startup error"),
    code: "",
    stack: ""
  };
}

if (require.main === module) {
  bootstrap().catch((error) => {
    const summary = summarizeKnownError(error, {
      stage: "startup",
      defaultSummary: "Service failed to start"
    });
    const details = formatStartupError(error);

    console.error("Startup failed. Service is shutting down.");
    console.error(`Reason: ${details.message || summary.summary}`);

    if (details.code) {
      console.error(`Code: ${details.code}`);
    }

    if (summary.likelyCause) {
      console.error(`Hint: ${summary.likelyCause}`);
    }

    if (env.nodeEnv === "development" && details.stack) {
      console.error("Stack trace:");
      console.error(details.stack);
    }

    process.exit(1);
  });
}

module.exports = {
  bootstrap
};
