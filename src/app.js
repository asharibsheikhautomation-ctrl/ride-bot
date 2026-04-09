const { env } = require("./config/env");
const { createLogger, summarizeKnownError } = require("./utils/logger");
const { DedupeStore } = require("./utils/dedupe");
const { createLocalExtractor } = require("./extraction/localExtractor");
const { createOpenAiNormalizer } = require("./extraction/openaiNormalizer");
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

  return {
    headless: true,
    args: RAILWAY_PUPPETEER_ARGS
  };
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

function registerShutdownHooks({ logger, getClient, getDedupe, sessionPath }) {
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
  registerShutdownHooks({
    logger,
    getClient: () => clientRef,
    getDedupe: () => dedupeRef,
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

  if (env.googleCredentialsBootstrapError) {
    const startupError = new Error(
      `Google credentials bootstrap failed: ${env.googleCredentialsBootstrapError}`
    );
    startupError.code = "GOOGLE_CREDENTIALS_JSON_INVALID";
    throw startupError;
  }

  if (env.googleCredentialsBootstrapApplied) {
    logger.info("Google credentials file written from env", {
      stage: "sheets_startup",
      reason: env.googleCredentialsBootstrapPath
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

  const sheetsStartupValidation = validateSheetsConfig({
    spreadsheetId: env.googleSheetsId,
    worksheetName: env.googleWorksheetName,
    range: env.googleSheetsRange,
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
      reason: sheetsReason
    });
    throw startupError;
  } else {
    logger.info("Google Sheets startup validation passed", {
      stage: "sheets_startup",
      fallbackUsed: false,
      reason: `worksheet=${env.googleWorksheetName}`
    });
  }

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
    worksheetName: env.googleWorksheetName,
    range: env.googleSheetsRange,
    credentialsPath: env.googleCredentialsPath,
    logger: logger.child({ component: "sheets-client" })
  });

  const appendRow = createAppendRow({
    sheetsClient,
    spreadsheetId: env.googleSheetsId,
    worksheetName: env.googleWorksheetName,
    range: env.googleSheetsRange,
    logger: logger.child({ component: "sheets-append" })
  });

  const onMessage = createMessageHandler({
    env,
    logger: logger.child({ component: "message-handler" }),
    dedupe,
    localExtractor,
    openaiNormalizer,
    geocoder,
    osrmClient,
    appendRow
  });

  const bootSummary = {
    allowedGroups: env.allowedGroups,
    whatsappClientId: env.whatsappClientId,
    sessionDir: sessionState.sessionPath,
    worksheetName: env.googleWorksheetName,
    geocodingProvider: env.geocodingProvider || "",
    sheetsConfigured: Boolean(sheetsClient && env.googleSheetsId && env.googleCredentialsPath),
    googleCredentialsPath: env.googleCredentialsPath,
    openaiConfigured: Boolean(env.openaiApiKey),
    dedupePersistence: env.dedupeStorePath
  };

  logger.info("Startup summary", {
    stage: "startup",
    reason: `allowedGroups=${bootSummary.allowedGroups.length}, clientId=${bootSummary.whatsappClientId}, sessionDir=${bootSummary.sessionDir}, geocoder=${bootSummary.geocodingProvider}, sheetsConfigured=${bootSummary.sheetsConfigured}, openaiConfigured=${bootSummary.openaiConfigured}`
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
    logger: logger.child({ component: "whatsapp-client" }),
    puppeteer: resolvePuppeteerOptions(),
    onMessage
  });
  clientRef = client;

  logger.info("WhatsApp startup completed", {
    stage: "whatsapp_connect"
  });

  return { client };
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
