const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const puppeteer = require("puppeteer");
const { Client, LocalAuth } = require("whatsapp-web.js");
const qrImage = require("qrcode");
const terminalQr = require("qrcode-terminal");
const { env } = require("../config/env");
const { createLogger, summarizeKnownError } = require("../utils/logger");
const { safeTrim } = require("../utils/text");
const { resolveLocalAuthPaths } = require("./session");

function isNonEmptyDirectory(dirPath) {
  try {
    const entries = fs.readdirSync(dirPath);
    return entries.length > 0;
  } catch (error) {
    return false;
  }
}

function createStartupError(message, code, options = {}) {
  const error = new Error(message);
  error.code = code || "WHATSAPP_STARTUP_FAILED";
  if (options.likelyCause) error.likelyCause = options.likelyCause;
  if (options.stage) error.stage = options.stage;
  if (options.details) error.details = options.details;
  if (options.cause) error.cause = options.cause;
  return error;
}

function normalizeTimeoutMs(value, minMs = 15000) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 90000;
  return Math.min(Math.max(Math.trunc(parsed), minMs), 300000);
}

function sanitizeFileSegment(value) {
  const clean = String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");

  return clean || "default";
}

function isHeadlessEnabled(headlessValue) {
  if (headlessValue === false) return false;
  return true;
}

function resolveBrowserExecutablePath(customPath = "") {
  const explicitPath = safeTrim(customPath);
  if (explicitPath && fs.existsSync(explicitPath)) {
    return explicitPath;
  }

  try {
    const bundledPath = safeTrim(puppeteer.executablePath());
    if (bundledPath && fs.existsSync(bundledPath)) {
      return bundledPath;
    }
  } catch (error) {
    // Ignore bundled browser resolution failures and try system browsers next.
  }

  if (process.platform === "win32") {
    const windowsCandidates = [
      "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
      "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
      "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
      "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe"
    ];

    for (const candidate of windowsCandidates) {
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }
  }

  return "";
}

function resolveTerminalQrRenderer(customRenderer) {
  if (typeof customRenderer === "function") return customRenderer;
  if (terminalQr && typeof terminalQr.generate === "function") {
    return (qrCode, options) => terminalQr.generate(qrCode, options);
  }
  return null;
}

function resolveQrImageGenerator(customGenerator) {
  if (customGenerator && typeof customGenerator.toFile === "function") return customGenerator;
  if (qrImage && typeof qrImage.toFile === "function") return qrImage;
  return null;
}

async function saveQrImageFile({ qrCode, qrImagePath, qrImageGenerator }) {
  const targetPath = safeTrim(qrImagePath);
  if (!targetPath) {
    throw new Error("QR image path is not configured");
  }

  if (!qrImageGenerator || typeof qrImageGenerator.toFile !== "function") {
    throw new Error("QR PNG generator is unavailable");
  }

  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  await qrImageGenerator.toFile(targetPath, String(qrCode || ""), {
    type: "png",
    width: 320,
    margin: 2
  });

  return targetPath;
}

function clearQrImageFile(qrImagePath) {
  const targetPath = safeTrim(qrImagePath);
  if (!targetPath || !fs.existsSync(targetPath)) return false;

  fs.unlinkSync(targetPath);
  return true;
}

function saveQrFallbackFile({ qrCode, clientId }) {
  const safeClientId = sanitizeFileSegment(clientId);
  const filePath = path.join(os.tmpdir(), `ride-bot-whatsapp-qr-${safeClientId}.txt`);

  const payload = [
    "WhatsApp QR fallback token",
    `generated_at=${new Date().toISOString()}`,
    `client_id=${safeClientId}`,
    `qr=${String(qrCode || "")}`
  ].join("\n");

  fs.writeFileSync(filePath, payload, "utf8");
  return filePath;
}

function createStartupTracker({ startupTimeoutMs, persistedSessionDetected }) {
  const stateHistory = [];
  let ready = false;
  let settled = false;
  let resolvePromise;
  let rejectPromise;

  const promise = new Promise((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });

  function pushState(state, reason = "") {
    stateHistory.push({
      state: safeTrim(state),
      reason: safeTrim(reason),
      at: new Date().toISOString()
    });
  }

  function hasState(state) {
    return stateHistory.some((entry) => entry.state === state);
  }

  function getLatestState() {
    return stateHistory[stateHistory.length - 1]?.state || "";
  }

  function setReady(value) {
    ready = Boolean(value);
  }

  function resolve(value) {
    if (settled) return;
    settled = true;
    resolvePromise(value);
  }

  function reject(error) {
    if (settled) return;
    settled = true;
    rejectPromise(error);
  }

  function getSnapshot() {
    return {
      ready,
      settled,
      persistedSessionDetected,
      latestState: getLatestState(),
      stateHistory: stateHistory.map((entry) => ({ ...entry }))
    };
  }

  return {
    promise,
    pushState,
    hasState,
    getLatestState,
    getSnapshot,
    setReady,
    resolve,
    reject,
    isReady: () => ready,
    isSettled: () => settled
  };
}

function emitStateChange({ onStateChange, logger }, state, details = {}) {
  if (typeof onStateChange !== "function") return;

  try {
    onStateChange(state, details);
  } catch (error) {
    logger.warn("WhatsApp state callback failed", {
      stage: "whatsapp_auth",
      fallbackUsed: true,
      reason: safeTrim(error?.message) || "Unknown state callback error"
    });
  }
}

function buildStartupTimeoutHint({ tracker, persistedSessionDetected }) {
  if (tracker.hasState("qr_required")) {
    return "QR was generated but login was not completed in time.";
  }

  if (tracker.hasState("authenticated") && !tracker.hasState("ready")) {
    return "WhatsApp authenticated but did not reach ready state; browser/page load may be stuck.";
  }

  if (persistedSessionDetected) {
    return "Saved session exists but was not reused successfully; session may be stale or linked device may have changed.";
  }

  return "No valid session became ready in time; check browser runtime and WhatsApp linked device status.";
}

function createDefaultClient({ sessionPath, clientId, puppeteerOptions, logger }) {
  if (logger) {
    logger.info("WhatsApp browser executable selected", {
      stage: "whatsapp_connect",
      fallbackUsed: false,
      reason: safeTrim(puppeteerOptions?.executablePath) || "default puppeteer resolution"
    });
  }

  return new Client({
    authStrategy: new LocalAuth({
      clientId,
      dataPath: sessionPath
    }),
    // Only pass executablePath when explicitly supplied in puppeteerOptions/config later.
    puppeteer: {
      headless: true,
      ...(puppeteerOptions || {})
    }
  });
}

function attachMessageListeners({ client, onMessage, logger }) {
  if (typeof onMessage !== "function") return;

  const recentlyDispatched = new Map();
  const dedupeWindowMs = 60 * 1000;

  function cleanupDispatched(now = Date.now()) {
    for (const [key, at] of recentlyDispatched.entries()) {
      if (now - at > dedupeWindowMs) {
        recentlyDispatched.delete(key);
      }
    }
  }

  function resolveDispatchKey(message) {
    const messageId = safeTrim(message?.id?._serialized || message?.id?.id || message?.id || "");
    if (messageId) return `id:${messageId}`;
    const from = safeTrim(message?.from || "");
    const timestamp = Number(message?.timestamp);
    const bodyPreview = safeTrim(String(message?.body || "").slice(0, 80));
    return `fallback:${from}|${Number.isFinite(timestamp) ? timestamp : "no-ts"}|${bodyPreview}`;
  }

  function dispatchMessage(message, sourceEvent) {
    const dispatchKey = resolveDispatchKey(message);
    cleanupDispatched();

    if (dispatchKey && recentlyDispatched.has(dispatchKey)) {
      logger.debug("Skipping duplicate event dispatch", {
        stage: "message_listener",
        reason: sourceEvent,
        messageId: safeTrim(message?.id?._serialized || message?.id?.id || message?.id || "")
      });
      return;
    }

    if (dispatchKey) {
      recentlyDispatched.set(dispatchKey, Date.now());
    }

    Promise.resolve(onMessage(message)).catch((error) => {
      const summary = summarizeKnownError(error, {
        stage: "message_pipeline",
        defaultSummary: "Message processing failed",
        fallbackUsed: true
      });

      logger.error(summary.summary, {
        stage: "message_pipeline",
        fallbackUsed: true,
        reason: summary.likelyCause || "Message was skipped after failure",
        error
      });
    });
  }

  client.on("message", (message) => {
    dispatchMessage(message, "message");
  });

  client.on("message_create", (message) => {
    dispatchMessage(message, "message_create");
  });

  logger.debug("Message listeners bound", {
    stage: "message_listener",
    reason: "message + message_create"
  });
}

function attachLifecycleListeners({
  client,
  logger,
  tracker,
  startupTimeoutMs,
  persistedSessionDetected,
  qrRenderer,
  qrImagePath,
  qrImageGenerator,
  onStateChange,
  clientId,
  headlessEnabled
}) {
  let timeoutHandle = null;

  function clearStartupTimeout() {
    if (!timeoutHandle) return;
    clearTimeout(timeoutHandle);
    timeoutHandle = null;
  }

  function rejectStartup(error) {
    clearStartupTimeout();
    tracker.reject(error);
  }

  function resolveStartup() {
    clearStartupTimeout();
    tracker.resolve();
  }

  tracker.pushState("init_started", "client initialization started");
  emitStateChange({ onStateChange, logger }, "starting", {
    qrAvailable: false,
    qrImagePath
  });

  if (persistedSessionDetected) {
    tracker.pushState("persisted_session_detected", "saved auth session found");
  }

  timeoutHandle = setTimeout(() => {
    tracker.pushState("startup_timeout", "ready state did not arrive in time");
    const hint = buildStartupTimeoutHint({ tracker, persistedSessionDetected });
    const timeoutError = createStartupError(
      "WhatsApp startup failed: persisted session did not become ready",
      "WHATSAPP_STARTUP_TIMEOUT",
      {
        stage: "whatsapp_connect",
        likelyCause: hint,
        details: {
          timeoutMs: startupTimeoutMs,
          latestState: tracker.getLatestState(),
          stateHistory: tracker.getSnapshot().stateHistory
        }
      }
    );

    logger.error("WhatsApp startup failed: persisted session did not become ready", {
      stage: "whatsapp_connect",
      fallbackUsed: true,
      reason: hint,
      timeoutMs: startupTimeoutMs,
      latestState: tracker.getLatestState()
    });

    rejectStartup(timeoutError);
  }, startupTimeoutMs);

  client.on("qr", async (qrCode) => {
    tracker.pushState("qr_required", "no valid persisted session");
    emitStateChange({ onStateChange, logger }, "qr_required", {
      qrAvailable: false,
      qrImagePath
    });
    logger.info("QR required because no valid session exists", {
      stage: "whatsapp_auth"
    });

    try {
      const savedQrPath = await saveQrImageFile({
        qrCode,
        qrImagePath,
        qrImageGenerator: resolveQrImageGenerator(qrImageGenerator)
      });
      const qrUpdatedAt = new Date().toISOString();

      emitStateChange({ onStateChange, logger }, "qr_required", {
        qrAvailable: true,
        qrImagePath: savedQrPath,
        qrUpdatedAt,
        error: ""
      });

      logger.info("QR generated and saved", {
        stage: "whatsapp_auth",
        reason: savedQrPath
      });
    } catch (error) {
      emitStateChange({ onStateChange, logger }, "qr_required", {
        qrAvailable: false,
        qrImagePath,
        error: safeTrim(error?.message) || "Unable to save QR PNG"
      });

      logger.warn("QR generated but PNG save failed", {
        stage: "whatsapp_auth",
        fallbackUsed: true,
        reason: safeTrim(qrImagePath) || "QR image path unavailable",
        error
      });
    }

    const terminalRenderer = resolveTerminalQrRenderer(qrRenderer);
    const terminalAvailable = Boolean(process.stdout && process.stdout.isTTY);

    try {
      if (!terminalAvailable) {
        throw new Error("Terminal is non-interactive (TTY unavailable)");
      }
      if (!terminalRenderer) {
        throw new Error("qrcode-terminal renderer is unavailable");
      }

      terminalRenderer(qrCode, { small: true });
      logger.info("QR rendered in terminal. Scan it with WhatsApp Linked Devices.", {
        stage: "whatsapp_auth"
      });
    } catch (error) {
      const browserHint = !headlessEnabled
        ? "Scan from the opened browser window."
        : "Run with puppeteer headless=false to scan in browser.";

      try {
        const fallbackFilePath = saveQrFallbackFile({
          qrCode,
          clientId
        });

        logger.warn(
          "QR generated but terminal rendering failed. Scan from the opened browser window or use fallback file.",
          {
            stage: "whatsapp_auth",
            fallbackUsed: true,
            reason: `${browserHint} Fallback file: ${fallbackFilePath}`,
            error
          }
        );
      } catch (fallbackError) {
        logger.warn(
          "QR generated but terminal rendering failed. Scan from the opened browser window or enable qrcode-terminal fallback.",
          {
            stage: "whatsapp_auth",
            fallbackUsed: true,
            reason: browserHint,
            error,
            fallbackError
          }
        );
      }
    }

    if (!headlessEnabled) {
      logger.info("Browser window is available for QR scanning", {
        stage: "whatsapp_auth",
        reason: "puppeteer headless=false"
      });
    }
  });

  client.on("authenticated", () => {
    tracker.pushState("authenticated", "session accepted");
    emitStateChange({ onStateChange, logger }, "authenticated", {
      qrAvailable: false,
      qrImagePath
    });
    logger.info("WhatsApp authenticated", {
      stage: "whatsapp_auth"
    });
  });

  client.on("ready", () => {
    tracker.pushState("ready", "client ready");
    tracker.setReady(true);
    let qrRemoved = false;

    try {
      qrRemoved = clearQrImageFile(qrImagePath);
    } catch (error) {
      logger.warn("Unable to clear stale QR image", {
        stage: "whatsapp_auth",
        fallbackUsed: true,
        reason: safeTrim(qrImagePath) || "QR image path unavailable",
        error
      });
    }

    emitStateChange({ onStateChange, logger }, "ready", {
      qrAvailable: false,
      qrImagePath,
      error: ""
    });
    logger.info("WhatsApp connected", {
      stage: "whatsapp_connect"
    });
    logger.info("Bot ready; QR not required", {
      stage: "whatsapp_connect",
      reason: qrRemoved ? "stale QR removed" : "no QR file to remove"
    });
    resolveStartup();
  });

  client.on("auth_failure", (message) => {
    tracker.pushState("auth_failed", safeTrim(message) || "authentication failed");
    const reason = safeTrim(message) || "Saved session is invalid or expired";
    emitStateChange({ onStateChange, logger }, "auth_failed", {
      qrAvailable: false,
      qrImagePath,
      error: reason
    });
    const startupError = createStartupError("WhatsApp authentication failed", "WHATSAPP_AUTH_FAILED", {
      stage: "whatsapp_auth",
      likelyCause: reason
    });

    logger.error("WhatsApp auth failure", {
      stage: "whatsapp_auth",
      fallbackUsed: true,
      reason
    });

    rejectStartup(startupError);
  });

  client.on("disconnected", (reason) => {
    const reasonText = safeTrim(reason) || "unknown";
    logger.warn("WhatsApp disconnected but session preserved", {
      stage: "whatsapp_connect",
      fallbackUsed: true,
      reason: reasonText
    });

    if (tracker.isReady() || tracker.isSettled()) return;

    if (/(logout|invalid|401|conflict|revoke)/i.test(reasonText)) {
      const startupError = createStartupError(
        "WhatsApp disconnected before ready",
        "WHATSAPP_DISCONNECTED_BEFORE_READY",
        {
          stage: "whatsapp_connect",
          likelyCause:
            "Persisted session appears invalid. Re-link the device if this repeats."
        }
      );
      rejectStartup(startupError);
    }
  });

  client.on("change_state", (state) => {
    logger.info("WhatsApp state changed", {
      stage: "whatsapp_connect",
      reason: safeTrim(state)
    });
  });

  client.on("loading_screen", (percent, message) => {
    logger.debug("WhatsApp loading", {
      stage: "whatsapp_connect",
      reason: safeTrim(message),
      percent: Number.isFinite(percent) ? percent : undefined
    });
  });

  client.on("error", (error) => {
    const summary = summarizeKnownError(error, {
      stage: "whatsapp_connect",
      defaultSummary: "WhatsApp runtime error",
      fallbackUsed: true
    });

    logger.error(summary.summary, {
      stage: "whatsapp_connect",
      fallbackUsed: true,
      reason: summary.likelyCause || "Browser page context issue",
      error
    });

    if (tracker.isReady() || tracker.isSettled()) return;

    const startupError = createStartupError("WhatsApp startup failed", "WHATSAPP_RUNTIME_ERROR", {
      stage: "whatsapp_connect",
      likelyCause: summary.likelyCause || "Client emitted error before ready state",
      cause: error
    });
    rejectStartup(startupError);
  });
}

async function initializeWhatsAppClient(options = {}) {
  const logger =
    options.logger ||
    createLogger(env.logLevel || "info", {
      mode: env.logMode,
      baseMeta: { component: "whatsapp-client" }
    });

  const resolvedPaths = resolveLocalAuthPaths({
    sessionPath: options.sessionPath || env.whatsappSessionPath,
    clientId: options.clientId || env.whatsappClientId
  });

  if (!resolvedPaths.clientId) {
    throw createStartupError(
      "WHATSAPP_CLIENT_ID is required for stable LocalAuth persistence",
      "WHATSAPP_CLIENT_ID_MISSING",
      { stage: "whatsapp_auth" }
    );
  }

  fs.mkdirSync(resolvedPaths.sessionPath, { recursive: true });

  const persistedSessionDetected =
    typeof options.persistedSessionDetected === "boolean"
      ? options.persistedSessionDetected
      : isNonEmptyDirectory(resolvedPaths.sessionFolderPath);

  const startupTimeoutMs = normalizeTimeoutMs(
    options.startupTimeoutMs || env.whatsappStartupTimeoutMs,
    options.startupTimeoutMinMs || 15000
  );
  const headlessEnabled = isHeadlessEnabled(options?.puppeteer?.headless);

  logger.info(
    persistedSessionDetected ? "WhatsApp session found" : "No WhatsApp session found",
    {
      stage: "whatsapp_auth",
      reason: resolvedPaths.sessionFolderPath
    }
  );
  logger.info(
    persistedSessionDetected ? "Reusing saved login" : "Waiting for QR scan to create login",
    {
      stage: "whatsapp_auth",
      reason: `clientId=${resolvedPaths.clientId}`
    }
  );

  const clientFactory =
    typeof options.clientFactory === "function"
      ? options.clientFactory
      : ({ sessionPath, clientId }) =>
          createDefaultClient({
            sessionPath,
            clientId,
            puppeteerOptions: options.puppeteer,
            logger
          });

  let client;
  try {
    client = clientFactory({
      sessionPath: resolvedPaths.sessionPath,
      clientId: resolvedPaths.clientId
    });
  } catch (error) {
    throw createStartupError("WhatsApp startup failed", "WHATSAPP_CLIENT_FACTORY_FAILED", {
      stage: "whatsapp_connect",
      likelyCause: "Unable to construct WhatsApp client",
      cause: error
    });
  }

  const tracker = createStartupTracker({
    startupTimeoutMs,
    persistedSessionDetected
  });

  attachLifecycleListeners({
    client,
    logger,
    tracker,
    startupTimeoutMs,
    persistedSessionDetected,
    qrRenderer: options.qrRenderer,
    qrImagePath: options.qrImagePath,
    qrImageGenerator: options.qrImageGenerator,
    onStateChange: options.onStateChange,
    clientId: resolvedPaths.clientId,
    headlessEnabled
  });
  attachMessageListeners({
    client,
    onMessage: options.onMessage,
    logger
  });

  logger.info("Waiting for WhatsApp ready state", {
    stage: "whatsapp_connect",
    reason: `timeout=${startupTimeoutMs}ms`
  });

  Promise.resolve()
    .then(() => client.initialize())
    .catch((error) => {
      if (tracker.isSettled()) {
        logger.warn("WhatsApp initialize reported late error", {
          stage: "whatsapp_connect",
          fallbackUsed: true
        });
        return;
      }

      const summary = summarizeKnownError(error, {
        stage: "whatsapp_connect",
        defaultSummary: "WhatsApp startup failed",
        fallbackUsed: true
      });

      const startupError = createStartupError("WhatsApp startup failed", "WHATSAPP_INIT_FAILED", {
        stage: "whatsapp_connect",
        likelyCause: summary.likelyCause || "Browser runtime failed to initialize",
        cause: error
      });

      logger.error("WhatsApp startup failed", {
        stage: "whatsapp_connect",
        fallbackUsed: true,
        reason: startupError.likelyCause,
        error
      });

      tracker.reject(startupError);
    });

  await tracker.promise;
  logger.info("Saved session reused and ready", {
    stage: "whatsapp_auth",
    reason: resolvedPaths.sessionFolderPath
  });

  return client;
}

module.exports = {
  initializeWhatsAppClient,
  createStartupError,
  createStartupTracker
};
