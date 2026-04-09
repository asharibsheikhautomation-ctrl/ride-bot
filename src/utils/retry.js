const RETRYABLE_STATUS_CODES = new Set([408, 409, 425, 429, 500, 502, 503, 504]);
const RETRYABLE_ERROR_CODES = new Set([
  "ECONNRESET",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "ENOTFOUND",
  "ECONNABORTED",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT"
]);

function defaultShouldRetry(error) {
  const status = Number(error?.status || error?.response?.status);
  const code = String(error?.code || "");

  if (RETRYABLE_STATUS_CODES.has(status)) return true;
  if (RETRYABLE_ERROR_CODES.has(code)) return true;
  return false;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function computeDelayMs(attempt, options) {
  const initialDelayMs = Number.isFinite(options.initialDelayMs) ? options.initialDelayMs : 300;
  const backoffFactor = Number.isFinite(options.backoffFactor) ? options.backoffFactor : 2;
  const maxDelayMs = Number.isFinite(options.maxDelayMs) ? options.maxDelayMs : 5000;
  const jitter = options.jitter !== false;

  const exponential = initialDelayMs * backoffFactor ** Math.max(0, attempt - 1);
  const bounded = clamp(exponential, 0, maxDelayMs);
  if (!jitter) return bounded;

  const randomMultiplier = 0.7 + Math.random() * 0.6;
  return Math.round(bounded * randomMultiplier);
}

async function executeWithRetry(operation, options = {}) {
  const maxAttempts = Number.isFinite(options.maxAttempts)
    ? Math.max(1, Math.trunc(options.maxAttempts))
    : 3;
  const shouldRetry =
    typeof options.shouldRetry === "function" ? options.shouldRetry : defaultShouldRetry;
  const onRetry = typeof options.onRetry === "function" ? options.onRetry : null;
  const onGiveUp = typeof options.onGiveUp === "function" ? options.onGiveUp : null;

  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await operation(attempt);
      return result;
    } catch (error) {
      lastError = error;
      const retryable = shouldRetry(error);

      if (!retryable || attempt >= maxAttempts) {
        if (onGiveUp) {
          onGiveUp({
            attempt,
            maxAttempts,
            retryable,
            error
          });
        }
        break;
      }

      const delayMs = computeDelayMs(attempt, options);
      if (onRetry) {
        onRetry({
          attempt,
          maxAttempts,
          delayMs,
          error
        });
      }
      await sleep(delayMs);
    }
  }

  if (lastError && typeof lastError === "object") {
    lastError.attempts = maxAttempts;
  }
  throw lastError || new Error("Retry operation failed");
}

module.exports = {
  executeWithRetry,
  defaultShouldRetry
};
