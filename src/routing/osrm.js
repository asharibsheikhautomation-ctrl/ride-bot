const axios = require("axios");
const { env } = require("../config/env");
const { createLogger, summarizeKnownError } = require("../utils/logger");
const { safeTrim } = require("../utils/text");
const { executeWithRetry, defaultShouldRetry } = require("../utils/retry");

const DEFAULT_OSRM_BASE_URL = "https://router.project-osrm.org";
const DEFAULT_TIMEOUT_MS = 12000;
const OSRM_PROFILE = "driving";

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function isValidCoordinate(lat, lng) {
  return (
    Number.isFinite(lat) &&
    Number.isFinite(lng) &&
    lat >= -90 &&
    lat <= 90 &&
    lng >= -180 &&
    lng <= 180
  );
}

function normalizePoint(point) {
  if (!point || typeof point !== "object") return null;

  const lat = toNumber(point.lat);
  const lng = toNumber(point.lng ?? point.lon ?? point.longitude);

  if (!isValidCoordinate(lat, lng)) return null;

  return {
    lat,
    lng
  };
}

function isRetryableOsrmError(error) {
  return defaultShouldRetry(error);
}

function formatDistanceText(distanceMeters) {
  if (!Number.isFinite(distanceMeters) || distanceMeters < 0) return "";
  const miles = distanceMeters / 1609.344;
  return String(Math.round(miles));
}

function formatDurationText(durationSeconds) {
  if (!Number.isFinite(durationSeconds) || durationSeconds < 0) return "";

  let remaining = Math.round(durationSeconds);
  const days = Math.floor(remaining / 86400);
  remaining %= 86400;

  const hours = Math.floor(remaining / 3600);
  remaining %= 3600;

  const minutes = Math.floor(remaining / 60);
  const seconds = remaining % 60;

  if (days > 0) return `${days}d ${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m`;
  return `${seconds}s`;
}

function buildRouteUrl(baseUrl, origin, destination) {
  const normalizedBase =
    safeTrim(baseUrl || "").replace(/\/+$/, "") || DEFAULT_OSRM_BASE_URL;
  const coordinates = `${origin.lng},${origin.lat};${destination.lng},${destination.lat}`;

  const url = new URL(`/route/v1/${OSRM_PROFILE}/${coordinates}`, normalizedBase);
  url.searchParams.set("overview", "false");
  url.searchParams.set("alternatives", "false");
  url.searchParams.set("steps", "false");
  return url.toString();
}

async function requestOsrmRoute({
  origin,
  destination,
  baseUrl,
  timeoutMs,
  httpClient,
  logger
}) {
  const from = normalizePoint(origin);
  const to = normalizePoint(destination);

  if (!from || !to) {
    logger.warn("Route lookup skipped: invalid coordinates", {
      stage: "osrm_route",
      originValid: Boolean(from),
      destinationValid: Boolean(to),
      fallbackUsed: true
    });
    return null;
  }

  const routeUrl = buildRouteUrl(baseUrl, from, to);

  try {
    const response = await executeWithRetry(
      async () =>
        httpClient.get(routeUrl, {
          timeout: timeoutMs
        }),
      {
        maxAttempts: 3,
        initialDelayMs: 250,
        maxDelayMs: 2000,
        shouldRetry: isRetryableOsrmError,
        onRetry: ({ attempt, maxAttempts, delayMs, error }) => {
          const summary = summarizeKnownError(error, {
            stage: "osrm_route",
            defaultSummary: "Route lookup failed, retrying",
            fallbackUsed: true
          });

          logger.warn(summary.summary, {
            stage: "osrm_route",
            fallbackUsed: true,
            attempt,
            maxAttempts,
            delayMs,
            status: summary.status,
            code: summary.code,
            reason: summary.likelyCause || "Temporary routing service issue",
            error
          });
        }
      }
    );

    if (response.data?.code !== "Ok") {
      logger.warn("Route distance unavailable", {
        stage: "osrm_route",
        reason: response.data?.code || "non-OK response",
        fallbackUsed: true
      });
      return null;
    }

    const route = response.data?.routes?.[0];
    const distanceMeters = toNumber(route?.distance);
    const durationSeconds = toNumber(route?.duration);

    if (!Number.isFinite(distanceMeters) || !Number.isFinite(durationSeconds)) {
      logger.warn("Route distance unavailable", {
        stage: "osrm_route",
        reason: "distance or duration missing",
        fallbackUsed: true
      });
      return null;
    }

    const result = {
      distance_meters: distanceMeters,
      duration_seconds: durationSeconds,
      distance_text: formatDistanceText(distanceMeters),
      duration_text: formatDurationText(durationSeconds)
    };

    logger.debug("Route lookup completed", {
      stage: "osrm_route",
      distance_meters: result.distance_meters,
      duration_seconds: result.duration_seconds
    });

    return result;
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "osrm_route",
      defaultSummary: "Route distance unavailable",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "osrm_route",
      status: summary.status,
      code: summary.code,
      reason: summary.likelyCause || "OSRM request failed",
      fallbackUsed: true,
      error
    });
    return null;
  }
}

function createOsrmClient(options = {}) {
  const logger =
    options.logger ||
    createLogger(env.logLevel || "info", {
      mode: env.logMode,
      baseMeta: { component: "osrm" }
    });
  const baseUrl =
    safeTrim(options.baseUrl || process.env.OSRM_BASE_URL || "") || DEFAULT_OSRM_BASE_URL;
  const timeoutMs =
    Number.isFinite(options.timeoutMs) && options.timeoutMs > 0
      ? options.timeoutMs
      : DEFAULT_TIMEOUT_MS;
  const httpClient = options.httpClient || axios;

  async function getRouteFromOSRM(origin, destination) {
    try {
      return await requestOsrmRoute({
        origin,
        destination,
        baseUrl,
        timeoutMs,
        httpClient,
        logger
      });
    } catch (error) {
      const summary = summarizeKnownError(error, {
        stage: "osrm_route",
        defaultSummary: "Route distance unavailable",
        fallbackUsed: true
      });

      logger.warn(summary.summary, {
        stage: "osrm_route",
        status: summary.status,
        code: summary.code,
        reason: summary.likelyCause || "Unexpected OSRM error",
        fallbackUsed: true,
        error
      });
      return null;
    }
  }

  return {
    getRouteFromOSRM,
    // Backward-compatible alias used by earlier scaffold code.
    route: getRouteFromOSRM
  };
}

const defaultOsrmClient = createOsrmClient();

async function getRouteFromOSRM(origin, destination) {
  return defaultOsrmClient.getRouteFromOSRM(origin, destination);
}

module.exports = {
  getRouteFromOSRM,
  createOsrmClient,
  formatDistanceText,
  formatDurationText
};
