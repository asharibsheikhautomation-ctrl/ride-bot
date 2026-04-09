const { normalizeText, safeTrim } = require("../utils/text");
const { summarizeKnownError } = require("../utils/logger");
const { generateRefer } = require("../utils/reference");
const { createEmptyRideObject, createEmptyNormalizationObject } = require("../extraction/schemas");
const { buildSheetRow } = require("../sheets/appendRow");
const { calculateFare, metersToKm } = require("../routing/fare");

const SYSTEM_MESSAGE_TYPES = new Set(["e2e_notification", "notification_template", "protocol"]);

function resolveMessageId(message) {
  return safeTrim(message?.id?._serialized || message?.id?.id || message?.id || "");
}

function resolveReceivedAtIso(message) {
  const timestamp = Number(message?.timestamp);
  if (Number.isFinite(timestamp) && timestamp > 0) {
    return new Date(timestamp * 1000).toISOString();
  }
  return new Date().toISOString();
}

function maskPhoneNumbers(value) {
  const text = String(value || "");
  return text.replace(/(\+?\d[\d\s().-]{6,}\d)/g, (match) => {
    const digits = match.replace(/\D/g, "");
    if (digits.length <= 2) return "***";
    return `***${digits.slice(-2)}`;
  });
}

function safePreview(text, length = 180) {
  return maskPhoneNumbers(String(text || "").slice(0, length));
}

function maskRowForLog(row) {
  return row.map((cell) => maskPhoneNumbers(cell));
}

function isBroadcastLike(message, chatId) {
  const from = String(message?.from || "");
  const to = String(message?.to || "");
  const candidate = chatId || from || to;

  if (candidate === "status@broadcast") return true;
  if (candidate.endsWith("@broadcast")) return true;
  if (message?.isStatus === true) return true;
  return false;
}

function shouldSkipMessage({ message, chat, chatId, allowFromMeMessages }) {
  if (message?.fromMe && !allowFromMeMessages) {
    return { skip: true, reason: "outgoing/self message" };
  }
  if (isBroadcastLike(message, chatId)) return { skip: true, reason: "broadcast/status message" };
  if (!chat?.isGroup) return { skip: true, reason: "non-group chat" };
  if (SYSTEM_MESSAGE_TYPES.has(String(message?.type || ""))) {
    return { skip: true, reason: `system message type: ${message.type}` };
  }

  const body = safeTrim(message?.body || "");
  if (!body) return { skip: true, reason: "empty body" };

  return { skip: false, reason: "" };
}

function mergeLocalAndAi(localExtracted, aiNormalized) {
  const local = createEmptyRideObject(localExtracted || {});
  const ai = createEmptyNormalizationObject(aiNormalized || {});
  const merged = createEmptyRideObject(local);

  const keys = Object.keys(ai);
  for (const key of keys) {
    const aiValue = safeTrim(ai[key]);
    const localValue = safeTrim(local[key]);

    if (key === "distance" || key === "fare") {
      merged[key] = localValue || "";
      continue;
    }

    merged[key] = aiValue || localValue || "";
  }

  return merged;
}

function normalizeAllowedGroupIds(groupIds) {
  return (Array.isArray(groupIds) ? groupIds : [])
    .map((value) => safeTrim(value))
    .filter(Boolean);
}

async function safeLocalExtraction(localExtractor, rawMessage, context, logger) {
  try {
    return createEmptyRideObject(localExtractor.extract(rawMessage, context));
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "local_extraction",
      defaultSummary: "Local extraction failed, using blank data",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "local_extraction",
      fallbackUsed: true,
      reason: summary.likelyCause || "Message format may be unsupported",
      error
    });
    return createEmptyRideObject(context);
  }
}

async function safeOpenAiNormalization(openaiNormalizer, rawMessage, extracted, logger) {
  try {
    if (!openaiNormalizer) {
      return createEmptyNormalizationObject(extracted);
    }

    if (typeof openaiNormalizer.normalizeWithOpenAI === "function") {
      return createEmptyNormalizationObject(
        await openaiNormalizer.normalizeWithOpenAI({
          rawMessage,
          extracted
        })
      );
    }

    if (typeof openaiNormalizer.normalize === "function") {
      return createEmptyNormalizationObject(await openaiNormalizer.normalize(extracted, rawMessage));
    }

    return createEmptyNormalizationObject(extracted);
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "openai_normalization",
      defaultSummary: "OpenAI normalization failed, using local data",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "openai_normalization",
      fallbackUsed: true,
      reason: summary.likelyCause || "API request failed or invalid response",
      error
    });
    return createEmptyNormalizationObject(extracted);
  }
}

async function safeGeocode(geocoder, address, logger, field) {
  if (!address || !geocoder) return null;

  try {
    const fn = geocoder.geocodeAddress || geocoder.geocode;
    if (typeof fn !== "function") return null;
    return await fn(address);
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "geocoding",
      defaultSummary: `Geocoding failed for ${field}`,
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: summary.likelyCause || "No usable location match",
      error
    });
    return null;
  }
}

async function safeRoute(osrmClient, origin, destination, logger) {
  if (!origin || !destination || !osrmClient) return null;

  try {
    const fn = osrmClient.getRouteFromOSRM || osrmClient.route;
    if (typeof fn !== "function") return null;
    return await fn(origin, destination);
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "osrm_route",
      defaultSummary: "Route distance unavailable",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: summary.likelyCause || "Routing service failed",
      error
    });
    return null;
  }
}

function createMessageHandler({
  env,
  logger,
  dedupe,
  localExtractor,
  openaiNormalizer,
  geocoder,
  osrmClient,
  appendRow
}) {
  const allowedGroupIds = normalizeAllowedGroupIds(env?.allowedGroups);
  const allowedGroups = new Set(allowedGroupIds);
  const allowFromMeMessages = Boolean(env?.allowFromMeMessages);
  let warnedAboutEmptyAllowList = false;

  logger.debug("Allowed groups loaded", {
    stage: "ingest_filter",
    reason: `count=${allowedGroupIds.length}`,
    allowedGroupIds,
    allowFromMeMessages
  });

  return async function handleMessage(message) {
    const fallbackMessageId = resolveMessageId(message);
    const fallbackChatId = safeTrim(message?.from || "");

    try {
      if (!message) return;

      const messageId = resolveMessageId(message);
      const chat = typeof message.getChat === "function" ? await message.getChat().catch(() => null) : null;
      const serializedChatId = safeTrim(chat?.id?._serialized || "");
      const fallbackIncomingChatId = safeTrim(message?.from || "");
      const chatId = serializedChatId || fallbackIncomingChatId;
      const chatName = safeTrim(chat?.name || chat?.formattedTitle || "");
      const sourceGroup = chatName || chatId;
      const receivedAt = resolveReceivedAtIso(message);
      const preview = safePreview(message?.body || "");

      logger.debug("Incoming message received", {
        stage: "ingest_filter",
        messageId,
        chatId,
        chatName,
        sourceGroup,
        isGroup: Boolean(chat?.isGroup),
        fromMe: Boolean(message?.fromMe),
        messageType: safeTrim(message?.type || ""),
        reason: preview,
        preview
      });

      const skipState = shouldSkipMessage({
        message,
        chat,
        chatId,
        allowFromMeMessages
      });
      if (skipState.skip) {
        logger.debug("Message skipped", {
          stage: "ingest_filter",
          reason: skipState.reason,
          messageId,
          chatId
        });
        return;
      }

      if (allowedGroups.size === 0) {
        if (!warnedAboutEmptyAllowList) {
          warnedAboutEmptyAllowList = true;
          logger.warn("No allowed groups configured; skipping messages", {
            stage: "ingest_filter",
            fallbackUsed: true
          });
        }
        return;
      }

      const groupFilterId = serializedChatId || chatId;
      const isAllowedGroup = allowedGroups.has(groupFilterId);

      logger.debug("Allowed group match evaluated", {
        stage: "ingest_filter",
        currentChatId: groupFilterId,
        allowedGroupIds,
        filterMatch: isAllowedGroup
      });

      if (!isAllowedGroup) {
        logger.debug("Message skipped: group not allowed", {
          stage: "ingest_filter",
          messageId,
          chatId
        });
        return;
      }

      logger.info("Message accepted for processing", {
        stage: "ingest_filter",
        messageId,
        sourceGroup,
        chatId
      });

      const rawBody = String(message.body || "");
      const normalizedRawText = normalizeText(rawBody);
      if (!normalizedRawText) {
        logger.debug("Message skipped: empty after text cleanup", {
          stage: "ingest_filter",
          messageId,
          chatId
        });
        return;
      }

      const dedupeKey = dedupe.buildDedupeKey({
        messageId,
        rawMessage: normalizedRawText,
        groupId: chatId,
        timestamp: message.timestamp || receivedAt
      });

      if (dedupe.hasProcessed(dedupeKey)) {
        logger.info("Message skipped as duplicate", {
          stage: "dedupe",
          messageId,
          chatId,
          fallbackUsed: true
        });
        return;
      }

      logger.info("Message received from allowed group", {
        stage: "ingest",
        messageId,
        sourceGroup,
        chatId,
        reason: safePreview(normalizedRawText)
      });

      const extractionContext = {
        source_group: sourceGroup,
        message_id: messageId,
        received_at: receivedAt
      };

      // 6) Local extraction
      const localExtracted = await safeLocalExtraction(
        localExtractor,
        normalizedRawText,
        extractionContext,
        logger
      );
      logger.info("Local extraction completed", {
        stage: "local_extraction",
        messageId,
        sourceGroup
      });

      // 7) OpenAI normalization
      const aiNormalized = await safeOpenAiNormalization(
        openaiNormalizer,
        normalizedRawText,
        localExtracted,
        logger
      );

      // 8) Merge local + AI safely
      const ride = mergeLocalAndAi(localExtracted, aiNormalized);
      ride.raw_message = normalizedRawText;
      ride.source_group = sourceGroup;
      ride.message_id = messageId;
      ride.received_at = receivedAt;

      // 9) Ensure refer exists
      if (!safeTrim(ride.refer)) {
        ride.refer = generateRefer({
          messageId,
          rawMessage: normalizedRawText,
          groupId: sourceGroup || chatId,
          timestamp: receivedAt
        });
      }

      // 10) Geocode
      const pickupGeocode = await safeGeocode(geocoder, ride.pickup, logger, "pickup");
      const dropOffGeocode = await safeGeocode(geocoder, ride.drop_off, logger, "drop_off");

      // 11) OSRM route if both geocodes exist
      const route = await safeRoute(osrmClient, pickupGeocode, dropOffGeocode, logger);

      // 12) Set distance
      let distanceKm = null;
      if (route && Number.isFinite(route.distance_meters)) {
        distanceKm = metersToKm(route.distance_meters);
        ride.distance = safeTrim(route.distance_text || "");
      } else {
        ride.distance = "";
        logger.warn("Route distance unavailable", {
          stage: "osrm_route",
          messageId,
          refer: ride.refer,
          fallbackUsed: true
        });
      }

      // 13) Fare calculation with preservation rule
      ride.fare = calculateFare(distanceKm, ride.fare, {
        baseFare: env.fareBase,
        perKmRate: env.farePerKm,
        currency: env.defaultCurrency
      });

      // 14) Build final row
      const finalRow = buildSheetRow(ride);

      // 15) Append to sheet
      try {
        await appendRow(ride);
      } catch (error) {
        const summary = summarizeKnownError(error, {
          stage: "sheets_append",
          defaultSummary: "Google Sheets append failed",
          retryExhausted: true,
          fallbackUsed: true
        });

        logger.error(summary.summary, {
          stage: "sheets_append",
          messageId,
          refer: ride.refer,
          sourceGroup,
          fallbackUsed: true,
          reason: summary.likelyCause || "Row was not saved; message will retry later",
          error
        });
        return;
      }

      // 16) Mark processed only after successful append
      dedupe.markProcessed(dedupeKey, {
        messageId,
        chatId,
        refer: ride.refer,
        processedAt: new Date().toISOString()
      });

      // 17) Final status log
      logger.info("Message processed and appended successfully", {
        stage: "completed",
        messageId,
        sourceGroup,
        refer: ride.refer,
        row: maskRowForLog(finalRow)
      });
    } catch (error) {
      const summary = summarizeKnownError(error, {
        stage: "message_pipeline",
        defaultSummary: "Message processing failed",
        fallbackUsed: true
      });

      logger.error(summary.summary, {
        stage: "message_pipeline",
        messageId: fallbackMessageId,
        chatId: fallbackChatId,
        fallbackUsed: true,
        reason: summary.likelyCause || "This message was skipped after failure",
        error
      });
    }
  };
}

module.exports = {
  createMessageHandler
};
