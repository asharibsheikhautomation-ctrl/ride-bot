const { normalizeText, safeTrim } = require("../utils/text");
const { summarizeKnownError } = require("../utils/logger");
const { generateRefer } = require("../utils/reference");
const { createEmptyRideObject, createEmptyNormalizationObject } = require("../extraction/schemas");
const { buildSheetRow } = require("../sheets/appendRow");
const {
  calculateDeterministicFare,
  detectCurrencyCodeFromMoneyString,
  metersToKm
} = require("../routing/fare");

const SYSTEM_MESSAGE_TYPES = new Set(["e2e_notification", "notification_template", "protocol"]);
const MERGE_PROTECTED_FIELDS = new Set([
  "distance",
  "fare_extracted",
  "currency",
  "fare_type",
  "calculated_fare",
  "final_fare"
]);

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

  const hasBody = Boolean(safeTrim(message?.body || ""));
  const hasMedia = Boolean(message?.hasMedia);
  if (!hasBody && !hasMedia) return { skip: true, reason: "empty body and no media" };

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

    if (MERGE_PROTECTED_FIELDS.has(key)) {
      merged[key] = localValue || "";
      continue;
    }

    merged[key] = aiValue || localValue || "";
  }

  return merged;
}

function hasCoordinates(result) {
  return Boolean(
    result &&
      Number.isFinite(Number(result.lat)) &&
      Number.isFinite(Number(result.lng))
  );
}

function countRideSignals(ride) {
  const signalFields = [
    "pickup",
    "drop_off",
    "required_vehicle",
    "fare_extracted",
    "pickup_time",
    "pickup_date",
    "day_label"
  ];

  return signalFields.filter((field) => Boolean(safeTrim(ride?.[field]))).length;
}

function determineReviewRouting({ ride, pickupGeocode, dropOffGeocode }) {
  const pickupMissing = !safeTrim(ride.pickup);
  const dropOffMissing = !safeTrim(ride.drop_off);
  const pickupResolved = hasCoordinates(pickupGeocode);
  const dropOffResolved = hasCoordinates(dropOffGeocode);
  const bothGeocodesFailed =
    !pickupResolved &&
    !dropOffResolved &&
    !pickupMissing &&
    !dropOffMissing;
  const weakSignal = countRideSignals(ride) < 2;

  let parserConfidence = "0.95";
  let status = "ready";
  let routeTarget = "rides";
  let reviewReason = "";

  if (pickupMissing || dropOffMissing) {
    parserConfidence = "0.25";
    status = "needs_review";
    routeTarget = "review";
    reviewReason =
      pickupMissing && dropOffMissing ? "pickup_drop_off_missing" : "pickup_or_drop_off_missing";
  } else if (bothGeocodesFailed) {
    parserConfidence = "0.40";
    status = "needs_review";
    routeTarget = "review";
    reviewReason = "both_geocodes_failed";
  } else if (weakSignal) {
    parserConfidence = "0.15";
    status = "needs_review";
    routeTarget = "review";
    reviewReason = "weak_parse";
  } else if (!pickupResolved || !dropOffResolved) {
    parserConfidence = "0.70";
    reviewReason = "partial_geocode";
  }

  return {
    parserConfidence,
    status,
    routeTarget,
    reviewReason,
    pickupResolved,
    dropOffResolved,
    bothGeocodesFailed,
    weakSignal
  };
}

function applyFareFieldsToRide(ride, distanceKm, env) {
  const extractedFare = safeTrim(ride.fare_extracted);
  const currency =
    safeTrim(ride.currency) ||
    detectCurrencyCodeFromMoneyString(extractedFare) ||
    safeTrim(env?.defaultCurrency || "");

  const calculatedFare = extractedFare
    ? ""
    : calculateDeterministicFare(distanceKm, {
        baseFare: env?.fareBase,
        perKmRate: env?.farePerKm,
        currency: currency || env?.defaultCurrency
      });

  const finalFare = extractedFare || calculatedFare;

  ride.fare_extracted = extractedFare;
  ride.currency = currency;
  ride.calculated_fare = calculatedFare;
  ride.final_fare = finalFare;
  ride.fare_type = extractedFare ? safeTrim(ride.fare_type) || "quoted" : finalFare ? "calculated" : "";
  return ride;
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
          raw_message: rawMessage,
          extracted,
          deterministicExtraction: extracted
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
  if (!safeTrim(address)) {
    logger.info("Geocode skipped", {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: "address_missing"
    });
    return null;
  }

  if (!geocoder) {
    logger.warn("Geocode skipped", {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: "geocoder_missing"
    });
    return null;
  }

  try {
    const fn = geocoder.geocodeAddress || geocoder.geocode;
    if (typeof fn !== "function") return null;
    const result = await fn(address);

    if (result && Number.isFinite(Number(result.lat)) && Number.isFinite(Number(result.lng))) {
      logger.info("Geocode completed", {
        stage: "geocoding",
        field,
        fallbackUsed: false,
        reason: "coordinates_resolved",
        lat: Number(result.lat),
        lng: Number(result.lng)
      });
      return result;
    }

    logger.warn("Geocode returned no coordinates", {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: "no_coordinates"
    });
    return null;
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
  if (!origin || !destination) {
    logger.info("OSRM skipped", {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: "coordinates_missing",
      originAvailable: Boolean(origin),
      destinationAvailable: Boolean(destination)
    });
    return null;
  }

  if (!osrmClient) {
    logger.warn("OSRM skipped", {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: "osrm_client_missing"
    });
    return null;
  }

  try {
    const fn = osrmClient.getRouteFromOSRM || osrmClient.route;
    if (typeof fn !== "function") return null;
    const result = await fn(origin, destination);

    if (result && Number.isFinite(result.distance_meters)) {
      logger.info("OSRM route completed", {
        stage: "osrm_route",
        fallbackUsed: false,
        reason: result.distance_text || "route_resolved",
        distanceMeters: result.distance_meters,
        durationSeconds: result.duration_seconds || 0
      });
      return result;
    }

    logger.warn("OSRM returned no route", {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: "route_missing"
    });
    return null;
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

function hasUsefulRawText(text) {
  const value = safeTrim(text);
  if (!value) return false;
  const alphaNumericCount = (value.match(/[a-z0-9]/gi) || []).length;
  return alphaNumericCount >= 6;
}

function createReviewPayload(ride) {
  return createEmptyRideObject({
    source_name: ride.source_name,
    group_name: ride.group_name,
    raw_message: ride.raw_message,
    message_id: ride.message_id,
    source_group: ride.source_group,
    received_at: ride.received_at,
    parser_confidence: ride.parser_confidence,
    status: ride.status
  });
}

async function safeExtractOcrText(ocrExtractor, message, logger, context = {}) {
  if (!message?.hasMedia || !ocrExtractor || typeof message.downloadMedia !== "function") {
    return "";
  }

  try {
    const media = await message.downloadMedia();
    if (!media || !ocrExtractor.isSupportedImageMimeType(media.mimetype)) {
      logger.debug("OCR skipped for non-image media", {
        stage: "ocr",
        fallbackUsed: true,
        reason: safeTrim(media?.mimetype) || "media_missing"
      });
      return "";
    }

    const ocrText = normalizeText(
      await ocrExtractor.extractTextFromMedia(media, {
        fileStem: context.fileStem
      })
    );

    if (!hasUsefulRawText(ocrText)) {
      logger.info("OCR yielded no useful text", {
        stage: "ocr",
        fallbackUsed: true,
        reason: safeTrim(media?.mimetype) || "image"
      });
      return "";
    }

    logger.info("OCR text extracted from media", {
      stage: "ocr",
      fallbackUsed: false,
      reason: safePreview(ocrText)
    });
    return ocrText;
  } catch (error) {
    const summary = summarizeKnownError(error, {
      stage: "ocr",
      defaultSummary: "OCR extraction failed",
      fallbackUsed: true
    });

    logger.warn(summary.summary, {
      stage: "ocr",
      fallbackUsed: true,
      reason: summary.likelyCause || "Unable to OCR media attachment",
      error
    });
    return "";
  }
}

async function buildMessageAttempts({ message, messageId, normalizedBody, ocrExtractor, logger }) {
  const attempts = [];
  const seen = new Set();

  function pushAttempt(sourceKind, rawText) {
    const normalizedRawText = normalizeText(rawText);
    if (!hasUsefulRawText(normalizedRawText)) return;

    const fingerprint = normalizedRawText.toLowerCase();
    if (seen.has(fingerprint)) return;
    seen.add(fingerprint);

    attempts.push({
      sourceKind,
      rawText: normalizedRawText,
      attemptMessageId: sourceKind === "text" ? messageId : `${messageId}:${sourceKind}`
    });
  }

  pushAttempt("text", normalizedBody);

  const ocrText = await safeExtractOcrText(ocrExtractor, message, logger, {
    fileStem: `${messageId || "message"}-ocr`
  });
  pushAttempt("ocr", ocrText);

  return attempts;
}

function createAttemptDedupeKey(dedupe, attempt, message, chatId, receivedAt) {
  return dedupe.buildDedupeKey({
    messageId: attempt.attemptMessageId,
    rawMessage: attempt.rawText,
    groupId: chatId,
    sourceKind: attempt.sourceKind,
    timestamp: message.timestamp || receivedAt
  });
}

async function processRideAttempt({
  attempt,
  message,
  env,
  logger,
  dedupe,
  localExtractor,
  openaiNormalizer,
  geocoder,
  osrmClient,
  appendRideRow,
  appendReviewRow,
  appendRow,
  groupName,
  groupFilterId,
  receivedAt,
  chatId
}) {
  const attemptDedupeKey = createAttemptDedupeKey(dedupe, attempt, message, chatId, receivedAt);
  if (dedupe.hasProcessed(attemptDedupeKey)) {
    logger.info("Attempt skipped as duplicate", {
      stage: "dedupe",
      messageId: attempt.attemptMessageId,
      chatId,
      fallbackUsed: true,
      reason: attempt.sourceKind
    });
    return { skipped: true, reason: "duplicate" };
  }

  logger.info("Processing ride attempt", {
    stage: "ingest",
    messageId: attempt.attemptMessageId,
    sourceGroup: groupName,
    chatId,
    reason: attempt.sourceKind
  });

  const extractionContext = {
    source_name: "whatsapp",
    group_name: groupName,
    source_group: groupFilterId,
    message_id: attempt.attemptMessageId,
    received_at: receivedAt
  };

  const localExtracted = await safeLocalExtraction(
    localExtractor,
    attempt.rawText,
    extractionContext,
    logger
  );

  const aiNormalized = await safeOpenAiNormalization(
    openaiNormalizer,
    attempt.rawText,
    localExtracted,
    logger
  );

  let ride = mergeLocalAndAi(localExtracted, aiNormalized);
  ride.raw_message = attempt.rawText;
  ride.source_name = safeTrim(ride.source_name) || "whatsapp";
  ride.group_name = safeTrim(ride.group_name) || groupName;
  ride.source_group = groupFilterId;
  ride.message_id = attempt.attemptMessageId;
  ride.received_at = receivedAt;
  ride = createEmptyRideObject(ride);

  if (!safeTrim(ride.refer)) {
    ride.refer = generateRefer({
      messageId: attempt.attemptMessageId,
      rawMessage: attempt.rawText,
      groupId: groupFilterId || chatId,
      timestamp: receivedAt
    });
  }

  const pickupGeocode = await safeGeocode(geocoder, ride.pickup, logger, "pickup");
  const dropOffGeocode = await safeGeocode(geocoder, ride.drop_off, logger, "drop_off");
  const route = await safeRoute(osrmClient, pickupGeocode, dropOffGeocode, logger);

  let distanceKm = null;
  if (route && Number.isFinite(route.distance_meters)) {
    distanceKm = metersToKm(route.distance_meters);
    ride.distance = safeTrim(route.distance_text || "");
  } else {
    ride.distance = "";
    logger.warn("Route distance unavailable", {
      stage: "osrm_route",
      messageId: attempt.attemptMessageId,
      refer: ride.refer,
      fallbackUsed: true
    });
  }

  applyFareFieldsToRide(ride, distanceKm, env);

  const routingDecision = determineReviewRouting({
    ride,
    pickupGeocode,
    dropOffGeocode
  });
  ride.parser_confidence = routingDecision.parserConfidence;
  ride.status = routingDecision.status;

  const ridePayload =
    routingDecision.routeTarget === "review" ? createReviewPayload(ride) : createEmptyRideObject(ride);
  const primaryAppender =
    routingDecision.routeTarget === "review"
      ? appendReviewRow || appendRideRow || appendRow
      : appendRideRow || appendReviewRow || appendRow;

  const finalRow = buildSheetRow(ridePayload);

  logger.info("Ride routing decision computed", {
    stage: "review_routing",
    messageId: attempt.attemptMessageId,
    refer: ride.refer,
    status: ride.status,
    parserConfidence: ride.parser_confidence,
    routeTarget: routingDecision.routeTarget,
    reviewReason: routingDecision.reviewReason || "ready_for_rides",
    pickupResolved: routingDecision.pickupResolved,
    dropOffResolved: routingDecision.dropOffResolved,
    sourceKind: attempt.sourceKind
  });

  if (typeof primaryAppender !== "function") {
    throw new Error("No sheet append function configured for routed ride");
  }

  await primaryAppender(ridePayload);

  dedupe.markProcessed(attemptDedupeKey, {
    messageId: attempt.attemptMessageId,
    chatId,
    refer: ride.refer,
    sourceKind: attempt.sourceKind,
    processedAt: new Date().toISOString()
  });

  logger.info("Ride attempt processed and appended successfully", {
    stage: "completed",
    messageId: attempt.attemptMessageId,
    sourceGroup: groupName,
    refer: ride.refer,
    status: ride.status,
    parserConfidence: ride.parser_confidence,
    sourceKind: attempt.sourceKind,
    row: maskRowForLog(finalRow)
  });

  return {
    skipped: false,
    ride,
    ridePayload,
    routingDecision
  };
}

function createMessageHandler({
  env,
  logger,
  dedupe,
  localExtractor,
  openaiNormalizer,
  ocrExtractor,
  geocoder,
  osrmClient,
  appendRideRow,
  appendReviewRow,
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
      const groupName = chatName || chatId;
      const receivedAt = resolveReceivedAtIso(message);
      const preview = safePreview(message?.body || "");

      logger.debug("Incoming message received", {
        stage: "ingest_filter",
        messageId,
        chatId,
        chatName,
        sourceGroup: groupName,
        isGroup: Boolean(chat?.isGroup),
        fromMe: Boolean(message?.fromMe),
        hasMedia: Boolean(message?.hasMedia),
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
        sourceGroup: groupName,
        chatId
      });

      const normalizedBody = normalizeText(String(message.body || ""));
      const attempts = await buildMessageAttempts({
        message,
        messageId,
        normalizedBody,
        ocrExtractor,
        logger
      });

      if (attempts.length === 0) {
        logger.info("Message skipped: no useful text payloads found", {
          stage: "ingest_filter",
          messageId,
          chatId,
          fallbackUsed: true
        });
        return;
      }

      for (const attempt of attempts) {
        try {
          await processRideAttempt({
            attempt,
            message,
            env,
            logger,
            dedupe,
            localExtractor,
            openaiNormalizer,
            geocoder,
            osrmClient,
            appendRideRow,
            appendReviewRow,
            appendRow,
            groupName,
            groupFilterId,
            receivedAt,
            chatId
          });
        } catch (error) {
          const summary = summarizeKnownError(error, {
            stage: "message_pipeline",
            defaultSummary: "Ride attempt processing failed",
            fallbackUsed: true
          });

          logger.error(summary.summary, {
            stage: "message_pipeline",
            messageId: attempt.attemptMessageId,
            chatId,
            fallbackUsed: true,
            reason: summary.likelyCause || "This ride attempt was skipped after failure",
            error
          });
          return;
        }
      }
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
  createMessageHandler,
  determineReviewRouting,
  createReviewPayload,
  buildMessageAttempts,
  hasUsefulRawText
};
