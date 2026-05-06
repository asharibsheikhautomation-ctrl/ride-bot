const { normalizeText, safeTrim } = require("../utils/text");
const { summarizeKnownError } = require("../utils/logger");
const { generateRefer } = require("../utils/reference");
const {
  createEmptyRideObject,
  createEmptyNormalizationObject,
  buildSheetRowObject
} = require("../extraction/schemas");
const { buildSheetRow } = require("../sheets/appendRow");
const { calculateDeterministicFare, metersToKm } = require("../routing/fare");

const SYSTEM_MESSAGE_TYPES = new Set(["e2e_notification", "notification_template", "protocol"]);
const MERGE_PROTECTED_FIELDS = new Set(["distance", "fare"]);

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

function formatSourceTime(receivedAt, timeZone = "Europe/London") {
  const date = new Date(receivedAt);
  if (Number.isNaN(date.getTime())) return "";

  return new Intl.DateTimeFormat("en-GB", {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
    timeZone
  }).format(date);
}

function normalizeWhatsappIdentity(value) {
  return safeTrim(String(value || "").replace(/@.+$/, ""));
}

async function resolveSourceName(message) {
  const notifyName = safeTrim(message?._data?.notifyName || message?.notifyName || "");
  if (notifyName) return notifyName;

  if (typeof message?.getContact === "function") {
    try {
      const contact = await message.getContact();
      const contactName = safeTrim(
        contact?.pushname || contact?.name || contact?.shortName || contact?.number
      );
      if (contactName) return contactName;
    } catch (error) {
      // Fall back to raw WhatsApp identifiers below.
    }
  }

  return (
    normalizeWhatsappIdentity(message?.author) ||
    normalizeWhatsappIdentity(message?.from) ||
    ""
  );
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

  for (const key of Object.keys(ai)) {
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

function applyFareFieldsToRide(ride, distanceKm, env) {
  if (safeTrim(ride.fare)) {
    return ride;
  }

  ride.fare = calculateDeterministicFare(distanceKm, {
    baseFare: env?.fareBase,
    perKmRate: env?.farePerKm,
    currency: env?.defaultCurrency,
    requiredVehicle: ride.required_vehicle
  });
  return ride;
}

function formatDistanceMiles(distanceMeters) {
  const numeric = Number(distanceMeters);
  if (!Number.isFinite(numeric) || numeric < 0) return "";
  return String(Math.round(numeric / 1609.344));
}

function isContaminatedLocationValue(value) {
  const text = safeTrim(value).toLowerCase();
  if (!text) return false;
  return /\b(?:same(?:-|\s)day\s+payment|cash|card|account|invoice|paid|prepaid|pending|unpaid|flight(?:\s+number)?|arriving\s+from|job\s*alert|price|fare|cost|net\s+fare|net\s+amount|required\s+vehicle|vehicle)\b/.test(text) ||
    /\btime\s*:/i.test(text);
}

function determineRoutingDecision({
  ride,
  analysis,
  pickupGeocode,
  dropOffGeocode
}) {
  const reasons = [];
  const selectedPickup = analysis?.selected?.pickup;
  const selectedDropOff = analysis?.selected?.drop_off;
  const selectedVehicle = analysis?.selected?.required_vehicle;

  if (!safeTrim(ride.pickup)) reasons.push("pickup_missing");
  if (!safeTrim(ride.drop_off)) reasons.push("drop_off_missing");
  if (selectedPickup?.contaminated || selectedDropOff?.contaminated) {
    reasons.push("route_contaminated");
  }
  if (isContaminatedLocationValue(ride.pickup) || isContaminatedLocationValue(ride.drop_off)) {
    reasons.push("route_contaminated");
  }
  if ((analysis?.hadDateLikeText && !ride.pickup_day_date) || (analysis?.hadTimeLikeText && !ride.starting_timing)) {
    reasons.push("schedule_unresolved");
  }
  if (selectedVehicle?.source === "weak") {
    reasons.push("vehicle_weak");
  }
  if (
    safeTrim(ride.pickup) &&
    safeTrim(ride.drop_off) &&
    !hasCoordinates(pickupGeocode) &&
    !hasCoordinates(dropOffGeocode)
  ) {
    reasons.push("both_geocodes_failed");
  }

  return {
    target: reasons.length > 0 ? "review" : "rides",
    reasons: [...new Set(reasons)]
  };
}

function normalizeAllowedGroupIds(groupIds) {
  return (Array.isArray(groupIds) ? groupIds : [])
    .map((value) => safeTrim(value))
    .filter(Boolean);
}

async function safeLocalExtraction(localExtractor, rawMessage, context, logger) {
  try {
    if (localExtractor && typeof localExtractor.extractWithAnalysis === "function") {
      const result = await localExtractor.extractWithAnalysis(rawMessage, context);
      return {
        ride: createEmptyRideObject(result?.record || result?.ride || result),
        analysis:
          result?.analysis && typeof result.analysis === "object" ? result.analysis : {}
      };
    }

    return {
      ride: createEmptyRideObject(localExtractor.extract(rawMessage, context)),
      analysis: {}
    };
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
    return {
      ride: createEmptyRideObject(context),
      analysis: {}
    };
  }
}

async function safeOpenAiNormalization(openaiNormalizer, rawMessage, extracted, analysis, logger) {
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
          deterministicExtraction: extracted,
          analysis
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
        fallbackUsed: false
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
        reason: result.distance_text || "route_resolved"
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

function splitBySeparatorBlocks(text) {
  const normalized = normalizeText(text);
  if (!normalized) return [];

  const parts = normalized
    .split(/\n-{5,}\n/gi)
    .map((part) => normalizeText(part))
    .filter(hasUsefulRawText);

  return parts.length > 1 ? parts : [];
}

function segmentByMarkerLines(text, markerPattern, minimumMarkers = 2) {
  const rawLines = String(text || "").replace(/\r\n?/g, "\n").split("\n");
  const cleanedLines = rawLines.map((line) => normalizeText(line));
  const markerIndexes = [];

  for (let index = 0; index < cleanedLines.length; index += 1) {
    if (markerPattern.test(cleanedLines[index])) {
      markerIndexes.push(index);
    }
  }

  if (markerIndexes.length < minimumMarkers) return [];

  const sharedPrefix = normalizeText(rawLines.slice(0, markerIndexes[0]).join("\n"));
  const blocks = [];

  for (let index = 0; index < markerIndexes.length; index += 1) {
    const start = markerIndexes[index];
    const end = index + 1 < markerIndexes.length ? markerIndexes[index + 1] : rawLines.length;
    const blockBody = normalizeText(rawLines.slice(start, end).join("\n"));
    if (!hasUsefulRawText(blockBody)) continue;

    const block = sharedPrefix ? normalizeText(`${sharedPrefix}\n${blockBody}`) : blockBody;
    if (hasUsefulRawText(block)) {
      blocks.push(block);
    }
  }

  return blocks;
}

function splitGoingComingBackBlocks(text) {
  const rawLines = String(text || "").replace(/\r\n?/g, "\n").split("\n");
  const cleanedLines = rawLines.map((line) =>
    normalizeText(String(line || "").replace(/^[^\p{L}\p{N}]+/u, ""))
  );
  const markerIndexes = [];

  for (let index = 0; index < cleanedLines.length; index += 1) {
    if (/^(?:going|coming\s+back)\b/i.test(cleanedLines[index])) {
      markerIndexes.push(index);
    }
  }

  if (markerIndexes.length < 2) return [];

  const blocks = [];
  for (let index = 0; index < markerIndexes.length; index += 1) {
    const start = markerIndexes[index];
    const end = index + 1 < markerIndexes.length ? markerIndexes[index + 1] : rawLines.length;
    const block = normalizeText(rawLines.slice(start, end).join("\n"));
    if (hasUsefulRawText(block)) {
      blocks.push(block);
    }
  }
  return blocks;
}

function splitMessageIntoRideBlocks(rawText) {
  const normalized = normalizeText(rawText);
  if (!normalized) return [];

  const strategies = [
    () => splitBySeparatorBlocks(normalized),
    () => splitGoingComingBackBlocks(normalized),
    () => segmentByMarkerLines(normalized, /^\(job\s*\d+\)/i, 2),
    () => segmentByMarkerLines(normalized, /^\d+\.\s*at\b/i, 2),
    () => segmentByMarkerLines(normalized, /^at\s*\d{1,2}(?::\d{1,2})?\b/i, 2),
    () =>
      segmentByMarkerLines(
        normalized,
        /^(?:today|tomorrow|tonight)\s*@?\s*\d{1,2}(?::\d{1,2})?\s*(?:am|pm)?\b/i,
        2
      )
  ];

  for (const strategy of strategies) {
    const blocks = strategy().map((block) => normalizeText(block)).filter(hasUsefulRawText);
    if (blocks.length > 1) {
      return blocks;
    }
  }

  return [normalized];
}

async function safeExtractOcrText(ocrExtractor, message, logger, context = {}) {
  if (!message?.hasMedia || !ocrExtractor || typeof message.downloadMedia !== "function") {
    return "";
  }

  try {
    const media = await message.downloadMedia();
    if (!media || !ocrExtractor.isSupportedImageMimeType(media.mimetype)) {
      return "";
    }

    const ocrText = normalizeText(
      await ocrExtractor.extractTextFromMedia(media, {
        fileStem: context.fileStem
      })
    );

    if (!hasUsefulRawText(ocrText)) {
      return "";
    }

    logger.info("OCR text extracted from media", {
      stage: "ocr",
      fallbackUsed: false
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
    const blocks = splitMessageIntoRideBlocks(normalizedRawText);

    for (let blockIndex = 0; blockIndex < blocks.length; blockIndex += 1) {
      const blockText = normalizeText(blocks[blockIndex]);
      if (!hasUsefulRawText(blockText)) continue;

      const fingerprint = blockText.toLowerCase();
      if (seen.has(fingerprint)) continue;
      seen.add(fingerprint);

      attempts.push({
        sourceKind: blocks.length > 1 ? `${sourceKind}:${blockIndex + 1}` : sourceKind,
        rawText: blockText,
        attemptMessageId:
          sourceKind === "text"
            ? blocks.length > 1
              ? `${messageId}:text:${blockIndex + 1}`
              : messageId
            : `${messageId}:${sourceKind}${blocks.length > 1 ? `:${blockIndex + 1}` : ""}`
      });
    }
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
  appendRow,
  appendReviewRow,
  groupName,
  sourceName,
  receivedAt,
  chatId
}) {
  const attemptDedupeKey = createAttemptDedupeKey(dedupe, attempt, message, chatId, receivedAt);
  if (dedupe.hasProcessed(attemptDedupeKey)) {
    logger.info("Attempt skipped as duplicate", {
      stage: "dedupe",
      messageId: attempt.attemptMessageId,
      reason: attempt.sourceKind
    });
    return { skipped: true, reason: "duplicate" };
  }

  logger.info("Processing ride attempt", {
    stage: "ingest",
    messageId: attempt.attemptMessageId,
    sourceGroup: groupName,
    sourceName,
    reason: attempt.sourceKind
  });

  const extractionContext = {
    source_name: sourceName,
    group_name: groupName,
    message_id: attempt.attemptMessageId,
    received_at: receivedAt,
    source_time: formatSourceTime(receivedAt, env?.appTimeZone)
  };

  const localExtractionResult = await safeLocalExtraction(
    localExtractor,
    attempt.rawText,
    extractionContext,
    logger
  );
  const localExtracted = localExtractionResult.ride;
  const localAnalysis = localExtractionResult.analysis || {};

  const aiNormalized = await safeOpenAiNormalization(
    openaiNormalizer,
    attempt.rawText,
    localExtracted,
    localAnalysis,
    logger
  );

  let ride = mergeLocalAndAi(localExtracted, aiNormalized);
  ride.group_name = safeTrim(ride.group_name) || groupName;
  ride.source_name = safeTrim(ride.source_name) || sourceName;
  ride.source_time =
    safeTrim(ride.source_time) || formatSourceTime(receivedAt, env?.appTimeZone);
  ride = createEmptyRideObject(ride);

  if (!safeTrim(ride.refer)) {
    ride.refer = generateRefer({
      messageId: attempt.attemptMessageId,
      rawMessage: attempt.rawText,
      groupId: chatId,
      timestamp: receivedAt
    });
  }

  const pickupGeocode = await safeGeocode(geocoder, ride.pickup, logger, "pickup");
  const dropOffGeocode = await safeGeocode(geocoder, ride.drop_off, logger, "drop_off");
  const route = await safeRoute(osrmClient, pickupGeocode, dropOffGeocode, logger);

  let distanceKm = null;
  if (route && Number.isFinite(route.distance_meters)) {
    distanceKm = metersToKm(route.distance_meters);
    ride.distance = formatDistanceMiles(route.distance_meters);
  } else {
    ride.distance = "";
  }

  applyFareFieldsToRide(ride, distanceKm, env);
  ride = createEmptyRideObject(ride);

  const routingDecision = determineRoutingDecision({
    ride,
    analysis: localAnalysis,
    pickupGeocode,
    dropOffGeocode
  });

  if (typeof appendRow !== "function") {
    throw new Error("No sheet append function configured");
  }

  const targetAppender =
    routingDecision.target === "review" && typeof appendReviewRow === "function"
      ? appendReviewRow
      : appendRow;

  logger.info("Final ride row prepared for Google Sheets", {
    stage: "sheets_append",
    fallbackUsed: false,
    reason: attempt.sourceKind,
    target: routingDecision.target,
    reviewReasons: routingDecision.reasons,
    rowObject: buildSheetRowObject(ride)
  });

  await targetAppender(ride);
  dedupe.markProcessed(attemptDedupeKey, {
    messageId: attempt.attemptMessageId,
    chatId,
    refer: ride.refer,
    sourceKind: attempt.sourceKind,
    target: routingDecision.target,
    processedAt: new Date().toISOString()
  });

  logger.info("Ride attempt processed", {
    stage: "completed",
    messageId: attempt.attemptMessageId,
    refer: ride.refer,
    sourceGroup: ride.group_name,
    target: routingDecision.target,
    reviewReasons: routingDecision.reasons,
    rowColumns: buildSheetRow(ride).length
  });

  return {
    skipped: false,
    ride,
    target: routingDecision.target
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
      const groupName = safeTrim(chat?.name || chat?.formattedTitle || "") || chatId;
      const sourceName = await resolveSourceName(message);
      const receivedAt = resolveReceivedAtIso(message);

      const skipState = shouldSkipMessage({
        message,
        chat,
        chatId,
        allowFromMeMessages
      });
      if (skipState.skip) {
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

      if (!isAllowedGroup) {
        return;
      }

      const normalizedBody = normalizeText(String(message.body || ""));
      const attempts = await buildMessageAttempts({
        message,
        messageId,
        normalizedBody,
        ocrExtractor,
        logger
      });

      if (attempts.length === 0) {
        return;
      }

      const primaryAppender = appendRideRow || appendRow;

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
            appendRow: primaryAppender,
            appendReviewRow,
            groupName,
            sourceName,
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
  buildMessageAttempts,
  hasUsefulRawText
};
