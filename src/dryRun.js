const fs = require("node:fs/promises");
const path = require("node:path");
const process = require("node:process");

const { env } = require("./config/env");
const { createLogger } = require("./utils/logger");
const { normalizeText, safeTrim } = require("./utils/text");
const { generateRefer } = require("./utils/reference");
const { createEmptyRideObject, createEmptyNormalizationObject } = require("./extraction/schemas");
const { createLocalExtractor } = require("./extraction/localExtractor");
const { createOpenAiNormalizer } = require("./extraction/openaiNormalizer");
const { createGeocoder } = require("./routing/geocode");
const { createOsrmClient } = require("./routing/osrm");
const {
  metersToKm,
  calculateDeterministicFare,
  detectCurrencyCodeFromMoneyString
} = require("./routing/fare");
const { createSheetsClient } = require("./sheets/sheetsClient");
const { createAppendRow, buildSheetRow } = require("./sheets/appendRow");

const MERGE_PROTECTED_FIELDS = new Set([
  "distance",
  "fare_extracted",
  "currency",
  "fare_type",
  "calculated_fare",
  "final_fare"
]);

const SAMPLE_DRY_RUN_MESSAGE = `Saloon Car (1 Persons)

Landing
Tuesday 7th October 2025, 20:05 pm

Route
 - Pick Up: Heathrow Airport, Terminal 4
 - Drop Off: 12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG

Head Passenger
Jessica Walker

Mobile Number
+447495292728

Flight
VY6652

Arriving From
Barcelona

\u00A350`;

function parseArgs(argv = []) {
  const options = {
    message: "",
    filePath: "",
    sourceGroup: "dry-run-group@g.us",
    messageId: "",
    receivedAt: "",
    skipGeocode: false,
    skipOsrm: false,
    appendSheet: false,
    useSample: false,
    help: false
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = String(argv[index] || "");

    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }

    if (token === "--message" || token === "-m") {
      options.message = String(argv[index + 1] || "");
      index += 1;
      continue;
    }

    if (token === "--file" || token === "-f") {
      options.filePath = String(argv[index + 1] || "");
      index += 1;
      continue;
    }

    if (token === "--group") {
      options.sourceGroup = String(argv[index + 1] || "");
      index += 1;
      continue;
    }

    if (token === "--message-id") {
      options.messageId = String(argv[index + 1] || "");
      index += 1;
      continue;
    }

    if (token === "--received-at") {
      options.receivedAt = String(argv[index + 1] || "");
      index += 1;
      continue;
    }

    if (token === "--skip-geocode") {
      options.skipGeocode = true;
      continue;
    }

    if (token === "--skip-osrm") {
      options.skipOsrm = true;
      continue;
    }

    if (token === "--append-sheet") {
      options.appendSheet = true;
      continue;
    }

    if (token === "--use-sample") {
      options.useSample = true;
      continue;
    }
  }

  return options;
}

function printHelp() {
  console.log(
    [
      "Dry-run mode: test extraction -> OpenAI -> geocode -> OSRM -> fare -> final row",
      "",
      "Usage:",
      "  npm run dry-run -- [options]",
      "",
      "Options:",
      "  -m, --message <text>      Raw WhatsApp message text",
      "  -f, --file <path>         Read message text from file",
      "      --group <name>        Source group label/id (default: dry-run-group@g.us)",
      "      --message-id <id>     Message id used in context/refer fallback",
      "      --received-at <iso>   ISO timestamp for context",
      "      --skip-geocode        Skip geocoding + OSRM stages",
      "      --skip-osrm           Skip OSRM stage after geocoding",
      "      --append-sheet        Append to Google Sheets (OFF by default)",
      "      --use-sample          Use built-in sample message",
      "  -h, --help                Show this help",
      "",
      "If no --message/--file/stdin is provided, built-in sample message is used."
    ].join("\n")
  );
}

function normalizeMessageInput(value) {
  return String(value || "").replace(/\\n/g, "\n");
}

function readStdin() {
  return new Promise((resolve, reject) => {
    const chunks = [];

    process.stdin.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    process.stdin.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    process.stdin.on("error", (error) => reject(error));
  });
}

async function loadRawMessage(options, logger) {
  if (safeTrim(options.message)) {
    return {
      source: "arg:message",
      rawMessage: normalizeMessageInput(options.message)
    };
  }

  if (safeTrim(options.filePath)) {
    const absolutePath = path.resolve(process.cwd(), options.filePath);
    const rawFromFile = await fs.readFile(absolutePath, "utf8");
    return {
      source: `file:${absolutePath}`,
      rawMessage: rawFromFile
    };
  }

  if (!process.stdin.isTTY) {
    const rawFromStdin = await readStdin();
    if (safeTrim(rawFromStdin)) {
      return {
        source: "stdin",
        rawMessage: rawFromStdin
      };
    }
  }

  if (!options.useSample) {
    logger.info("No input message found; using built-in sample message");
  }

  return {
    source: "sample",
    rawMessage: SAMPLE_DRY_RUN_MESSAGE
  };
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
  ride.fare_type = extractedFare ? "quoted" : finalFare ? "calculated" : "";
  return ride;
}

function hasCoordinates(result) {
  return Boolean(
    result &&
      Number.isFinite(Number(result.lat)) &&
      Number.isFinite(Number(result.lng))
  );
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

  let parserConfidence = "0.95";
  let status = "ready";
  let routeTarget = "rides";
  let reviewReason = "";

  if (pickupMissing || dropOffMissing) {
    parserConfidence = "0.25";
    status = "needs_review";
    routeTarget = "review";
    reviewReason = pickupMissing && dropOffMissing ? "pickup_drop_off_missing" : "pickup_or_drop_off_missing";
  } else if (bothGeocodesFailed) {
    parserConfidence = "0.40";
    status = "needs_review";
    routeTarget = "review";
    reviewReason = "both_geocodes_failed";
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
    dropOffResolved
  };
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

async function safeGeocode(geocoder, address, field, logger) {
  try {
    if (!safeTrim(address)) {
      logger.info("Dry-run geocode skipped", {
        stage: "geocoding",
        field,
        fallbackUsed: true,
        reason: "address_missing"
      });
      return null;
    }
    const fn = geocoder?.geocodeAddress || geocoder?.geocode;
    if (typeof fn !== "function") {
      logger.warn("Dry-run geocode skipped", {
        stage: "geocoding",
        field,
        fallbackUsed: true,
        reason: "geocoder_missing"
      });
      return null;
    }
    const result = await fn(address);
    if (result && Number.isFinite(Number(result.lat)) && Number.isFinite(Number(result.lng))) {
      logger.info("Dry-run geocode completed", {
        stage: "geocoding",
        field,
        fallbackUsed: false,
        reason: "coordinates_resolved",
        lat: Number(result.lat),
        lng: Number(result.lng)
      });
      return result;
    }

    logger.warn("Dry-run geocode returned no coordinates", {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: "no_coordinates"
    });
    return null;
  } catch (error) {
    logger.warn("Dry-run geocoding failed", {
      stage: "geocoding",
      field,
      fallbackUsed: true,
      reason: "request_failed",
      error: error?.message || String(error)
    });
    return null;
  }
}

async function safeRoute(osrmClient, origin, destination, logger) {
  try {
    const fn = osrmClient?.getRouteFromOSRM || osrmClient?.route;
    if (!origin || !destination) {
      logger.info("Dry-run OSRM skipped", {
        stage: "osrm_route",
        fallbackUsed: true,
        reason: "coordinates_missing",
        originAvailable: Boolean(origin),
        destinationAvailable: Boolean(destination)
      });
      return null;
    }
    if (typeof fn !== "function") {
      logger.warn("Dry-run OSRM skipped", {
        stage: "osrm_route",
        fallbackUsed: true,
        reason: "osrm_client_missing"
      });
      return null;
    }
    const result = await fn(origin, destination);
    if (result && Number.isFinite(result.distance_meters)) {
      logger.info("Dry-run OSRM completed", {
        stage: "osrm_route",
        fallbackUsed: false,
        reason: result.distance_text || "route_resolved",
        distanceMeters: result.distance_meters,
        durationSeconds: result.duration_seconds || 0
      });
      return result;
    }

    logger.warn("Dry-run OSRM returned no route", {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: "route_missing"
    });
    return null;
  } catch (error) {
    logger.warn("Dry-run OSRM route failed", {
      stage: "osrm_route",
      fallbackUsed: true,
      reason: "request_failed",
      error: error?.message || String(error)
    });
    return null;
  }
}

async function runDryRun(options = {}) {
  const logger = createLogger(env.logLevel || "info", {
    mode: env.logMode,
    baseMeta: { service: "ride-bot", component: "dry-run" }
  });

  const resolvedOptions = {
    ...parseArgs([]),
    ...options
  };

  const input = await loadRawMessage(resolvedOptions, logger);
  const normalizedRawText = normalizeText(input.rawMessage);
  const sourceGroup = safeTrim(resolvedOptions.sourceGroup) || "dry-run-group@g.us";
  const messageId = safeTrim(resolvedOptions.messageId) || `dryrun-${Date.now()}`;
  const receivedAt = safeTrim(resolvedOptions.receivedAt) || new Date().toISOString();

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

  const context = {
    source_name: "whatsapp",
    group_name: sourceGroup,
    source_group: sourceGroup,
    message_id: messageId,
    received_at: receivedAt
  };

  let localExtracted = createEmptyRideObject(context);
  try {
    localExtracted = createEmptyRideObject(localExtractor.extract(normalizedRawText, context));
  } catch (error) {
    logger.warn("Dry-run local extraction failed; using blank-safe fallback", {
      error: error?.message || String(error)
    });
  }

  let aiNormalized = createEmptyNormalizationObject(localExtracted);
  try {
    aiNormalized = createEmptyNormalizationObject(
      await openaiNormalizer.normalizeWithOpenAI({
        rawMessage: normalizedRawText,
        raw_message: normalizedRawText,
        extracted: localExtracted,
        deterministicExtraction: localExtracted
      })
    );
  } catch (error) {
    logger.warn("Dry-run OpenAI normalization failed; using local fallback", {
      error: error?.message || String(error)
    });
  }

  let ride = mergeLocalAndAi(localExtracted, aiNormalized);
  ride.raw_message = normalizedRawText;
  ride.source_name = safeTrim(ride.source_name) || "whatsapp";
  ride.group_name = safeTrim(ride.group_name) || sourceGroup;
  ride.source_group = sourceGroup;
  ride.message_id = messageId;
  ride.received_at = receivedAt;
  ride = createEmptyRideObject(ride);

  if (!safeTrim(ride.refer)) {
    ride.refer = generateRefer({
      messageId,
      rawMessage: normalizedRawText,
      groupId: sourceGroup,
      timestamp: receivedAt
    });
  }

  let pickupGeocode = null;
  let dropOffGeocode = null;
  let route = null;
  let distanceKm = null;

  if (!resolvedOptions.skipGeocode) {
    pickupGeocode = await safeGeocode(geocoder, ride.pickup, "pickup", logger);
    dropOffGeocode = await safeGeocode(geocoder, ride.drop_off, "drop_off", logger);

    if (!resolvedOptions.skipOsrm && pickupGeocode && dropOffGeocode) {
      route = await safeRoute(osrmClient, pickupGeocode, dropOffGeocode, logger);
    }
  }

  if (route && Number.isFinite(route.distance_meters)) {
    distanceKm = metersToKm(route.distance_meters);
    ride.distance = safeTrim(route.distance_text || "");
  } else {
    ride.distance = "";
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
  const finalRow = buildSheetRow(ridePayload);
  const appendOutcome = {
    enabled: Boolean(resolvedOptions.appendSheet),
    attempted: false,
    success: false,
    error: ""
  };

  if (resolvedOptions.appendSheet) {
    appendOutcome.attempted = true;

    try {
      const sheetsClient = createSheetsClient({
        spreadsheetId: env.googleSheetsId,
        worksheetName: env.googleWorksheetName,
        range: env.googleSheetsRange,
        credentialsJson: env.googleCredentialsJson,
        credentialsPath: env.googleCredentialsPath,
        logger: logger.child({ component: "sheets-client" })
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

      const appendResult =
        routingDecision.routeTarget === "review"
          ? await appendReviewRow(ridePayload)
          : await appendRideRow(ridePayload);
      appendOutcome.success = true;
      appendOutcome.result = appendResult;
    } catch (error) {
      appendOutcome.error = error?.message || String(error);
      logger.error("Dry-run sheet append failed", {
        code: error?.code || "",
        error: appendOutcome.error
      });
    }
  }

  const report = {
    mode: "dry-run",
    timestamp: new Date().toISOString(),
    input: {
      source: input.source,
      source_group: sourceGroup,
      message_id: messageId,
      received_at: receivedAt
    },
    options: {
      skipGeocode: Boolean(resolvedOptions.skipGeocode),
      skipOsrm: Boolean(resolvedOptions.skipOsrm),
      appendSheet: Boolean(resolvedOptions.appendSheet)
    },
    normalized_text: normalizedRawText,
    extracted_local: localExtracted,
    normalized_openai: aiNormalized,
    geocoding: {
      pickup: pickupGeocode,
      drop_off: dropOffGeocode
    },
    osrm_route: route,
    routing: routingDecision,
    final_ride: createEmptyRideObject(ride),
    final_sheet_payload: ridePayload,
    final_row: finalRow,
    sheets_append: appendOutcome
  };

  return report;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (args.help) {
    printHelp();
    return;
  }

  const report = await runDryRun(args);
  console.log(JSON.stringify(report, null, 2));

  if (report.sheets_append.enabled && !report.sheets_append.success) {
    process.exitCode = 1;
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(
      JSON.stringify(
        {
          level: "error",
          message: "Dry-run execution failed",
          error: error?.message || String(error),
          stack: error?.stack
        },
        null,
        2
      )
    );
    process.exit(1);
  });
}

module.exports = {
  SAMPLE_DRY_RUN_MESSAGE,
  parseArgs,
  runDryRun
};
