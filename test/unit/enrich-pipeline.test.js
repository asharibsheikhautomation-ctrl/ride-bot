const test = require("node:test");
const assert = require("node:assert/strict");
const { createMessageHandler } = require("../../src/whatsapp/messageHandler");
const { DedupeStore } = require("../../src/utils/dedupe");
const { createSilentLogger } = require("../helpers");

function createRecordingLogger() {
  const entries = [];
  const logger = {
    info: (message, meta = {}) => entries.push({ level: "info", message, meta }),
    warn: (message, meta = {}) => entries.push({ level: "warn", message, meta }),
    error: (message, meta = {}) => entries.push({ level: "error", message, meta }),
    debug: (message, meta = {}) => entries.push({ level: "debug", message, meta }),
    child: () => logger
  };

  return {
    logger,
    entries
  };
}

function createAllowedGroupMessage(body, groupId = "120363408968321565@g.us") {
  return {
    body,
    fromMe: false,
    type: "chat",
    timestamp: 1760000000,
    from: groupId,
    id: { _serialized: `MSG-${Math.random().toString(16).slice(2, 10)}` },
    getChat: async () => ({
      isGroup: true,
      id: { _serialized: groupId },
      name: "Test Allowed Group"
    })
  };
}

test("enrich pipeline geocodes after normalization, runs OSRM only with both coordinates, and preserves extracted fare", async () => {
  const { logger, entries } = createRecordingLogger();
  const appendRideCalls = [];
  const appendReviewCalls = [];
  const geocodeCalls = [];
  const osrmCalls = [];
  const allowedGroup = "120363408968321565@g.us";

  const handler = createMessageHandler({
    env: {
      allowedGroups: [allowedGroup],
      fareBase: 250,
      farePerKm: 95,
      defaultCurrency: "PKR"
    },
    logger,
    dedupe: new DedupeStore({ logger: createSilentLogger() }),
    localExtractor: {
      extract: () => ({
        pickup: "LOCAL PICKUP",
        drop_off: "LOCAL DROP",
        fare_extracted: "£50",
        currency: "GBP",
        final_fare: "£50"
      })
    },
    openaiNormalizer: {
      normalizeWithOpenAI: async () => ({
        pickup: "AI PICKUP",
        drop_off: "AI DROP",
        fare_extracted: "",
        special_notes: "normalized"
      })
    },
    geocoder: {
      geocodeAddress: async (address) => {
        geocodeCalls.push(address);
        if (address === "AI PICKUP") return { lat: 51.47, lng: -0.45 };
        if (address === "AI DROP") return { lat: 51.6, lng: -0.3 };
        return null;
      }
    },
    osrmClient: {
      getRouteFromOSRM: async (origin, destination) => {
        osrmCalls.push({ origin, destination });
        return {
          distance_meters: 10000,
          duration_seconds: 1200,
          distance_text: "10.0 km",
          duration_text: "20m"
        };
      }
    },
    appendRideRow: async (ride) => {
      appendRideCalls.push(ride);
      return { updatedRange: "Rides!A2", updatedRows: 1 };
    },
    appendReviewRow: async (ride) => {
      appendReviewCalls.push(ride);
      return { updatedRange: "'Needs Review'!A2", updatedRows: 1 };
    }
  });

  await handler(createAllowedGroupMessage("sample"));

  assert.deepEqual(geocodeCalls, ["AI PICKUP", "AI DROP"]);
  assert.equal(osrmCalls.length, 1);
  assert.equal(appendRideCalls.length, 1);
  assert.equal(appendReviewCalls.length, 0);
  assert.equal(appendRideCalls[0].distance, "10.0 km");
  assert.equal(appendRideCalls[0].calculated_fare, "");
  assert.equal(appendRideCalls[0].final_fare, "£50");
  assert.equal(appendRideCalls[0].pickup, "AI PICKUP");
  assert.equal(appendRideCalls[0].drop_off, "AI DROP");
  assert.equal(appendRideCalls[0].status, "ready");
  assert.equal(appendRideCalls[0].parser_confidence, "0.95");

  const geocodeSuccessLogs = entries.filter(
    (entry) => entry.message === "Geocode completed" && entry.level === "info"
  );
  const osrmSuccessLogs = entries.filter(
    (entry) => entry.message === "OSRM route completed" && entry.level === "info"
  );

  assert.equal(geocodeSuccessLogs.length, 2);
  assert.equal(osrmSuccessLogs.length, 1);
});

test("enrich pipeline computes calculated_fare only when extracted fare is missing and skips OSRM without both coordinates", async () => {
  const { logger, entries } = createRecordingLogger();
  const appendRideCalls = [];
  const appendReviewCalls = [];
  const osrmCalls = [];
  const allowedGroup = "120363408968321565@g.us";

  const handler = createMessageHandler({
    env: {
      allowedGroups: [allowedGroup],
      fareBase: 250,
      farePerKm: 95,
      defaultCurrency: "PKR"
    },
    logger,
    dedupe: new DedupeStore({ logger: createSilentLogger() }),
    localExtractor: {
      extract: () => ({
        pickup: "PICKUP A",
        drop_off: "DROP B",
        fare_extracted: ""
      })
    },
    openaiNormalizer: {
      normalizeWithOpenAI: async ({ extracted }) => extracted
    },
    geocoder: {
      geocodeAddress: async (address) => {
        if (address === "PICKUP A") return { lat: 51.47, lng: -0.45 };
        return null;
      }
    },
    osrmClient: {
      getRouteFromOSRM: async (origin, destination) => {
        osrmCalls.push({ origin, destination });
        return null;
      }
    },
    appendRideRow: async (ride) => {
      appendRideCalls.push(ride);
      return { updatedRange: "Rides!A2", updatedRows: 1 };
    },
    appendReviewRow: async (ride) => {
      appendReviewCalls.push(ride);
      return { updatedRange: "'Needs Review'!A2", updatedRows: 1 };
    }
  });

  await handler(createAllowedGroupMessage("sample-2"));

  assert.equal(osrmCalls.length, 0);
  assert.equal(appendRideCalls.length, 1);
  assert.equal(appendReviewCalls.length, 0);
  assert.equal(appendRideCalls[0].distance, "");
  assert.equal(appendRideCalls[0].fare_extracted, "");
  assert.match(appendRideCalls[0].calculated_fare, /^PKR /);
  assert.equal(appendRideCalls[0].final_fare, appendRideCalls[0].calculated_fare);
  assert.equal(appendRideCalls[0].status, "ready");
  assert.equal(appendRideCalls[0].parser_confidence, "0.70");

  const geocodeFailureLogs = entries.filter(
    (entry) => entry.message === "Geocode returned no coordinates" && entry.level === "warn"
  );
  const osrmSkipLogs = entries.filter(
    (entry) => entry.message === "OSRM skipped" && entry.level === "info"
  );

  assert.equal(geocodeFailureLogs.length, 1);
  assert.equal(osrmSkipLogs.length, 1);
});

test("review routing sends rides with missing drop_off to Needs Review", async () => {
  const { logger } = createRecordingLogger();
  const appendRideCalls = [];
  const appendReviewCalls = [];
  const allowedGroup = "120363408968321565@g.us";

  const handler = createMessageHandler({
    env: {
      allowedGroups: [allowedGroup],
      fareBase: 250,
      farePerKm: 95,
      defaultCurrency: "PKR"
    },
    logger,
    dedupe: new DedupeStore({ logger: createSilentLogger() }),
    localExtractor: {
      extract: () => ({
        pickup: "PICKUP ONLY",
        drop_off: ""
      })
    },
    openaiNormalizer: {
      normalizeWithOpenAI: async ({ extracted }) => extracted
    },
    geocoder: {
      geocodeAddress: async () => ({ lat: 51.47, lng: -0.45 })
    },
    osrmClient: {
      getRouteFromOSRM: async () => null
    },
    appendRideRow: async (ride) => {
      appendRideCalls.push(ride);
      return { updatedRange: "Rides!A2", updatedRows: 1 };
    },
    appendReviewRow: async (ride) => {
      appendReviewCalls.push(ride);
      return { updatedRange: "'Needs Review'!A2", updatedRows: 1 };
    }
  });

  await handler(createAllowedGroupMessage("sample-3"));

  assert.equal(appendRideCalls.length, 0);
  assert.equal(appendReviewCalls.length, 1);
  assert.equal(appendReviewCalls[0].status, "needs_review");
  assert.equal(appendReviewCalls[0].parser_confidence, "0.25");
  assert.equal(appendReviewCalls[0].raw_message, "sample-3");
  assert.equal(appendReviewCalls[0].pickup, "");
  assert.equal(appendReviewCalls[0].drop_off, "");
});

test("review routing sends rides with both geocodes failed to Needs Review", async () => {
  const { logger } = createRecordingLogger();
  const appendRideCalls = [];
  const appendReviewCalls = [];
  const allowedGroup = "120363408968321565@g.us";

  const handler = createMessageHandler({
    env: {
      allowedGroups: [allowedGroup],
      fareBase: 250,
      farePerKm: 95,
      defaultCurrency: "PKR"
    },
    logger,
    dedupe: new DedupeStore({ logger: createSilentLogger() }),
    localExtractor: {
      extract: () => ({
        pickup: "UNKNOWN PICKUP",
        drop_off: "UNKNOWN DROP"
      })
    },
    openaiNormalizer: {
      normalizeWithOpenAI: async ({ extracted }) => extracted
    },
    geocoder: {
      geocodeAddress: async () => null
    },
    osrmClient: {
      getRouteFromOSRM: async () => null
    },
    appendRideRow: async (ride) => {
      appendRideCalls.push(ride);
      return { updatedRange: "Rides!A2", updatedRows: 1 };
    },
    appendReviewRow: async (ride) => {
      appendReviewCalls.push(ride);
      return { updatedRange: "'Needs Review'!A2", updatedRows: 1 };
    }
  });

  await handler(createAllowedGroupMessage("sample-4"));

  assert.equal(appendRideCalls.length, 0);
  assert.equal(appendReviewCalls.length, 1);
  assert.equal(appendReviewCalls[0].status, "needs_review");
  assert.equal(appendReviewCalls[0].parser_confidence, "0.40");
  assert.equal(appendReviewCalls[0].raw_message, "sample-4");
  assert.equal(appendReviewCalls[0].pickup, "");
  assert.equal(appendReviewCalls[0].drop_off, "");
});
