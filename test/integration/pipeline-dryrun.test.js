const test = require("node:test");
const assert = require("node:assert/strict");
const { createMessageHandler } = require("../../src/whatsapp/messageHandler");
const { DedupeStore } = require("../../src/utils/dedupe");
const { createLocalExtractor } = require("../../src/extraction/localExtractor");
const { createSilentLogger } = require("../helpers");

const SAMPLE_MESSAGE = `Saloon Car (1 Persons)

Landing
Tuesday 7th October 2025, 20:05 pm

Route
 - Pick Up: Heathrow Airport, Terminal 4
 - Drop Off: 12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG

\u00A350`;

test("end-to-end message pipeline with mocks maps final row and dedupes", async (t) => {
  if (!process.env.RIDE_BOT_RUN_INTEGRATION) {
    t.skip("Set RIDE_BOT_RUN_INTEGRATION=1 to run integration tests.");
    return;
  }

  const logger = createSilentLogger();
  const appendCalls = [];
  const allowedGroup = "120363408968321565@g.us";

  const handler = createMessageHandler({
    env: {
      allowedGroups: [allowedGroup],
      fareBase: 250,
      farePerKm: 95,
      defaultCurrency: "PKR"
    },
    logger,
    dedupe: new DedupeStore({ logger }),
    localExtractor: createLocalExtractor({ logger }),
    openaiNormalizer: {
      normalizeWithOpenAI: async ({ extracted }) => extracted
    },
    geocoder: {
      geocodeAddress: async () => ({ lat: 51.47, lng: -0.45, formatted_address: "ok" })
    },
    osrmClient: {
      getRouteFromOSRM: async () => ({
        distance_meters: 10000,
        duration_seconds: 1200,
        distance_text: "10.0 km",
        duration_text: "20m"
      })
    },
    appendRow: async (ride) => {
      appendCalls.push(ride);
      return { updatedRange: "Sheet1!A2:J2", updatedRows: 1 };
    }
  });

  const message = {
    body: SAMPLE_MESSAGE,
    fromMe: false,
    type: "chat",
    timestamp: 1760000000,
    from: allowedGroup,
    id: { _serialized: "ABCD1234" },
    getChat: async () => ({
      isGroup: true,
      id: { _serialized: allowedGroup },
      name: "Test Allowed Group"
    })
  };

  await handler(message);
  assert.equal(appendCalls.length, 1);
  assert.equal(appendCalls[0].distance, "10.0 km");
  assert.equal(appendCalls[0].fare, "\u00A350");

  // Same message should be skipped by dedupe after successful append.
  await handler(message);
  assert.equal(appendCalls.length, 1);
});
