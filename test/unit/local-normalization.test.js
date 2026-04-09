const test = require("node:test");
const assert = require("node:assert/strict");
const { createLocalExtractor } = require("../../src/extraction/localExtractor");
const { createOpenAiNormalizer } = require("../../src/extraction/openaiNormalizer");
const { createSilentLogger } = require("../helpers");

const SAMPLE_MESSAGE = `Saloon Car (1 Persons)

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

test("local extractor fills deterministic fields and keeps blanks", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    source_group: "120363408968321565@g.us",
    message_id: "ABCD1234",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.required_vehicle, "Saloon Car");
  assert.equal(extracted.day_date, "Tuesday 7th October 2025");
  assert.equal(extracted.starting, "20:05 pm");
  assert.equal(extracted.fare, "\u00A350");
  assert.equal(extracted.distance, "");
  assert.equal(extracted.expires, "");
  assert.equal(extracted.expires_utc, "");
  assert.ok(extracted.refer.startsWith("RID-"));
});

test("OpenAI normalizer falls back to local data when API key is missing", async () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });
  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    source_group: "group@g.us",
    message_id: "MSG123"
  });

  const normalizer = createOpenAiNormalizer({
    apiKey: "",
    logger
  });

  const normalized = await normalizer.normalizeWithOpenAI({
    rawMessage: SAMPLE_MESSAGE,
    extracted
  });

  assert.equal(normalized.pickup, extracted.pickup);
  assert.equal(normalized.drop_off, extracted.drop_off);
  assert.equal(normalized.fare, extracted.fare);
  assert.equal(normalized.distance, extracted.distance);
});
