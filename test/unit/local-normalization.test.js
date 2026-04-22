const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { createLocalExtractor } = require("../../src/extraction/localExtractor");
const {
  createOpenAiNormalizer,
  CANONICAL_SCHEMA_TARGET,
  buildNormalizationPrompt,
  extractFirstJsonObject,
  parseModelJson,
  validateNormalizedOutput
} = require("../../src/extraction/openaiNormalizer");
const { createSilentLogger } = require("../helpers");

function readFixture(...parts) {
  return fs.readFileSync(path.join(__dirname, "..", "fixtures", ...parts), "utf8");
}

const SAMPLE_MESSAGE = readFixture("labeled", "sample-message.txt");
const SHORT_CARD_FIXTURE = readFixture("short-card", "basic-net.txt");

const SHORT_CARD_CASES = [
  {
    name: "basic tomorrow card with net fare",
    message: SHORT_CARD_FIXTURE,
    expected: {
      required_vehicle: "ESTATE",
      pickup_date: "TOMORROW",
      pickup_time: "22:25",
      pickup_datetime: "TOMORROW, 22:25",
      pickup: "STN",
      drop_off: "NW11",
      fare_extracted: "\u00A365",
      currency: "GBP",
      fare_type: "net",
      special_notes: ""
    }
  },
  {
    name: "today card with cash fare",
    message: `SALOON
TODAY 09:15
LHR TO SW1
FARE \u00A345 CASH`,
    expected: {
      required_vehicle: "SALOON",
      pickup_date: "TODAY",
      pickup_time: "09:15",
      pickup: "LHR",
      drop_off: "SW1",
      fare_extracted: "\u00A345",
      currency: "GBP",
      fare_type: "cash"
    }
  },
  {
    name: "tonight card with quoted fare default",
    message: `MPV
TONIGHT 23:40
W2 TO UB3
FARE \u00A380`,
    expected: {
      required_vehicle: "MPV",
      pickup_date: "TONIGHT",
      pickup_time: "23:40",
      pickup: "W2",
      drop_off: "UB3",
      fare_extracted: "\u00A380",
      currency: "GBP",
      fare_type: "quoted"
    }
  },
  {
    name: "asap card marks asap",
    message: `ESTATE
ASAP 14:05
EC1 TO N3
FARE \u00A355 NET`,
    expected: {
      pickup_date: "",
      day_label: "ASAP",
      asap: "yes",
      pickup_time: "14:05",
      pickup: "EC1",
      drop_off: "N3",
      fare_type: "net"
    }
  },
  {
    name: "route arrow variant is supported",
    message: `EXEC
TOMORROW 18:00
LTN -> SE10
FARE \u00A390 NET`,
    expected: {
      required_vehicle: "EXEC",
      pickup: "LTN",
      drop_off: "SE10",
      fare_extracted: "\u00A390"
    }
  },
  {
    name: "mixed case net fare works",
    message: `Estate
Tomorrow 22:25
Stn to Nw11
Fare \u00A365 Net`,
    expected: {
      required_vehicle: "Estate",
      pickup: "Stn",
      drop_off: "Nw11",
      fare_type: "net"
    }
  },
  {
    name: "unknown extra line is preserved in notes",
    message: `ESTATE
TOMORROW 22:25
STN TO NW11
2 PAX + 2 CASES
FARE \u00A365 NET`,
    expected: {
      special_notes: "2 PAX + 2 CASES"
    }
  },
  {
    name: "multiple unknown lines are preserved in order",
    message: `ESTATE
TOMORROW 22:25
STN TO NW11
MEET & GREET
2 PAX
FARE \u00A365 NET`,
    expected: {
      special_notes: "MEET & GREET\n2 PAX"
    }
  },
  {
    name: "account fare type is parsed",
    message: `VAN
TODAY 07:30
CR4 TO HA8
FARE \u00A370 ACCOUNT`,
    expected: {
      fare_type: "account",
      fare_extracted: "\u00A370"
    }
  },
  {
    name: "usd currency is detected",
    message: `SUV
TOMORROW 12:10
JFK TO MANHATTAN
FARE $120 NET`,
    expected: {
      currency: "USD",
      fare_extracted: "$120",
      fare_type: "net"
    }
  },
  {
    name: "eur currency is detected",
    message: `ESTATE
TOMORROW 16:45
CDG TO PARIS 15
FARE \u20AC75`,
    expected: {
      currency: "EUR",
      fare_extracted: "\u20AC75",
      fare_type: "quoted"
    }
  }
];

test("local extractor fills deterministic fields and keeps blanks", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    source_group: "120363408968321565@g.us",
    message_id: "ABCD1234",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.required_vehicle, "Saloon Car");
  assert.equal(extracted.seat_count, "1");
  assert.equal(extracted.passenger_count, "1");
  assert.equal(extracted.day_label, "Tuesday");
  assert.equal(extracted.pickup_date, "7th October 2025");
  assert.equal(extracted.pickup_time, "20:05 pm");
  assert.equal(extracted.pickup_datetime, "Tuesday 7th October 2025, 20:05 pm");
  assert.equal(extracted.fare_extracted, "\u00A350");
  assert.equal(extracted.final_fare, "\u00A350");
  assert.equal(extracted.currency, "GBP");
  assert.equal(extracted.distance, "");
  assert.equal(extracted.expiry, "");
  assert.equal(extracted.expiry_utc, "");
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
  assert.equal(normalized.fare_extracted, extracted.fare_extracted);
  assert.equal(normalized.final_fare, extracted.final_fare);
  assert.equal(normalized.distance, extracted.distance);
});

test("OpenAI prompt includes raw message, deterministic extraction, and canonical target", () => {
  const prompt = buildNormalizationPrompt({
    raw_message: "ESTATE\nTOMORROW 22:25\nSTN TO NW11\nFARE £65 NET",
    deterministic_extraction: {
      required_vehicle: "ESTATE",
      fare_extracted: "£65"
    },
    canonical_schema_target: CANONICAL_SCHEMA_TARGET
  });

  assert.match(prompt, /raw_message:/);
  assert.match(prompt, /deterministic_extraction:/);
  assert.match(prompt, /canonical_schema_target:/);
  assert.match(prompt, /special_notes/);
});

test("OpenAI JSON parser can recover the first JSON object from noisy text", () => {
  const extractedJson = extractFirstJsonObject('Here you go\n{"pickup":"STN","drop_off":"NW11"}\nthanks');
  assert.equal(extractedJson, '{"pickup":"STN","drop_off":"NW11"}');

  const parsed = parseModelJson('```json\n{"pickup":"STN","drop_off":"NW11"}\n```');
  assert.equal(parsed.pickup, "STN");
  assert.equal(parsed.drop_off, "NW11");
});

test("OpenAI validator enforces canonical schema and preserves protected deterministic fare fields", () => {
  const validated = validateNormalizedOutput(
    {
      pickup: " STN ",
      drop_off: "NW11",
      fare_extracted: "",
      final_fare: "",
      special_notes: ""
    },
    {
      deterministicExtraction: {
        fare_extracted: "£65",
        currency: "GBP",
        fare_type: "net",
        final_fare: "£65",
        special_notes: "2 PAX"
      },
      canonicalSchemaTarget: CANONICAL_SCHEMA_TARGET
    }
  );

  assert.equal(validated.pickup, "STN");
  assert.equal(validated.drop_off, "NW11");
  assert.equal(validated.fare_extracted, "£65");
  assert.equal(validated.currency, "GBP");
  assert.equal(validated.fare_type, "net");
  assert.equal(validated.final_fare, "£65");
  assert.equal(validated.special_notes, "2 PAX");
});

test("OpenAI validator rejects unexpected keys", () => {
  assert.throws(
    () =>
      validateNormalizedOutput(
        {
          pickup: "STN",
          unexpected_field: "boom"
        },
        {
          deterministicExtraction: {},
          canonicalSchemaTarget: CANONICAL_SCHEMA_TARGET
        }
      ),
    /Unexpected key/
  );
});

test("OpenAI normalizer falls back to deterministic extraction when model output is invalid JSON", async () => {
  const logger = createSilentLogger();
  const normalizer = createOpenAiNormalizer({
    apiKey: "test-key",
    logger
  });

  const deterministicExtraction = {
    pickup: "STN",
    drop_off: "NW11",
    fare_extracted: "£65",
    currency: "GBP",
    fare_type: "net",
    final_fare: "£65",
    special_notes: "2 PAX"
  };

  const normalized = await normalizer.normalizeWithOpenAI({
    raw_message: "ESTATE\nTOMORROW 22:25\nSTN TO NW11\nFARE £65 NET",
    deterministicExtraction,
    canonical_schema_target: CANONICAL_SCHEMA_TARGET,
    client: {
      responses: {
        create: async () => ({
          output_text: '{"pickup":"STN"'
        })
      }
    }
  });

  assert.equal(normalized.pickup, "STN");
  assert.equal(normalized.drop_off, "NW11");
  assert.equal(normalized.fare_extracted, "£65");
  assert.equal(normalized.final_fare, "£65");
  assert.equal(normalized.special_notes, "2 PAX");
});

test("short ride card parser supports compact whatsapp card variations", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  for (const cardCase of SHORT_CARD_CASES) {
    const extracted = extractor.extract(cardCase.message, {
      source_group: "compact-cards@g.us",
      message_id: `MSG-${cardCase.name}`,
      received_at: "2026-03-11T10:15:00.000Z"
    });

    for (const [field, expectedValue] of Object.entries(cardCase.expected)) {
      assert.equal(
        extracted[field],
        expectedValue,
        `${cardCase.name}: expected ${field}=${expectedValue} but received ${extracted[field]}`
      );
    }

    assert.ok(extracted.refer.startsWith("RID-"), `${cardCase.name}: refer should exist`);
    assert.equal(extracted.final_fare, extracted.fare_extracted, `${cardCase.name}: final fare`);
  }
});

test("short ride card parser does not break labeled parser", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    source_group: "labeled@g.us",
    message_id: "LABELED-1",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.pickup, "Heathrow Airport, Terminal 4");
  assert.equal(
    extracted.drop_off,
    "12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG"
  );
  assert.match(extracted.special_notes, /Jessica Walker/);
  assert.match(extracted.special_notes, /VY6652/);
});

test("fixture samples stay parseable for labeled and short-card message formats", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const shortCard = extractor.extract(SHORT_CARD_FIXTURE, {
    source_group: "fixture-short@g.us",
    message_id: "FIXTURE-SHORT",
    received_at: "2026-03-11T10:15:00.000Z"
  });
  const labeled = extractor.extract(SAMPLE_MESSAGE, {
    source_group: "fixture-labeled@g.us",
    message_id: "FIXTURE-LABELED",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(shortCard.required_vehicle, "ESTATE");
  assert.equal(shortCard.pickup, "STN");
  assert.equal(shortCard.drop_off, "NW11");
  assert.equal(shortCard.fare_extracted, "\u00A365");

  assert.equal(labeled.required_vehicle, "Saloon Car");
  assert.equal(labeled.pickup, "Heathrow Airport, Terminal 4");
  assert.match(labeled.special_notes, /Jessica Walker/);
});
