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
const ACE_SAMPLE_MESSAGE = `ACE 80807-1 (Ai) - 1 Pax

Saloon Car (1 Persons)

Friday 13th March 2026, 6:25 am

Route
Pick Up: Heathrow Airport, Terminal 5
Drop Off: 10 Oakland Villas, Hay-on-Wye, Hereford, HR3 5PH

Myriam Adyadou
+18482645412

Flight
BA184

Arriving From
New York

\u2022 Meet & Greet

\u00A3145`;

const SHORT_CARD_CASES = [
  {
    name: "basic tomorrow card",
    message: SHORT_CARD_FIXTURE,
    expected: {
      required_vehicle: "Estate",
      pickup_day_date: "Thursday 12th March 2026",
      starting_timing: "10:25 pm",
      pickup: "STN",
      drop_off: "NW11",
      fare: "65",
      payment_status: ""
    }
  },
  {
    name: "today card with cash payment",
    message: `SALOON
TODAY 09:15
LHR TO SW1
FARE \u00A345 CASH`,
    expected: {
      required_vehicle: "Saloon",
      pickup_day_date: "Wednesday 11th March 2026",
      starting_timing: "9:15 am",
      pickup: "LHR",
      drop_off: "SW1",
      fare: "45",
      payment_status: "Cash"
    }
  },
  {
    name: "tonight card with account payment",
    message: `MPV
TONIGHT 23:40
W2 TO UB3
ACCOUNT JOB`,
    expected: {
      required_vehicle: "MPV",
      pickup_day_date: "Wednesday 11th March 2026",
      starting_timing: "11:40 pm",
      pickup: "W2",
      drop_off: "UB3",
      payment_status: "Account"
    }
  },
  {
    name: "route arrow variant is supported",
    message: `EXEC
TOMORROW 18:00
LTN -> SE10`,
    expected: {
      required_vehicle: "Executive",
      pickup_day_date: "Thursday 12th March 2026",
      starting_timing: "6:00 pm",
      pickup: "LTN",
      drop_off: "SE10"
    }
  },
  {
    name: "mixed case route works",
    message: `Estate
Tomorrow 22:25
Stn to Nw11`,
    expected: {
      required_vehicle: "Estate",
      pickup_day_date: "Thursday 12th March 2026",
      starting_timing: "10:25 pm",
      pickup: "Stn",
      drop_off: "Nw11"
    }
  },
  {
    name: "now becomes asap day marker",
    message: `VAN
NOW 14:05
EC1 TO N3`,
    expected: {
      pickup_day_date: "Wednesday 11th March 2026",
      starting_timing: "2:05 pm",
      pickup: "EC1",
      drop_off: "N3"
    }
  }
];

const SCREENSHOT_FAILURE_CASES = [
  {
    name: "location pair stays out of required vehicle",
    message: `22, Clifton Park Road, Caversham
Heathrow Airport, Terminal 3
75
Today @16:30`,
    expected: {
      pickup_day_date: "Wednesday 11th March 2026",
      starting_timing: "4:30 pm",
      pickup: "22, Clifton Park Road, Caversham",
      drop_off: "Heathrow Airport, Terminal 3",
      fare: "75",
      required_vehicle: ""
    }
  },
  {
    name: "payment lines do not become pickup or drop off",
    message: `60
Same-day Payment`,
    expected: {
      pickup: "",
      drop_off: "",
      fare: "60",
      payment_status: "Same Day Payment",
      required_vehicle: ""
    }
  },
  {
    name: "flight and mixed postcode line do not become route or vehicle",
    message: `Flight number RK 1393 STN
WIJ 7RJ Mpv £90`,
    expected: {
      pickup: "",
      drop_off: "",
      required_vehicle: "",
      payment_status: ""
    }
  },
  {
    name: "time and fare pollution stays out of route fields",
    message: `Time: 13:05 Heathrow
SW1V, £42 net`,
    expected: {
      pickup: "",
      drop_off: ""
    }
  }
];

const ROUTE_SPLIT_CASES = [
  {
    name: "from X to Y route line splits correctly",
    message: `Saloon
Tuesday 7th October 2025, 20:05 pm
From Heathrow Airport Terminal 2 to 12 Woodlands Close, Dibden Purlieu, Southampton`,
    expected: {
      pickup: "Heathrow Airport Terminal 2",
      drop_off: "12 Woodlands Close, Dibden Purlieu, Southampton"
    }
  },
  {
    name: "pickup and drop labels in one line split correctly",
    message: `Saloon
Tuesday 7th October 2025, 20:05 pm
Pickup: Heathrow Airport Terminal 2 Drop: 12 Woodlands Close, Dibden Purlieu, Southampton`,
    expected: {
      pickup: "Heathrow Airport Terminal 2",
      drop_off: "12 Woodlands Close, Dibden Purlieu, Southampton"
    }
  },
  {
    name: "dash route splits correctly",
    message: `Saloon
Tuesday 7th October 2025, 20:05 pm
Heathrow Airport Terminal 2 - 12 Woodlands Close, Dibden Purlieu, Southampton`,
    expected: {
      pickup: "Heathrow Airport Terminal 2",
      drop_off: "12 Woodlands Close, Dibden Purlieu, Southampton"
    }
  },
  {
    name: "merged pickup line with address boundary is separated",
    message: `Saloon
Tuesday 7th October 2025, 20:05 pm
Pick Up: Heathrow Airport Terminal 2, 12 Woodlands Close, Dibden Purlieu, Southampton`,
    expected: {
      pickup: "Heathrow Airport Terminal 2",
      drop_off: "12 Woodlands Close, Dibden Purlieu, Southampton"
    }
  }
];

const FARE_AND_PAYMENT_CASES = [
  {
    name: "price net does not become payment status",
    message: `Sunday 15/03/2026 @ 16:15 PM
Pickup: T4 Heathrow to DA16 1QW
8 Seater
Price: \u00A375 net`,
    expected: {
      pickup: "T4 Heathrow",
      drop_off: "DA16 1QW",
      required_vehicle: "8 Seater",
      fare: "75",
      payment_status: ""
    }
  },
  {
    name: "vehicle line keeps MPV-8 and same day payment",
    message: `14-March-2026 Tomorrow
At 07:15 from SG10 6DF to LTN
Vehicle: MPV-8 (Same Day Payment)
\u00A370 NET AMOUNT`,
    expected: {
      pickup: "SG10 6DF",
      drop_off: "LTN",
      required_vehicle: "MPV-8",
      fare: "70",
      payment_status: "Same Day Payment"
    }
  },
  {
    name: "net fare line keeps same day payment separate",
    message: `14 March @ 13:35
Pickup: Stansted Airport
Drop off: W2 4AD
Net Fare: 60\u00A3 SAME DAY PAYMENT
Any estate car`,
    expected: {
      pickup: "Stansted Airport",
      drop_off: "W2 4AD",
      required_vehicle: "Any estate car",
      fare: "60",
      payment_status: "Same Day Payment"
    }
  },
  {
    name: "inline route amount extracts fare and payment status",
    message: `Tomorrow @ 12:00pm LHR to NN3 9EG = 75
(Same day payment)`,
    expected: {
      pickup: "LHR",
      drop_off: "NN3 9EG",
      fare: "75",
      payment_status: "Same Day Payment"
    }
  },
  {
    name: "bare net line normalizes GBP fare and vehicle casing",
    message: `Tomorrow 8 am
Heathrow to EX32 0BQ
saloon
200 net`,
    expected: {
      pickup: "Heathrow",
      drop_off: "EX32 0BQ",
      required_vehicle: "Saloon",
      fare: "200",
      payment_status: ""
    }
  }
];

test("local extractor fills strict deterministic fields and keeps blanks", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    group_name: "Dispatch Group",
    source_name: "447700900123",
    message_id: "ABCD1234",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.group_name, "Dispatch Group");
  assert.equal(extracted.source_name, "447700900123");
  assert.equal(extracted.required_vehicle, "Saloon Car");
  assert.equal(extracted.pickup_day_date, "Tuesday 7th October 2025");
  assert.equal(extracted.starting_timing, "8:05 pm");
  assert.equal(extracted.pickup, "Heathrow Airport, Terminal 4");
  assert.equal(
    extracted.drop_off,
    "12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG"
  );
  assert.equal(extracted.distance, "");
  assert.equal(extracted.fare, "50");
  assert.equal(extracted.payment_status, "");
  assert.ok(extracted.refer.startsWith("RID-"));
});

test("OpenAI normalizer falls back to local data when API key is missing", async () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });
  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    group_name: "Dispatch Group",
    source_name: "447700900123",
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
  assert.equal(normalized.required_vehicle, extracted.required_vehicle);
  assert.equal(normalized.distance, "");
  assert.equal(normalized.fare, "50");
});

test("OpenAI prompt includes raw message, deterministic extraction, and canonical target", () => {
  const prompt = buildNormalizationPrompt({
    raw_message: "ESTATE\nTOMORROW 22:25\nSTN TO NW11",
    deterministic_extraction: {
      required_vehicle: "ESTATE",
      pickup: "STN"
    },
    canonical_schema_target: CANONICAL_SCHEMA_TARGET
  });

  assert.match(prompt, /raw_message:/);
  assert.match(prompt, /deterministic_extraction:/);
  assert.match(prompt, /canonical_schema_target:/);
  assert.match(prompt, /payment_status/);
});

test("OpenAI JSON parser can recover the first JSON object from noisy text", () => {
  const extractedJson = extractFirstJsonObject(
    'Here you go\n{"pickup":"STN","drop_off":"NW11"}\nthanks'
  );
  assert.equal(extractedJson, '{"pickup":"STN","drop_off":"NW11"}');

  const parsed = parseModelJson('```json\n{"pickup":"STN","drop_off":"NW11"}\n```');
  assert.equal(parsed.pickup, "STN");
  assert.equal(parsed.drop_off, "NW11");
});

test("OpenAI validator enforces strict schema and preserves protected distance/fare fields", () => {
  const validated = validateNormalizedOutput(
    {
      pickup: " STN ",
      drop_off: "NW11",
      distance: "100 km",
      fare: "PKR 1000.00"
    },
    {
      deterministicExtraction: {
        distance: "",
        fare: "",
        source_name: "447700900123"
      },
      canonicalSchemaTarget: CANONICAL_SCHEMA_TARGET
    }
  );

  assert.equal(validated.pickup, "STN");
  assert.equal(validated.drop_off, "NW11");
  assert.equal(validated.distance, "");
  assert.equal(validated.fare, "");
  assert.equal(validated.source_name, "447700900123");
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
    required_vehicle: "ESTATE",
    source_name: "447700900123"
  };

  const normalized = await normalizer.normalizeWithOpenAI({
    raw_message: "ESTATE\nTOMORROW 22:25\nSTN TO NW11",
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
  assert.equal(normalized.required_vehicle, "ESTATE");
  assert.equal(normalized.fare, "");
});

test("short ride card parser supports compact whatsapp card variations", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  for (const cardCase of SHORT_CARD_CASES) {
    const extracted = extractor.extract(cardCase.message, {
      group_name: "compact-cards",
      source_name: "447700900123",
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
    assert.equal(extracted.distance, "", `${cardCase.name}: distance should remain blank`);
  }
});

test("short ride card parser does not break labeled parser", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(SAMPLE_MESSAGE, {
    group_name: "labeled",
    source_name: "447700900123",
    message_id: "LABELED-1",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.pickup, "Heathrow Airport, Terminal 4");
  assert.equal(
    extracted.drop_off,
    "12, Woodlands Close, Dibden Purlieu, Southampton, SO45 4JG"
  );
  assert.equal(extracted.required_vehicle, "Saloon Car");
});

test("fare extraction keeps payment status separate across business examples", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  for (const exampleCase of FARE_AND_PAYMENT_CASES) {
    const extracted = extractor.extract(exampleCase.message, {
      group_name: "payment-status-tests",
      source_name: "447700900123",
      message_id: `PAY-${exampleCase.name}`,
      received_at: "2026-03-11T10:15:00.000Z"
    });

    for (const [field, expectedValue] of Object.entries(exampleCase.expected)) {
      assert.equal(
        extracted[field],
        expectedValue,
        `${exampleCase.name}: expected ${field}=${expectedValue} but received ${extracted[field]}`
      );
    }
  }
});

test("route splitting keeps pickup and drop off separated for merged route text", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  for (const routeCase of ROUTE_SPLIT_CASES) {
    const extracted = extractor.extract(routeCase.message, {
      group_name: "route-split",
      source_name: "447700900123",
      message_id: `ROUTE-${routeCase.name}`,
      received_at: "2026-03-11T10:15:00.000Z"
    });

    assert.equal(
      extracted.pickup,
      routeCase.expected.pickup,
      `${routeCase.name}: pickup should be origin only`
    );
    assert.equal(
      extracted.drop_off,
      routeCase.expected.drop_off,
      `${routeCase.name}: drop_off should be destination only`
    );
  }
});

test("smart parser keeps screenshot-style noisy lines out of the wrong columns", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  for (const exampleCase of SCREENSHOT_FAILURE_CASES) {
    const extracted = extractor.extract(exampleCase.message, {
      group_name: "screenshot-cases",
      source_name: "447700900123",
      message_id: `SHOT-${exampleCase.name}`,
      received_at: "2026-03-11T10:15:00.000Z"
    });

    for (const [field, expectedValue] of Object.entries(exampleCase.expected)) {
      assert.equal(
        extracted[field],
        expectedValue,
        `${exampleCase.name}: expected ${field}=${expectedValue} but received ${extracted[field]}`
      );
    }
  }
});

test("local extractor captures standalone fare line and known vehicle from ACE ride message", () => {
  const logger = createSilentLogger();
  const extractor = createLocalExtractor({ logger });

  const extracted = extractor.extract(ACE_SAMPLE_MESSAGE, {
    group_name: "Testing",
    source_name: "Hafiz Ashari",
    message_id: "ACE-1",
    received_at: "2026-03-11T10:15:00.000Z"
  });

  assert.equal(extracted.pickup_day_date, "Friday 13th March 2026");
  assert.equal(extracted.starting_timing, "6:25 am");
  assert.equal(extracted.pickup, "Heathrow Airport, Terminal 5");
  assert.equal(extracted.drop_off, "10 Oakland Villas, Hay-on-Wye, Hereford, HR3 5PH");
  assert.equal(extracted.required_vehicle, "Saloon Car");
  assert.equal(extracted.fare, "145");
  assert.equal(extracted.payment_status, "");
});
