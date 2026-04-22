const {
  NORMALIZATION_FIELDS,
  createEmptyNormalizationObject,
  pickNormalizationFields
} = require("./schemas");
const { executeWithRetry } = require("../utils/retry");
const { summarizeKnownError } = require("../utils/logger");

const PROTECTED_FIELDS = new Set([
  "distance",
  "fare_extracted",
  "currency",
  "fare_type",
  "calculated_fare",
  "final_fare"
]);
const CANONICAL_SCHEMA_TARGET = Object.freeze([...NORMALIZATION_FIELDS]);

const NORMALIZATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    refer: { type: "string" },
    day_label: { type: "string" },
    pickup_date: { type: "string" },
    pickup_time: { type: "string" },
    pickup_datetime: { type: "string" },
    asap: { type: "string" },
    pickup: { type: "string" },
    via_1: { type: "string" },
    via_2: { type: "string" },
    via_3: { type: "string" },
    drop_off: { type: "string" },
    route_summary: { type: "string" },
    distance: { type: "string" },
    fare_extracted: { type: "string" },
    currency: { type: "string" },
    fare_type: { type: "string" },
    calculated_fare: { type: "string" },
    final_fare: { type: "string" },
    required_vehicle: { type: "string" },
    seat_count: { type: "string" },
    child_seat: { type: "string" },
    wait_and_return: { type: "string" },
    passenger_count: { type: "string" },
    pet_dog: { type: "string" },
    payment_status: { type: "string" },
    special_notes: { type: "string" },
    expiry: { type: "string" },
    expiry_utc: { type: "string" },
    head_passenger: { type: "string" },
    mobile_number: { type: "string" },
    flight_number: { type: "string" },
    arriving_from: { type: "string" }
  },
  required: [...NORMALIZATION_FIELDS]
};

const SYSTEM_INSTRUCTION = [
  "You normalize WhatsApp ride-booking text into a canonical ride JSON object.",
  "Return JSON only.",
  "The output must match the canonical schema target exactly.",
  "Every schema key must be present.",
  "Every value must be a string.",
  "Use empty string for any missing or unknown value.",
  "Treat deterministic extraction as the source of truth when available.",
  "Do not invent or calculate route distance.",
  "Do not invent or calculate fare_extracted, calculated_fare, or final_fare.",
  "If fare_extracted is present in deterministic extraction, preserve it.",
  "Preserve pickup, via, and drop_off text faithfully.",
  "Put leftover useful ride text that does not fit another field into special_notes.",
  "Do not look up maps, distances, routes, or external data.",
  "Only clean formatting, whitespace, splitting, and obvious field assignment."
].join(" ");

function safeString(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function isRetryableOpenAiError(error) {
  const status = Number(error?.status || error?.response?.status);
  const code = String(error?.code || "");

  if ([408, 409, 425, 429, 500, 502, 503, 504].includes(status)) return true;
  if (
    ["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ENOTFOUND", "ECONNABORTED"].includes(code)
  ) {
    return true;
  }
  return false;
}

function extractResponseText(response) {
  if (!response) return "";

  if (typeof response.output_text === "string" && response.output_text.trim()) {
    return response.output_text;
  }

  const outputs = Array.isArray(response.output) ? response.output : [];
  const textParts = [];

  for (const outputItem of outputs) {
    const contentItems = Array.isArray(outputItem.content) ? outputItem.content : [];
    for (const contentItem of contentItems) {
      if (typeof contentItem.text === "string" && contentItem.text.trim()) {
        textParts.push(contentItem.text);
      }
    }
  }

  return textParts.join("\n").trim();
}

function normalizeObjectShape(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return createEmptyNormalizationObject();
  }

  return createEmptyNormalizationObject(value);
}

function extractFirstJsonObject(value) {
  const text = safeString(value);
  if (!text) return "";

  const startIndex = text.indexOf("{");
  if (startIndex < 0) return "";

  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = startIndex; index < text.length; index += 1) {
    const char = text[index];

    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (char === "\\") {
        escaped = true;
        continue;
      }
      if (char === '"') {
        inString = false;
      }
      continue;
    }

    if (char === '"') {
      inString = true;
      continue;
    }

    if (char === "{") {
      depth += 1;
      continue;
    }

    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return text.slice(startIndex, index + 1);
      }
    }
  }

  return "";
}

function parseModelJson(responseText) {
  const text = safeString(responseText);
  if (!text) {
    const error = new Error("Model response was empty");
    error.code = "OPENAI_EMPTY_RESPONSE";
    throw error;
  }

  try {
    return JSON.parse(text);
  } catch (firstError) {
    const extractedJson = extractFirstJsonObject(text);
    if (extractedJson) {
      try {
        return JSON.parse(extractedJson);
      } catch (secondError) {
        // Fall through to structured parse error below.
      }
    }

    const error = new Error("Model output was not valid JSON");
    error.code = "OPENAI_INVALID_JSON";
    error.responseText = text;
    throw error;
  }
}

function validateNormalizedOutput(value, options = {}) {
  const canonicalSchemaTarget = Array.isArray(options.canonicalSchemaTarget)
    ? options.canonicalSchemaTarget
    : CANONICAL_SCHEMA_TARGET;
  const deterministicExtraction = createEmptyNormalizationObject(
    options.deterministicExtraction || {}
  );

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    const error = new Error("Normalized output must be a JSON object");
    error.code = "OPENAI_INVALID_SCHEMA";
    throw error;
  }

  const candidate = {};
  for (const key of canonicalSchemaTarget) {
    const rawValue = Object.prototype.hasOwnProperty.call(value, key) ? value[key] : "";
    candidate[key] =
      typeof rawValue === "string"
        ? rawValue.trim()
        : rawValue === null || rawValue === undefined
          ? ""
          : String(rawValue).trim();
  }

  for (const [key, rawValue] of Object.entries(value)) {
    if (!canonicalSchemaTarget.includes(key) && rawValue) {
      const error = new Error(`Unexpected key in normalized output: ${key}`);
      error.code = "OPENAI_INVALID_SCHEMA";
      throw error;
    }
  }

  if (!candidate.special_notes) {
    candidate.special_notes = deterministicExtraction.special_notes || "";
  }

  for (const field of PROTECTED_FIELDS) {
    if (safeString(deterministicExtraction[field])) {
      candidate[field] = safeString(deterministicExtraction[field]);
    } else if (!candidate[field]) {
      candidate[field] = "";
    }
  }

  return createEmptyNormalizationObject(candidate);
}

function mergeNormalized({ local, fromAi }) {
  const merged = createEmptyNormalizationObject();

  for (const key of NORMALIZATION_FIELDS) {
    const localValue = safeString(local[key]);
    const aiValue = safeString(fromAi[key]);

    if (PROTECTED_FIELDS.has(key)) {
      merged[key] = localValue;
      continue;
    }

    merged[key] = aiValue || localValue || "";
  }

  return merged;
}

function buildNormalizationPrompt({
  raw_message,
  deterministic_extraction,
  canonical_schema_target
}) {
  return [
    "Normalize the deterministic extraction into the canonical schema target.",
    "Return schema-matching JSON only.",
    "If the raw message contains useful leftover details that do not belong elsewhere, place them in special_notes.",
    `canonical_schema_target:\n${JSON.stringify(canonical_schema_target)}`,
    `deterministic_extraction:\n${JSON.stringify(deterministic_extraction)}`,
    `raw_message:\n${raw_message}`
  ].join("\n\n");
}

async function requestStructuredNormalization({
  client,
  model,
  raw_message,
  deterministic_extraction,
  canonical_schema_target
}) {
  const prompt = buildNormalizationPrompt({
    raw_message,
    deterministic_extraction,
    canonical_schema_target
  });

  if (client?.responses?.create) {
    const response = await client.responses.create({
      model,
      input: [
        { role: "system", content: SYSTEM_INSTRUCTION },
        {
          role: "user",
          content: prompt
        }
      ],
      text: {
        format: {
          type: "json_schema",
          name: "ride_normalized_output",
          strict: true,
          schema: NORMALIZATION_SCHEMA
        }
      }
    });

    return extractResponseText(response);
  }

  if (client?.chat?.completions?.create) {
    const response = await client.chat.completions.create({
      model,
      messages: [
        { role: "system", content: SYSTEM_INSTRUCTION },
        {
          role: "user",
          content: prompt
        }
      ],
      response_format: {
        type: "json_schema",
        json_schema: {
          name: "ride_normalized_output",
          strict: true,
          schema: NORMALIZATION_SCHEMA
        }
      }
    });

    return safeString(response?.choices?.[0]?.message?.content);
  }

  throw new Error("OpenAI SDK does not expose a supported structured output API");
}

function createOpenAiNormalizer(options = {}) {
  const { apiKey, model = "gpt-4.1-mini", logger, client: providedClient } = options;
  const safeLogger = logger || {
    debug: () => {},
    info: () => {},
    warn: () => {},
    error: () => {}
  };
  let client = providedClient || null;
  if (!client && apiKey) {
    try {
      // Lazy load so app can still boot in environments where deps are not installed yet.
      // eslint-disable-next-line global-require
      const OpenAI = require("openai");
      client = new OpenAI({ apiKey });
    } catch (error) {
      safeLogger.warn("OpenAI SDK unavailable, using local data", {
        stage: "openai_normalization",
        fallbackUsed: true,
        reason: "openai package not installed"
      });
    }
  }

  async function normalizeWithOpenAI(input = {}) {
    const raw_message = safeString(input.raw_message ?? input.rawMessage);
    const deterministic_extraction = pickNormalizationFields(
      input.deterministic_extraction ?? input.deterministicExtraction ?? input.extracted ?? {}
    );
    const canonical_schema_target = Array.isArray(input.canonical_schema_target)
      ? [...input.canonical_schema_target]
      : Array.isArray(input.canonicalSchemaTarget)
        ? [...input.canonicalSchemaTarget]
        : [...CANONICAL_SCHEMA_TARGET];
    const localNormalized = createEmptyNormalizationObject(deterministic_extraction);
    const safeFallback = createEmptyNormalizationObject(localNormalized);

    const activeClient = input.client || client;

    if (!activeClient) {
      safeLogger.info("OpenAI normalization skipped, using local data", {
        stage: "openai_normalization",
        fallbackUsed: true,
        reason: "API key or SDK unavailable"
      });
      return safeFallback;
    }

    try {
      const responseText = await executeWithRetry(
        async () =>
          requestStructuredNormalization({
            client: activeClient,
            model,
            raw_message,
            deterministic_extraction: localNormalized,
            canonical_schema_target
          }),
        {
          maxAttempts: 3,
          initialDelayMs: 400,
          maxDelayMs: 3000,
          shouldRetry: isRetryableOpenAiError,
          onRetry: ({ attempt, maxAttempts, delayMs, error }) => {
            const summary = summarizeKnownError(error, {
              stage: "openai_normalization",
              defaultSummary: "OpenAI request failed, retrying",
              fallbackUsed: true
            });

            safeLogger.warn(summary.summary, {
              stage: "openai_normalization",
              fallbackUsed: true,
              attempt,
              maxAttempts,
              delayMs,
              status: summary.status,
              code: summary.code,
              reason: summary.likelyCause || "Temporary API/network issue",
              error
            });
          }
        }
      );

      const parsed = parseModelJson(responseText);
      const aiObject = validateNormalizedOutput(parsed, {
        deterministicExtraction: localNormalized,
        canonicalSchemaTarget: canonical_schema_target
      });
      const merged = mergeNormalized({
        local: localNormalized,
        fromAi: normalizeObjectShape(aiObject)
      });

      safeLogger.info("OpenAI normalization completed", {
        stage: "openai_normalization",
        fallbackUsed: false,
        model,
        reason: `filledFields=${NORMALIZATION_FIELDS.filter((key) => Boolean(merged[key])).length}`
      });

      return merged;
    } catch (error) {
      const summary = summarizeKnownError(error, {
        stage: "openai_normalization",
        defaultSummary: "OpenAI normalization failed, using local data",
        fallbackUsed: true
      });

      safeLogger.warn(summary.summary, {
        stage: "openai_normalization",
        fallbackUsed: true,
        model,
        status: summary.status,
        code: summary.code,
        reason: summary.likelyCause || "Request failed",
        error
      });
      return safeFallback;
    }
  }

  // Backward-compatible method name used by earlier scaffold code.
  async function normalize(localRecord, rawText) {
    return normalizeWithOpenAI({
      rawMessage: rawText,
      extracted: localRecord
    });
  }

  return {
    normalizeWithOpenAI,
    normalize,
    normalizationSchema: NORMALIZATION_SCHEMA,
    systemInstruction: SYSTEM_INSTRUCTION,
    canonicalSchemaTarget: CANONICAL_SCHEMA_TARGET
  };
}

module.exports = {
  createOpenAiNormalizer,
  CANONICAL_SCHEMA_TARGET,
  NORMALIZATION_SCHEMA,
  SYSTEM_INSTRUCTION,
  buildNormalizationPrompt,
  extractFirstJsonObject,
  parseModelJson,
  validateNormalizedOutput
};
