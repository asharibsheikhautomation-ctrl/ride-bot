const {
  NORMALIZATION_FIELDS,
  createEmptyNormalizationObject,
  pickNormalizationFields
} = require("./schemas");
const {
  containsLocationContamination,
  normalizeVehicleText
} = require("./localExtractor");
const { executeWithRetry } = require("../utils/retry");
const { summarizeKnownError } = require("../utils/logger");

const PROTECTED_FIELDS = new Set(["distance", "fare", "source_time"]);
const CANONICAL_SCHEMA_TARGET = Object.freeze([...NORMALIZATION_FIELDS]);
const LOCKABLE_FIELDS = new Set([
  "pickup_day_date",
  "starting_timing",
  "pickup",
  "drop_off",
  "required_vehicle",
  "payment_status",
  "fare",
  "source_time"
]);

const NORMALIZATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    refer: { type: "string" },
    group_name: { type: "string" },
    source_name: { type: "string" },
    source_time: { type: "string" },
    pickup_day_date: { type: "string" },
    starting_timing: { type: "string" },
    pickup: { type: "string" },
    drop_off: { type: "string" },
    distance: { type: "string" },
    fare: { type: "string" },
    required_vehicle: { type: "string" },
    payment_status: { type: "string" }
  },
  required: [...NORMALIZATION_FIELDS]
};

const SYSTEM_INSTRUCTION = [
  "You normalize WhatsApp ride-booking text into a strict Google Sheets row schema.",
  "Return JSON only.",
  "The output must match the canonical schema target exactly.",
  "Every schema key must be present.",
  "Every value must be a string.",
  "Use empty string for any missing or unknown value.",
  "Treat deterministic extraction as the source of truth when available.",
  "Do not invent or calculate distance.",
  "Do not invent or calculate fare.",
  "Do not invent source_time. Keep runtime source_time exactly as provided by deterministic extraction.",
  "pickup must contain only the origin.",
  "drop_off must contain only the destination.",
  "Never put fare, payment, flight, reference, or time text into pickup or drop_off.",
  "required_vehicle must be a clean vehicle type only, or empty string.",
  "If the source says from X to Y, X -> Y, X - Y, or pickup X drop Y, split them correctly.",
  "Only clean formatting and place source text into the matching schema fields.",
  "Do not add extra metadata or explanation."
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
  const lockedFields = new Set(
    Array.isArray(options.lockedFields)
      ? options.lockedFields.filter((field) => LOCKABLE_FIELDS.has(field))
      : []
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

  for (const key of canonicalSchemaTarget) {
    if (!candidate[key] && safeString(deterministicExtraction[key])) {
      candidate[key] = safeString(deterministicExtraction[key]);
    }
  }

  for (const field of PROTECTED_FIELDS) {
    candidate[field] = safeString(deterministicExtraction[field]);
  }

  for (const field of lockedFields) {
    if (safeString(deterministicExtraction[field])) {
      candidate[field] = safeString(deterministicExtraction[field]);
    }
  }

  if (
    (candidate.pickup && containsLocationContamination(candidate.pickup)) ||
    (candidate.drop_off && containsLocationContamination(candidate.drop_off))
  ) {
    const error = new Error("Normalized output contaminated location fields");
    error.code = "OPENAI_INVALID_SCHEMA";
    throw error;
  }

  if (candidate.required_vehicle) {
    const normalizedVehicle = normalizeVehicleText(candidate.required_vehicle);
    if (!normalizedVehicle) {
      const error = new Error("Normalized output returned an invalid vehicle value");
      error.code = "OPENAI_INVALID_SCHEMA";
      throw error;
    }
    candidate.required_vehicle = normalizedVehicle;
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
  canonical_schema_target,
  numbered_lines,
  deterministic_candidate_map
}) {
  return [
    "Normalize the deterministic extraction into the canonical schema target.",
    "Return schema-matching JSON only.",
    "Keep pickup and drop_off separated. Never merge both locations into pickup.",
    `canonical_schema_target:\n${JSON.stringify(canonical_schema_target)}`,
    `deterministic_extraction:\n${JSON.stringify(deterministic_extraction)}`,
    `deterministic_candidate_map:\n${JSON.stringify(deterministic_candidate_map || {}, null, 2)}`,
    `numbered_message_lines:\n${Array.isArray(numbered_lines) ? numbered_lines.join("\n") : ""}`,
    `raw_message:\n${raw_message}`
  ].join("\n\n");
}

async function requestStructuredNormalization({
  client,
  model,
  raw_message,
  deterministic_extraction,
  canonical_schema_target,
  numbered_lines,
  deterministic_candidate_map
}) {
  const prompt = buildNormalizationPrompt({
    raw_message,
    deterministic_extraction,
    canonical_schema_target,
    numbered_lines,
    deterministic_candidate_map
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
    const analysis =
      input.analysis && typeof input.analysis === "object" && !Array.isArray(input.analysis)
        ? input.analysis
        : {};
    const canonical_schema_target = Array.isArray(input.canonical_schema_target)
      ? [...input.canonical_schema_target]
      : Array.isArray(input.canonicalSchemaTarget)
        ? [...input.canonicalSchemaTarget]
        : [...CANONICAL_SCHEMA_TARGET];
    const numbered_lines = Array.isArray(input.numbered_lines)
      ? [...input.numbered_lines]
      : Array.isArray(input.numberedLines)
        ? [...input.numberedLines]
        : Array.isArray(analysis.numberedLines)
          ? [...analysis.numberedLines]
          : [];
    const deterministic_candidate_map =
      input.deterministic_candidate_map ||
      input.deterministicCandidateMap ||
      analysis.deterministicCandidateMap ||
      {};
    const lockedFields = Array.isArray(input.lockedFields)
      ? [...input.lockedFields]
      : Array.isArray(analysis.lockedFields)
        ? [...analysis.lockedFields]
        : [];
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
            canonical_schema_target,
            numbered_lines,
            deterministic_candidate_map
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
        canonicalSchemaTarget: canonical_schema_target,
        lockedFields
      });
      const merged = mergeNormalized({
        local: localNormalized,
        fromAi: normalizeObjectShape(aiObject)
      });

      safeLogger.info("OpenAI normalization completed", {
        stage: "openai_normalization",
        fallbackUsed: false,
        model
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
