const {
  NORMALIZATION_FIELDS,
  createEmptyNormalizationObject,
  pickNormalizationFields
} = require("./schemas");
const { executeWithRetry } = require("../utils/retry");
const { summarizeKnownError } = require("../utils/logger");

const NORMALIZATION_SCHEMA = {
  type: "object",
  additionalProperties: false,
  properties: {
    refer: { type: "string" },
    day_date: { type: "string" },
    starting: { type: "string" },
    pickup: { type: "string" },
    drop_off: { type: "string" },
    distance: { type: "string" },
    fare: { type: "string" },
    required_vehicle: { type: "string" },
    expires: { type: "string" },
    expires_utc: { type: "string" },
    head_passenger: { type: "string" },
    mobile_number: { type: "string" },
    flight_number: { type: "string" },
    arriving_from: { type: "string" }
  },
  required: [...NORMALIZATION_FIELDS]
};

const SYSTEM_INSTRUCTION = [
  "You normalize ride-booking extraction output into strict JSON.",
  "Return only JSON that matches the provided schema exactly.",
  "Include every key in the schema.",
  "Use empty string for any missing value.",
  "Do not infer or calculate route distance.",
  "Do not look up maps or locations.",
  "Do not invent a fare when it is absent.",
  "Preserve pickup and drop_off text faithfully.",
  "Only clean formatting and whitespace."
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

function mergeNormalized({ local, fromAi }) {
  const merged = createEmptyNormalizationObject();

  for (const key of NORMALIZATION_FIELDS) {
    const localValue = safeString(local[key]);
    const aiValue = safeString(fromAi[key]);

    if (key === "distance" || key === "fare") {
      // Keep deterministic/local values only for distance/fare safety.
      merged[key] = localValue;
      continue;
    }

    merged[key] = aiValue || localValue || "";
  }

  return merged;
}

async function requestStructuredNormalization({ client, model, rawMessage, extracted }) {
  if (client?.responses?.create) {
    const response = await client.responses.create({
      model,
      input: [
        { role: "system", content: SYSTEM_INSTRUCTION },
        {
          role: "user",
          content: [
            "Normalize the extracted ride object.",
            "Return schema-matching JSON only.",
            `raw_message:\n${rawMessage}`,
            `extracted:\n${JSON.stringify(extracted)}`
          ].join("\n\n")
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
          content: [
            "Normalize the extracted ride object.",
            "Return schema-matching JSON only.",
            `raw_message:\n${rawMessage}`,
            `extracted:\n${JSON.stringify(extracted)}`
          ].join("\n\n")
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

function createOpenAiNormalizer({ apiKey, model = "gpt-4.1-mini", logger } = {}) {
  const safeLogger = logger || { debug: () => {}, info: () => {}, warn: () => {}, error: () => {} };
  let client = null;
  if (apiKey) {
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

  async function normalizeWithOpenAI({ rawMessage, extracted } = {}) {
    const localNormalized = pickNormalizationFields(extracted || {});
    const safeFallback = createEmptyNormalizationObject(localNormalized);

    if (!client) {
      safeLogger.info("OpenAI normalization skipped, using local data", {
        stage: "openai_normalization",
        fallbackUsed: true,
        reason: "API key or SDK unavailable"
      });
      return safeFallback;
    }

    try {
      const rawText = safeString(rawMessage);
      const responseText = await executeWithRetry(
        async () =>
          requestStructuredNormalization({
            client,
            model,
            rawMessage: rawText,
            extracted: localNormalized
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

      if (!responseText) {
        safeLogger.warn("OpenAI normalization failed, using local data", {
          stage: "openai_normalization",
          fallbackUsed: true,
          reason: "Empty model response"
        });
        return safeFallback;
      }

      const parsed = JSON.parse(responseText);
      const aiObject = normalizeObjectShape(parsed);
      const merged = mergeNormalized({
        local: localNormalized,
        fromAi: aiObject
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
    systemInstruction: SYSTEM_INSTRUCTION
  };
}

module.exports = {
  createOpenAiNormalizer,
  NORMALIZATION_SCHEMA,
  SYSTEM_INSTRUCTION
};
