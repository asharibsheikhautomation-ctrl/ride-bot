const test = require("node:test");
const assert = require("node:assert/strict");
const { createTesseractOcr } = require("../../src/extraction/tesseractOcr");
const { buildMessageAttempts, hasUsefulRawText } = require("../../src/whatsapp/messageHandler");
const { createSilentLogger } = require("../helpers");

test("tesseract OCR normalizes extracted stdout text", async () => {
  const ocr = createTesseractOcr({
    tesseractPath: "tesseract",
    runProcess: async () => ({
      stdout: "ESTATE\r\n\r\nTOMORROW 22:25\r\nSTN TO NW11\r\nFARE £65 NET\r\n"
    }),
    logger: createSilentLogger()
  });

  const text = await ocr.extractTextFromImage("fake-image.png");
  assert.equal(text, "ESTATE\n\nTOMORROW 22:25\nSTN TO NW11\nFARE £65 NET");
});

test("buildMessageAttempts creates separate text and OCR attempts when both are useful and distinct", async () => {
  const attempts = await buildMessageAttempts({
    message: {
      hasMedia: true,
      downloadMedia: async () => ({
        mimetype: "image/png",
        data: Buffer.from("fake").toString("base64")
      })
    },
    messageId: "MSG-123",
    normalizedBody: "ESTATE\nTOMORROW 22:25\nSTN TO NW11\nFARE £65 NET",
    ocrExtractor: {
      isSupportedImageMimeType: () => true,
      extractTextFromMedia: async () => "SALOON\nTODAY 09:15\nLHR TO SW1\nFARE £45 CASH"
    },
    logger: createSilentLogger()
  });

  assert.equal(attempts.length, 2);
  assert.equal(attempts[0].sourceKind, "text");
  assert.equal(attempts[1].sourceKind, "ocr");
  assert.equal(attempts[1].attemptMessageId, "MSG-123:ocr");
});

test("buildMessageAttempts dedupes OCR payload when it matches caption text", async () => {
  const rawText = "ESTATE\nTOMORROW 22:25\nSTN TO NW11\nFARE £65 NET";
  const attempts = await buildMessageAttempts({
    message: {
      hasMedia: true,
      downloadMedia: async () => ({
        mimetype: "image/png",
        data: Buffer.from("fake").toString("base64")
      })
    },
    messageId: "MSG-124",
    normalizedBody: rawText,
    ocrExtractor: {
      isSupportedImageMimeType: () => true,
      extractTextFromMedia: async () => rawText
    },
    logger: createSilentLogger()
  });

  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].sourceKind, "text");
});

test("buildMessageAttempts accepts OCR-only image messages", async () => {
  const attempts = await buildMessageAttempts({
    message: {
      hasMedia: true,
      downloadMedia: async () => ({
        mimetype: "image/jpeg",
        data: Buffer.from("fake").toString("base64")
      })
    },
    messageId: "MSG-125",
    normalizedBody: "",
    ocrExtractor: {
      isSupportedImageMimeType: () => true,
      extractTextFromMedia: async () => "MPV\nTONIGHT 23:40\nW2 TO UB3\nFARE £80"
    },
    logger: createSilentLogger()
  });

  assert.equal(attempts.length, 1);
  assert.equal(attempts[0].sourceKind, "ocr");
});

test("weak OCR output is ignored", async () => {
  const attempts = await buildMessageAttempts({
    message: {
      hasMedia: true,
      downloadMedia: async () => ({
        mimetype: "image/png",
        data: Buffer.from("fake").toString("base64")
      })
    },
    messageId: "MSG-126",
    normalizedBody: "",
    ocrExtractor: {
      isSupportedImageMimeType: () => true,
      extractTextFromMedia: async () => "cab"
    },
    logger: createSilentLogger()
  });

  assert.equal(attempts.length, 0);
  assert.equal(hasUsefulRawText("cab"), false);
  assert.equal(hasUsefulRawText("STN TO NW11"), true);
});
