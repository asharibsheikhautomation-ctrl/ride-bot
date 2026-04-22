const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { env } = require("../config/env");
const { safeTrim, normalizeText } = require("../utils/text");

function createProcessRunner() {
  return function runProcess(command, args, options = {}) {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        cwd: options.cwd,
        windowsHide: true
      });
      let stdout = "";
      let stderr = "";
      let timedOut = false;
      let timeoutHandle = null;

      if (Number.isFinite(options.timeoutMs) && options.timeoutMs > 0) {
        timeoutHandle = setTimeout(() => {
          timedOut = true;
          child.kill("SIGTERM");
        }, options.timeoutMs);
      }

      child.stdout.on("data", (chunk) => {
        stdout += String(chunk);
      });

      child.stderr.on("data", (chunk) => {
        stderr += String(chunk);
      });

      child.on("error", (error) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        reject(error);
      });

      child.on("close", (code) => {
        if (timeoutHandle) clearTimeout(timeoutHandle);
        if (timedOut) {
          const error = new Error("Tesseract OCR timed out");
          error.code = "OCR_TIMEOUT";
          error.stdout = stdout;
          error.stderr = stderr;
          reject(error);
          return;
        }

        if (code !== 0) {
          const error = new Error(`Tesseract OCR failed with exit code ${code}`);
          error.code = "OCR_PROCESS_FAILED";
          error.exitCode = code;
          error.stdout = stdout;
          error.stderr = stderr;
          reject(error);
          return;
        }

        resolve({
          stdout,
          stderr,
          exitCode: code
        });
      });
    });
  };
}

function isSupportedImageMimeType(mimeType) {
  const value = safeTrim(mimeType).toLowerCase();
  return value.startsWith("image/");
}

function inferExtensionFromMimeType(mimeType) {
  const value = safeTrim(mimeType).toLowerCase();
  switch (value) {
    case "image/jpeg":
      return ".jpg";
    case "image/png":
      return ".png";
    case "image/webp":
      return ".webp";
    case "image/bmp":
      return ".bmp";
    case "image/tiff":
      return ".tif";
    default:
      return ".img";
  }
}

function writeMediaToTempFile({ media, tempDir, fileStem }) {
  const mimeType = safeTrim(media?.mimetype);
  if (!isSupportedImageMimeType(mimeType)) {
    return null;
  }

  const base64Payload = safeTrim(media?.data);
  if (!base64Payload) {
    return null;
  }

  fs.mkdirSync(tempDir, { recursive: true });
  const extension = inferExtensionFromMimeType(mimeType);
  const safeStem = safeTrim(fileStem).replace(/[^a-zA-Z0-9_-]+/g, "-") || `media-${Date.now()}`;
  const filePath = path.join(tempDir, `${safeStem}${extension}`);
  fs.writeFileSync(filePath, Buffer.from(base64Payload, "base64"));
  return filePath;
}

function createTesseractOcr(options = {}) {
  const tesseractPath = safeTrim(options.tesseractPath || env.ocrTesseractPath || "tesseract");
  const timeoutMs = Number.isFinite(options.timeoutMs) ? options.timeoutMs : env.ocrTimeoutMs;
  const tempDir = safeTrim(options.tempDir || env.ocrTempDir);
  const runProcess = options.runProcess || createProcessRunner();
  const logger = options.logger || {
    info: () => {},
    warn: () => {},
    error: () => {},
    debug: () => {}
  };

  async function extractTextFromImage(filePath) {
    const absoluteFilePath = path.resolve(String(filePath || ""));
    const result = await runProcess(tesseractPath, [absoluteFilePath, "stdout"], {
      timeoutMs
    });

    const text = normalizeText(result.stdout || "");
    logger.info("OCR extraction completed", {
      stage: "ocr",
      fallbackUsed: false,
      reason: absoluteFilePath,
      textLength: text.length
    });

    return text;
  }

  return {
    tempDir,
    tesseractPath,
    isSupportedImageMimeType,
    writeMediaToTempFile,
    async extractTextFromMedia(media, options = {}) {
      const filePath = writeMediaToTempFile({
        media,
        tempDir,
        fileStem: options.fileStem
      });

      if (!filePath) {
        return "";
      }

      try {
        return await extractTextFromImage(filePath);
      } finally {
        try {
          fs.unlinkSync(filePath);
        } catch (error) {
          logger.debug("OCR temp file cleanup skipped", {
            stage: "ocr",
            fallbackUsed: true,
            reason: filePath
          });
        }
      }
    },
    extractTextFromImage
  };
}

module.exports = {
  createTesseractOcr,
  createProcessRunner,
  isSupportedImageMimeType,
  inferExtensionFromMimeType,
  writeMediaToTempFile
};
