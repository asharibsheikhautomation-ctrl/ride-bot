const crypto = require("node:crypto");

function toDatePart(input) {
  const date = input ? new Date(input) : new Date();
  const validDate = Number.isNaN(date.getTime()) ? new Date() : date;

  const year = validDate.getUTCFullYear();
  const month = String(validDate.getUTCMonth() + 1).padStart(2, "0");
  const day = String(validDate.getUTCDate()).padStart(2, "0");
  return `${year}${month}${day}`;
}

function shortHash(input, length = 4) {
  return crypto
    .createHash("sha256")
    .update(String(input || ""))
    .digest("hex")
    .slice(0, Math.max(4, length))
    .toUpperCase();
}

function resolveMessageId(payload) {
  if (!payload) return "";
  if (typeof payload === "string") return payload;

  return (
    payload.messageId ||
    payload.id?._serialized ||
    payload.id?.id ||
    payload.id ||
    ""
  );
}

function generateRefer(payload = {}) {
  const datePart = toDatePart(payload.timestamp || payload.receivedAt);
  const messageId = resolveMessageId(payload);

  if (messageId) {
    return `RID-${datePart}-${shortHash(messageId, 4)}`;
  }

  const fallbackSource = [
    payload.rawMessage || payload.body || payload.text || "",
    payload.groupId || payload.chatId || payload.from || "",
    payload.timestamp || payload.receivedAt || ""
  ].join("|");

  return `RID-${datePart}-${shortHash(fallbackSource, 4)}`;
}

function generateReference(seedOrPayload) {
  if (seedOrPayload && typeof seedOrPayload === "object") {
    return generateRefer(seedOrPayload);
  }

  return generateRefer({
    rawMessage: seedOrPayload || "",
    timestamp: Date.now()
  });
}

function buildMessageSignature({ chatId, messageId, body, timestamp } = {}) {
  const source = [chatId || "", messageId || "", body || "", timestamp || ""].join("|");
  return crypto.createHash("sha256").update(source).digest("hex");
}

module.exports = {
  generateRefer,
  generateReference,
  buildMessageSignature
};
