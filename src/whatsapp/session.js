const fs = require("node:fs");
const path = require("node:path");

function safeString(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function isNonEmptyDirectory(dirPath) {
  try {
    const entries = fs.readdirSync(dirPath);
    return entries.length > 0;
  } catch (error) {
    return false;
  }
}

function resolveLocalAuthPaths({ sessionPath, clientId }) {
  const resolvedSessionPath = path.resolve(safeString(sessionPath));
  const stableClientId = safeString(clientId);
  const sessionFolderName = stableClientId ? `session-${stableClientId}` : "session";
  const sessionFolderPath = path.join(resolvedSessionPath, sessionFolderName);

  return {
    sessionPath: resolvedSessionPath,
    clientId: stableClientId,
    sessionFolderName,
    sessionFolderPath
  };
}

function validateAndPrepareSessionStorage({ sessionPath, clientId }) {
  const resolved = resolveLocalAuthPaths({ sessionPath, clientId });

  if (!resolved.sessionPath) {
    throw new Error("WHATSAPP_SESSION_PATH is required");
  }

  if (!resolved.clientId) {
    throw new Error("WHATSAPP_CLIENT_ID is required and must be stable");
  }

  const storageRootExisted = fs.existsSync(resolved.sessionPath);
  fs.mkdirSync(resolved.sessionPath, { recursive: true });

  const sessionFolderExists = fs.existsSync(resolved.sessionFolderPath);
  const sessionFolderHasData =
    sessionFolderExists && isNonEmptyDirectory(resolved.sessionFolderPath);

  return {
    ...resolved,
    storageRootExisted,
    sessionFolderExists,
    sessionFolderHasData
  };
}

module.exports = {
  resolveLocalAuthPaths,
  validateAndPrepareSessionStorage
};
