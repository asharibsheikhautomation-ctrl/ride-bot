const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { validateSheetsConfig } = require("../../src/sheets/sheetsClient");
const { classifyAppendFailure, buildAppendRange } = require("../../src/sheets/appendRow");

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "ride-bot-sheets-test-"));
}

function writeFile(filePath, content) {
  fs.writeFileSync(filePath, content, "utf8");
}

function cleanup(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
}

test("validateSheetsConfig catches missing required values", () => {
  const result = validateSheetsConfig({
    spreadsheetId: "",
    worksheetName: "",
    range: "",
    credentialsPath: ""
  });

  assert.equal(result.valid, false);
  assert.ok(result.missing.includes("GOOGLE_SHEETS_ID"));
  assert.ok(result.missing.includes("GOOGLE_CREDENTIALS_JSON or GOOGLE_APPLICATION_CREDENTIALS"));
});

test("validateSheetsConfig detects invalid JSON credentials file", () => {
  const tempDir = makeTempDir();
  const credentialsPath = path.join(tempDir, "credentials.json");
  writeFile(credentialsPath, "{not json}");

  const result = validateSheetsConfig({
    spreadsheetId: "sheet-id",
    worksheetName: "Sheet1",
    credentialsPath
  });

  assert.equal(result.valid, false);
  assert.equal(result.credentialsStatus.code, "GOOGLE_CREDENTIALS_JSON_INVALID");

  cleanup(tempDir);
});

test("validateSheetsConfig accepts service account JSON directly from env-style input", () => {
  const result = validateSheetsConfig({
    spreadsheetId: "sheet-id",
    worksheetName: "Sheet1",
    credentialsJson: {
      type: "service_account",
      client_email: "bot@example.iam.gserviceaccount.com",
      private_key: "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n"
    }
  });

  assert.equal(result.valid, true);
  assert.equal(result.credentialsStatus.code, "GOOGLE_CREDENTIALS_READY");
  assert.equal(result.credentialsStatus.source, "env_json");
});

test("validateSheetsConfig detects missing required fields in credentials file", () => {
  const tempDir = makeTempDir();
  const credentialsPath = path.join(tempDir, "credentials.json");
  writeFile(credentialsPath, JSON.stringify({ type: "service_account" }));

  const result = validateSheetsConfig({
    spreadsheetId: "sheet-id",
    worksheetName: "Sheet1",
    credentialsPath
  });

  assert.equal(result.valid, false);
  assert.equal(result.credentialsStatus.code, "GOOGLE_CREDENTIALS_FIELDS_MISSING");

  cleanup(tempDir);
});

test("buildAppendRange uses quoted worksheet names safely", () => {
  const range = buildAppendRange({
    range: "",
    worksheetName: "Ride Sheet"
  });

  assert.equal(range, "'Ride Sheet'!A:J");
});

test("classifyAppendFailure distinguishes common Google Sheets failures", () => {
  const auth = classifyAppendFailure({
    response: { status: 401, data: { error: { message: "Request had invalid credentials." } } }
  });
  assert.equal(auth.errorCode, "SHEETS_AUTH_FAILED");

  const perm = classifyAppendFailure({
    response: { status: 403, data: { error: { message: "The caller does not have permission" } } }
  });
  assert.equal(perm.errorCode, "SHEETS_PERMISSION_DENIED");

  const spread = classifyAppendFailure({
    response: {
      status: 404,
      data: { error: { message: "Requested entity was not found." } }
    }
  });
  assert.equal(spread.errorCode, "SHEETS_SPREADSHEET_NOT_FOUND");

  const worksheet = classifyAppendFailure({
    response: {
      status: 400,
      data: { error: { message: "Unable to parse range: MissingSheet!A:J" } }
    }
  });
  assert.equal(worksheet.errorCode, "SHEETS_WORKSHEET_NOT_FOUND");

  const timeout = classifyAppendFailure({
    code: "ETIMEDOUT",
    response: { status: 503, data: { error: { message: "Backend Error" } } }
  });
  assert.equal(timeout.errorCode, "SHEETS_NETWORK_TIMEOUT");
});
