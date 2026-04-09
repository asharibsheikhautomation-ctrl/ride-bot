const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { EventEmitter } = require("node:events");
const { initializeWhatsAppClient } = require("../../src/whatsapp/client");
const { createSilentLogger } = require("../helpers");

class FakeWhatsAppClient extends EventEmitter {
  constructor(mode) {
    super();
    this.mode = mode;
    this.initializeCalled = false;
    this.destroyCalled = false;
  }

  async initialize() {
    this.initializeCalled = true;

    setTimeout(() => {
      this.emit("change_state", "OPENING");
      if (this.mode === "ready") {
        this.emit("authenticated");
        this.emit("ready");
      }
      if (this.mode === "auth_failure") {
        this.emit("auth_failure", "invalid session");
      }
    }, 20);
  }

  async destroy() {
    this.destroyCalled = true;
  }
}

function createTempSessionPath() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "ride-bot-wa-"));
}

function removeDirectory(dirPath) {
  if (!dirPath) return;
  fs.rmSync(dirPath, { recursive: true, force: true });
}

test("WhatsApp startup resolves when ready event arrives", async () => {
  const tempPath = createTempSessionPath();
  const fakeClient = new FakeWhatsAppClient("ready");

  try {
    const client = await initializeWhatsAppClient({
      sessionPath: tempPath,
      clientId: "startup-smoke",
      startupTimeoutMs: 120,
      startupTimeoutMinMs: 50,
      persistedSessionDetected: true,
      logger: createSilentLogger(),
      qrRenderer: () => {},
      clientFactory: () => fakeClient
    });

    assert.equal(client, fakeClient);
    assert.equal(fakeClient.initializeCalled, true);
  } finally {
    removeDirectory(tempPath);
  }
});

test("WhatsApp startup fails with explicit timeout code when ready does not arrive", async () => {
  const tempPath = createTempSessionPath();
  const fakeClient = new FakeWhatsAppClient("stall");

  try {
    await assert.rejects(
      () =>
        initializeWhatsAppClient({
          sessionPath: tempPath,
          clientId: "startup-timeout",
          startupTimeoutMs: 120,
          startupTimeoutMinMs: 50,
          persistedSessionDetected: true,
          logger: createSilentLogger(),
          qrRenderer: () => {},
          clientFactory: () => fakeClient
        }),
      (error) => {
        assert.equal(error?.code, "WHATSAPP_STARTUP_TIMEOUT");
        return true;
      }
    );
  } finally {
    removeDirectory(tempPath);
  }
});
