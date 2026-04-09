const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const DEFAULT_TTL_MS = 6 * 60 * 60 * 1000;
const DEFAULT_MAX_ENTRIES = 20000;
const DEFAULT_FLUSH_DELAY_MS = 250;

class DedupeStore {
  constructor({
    ttlMs = DEFAULT_TTL_MS,
    maxEntries = DEFAULT_MAX_ENTRIES,
    logger,
    filePath = "",
    flushDelayMs = DEFAULT_FLUSH_DELAY_MS
  } = {}) {
    this.ttlMs = Number.isFinite(ttlMs) ? ttlMs : DEFAULT_TTL_MS;
    this.maxEntries = Number.isFinite(maxEntries) ? maxEntries : DEFAULT_MAX_ENTRIES;
    this.logger = logger || { debug: () => {}, warn: () => {} };
    this.filePath = filePath ? path.resolve(String(filePath)) : "";
    this.flushDelayMs = Number.isFinite(flushDelayMs) ? Math.max(50, flushDelayMs) : 250;
    this.store = new Map();
    this._dirty = false;
    this._flushTimer = null;

    if (this.filePath) {
      this.#loadFromDisk();
    }
  }

  hasProcessed(key) {
    if (!key) return false;

    const entry = this.store.get(key);
    if (!entry) return false;

    if (Date.now() - entry.processedAt >= this.ttlMs) {
      this.store.delete(key);
      this.#markDirty();
      return false;
    }

    return true;
  }

  markProcessed(key, payload = {}) {
    if (!key) return false;

    this.store.set(key, {
      processedAt: Date.now(),
      payload: this.#safePayload(payload)
    });

    this.cleanup();
    this.#trim();
    this.#markDirty();
    return true;
  }

  buildDedupeKey(message = {}) {
    const messageId =
      message.messageId || message.id?._serialized || message.id?.id || message.id || "";

    if (messageId) {
      return `msg:${String(messageId)}`;
    }

    const source = [
      message.rawMessage || message.raw_message || message.body || "",
      message.group || message.groupId || message.chatId || message.from || "",
      message.timestamp || message.receivedAt || ""
    ].join("|");

    const fallbackHash = crypto.createHash("sha256").update(source).digest("hex");
    return `hash:${fallbackHash}`;
  }

  // Backward-compatible helper used by existing scaffold code.
  isDuplicate(key, payload = {}) {
    if (this.hasProcessed(key)) return true;
    this.markProcessed(key, payload);
    return false;
  }

  cleanup() {
    const now = Date.now();
    let removed = 0;

    for (const [key, entry] of this.store.entries()) {
      if (now - entry.processedAt >= this.ttlMs) {
        this.store.delete(key);
        removed += 1;
      }
    }

    if (removed > 0) {
      this.#markDirty();
    }

    this.logger.debug("Dedupe cleanup complete", { size: this.store.size });
  }

  flush() {
    if (!this.filePath || !this._dirty) return false;

    try {
      const directory = path.dirname(this.filePath);
      fs.mkdirSync(directory, { recursive: true });

      const payload = {
        version: 1,
        updatedAt: new Date().toISOString(),
        ttlMs: this.ttlMs,
        entries: Array.from(this.store.entries()).map(([key, value]) => ({
          key,
          processedAt: value.processedAt,
          payload: value.payload
        }))
      };

      fs.writeFileSync(this.filePath, JSON.stringify(payload), "utf8");
      this._dirty = false;
      return true;
    } catch (error) {
      this.logger.warn("Dedupe persistence write failed", {
        stage: "dedupe",
        fallbackUsed: true,
        reason: this.filePath
      });
      return false;
    }
  }

  #trim() {
    if (this.store.size <= this.maxEntries) return;

    const ordered = Array.from(this.store.entries()).sort(
      (a, b) => Number(a[1]?.processedAt || 0) - Number(b[1]?.processedAt || 0)
    );

    let index = 0;
    while (this.store.size > this.maxEntries && index < ordered.length) {
      this.store.delete(ordered[index][0]);
      index += 1;
    }

    this.#markDirty();
    this.logger.warn("Dedupe store trimmed to max entries", {
      maxEntries: this.maxEntries
    });
  }

  #loadFromDisk() {
    try {
      if (!fs.existsSync(this.filePath)) return;

      const raw = fs.readFileSync(this.filePath, "utf8");
      if (!raw) return;

      const parsed = JSON.parse(raw);
      const entries = Array.isArray(parsed?.entries) ? parsed.entries : [];
      let loaded = 0;

      for (const entry of entries) {
        const key = entry?.key ? String(entry.key) : "";
        const processedAt = Number(entry?.processedAt);
        if (!key || !Number.isFinite(processedAt)) continue;

        if (Date.now() - processedAt >= this.ttlMs) continue;
        this.store.set(key, {
          processedAt,
          payload: this.#safePayload(entry?.payload || {})
        });
        loaded += 1;
      }

      this.#trim();
      this.logger.debug("Dedupe store loaded from disk", {
        stage: "dedupe",
        size: loaded,
        reason: this.filePath
      });
    } catch (error) {
      this.logger.warn("Dedupe persistence read failed; continuing with empty store", {
        stage: "dedupe",
        fallbackUsed: true,
        reason: this.filePath
      });
      this.store = new Map();
    }
  }

  #markDirty() {
    if (!this.filePath) return;
    this._dirty = true;

    if (this._flushTimer) return;
    this._flushTimer = setTimeout(() => {
      this._flushTimer = null;
      this.flush();
    }, this.flushDelayMs);

    if (typeof this._flushTimer.unref === "function") {
      this._flushTimer.unref();
    }
  }

  #safePayload(payload) {
    try {
      return JSON.parse(JSON.stringify(payload));
    } catch (error) {
      return { note: "non-serializable payload omitted" };
    }
  }
}

module.exports = {
  DedupeStore
};
