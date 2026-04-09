const test = require("node:test");
const assert = require("node:assert/strict");
const { cleanAddressForGeocoding, createGeocoder } = require("../../src/routing/geocode");
const { createSilentLogger } = require("../helpers");

test("cleanAddressForGeocoding removes bullets and pickup/drop labels", () => {
  const raw = "â€¢ Pick Up:   Heathrow Airport,\n   Terminal 4   ";
  const cleaned = cleanAddressForGeocoding(raw);
  assert.equal(cleaned, "Heathrow Airport, Terminal 4");
});
test("cleanAddressForGeocoding does not merge pickup and drop blocks", () => {
  const raw =
    "• Pick Up: Heathrow Airport, Terminal 4\n• Drop Off: 12, Woodlands Close, Dibden Purlieu, Southampton";
  const cleaned = cleanAddressForGeocoding(raw);
  assert.equal(cleaned, "Heathrow Airport, Terminal 4");
});

test("geocoder sends Nominatim jsonv2 contract with cleaned q value", async () => {
  const calls = [];
  const httpClient = {
    get: async (url, config) => {
      calls.push({ url, config });
      return {
        data: [
          {
            lat: "51.469",
            lon: "-0.4543",
            display_name: "Heathrow Airport Terminal 4, London"
          }
        ]
      };
    }
  };

  const geocoder = createGeocoder({
    provider: "nominatim",
    baseUrl: "https://nominatim.openstreetmap.org/search",
    userAgent: "ride-bot-test/1.0",
    timeoutMs: 4321,
    httpClient,
    logger: createSilentLogger()
  });

  const result = await geocoder.geocodeAddress("â€¢ Pick-up: Heathrow Airport, Terminal 4");
  assert.ok(result);
  assert.equal(result.lat, 51.469);
  assert.equal(result.lng, -0.4543);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].config.params.q, "Heathrow Airport, Terminal 4");
  assert.equal(calls[0].config.params.format, "jsonv2");
  assert.equal(calls[0].config.params.limit, 1);
  assert.equal(calls[0].config.headers["User-Agent"], "ride-bot-test/1.0");
  assert.equal(calls[0].config.timeout, 4321);
});

test("geocoder returns null when provider returns no results", async () => {
  const geocoder = createGeocoder({
    provider: "nominatim",
    httpClient: {
      get: async () => ({ data: [] })
    },
    logger: createSilentLogger()
  });

  const result = await geocoder.geocodeAddress("Drop Off: Unknown place");
  assert.equal(result, null);
});

