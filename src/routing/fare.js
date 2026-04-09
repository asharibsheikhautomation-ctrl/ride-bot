const { env } = require("../config/env");
const { safeTrim } = require("../utils/text");

function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeMoneyString(value) {
  if (value === null || value === undefined) return "";

  const text = safeTrim(value);
  if (!text) return "";

  // Keep user/source-provided fare intact except whitespace normalization.
  return text.replace(/\s+/g, " ");
}

function metersToKm(meters) {
  const value = toFiniteNumber(meters);
  if (value === null || value < 0) return 0;
  return value / 1000;
}

function resolveCurrencySymbol(currencyCode) {
  const code = safeTrim(currencyCode || "GBP").toUpperCase();
  const map = {
    GBP: "\u00A3",
    USD: "$",
    EUR: "EUR ",
    PKR: "PKR "
  };
  return map[code] || `${code} `;
}

function formatMoney(amount, currencyCode) {
  const value = toFiniteNumber(amount);
  if (value === null) return "";

  const symbol = resolveCurrencySymbol(currencyCode);
  return `${symbol}${value.toFixed(2)}`;
}

/*
Priority rule:
1) If extracted fare exists, preserve it exactly (normalized whitespace only).
2) Otherwise compute deterministic fare from distance and config.
*/
function calculateFare(distanceKm, extractedFare, cfg = {}) {
  const preserved = normalizeMoneyString(extractedFare);
  if (preserved) {
    return preserved;
  }

  const km = toFiniteNumber(distanceKm);
  if (km === null || km < 0) {
    return "";
  }

  const baseFare = toFiniteNumber(cfg.baseFare ?? cfg.fareBase ?? env.fareBase);
  const perKmRate = toFiniteNumber(cfg.perKmRate ?? cfg.farePerKm ?? env.farePerKm);
  const currency = safeTrim(
    cfg.currency ?? cfg.defaultCurrency ?? env.defaultCurrency ?? "GBP"
  );

  if (baseFare === null || perKmRate === null) {
    return "";
  }

  const total = baseFare + km * perKmRate;
  return formatMoney(total, currency);
}

/*
Examples:
calculateFare(12.5, "", { baseFare: 5, perKmRate: 2, currency: "GBP" }) -> "\u00A330.00"
calculateFare(8, "\u00A350", { baseFare: 5, perKmRate: 2, currency: "GBP" }) -> "\u00A350"
calculateFare(metersToKm(128400), "", { baseFare: 10, perKmRate: 1.25, currency: "GBP" }) -> "\u00A3170.50"
*/

module.exports = {
  metersToKm,
  normalizeMoneyString,
  calculateFare
};

