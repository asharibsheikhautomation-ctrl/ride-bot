const { env } = require("../config/env");
const { safeTrim } = require("../utils/text");

const VEHICLE_MULTIPLIERS = Object.freeze([
  { pattern: /\b(saloon|sedan)\b/i, multiplier: 1 },
  { pattern: /\bestate\b/i, multiplier: 1.1 },
  { pattern: /\b(exec|executive)\b/i, multiplier: 1.35 },
  { pattern: /\bmpv\b/i, multiplier: 1.45 },
  { pattern: /\b(suv|4x4)\b/i, multiplier: 1.55 },
  { pattern: /\b(van|minibus)\b/i, multiplier: 1.7 }
]);

function toFiniteNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeMoneyString(value) {
  if (value === null || value === undefined) return "";

  const text = safeTrim(value);
  if (!text) return "";

  return text.replace(/\s+/g, " ");
}

function detectCurrencyCodeFromMoneyString(value, fallback = "") {
  const text = normalizeMoneyString(value);
  if (!text) return safeTrim(fallback).toUpperCase();

  if (text.includes("\u00A3")) return "GBP";
  if (text.includes("$")) return "USD";
  if (text.includes("\u20AC") || /\beur\b/i.test(text)) return "EUR";
  if (/\bpkr\b/i.test(text) || /\brs\.?\b/i.test(text)) return "PKR";

  return safeTrim(fallback).toUpperCase();
}

function metersToKm(meters) {
  const value = toFiniteNumber(meters);
  if (value === null || value < 0) return 0;
  return value / 1000;
}

function formatMoney(amount, currencyCode) {
  const value = toFiniteNumber(amount);
  if (value === null) return "";

  void currencyCode;
  return value.toFixed(2);
}

function resolveVehicleMultiplier(vehicleType) {
  const normalizedVehicleType = safeTrim(vehicleType);
  if (!normalizedVehicleType) return null;

  for (const candidate of VEHICLE_MULTIPLIERS) {
    if (candidate.pattern.test(normalizedVehicleType)) {
      return candidate.multiplier;
    }
  }

  return null;
}

function calculateDeterministicFare(distanceKm, cfg = {}) {
  const km = toFiniteNumber(distanceKm);
  if (km === null || km < 0) {
    return "";
  }

  const baseFare = toFiniteNumber(cfg.baseFare ?? cfg.fareBase ?? env.fareBase);
  const perKmRate = toFiniteNumber(cfg.perKmRate ?? cfg.farePerKm ?? env.farePerKm);
  const currency = safeTrim(
    cfg.currency ?? cfg.defaultCurrency ?? env.defaultCurrency ?? "GBP"
  );
  const vehicleMultiplier = resolveVehicleMultiplier(
    cfg.requiredVehicle ?? cfg.required_vehicle ?? cfg.vehicleType
  );

  if (baseFare === null || perKmRate === null || vehicleMultiplier === null) {
    return "";
  }

  const total = (baseFare + km * perKmRate) * vehicleMultiplier;
  return formatMoney(total, currency);
}

function calculateFare(distanceKm, extractedFare, cfg = {}) {
  const preserved = normalizeMoneyString(extractedFare);
  if (preserved) {
    return preserved;
  }

  return calculateDeterministicFare(distanceKm, cfg);
}

module.exports = {
  metersToKm,
  normalizeMoneyString,
  detectCurrencyCodeFromMoneyString,
  resolveVehicleMultiplier,
  calculateDeterministicFare,
  calculateFare
};
