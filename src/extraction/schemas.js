const SHEET_COLUMNS = [
  "Refer",
  "Day & Date",
  "Starting",
  "Pickup",
  "Drop Off",
  "Distance",
  "Fare",
  "Required Vehicle",
  "Expires",
  "Expires UTC"
];

const RIDE_OBJECT_TEMPLATE = Object.freeze({
  refer: "",
  day_date: "",
  starting: "",
  pickup: "",
  drop_off: "",
  distance: "",
  fare: "",
  required_vehicle: "",
  expires: "",
  expires_utc: "",
  raw_message: "",
  head_passenger: "",
  mobile_number: "",
  flight_number: "",
  arriving_from: "",
  source_group: "",
  message_id: "",
  received_at: ""
});

const NORMALIZATION_FIELDS = Object.freeze([
  "refer",
  "day_date",
  "starting",
  "pickup",
  "drop_off",
  "distance",
  "fare",
  "required_vehicle",
  "expires",
  "expires_utc",
  "head_passenger",
  "mobile_number",
  "flight_number",
  "arriving_from"
]);

const NORMALIZATION_TEMPLATE = Object.freeze(
  NORMALIZATION_FIELDS.reduce((acc, key) => {
    acc[key] = "";
    return acc;
  }, {})
);

function toCell(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function createEmptyRideObject(overrides = {}) {
  const base = { ...RIDE_OBJECT_TEMPLATE };
  for (const key of Object.keys(RIDE_OBJECT_TEMPLATE)) {
    if (Object.prototype.hasOwnProperty.call(overrides, key)) {
      base[key] = toCell(overrides[key]);
    }
  }
  return base;
}

function normalizeLegacyRecord(record = {}) {
  return {
    refer: record.refer,
    day_date: record.day_date || record.dayAndDate,
    starting: record.starting,
    pickup: record.pickup,
    drop_off: record.drop_off || record.dropOff,
    distance: record.distance,
    fare: record.fare,
    required_vehicle: record.required_vehicle || record.requiredVehicle,
    expires: record.expires,
    expires_utc: record.expires_utc || record.expiresUtc,
    raw_message: record.raw_message || record.rawMessage,
    head_passenger: record.head_passenger || record.headPassenger,
    mobile_number: record.mobile_number || record.mobileNumber,
    flight_number: record.flight_number || record.flightNumber,
    arriving_from: record.arriving_from || record.arrivingFrom,
    source_group: record.source_group || record.sourceGroup,
    message_id: record.message_id || record.messageId,
    received_at: record.received_at || record.receivedAt
  };
}

function buildRowFromRideObject(record = {}) {
  const merged = createEmptyRideObject(normalizeLegacyRecord(record));

  return [
    merged.refer,
    merged.day_date,
    merged.starting,
    merged.pickup,
    merged.drop_off,
    merged.distance,
    merged.fare,
    merged.required_vehicle,
    merged.expires,
    merged.expires_utc
  ];
}

function createEmptyNormalizationObject(overrides = {}) {
  const base = { ...NORMALIZATION_TEMPLATE };

  for (const key of NORMALIZATION_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(overrides, key)) {
      base[key] = toCell(overrides[key]);
    }
  }

  return base;
}

function pickNormalizationFields(source = {}) {
  const out = {};
  for (const key of NORMALIZATION_FIELDS) {
    out[key] = toCell(source[key] || "");
  }
  return createEmptyNormalizationObject(out);
}

// Backward-compatible aliases used by earlier scaffold code.
const emptyRideRecord = createEmptyRideObject;
const buildRowFromRecord = buildRowFromRideObject;

module.exports = {
  SHEET_COLUMNS,
  RIDE_OBJECT_TEMPLATE,
  NORMALIZATION_FIELDS,
  NORMALIZATION_TEMPLATE,
  createEmptyRideObject,
  createEmptyNormalizationObject,
  pickNormalizationFields,
  buildRowFromRideObject,
  emptyRideRecord,
  buildRowFromRecord
};
