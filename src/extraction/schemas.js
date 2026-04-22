const { detectCurrencyCodeFromMoneyString, normalizeMoneyString } = require("../routing/fare");

const CANONICAL_RIDE_FIELDS = Object.freeze([
  "refer",
  "source_name",
  "group_name",
  "day_label",
  "pickup_date",
  "pickup_time",
  "pickup_datetime",
  "asap",
  "pickup",
  "via_1",
  "via_2",
  "via_3",
  "drop_off",
  "route_summary",
  "distance",
  "fare_extracted",
  "currency",
  "fare_type",
  "calculated_fare",
  "final_fare",
  "required_vehicle",
  "seat_count",
  "child_seat",
  "wait_and_return",
  "passenger_count",
  "pet_dog",
  "payment_status",
  "special_notes",
  "expiry",
  "expiry_utc",
  "raw_message",
  "message_id",
  "source_group",
  "received_at",
  "parser_confidence",
  "status"
]);

const OPTIONAL_RIDE_FIELDS = Object.freeze([
  "head_passenger",
  "mobile_number",
  "flight_number",
  "arriving_from"
]);

const LEGACY_SHEET_HEADERS = Object.freeze([
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
]);

const DEFAULT_SHEET_HEADERS = Object.freeze([...CANONICAL_RIDE_FIELDS]);
const ALL_RIDE_FIELDS = [...CANONICAL_RIDE_FIELDS, ...OPTIONAL_RIDE_FIELDS];

const RIDE_OBJECT_TEMPLATE = Object.freeze(
  ALL_RIDE_FIELDS.reduce((acc, key) => {
    acc[key] = "";
    return acc;
  }, {})
);

const NORMALIZATION_FIELDS = Object.freeze([
  "refer",
  "day_label",
  "pickup_date",
  "pickup_time",
  "pickup_datetime",
  "asap",
  "pickup",
  "via_1",
  "via_2",
  "via_3",
  "drop_off",
  "route_summary",
  "distance",
  "fare_extracted",
  "currency",
  "fare_type",
  "calculated_fare",
  "final_fare",
  "required_vehicle",
  "seat_count",
  "child_seat",
  "wait_and_return",
  "passenger_count",
  "pet_dog",
  "payment_status",
  "special_notes",
  "expiry",
  "expiry_utc",
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

const WEEKDAY_PATTERN =
  /^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b\s*(.*)$/i;

const HEADER_ALIASES = Object.freeze({
  refer: ["reference"],
  source_name: ["source", "source name"],
  group_name: ["group", "group name", "whatsapp group"],
  day_label: ["day", "day label"],
  pickup_date: ["date", "pickup date", "day & date", "day and date"],
  pickup_time: ["pickup time", "starting", "start time", "time"],
  pickup_datetime: ["pickup datetime", "pickup date time", "pickup at"],
  asap: ["asap", "urgent", "priority asap"],
  pickup: ["pick up", "pick-up", "pickup location"],
  via_1: ["via", "via 1", "stop 1"],
  via_2: ["via 2", "stop 2"],
  via_3: ["via 3", "stop 3"],
  drop_off: ["drop off", "drop-off", "dropoff", "drop off location"],
  route_summary: ["route summary", "route"],
  distance: ["route distance"],
  fare_extracted: ["extracted fare", "fare extracted", "quoted fare", "fare quote"],
  currency: ["fare currency"],
  fare_type: ["fare type"],
  calculated_fare: ["calculated fare", "system fare"],
  final_fare: ["final fare", "fare"],
  required_vehicle: ["vehicle", "required vehicle"],
  seat_count: ["seat count", "seats"],
  child_seat: ["child seat", "booster seat"],
  wait_and_return: ["wait and return", "wait return"],
  passenger_count: ["passenger count", "passengers"],
  pet_dog: ["pet dog", "pet"],
  payment_status: ["payment", "payment status"],
  special_notes: ["notes", "special notes"],
  expiry: ["expires", "expiry"],
  expiry_utc: ["expires utc", "expiry utc"],
  raw_message: ["raw message", "message body"],
  message_id: ["message id"],
  source_group: ["source group", "group id", "chat id"],
  received_at: ["received at"],
  parser_confidence: ["parser confidence", "confidence"],
  status: ["status", "ride status", "review status"],
  head_passenger: ["head passenger"],
  mobile_number: ["mobile number", "phone number"],
  flight_number: ["flight", "flight number"],
  arriving_from: ["arriving from"]
});

function toCell(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function normalizeHeaderName(value) {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/[^a-zA-Z0-9 ]+/g, " ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function splitDayLabelFromDate(value) {
  const text = toCell(value);
  if (!text) {
    return {
      day_label: "",
      pickup_date: ""
    };
  }

  const match = text.match(WEEKDAY_PATTERN);
  if (!match) {
    return {
      day_label: "",
      pickup_date: text
    };
  }

  return {
    day_label: toCell(match[1]),
    pickup_date: toCell(match[2] || "")
  };
}

function buildDisplayDayDate(record = {}) {
  const dayLabel = toCell(record.day_label);
  const pickupDate = toCell(record.pickup_date);
  if (!dayLabel) return pickupDate;
  if (!pickupDate) return dayLabel;

  const normalizedDate = normalizeHeaderName(pickupDate);
  const normalizedDay = normalizeHeaderName(dayLabel);
  if (normalizedDate.startsWith(normalizedDay)) {
    return pickupDate;
  }

  return `${dayLabel} ${pickupDate}`.trim();
}

function buildPickupDateTime(record = {}) {
  const explicit = toCell(record.pickup_datetime);
  if (explicit) return explicit;

  const displayDate = buildDisplayDayDate(record);
  const pickupTime = toCell(record.pickup_time);
  return [displayDate, pickupTime].filter(Boolean).join(", ");
}

function buildRouteSummary(record = {}) {
  const parts = [
    toCell(record.pickup),
    toCell(record.via_1),
    toCell(record.via_2),
    toCell(record.via_3),
    toCell(record.drop_off)
  ].filter(Boolean);

  return parts.join(" -> ");
}

function buildSpecialNotes(record = {}) {
  const explicit = toCell(record.special_notes);
  if (explicit) return explicit;

  const notes = [];
  if (toCell(record.head_passenger)) {
    notes.push(`Head Passenger: ${toCell(record.head_passenger)}`);
  }
  if (toCell(record.mobile_number)) {
    notes.push(`Mobile Number: ${toCell(record.mobile_number)}`);
  }
  if (toCell(record.flight_number)) {
    notes.push(`Flight: ${toCell(record.flight_number)}`);
  }
  if (toCell(record.arriving_from)) {
    notes.push(`Arriving From: ${toCell(record.arriving_from)}`);
  }

  return notes.join(" | ");
}

function deriveFareType(record = {}) {
  const explicit = toCell(record.fare_type);
  if (explicit) return explicit;
  if (toCell(record.fare_extracted)) return "quoted";
  if (toCell(record.final_fare) || toCell(record.calculated_fare)) return "calculated";
  return "";
}

function buildHeaderFieldMap() {
  const map = {};

  for (const field of ALL_RIDE_FIELDS) {
    map[normalizeHeaderName(field)] = field;
    const aliases = HEADER_ALIASES[field] || [];
    for (const alias of aliases) {
      map[normalizeHeaderName(alias)] = field;
    }
  }

  return map;
}

const HEADER_FIELD_MAP = Object.freeze(buildHeaderFieldMap());

function normalizeLegacyRecord(record = {}) {
  const dayDate = toCell(record.pickup_date || record.day_date || record.dayAndDate);
  const dayDateParts = splitDayLabelFromDate(dayDate);
  const pickupTime = toCell(record.pickup_time || record.starting);
  const fareExtracted = normalizeMoneyString(record.fare_extracted || record.fare);
  const calculatedFare = normalizeMoneyString(record.calculated_fare);
  const finalFare = normalizeMoneyString(record.final_fare || record.fare);

  return {
    refer: record.refer,
    source_name: record.source_name || record.sourceName,
    group_name: record.group_name || record.groupName || record.source_group || record.sourceGroup,
    day_label: record.day_label || record.dayLabel || dayDateParts.day_label,
    pickup_date: record.pickup_date || record.pickupDate || dayDateParts.pickup_date,
    pickup_time: pickupTime,
    pickup_datetime: record.pickup_datetime || record.pickupDatetime,
    asap: record.asap,
    pickup: record.pickup,
    via_1: record.via_1 || record.via1,
    via_2: record.via_2 || record.via2,
    via_3: record.via_3 || record.via3,
    drop_off: record.drop_off || record.dropOff,
    route_summary: record.route_summary || record.routeSummary,
    distance: record.distance,
    fare_extracted: fareExtracted,
    currency:
      record.currency ||
      detectCurrencyCodeFromMoneyString(fareExtracted || finalFare || calculatedFare),
    fare_type: record.fare_type || record.fareType,
    calculated_fare: calculatedFare,
    final_fare: finalFare,
    required_vehicle: record.required_vehicle || record.requiredVehicle,
    seat_count: record.seat_count || record.seatCount,
    child_seat: record.child_seat || record.childSeat,
    wait_and_return: record.wait_and_return || record.waitAndReturn,
    passenger_count: record.passenger_count || record.passengerCount,
    pet_dog: record.pet_dog || record.petDog,
    payment_status: record.payment_status || record.paymentStatus,
    special_notes: record.special_notes || record.specialNotes || record.notes,
    expiry: record.expiry || record.expires,
    expiry_utc: record.expiry_utc || record.expiryUtc || record.expires_utc || record.expiresUtc,
    raw_message: record.raw_message || record.rawMessage,
    message_id: record.message_id || record.messageId,
    source_group: record.source_group || record.sourceGroup,
    received_at: record.received_at || record.receivedAt,
    parser_confidence: record.parser_confidence || record.parserConfidence,
    status: record.status,
    head_passenger: record.head_passenger || record.headPassenger,
    mobile_number: record.mobile_number || record.mobileNumber,
    flight_number: record.flight_number || record.flightNumber,
    arriving_from: record.arriving_from || record.arrivingFrom
  };
}

function createEmptyRideObject(overrides = {}) {
  const normalized = normalizeLegacyRecord(overrides);
  const base = { ...RIDE_OBJECT_TEMPLATE };

  for (const key of ALL_RIDE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(normalized, key)) {
      base[key] = toCell(normalized[key]);
    }
  }

  if (!base.source_name && (base.source_group || base.group_name)) {
    base.source_name = "whatsapp";
  }

  if (!base.group_name && base.source_group) {
    base.group_name = base.source_group;
  }

  if (!base.pickup_datetime) {
    base.pickup_datetime = buildPickupDateTime(base);
  }

  if (!base.route_summary) {
    base.route_summary = buildRouteSummary(base);
  }

  if (!base.currency) {
    base.currency = detectCurrencyCodeFromMoneyString(
      base.fare_extracted || base.final_fare || base.calculated_fare
    );
  }

  if (!base.fare_type) {
    base.fare_type = deriveFareType(base);
  }

  if (!base.final_fare && base.fare_extracted) {
    base.final_fare = base.fare_extracted;
  }

  if (!base.passenger_count && base.seat_count) {
    base.passenger_count = base.seat_count;
  }

  if (!base.special_notes) {
    base.special_notes = buildSpecialNotes(base);
  }

  return base;
}

function createEmptyNormalizationObject(overrides = {}) {
  const base = { ...NORMALIZATION_TEMPLATE };
  const normalized = normalizeLegacyRecord(overrides);

  for (const key of NORMALIZATION_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(normalized, key)) {
      base[key] = toCell(normalized[key]);
    }
  }

  if (!base.pickup_datetime) {
    base.pickup_datetime = buildPickupDateTime(base);
  }

  if (!base.route_summary) {
    base.route_summary = buildRouteSummary(base);
  }

  if (!base.currency) {
    base.currency = detectCurrencyCodeFromMoneyString(
      base.fare_extracted || base.final_fare || base.calculated_fare
    );
  }

  if (!base.fare_type) {
    base.fare_type = deriveFareType(base);
  }

  if (!base.special_notes) {
    base.special_notes = buildSpecialNotes(base);
  }

  return base;
}

function pickNormalizationFields(source = {}) {
  const normalized = createEmptyNormalizationObject(source);
  const out = {};

  for (const key of NORMALIZATION_FIELDS) {
    out[key] = toCell(normalized[key]);
  }

  return createEmptyNormalizationObject(out);
}

function resolveRideFieldForHeader(header) {
  const normalizedHeader = normalizeHeaderName(header);
  if (!normalizedHeader) return "";
  return HEADER_FIELD_MAP[normalizedHeader] || "";
}

function resolveRideValueForHeader(header, ride = {}, options = {}) {
  const normalizedHeader = normalizeHeaderName(header);
  const normalizedRide = options.normalized ? ride : createEmptyRideObject(ride);

  switch (normalizedHeader) {
    case "day date":
    case "day and date":
      return buildDisplayDayDate(normalizedRide);
    case "starting":
    case "start time":
      return normalizedRide.pickup_time;
    case "fare":
      return normalizedRide.final_fare || normalizedRide.fare_extracted;
    case "expires":
      return normalizedRide.expiry;
    case "expires utc":
      return normalizedRide.expiry_utc;
    default: {
      const field = resolveRideFieldForHeader(header);
      if (!field) return "";
      return normalizedRide[field];
    }
  }
}

function buildRowFromRideObject(record = {}, headers = DEFAULT_SHEET_HEADERS) {
  const safeHeaders =
    Array.isArray(headers) && headers.length > 0 ? headers : DEFAULT_SHEET_HEADERS;
  const normalizedRide = createEmptyRideObject(record);

  return safeHeaders.map((header) =>
    toCell(resolveRideValueForHeader(header, normalizedRide, { normalized: true }))
  );
}

// Backward-compatible aliases used by earlier scaffold code.
const SHEET_COLUMNS = LEGACY_SHEET_HEADERS;
const emptyRideRecord = createEmptyRideObject;
const buildRowFromRecord = buildRowFromRideObject;

module.exports = {
  CANONICAL_RIDE_FIELDS,
  DEFAULT_SHEET_HEADERS,
  LEGACY_SHEET_HEADERS,
  SHEET_COLUMNS,
  RIDE_OBJECT_TEMPLATE,
  NORMALIZATION_FIELDS,
  NORMALIZATION_TEMPLATE,
  createEmptyRideObject,
  createEmptyNormalizationObject,
  pickNormalizationFields,
  normalizeLegacyRecord,
  normalizeHeaderName,
  resolveRideFieldForHeader,
  resolveRideValueForHeader,
  buildRowFromRideObject,
  emptyRideRecord,
  buildRowFromRecord
};
