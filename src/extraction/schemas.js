const CANONICAL_RIDE_FIELDS = Object.freeze([
  "refer",
  "group_name",
  "source_name",
  "source_time",
  "pickup_day_date",
  "starting_timing",
  "pickup",
  "drop_off",
  "distance",
  "fare",
  "required_vehicle",
  "payment_status"
]);

const STRICT_SHEET_HEADERS = Object.freeze([
  "Refer",
  "Group Name",
  "Source Name",
  "Source Time",
  "Pickup Day & Date",
  "Starting Timing",
  "Pickup",
  "Drop Off",
  "Distance",
  "Fare",
  "Required Vehicle",
  "Payment Status"
]);

const STRICT_HEADER_FIELD_MAP = Object.freeze({
  Refer: "refer",
  "Group Name": "group_name",
  "Source Name": "source_name",
  "Source Time": "source_time",
  "Pickup Day & Date": "pickup_day_date",
  "Starting Timing": "starting_timing",
  Pickup: "pickup",
  "Drop Off": "drop_off",
  Distance: "distance",
  Fare: "fare",
  "Required Vehicle": "required_vehicle",
  "Payment Status": "payment_status"
});

const HEADER_TO_FIELD = Object.freeze({
  refer: "refer",
  "group name": "group_name",
  "source name": "source_name",
  "source time": "source_time",
  "pickup day date": "pickup_day_date",
  "starting timing": "starting_timing",
  pickup: "pickup",
  "drop off": "drop_off",
  distance: "distance",
  fare: "fare",
  "required vehicle": "required_vehicle",
  "payment status": "payment_status"
});

const FIELD_ALIASES = Object.freeze({
  refer: ["reference"],
  group_name: ["group", "group name", "whatsapp group", "source group"],
  source_name: ["source", "source name", "sender", "sender name"],
  source_time: ["source time", "received time", "message time", "arrival time"],
  pickup_day_date: [
    "pickup day date",
    "pickup day & date",
    "pickup date",
    "day & date",
    "day and date",
    "day date"
  ],
  starting_timing: ["starting timing", "pickup time", "starting", "start time", "time"],
  pickup: ["pick up", "pick-up", "pickup location"],
  drop_off: ["drop off", "drop-off", "dropoff", "drop off location"],
  distance: ["route distance"],
  fare: ["final fare", "calculated fare", "quoted fare", "fare extracted"],
  required_vehicle: ["vehicle", "required vehicle"],
  payment_status: ["payment", "payment status"]
});

const RIDE_OBJECT_TEMPLATE = Object.freeze(
  CANONICAL_RIDE_FIELDS.reduce((acc, key) => {
    acc[key] = "";
    return acc;
  }, {})
);

const NORMALIZATION_FIELDS = Object.freeze([...CANONICAL_RIDE_FIELDS]);
const NORMALIZATION_TEMPLATE = RIDE_OBJECT_TEMPLATE;

function toCell(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function normalizeHeaderName(value) {
  return String(value || "")
    .replace(/[_-]+/g, " ")
    .replace(/[^a-zA-Z0-9& ]+/g, " ")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function buildPickupDayDate(value, dayLabelValue) {
  const pickupDayDate = toCell(value);
  if (pickupDayDate) return pickupDayDate;

  const pickupDate = toCell(dayLabelValue?.pickup_date || dayLabelValue?.pickupDate);
  const dayLabel = toCell(dayLabelValue?.day_label || dayLabelValue?.dayLabel);
  if (!dayLabel) return pickupDate;
  if (!pickupDate) return dayLabel;

  const normalizedDay = normalizeHeaderName(dayLabel);
  const normalizedPickupDate = normalizeHeaderName(pickupDate);
  if (normalizedPickupDate.startsWith(normalizedDay)) {
    return pickupDate;
  }

  return `${dayLabel} ${pickupDate}`.trim();
}

function buildHeaderFieldMap() {
  const map = { ...HEADER_TO_FIELD };

  for (const field of CANONICAL_RIDE_FIELDS) {
    map[normalizeHeaderName(field)] = field;
    const aliases = FIELD_ALIASES[field] || [];
    for (const alias of aliases) {
      map[normalizeHeaderName(alias)] = field;
    }
  }

  return map;
}

const HEADER_FIELD_MAP = Object.freeze(buildHeaderFieldMap());

function normalizeAliases(record = {}) {
  const pickupDayDate = buildPickupDayDate(
    record.pickup_day_date || record.pickupDayDate,
    record
  );

  return {
    refer: record.refer,
    group_name:
      record.group_name || record.groupName || record.source_group || record.sourceGroup,
    source_name: record.source_name || record.sourceName,
    source_time: record.source_time || record.sourceTime,
    pickup_day_date: pickupDayDate,
    starting_timing:
      record.starting_timing || record.startingTiming || record.pickup_time || record.starting,
    pickup: record.pickup,
    drop_off: record.drop_off || record.dropOff,
    distance: record.distance,
    fare:
      record.fare ||
      record.final_fare ||
      record.finalFare ||
      record.calculated_fare ||
      record.calculatedFare ||
      "",
    required_vehicle: record.required_vehicle || record.requiredVehicle,
    payment_status: record.payment_status || record.paymentStatus
  };
}

function createEmptyRideObject(overrides = {}) {
  const normalized = normalizeAliases(overrides);
  const base = { ...RIDE_OBJECT_TEMPLATE };

  for (const key of CANONICAL_RIDE_FIELDS) {
    base[key] = toCell(normalized[key]);
  }

  return base;
}

function createEmptyNormalizationObject(overrides = {}) {
  return createEmptyRideObject(overrides);
}

function pickNormalizationFields(source = {}) {
  return createEmptyNormalizationObject(source);
}

function resolveRideFieldForHeader(header) {
  return HEADER_FIELD_MAP[normalizeHeaderName(header)] || "";
}

function resolveRideValueForHeader(header, ride = {}, options = {}) {
  const normalizedRide = options.normalized ? ride : createEmptyRideObject(ride);
  const field = resolveRideFieldForHeader(header);
  if (!field) return "";
  return normalizedRide[field];
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function cleanDropOffForSheet(value, ride = {}) {
  const rawValue = toCell(value);
  if (!rawValue) return "";

  const requiredVehicle = toCell(
    ride.required_vehicle || ride.requiredVehicle || ride.vehicle
  );

  const cleanedLines = rawValue
    .replace(/\r\n?/g, "\n")
    .split("\n")
    .map((line) => toCell(line))
    .filter(Boolean)
    .filter((line) => {
      if (/^(required\s+vehicle|vehicle|fare|payment\s+status)\b/i.test(line)) {
        return false;
      }

      if (requiredVehicle && normalizeHeaderName(line) === normalizeHeaderName(requiredVehicle)) {
        return false;
      }

      return true;
    });

  let cleanedValue = cleanedLines.join(", ").replace(/\s*,\s*/g, ", ");

  if (requiredVehicle) {
    const vehicleSuffixPattern = new RegExp(
      `(?:\\s*[|,;:-]\\s*|\\s+)${escapeRegExp(requiredVehicle)}$`,
      "i"
    );
    cleanedValue = cleanedValue.replace(vehicleSuffixPattern, "").trim();
  }

  return toCell(cleanedValue);
}

function buildSheetRowObject(record = {}) {
  const normalizedRide = createEmptyRideObject(record);

  return {
    Refer: normalizedRide.refer || "",
    "Group Name": normalizedRide.group_name || "",
    "Source Name": normalizedRide.source_name || "",
    "Source Time": normalizedRide.source_time || "",
    "Pickup Day & Date":
      normalizedRide.pickup_day_date ||
      toCell(record.pickup_date || record.pickupDate) ||
      "",
    "Starting Timing":
      normalizedRide.starting_timing ||
      toCell(record.pickup_time || record.pickupTime) ||
      "",
    Pickup: normalizedRide.pickup || "",
    "Drop Off": cleanDropOffForSheet(normalizedRide.drop_off, normalizedRide),
    Distance: normalizedRide.distance || "",
    Fare: normalizedRide.fare || "",
    "Required Vehicle":
      normalizedRide.required_vehicle ||
      toCell(record.vehicle || record.requiredVehicle) ||
      "",
    "Payment Status": normalizedRide.payment_status || ""
  };
}

function buildRowFromRideObject(record = {}, headers = STRICT_SHEET_HEADERS) {
  const safeHeaders =
    Array.isArray(headers) && headers.length > 0 ? headers : STRICT_SHEET_HEADERS;
  const rowObject = buildSheetRowObject(record);

  return safeHeaders.map((header) => {
    if (Object.prototype.hasOwnProperty.call(rowObject, header)) {
      return toCell(rowObject[header]);
    }

    const strictField = STRICT_HEADER_FIELD_MAP[header];
    if (strictField) {
      return toCell(createEmptyRideObject(record)[strictField]);
    }

    return "";
  });
}

const DEFAULT_SHEET_HEADERS = STRICT_SHEET_HEADERS;
const LEGACY_SHEET_HEADERS = STRICT_SHEET_HEADERS;
const SHEET_COLUMNS = STRICT_SHEET_HEADERS;
const emptyRideRecord = createEmptyRideObject;
const buildRowFromRecord = buildRowFromRideObject;

module.exports = {
  CANONICAL_RIDE_FIELDS,
  STRICT_SHEET_HEADERS,
  DEFAULT_SHEET_HEADERS,
  LEGACY_SHEET_HEADERS,
  SHEET_COLUMNS,
  RIDE_OBJECT_TEMPLATE,
  NORMALIZATION_FIELDS,
  NORMALIZATION_TEMPLATE,
  createEmptyRideObject,
  createEmptyNormalizationObject,
  pickNormalizationFields,
  normalizeHeaderName,
  resolveRideFieldForHeader,
  resolveRideValueForHeader,
  cleanDropOffForSheet,
  buildSheetRowObject,
  buildRowFromRideObject,
  emptyRideRecord,
  buildRowFromRecord
};
