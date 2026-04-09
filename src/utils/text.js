function safeTrim(input) {
  if (input === null || input === undefined) return "";
  return String(input).trim();
}

function normalizeLineEndings(input) {
  return String(input || "").replace(/\r\n?/g, "\n");
}

function collapseWhitespace(input) {
  return safeTrim(String(input || "").replace(/\s+/g, " "));
}

function normalizeText(input) {
  const lines = normalizeLineEndings(input)
    .split("\n")
    .map((line) => collapseWhitespace(line))
    .filter((line, index, all) => line.length > 0 || (index > 0 && all[index - 1] !== ""));

  return lines.join("\n").trim();
}

// Backward-compatible aliases used by existing scaffold modules.
const normalizeWhitespace = collapseWhitespace;
const toSafeString = safeTrim;

module.exports = {
  normalizeText,
  safeTrim,
  collapseWhitespace,
  normalizeLineEndings,
  normalizeWhitespace,
  toSafeString
};
