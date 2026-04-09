const fs = require("node:fs");
const path = require("node:path");

const category = String(process.argv[2] || "all").toLowerCase();
const root = __dirname;

const CATEGORY_DIRS = {
  unit: ["unit"],
  smoke: ["smoke"],
  integration: ["integration"],
  all: ["unit", "smoke", "integration"]
};

function listTestFiles(directory) {
  if (!fs.existsSync(directory)) return [];

  return fs
    .readdirSync(directory, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".test.js"))
    .map((entry) => path.join(directory, entry.name))
    .sort((a, b) => a.localeCompare(b));
}

function run() {
  const directories = CATEGORY_DIRS[category];
  if (!directories) {
    throw new Error(`Unknown test category: ${category}`);
  }

  const files = directories.flatMap((dir) => listTestFiles(path.join(root, dir)));
  if (files.length === 0) {
    // eslint-disable-next-line no-console
    console.warn(`No test files found for category: ${category}`);
    return;
  }

  for (const file of files) {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    require(file);
  }
}

run();
