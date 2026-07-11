import { existsSync, readFileSync } from "node:fs";

const files = [
  "postman/assetar.openapi.json",
  "postman/Assetar.postman_collection.json",
  "postman/Assetar.local.postman_environment.json",
  "postman/Assetar.production.postman_environment.json",
];

for (const file of files) {
  if (!existsSync(file)) {
    console.error(`Missing ${file}. Run npm run postman:collection first.`);
    process.exit(1);
  }
  JSON.parse(readFileSync(file, "utf8"));
}

const spec = JSON.parse(readFileSync("postman/assetar.openapi.json", "utf8"));
const requiredPaths = [
  "/ops/login",
  "/ops/search",
  "/ops/health",
  "/ops/finance/summary",
  "/ops/webhooks",
  "/ops/notes",
  "/swap/ops",
  "/swap/ops/{id}",
  "/swap/ops/{id}/timeline",
  "/swap/ops/{id}/refresh",
  "/swap/ops/{id}/reconcile",
  "/giftcards/ops/orders",
  "/giftcards/ops/orders/{order_ref}",
  "/giftcards/ops/orders/{order_ref}/retry",
  "/giftcards/ops/orders/{order_ref}/reconcile",
  "/giftcards/ops/orders/{order_ref}/reveal",
  "/whatsapp/ops/conversations",
  "/whatsapp/ops/conversations/{wa_id}",
];

const paths = new Set(Object.keys(spec.paths ?? {}));
const missing = requiredPaths.filter((path) => !paths.has(path));

if (missing.length > 0) {
  console.error("Generated OpenAPI spec is missing required Assetar ops paths:");
  for (const path of missing) {
    console.error(`  - ${path}`);
  }
  console.error("Regenerate from a backend that includes the latest code.");
  process.exit(1);
}

console.log("Postman JSON files are valid and required ops paths are present.");
