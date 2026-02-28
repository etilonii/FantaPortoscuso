const JOB_NAME = "leghe_sync";

function resolveBaseUrl(): string {
  const candidates = [
    process.env.RAILWAY_SERVICE_FANTAPORTOSCUSO_URL,
    process.env.RAILWAY_PRIVATE_DOMAIN,
    process.env.RAILWAY_PUBLIC_DOMAIN,
  ]
    .map((value) => String(value || "").trim())
    .filter(Boolean);
  for (const raw of candidates) {
    if (raw.startsWith("http://") || raw.startsWith("https://")) {
      return raw.replace(/\/+$/, "");
    }
    return `https://${raw.replace(/\/+$/, "")}`;
  }
  throw new Error("No Railway service domain available in env");
}

const importSecret = String(process.env.IMPORT_SECRET || "").trim();
if (!importSecret) {
  throw new Error("Missing IMPORT_SECRET");
}

const baseUrl = resolveBaseUrl();
const url = new URL(`${baseUrl}/data/internal/scheduler/run`);
url.searchParams.set("job", JOB_NAME);
url.searchParams.set("run_pipeline", "true");

const response = await fetch(url.toString(), {
  method: "POST",
  headers: {
    "X-Import-Secret": importSecret,
    Accept: "application/json",
  },
});
const rawText = await response.text();
let payload: unknown = rawText;
try {
  payload = JSON.parse(rawText);
} catch {
  payload = rawText;
}
if (!response.ok) {
  throw new Error(
    `[${JOB_NAME}] scheduler call failed (${response.status}): ${typeof payload === "string" ? payload : JSON.stringify(payload)}`
  );
}
console.log(`[${JOB_NAME}] ok`, typeof payload === "string" ? payload : JSON.stringify(payload));
