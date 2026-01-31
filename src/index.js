export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(main(env));
  },
};

async function main(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "CSV_URL_BUILDINGS",
    "CSV_URL_LISTINGS",
    "CSV_URL_INTEL",
  ];

  for (const k of required) {
    if (!env[k]) throw new Error(`Missing env var: ${k}`);
  }

  // 🔔 HEARTBEAT: guarantees at least one row per run
  await supabaseInsert(env, "ingest_runs", [
    {
      source: "heartbeat",
      status: "ok",
      detail: "scheduled run started",
      counts: {},
      ran_at: new Date().toISOString(),
    },
  ]);

  // Run all ingests, each logs ok/error into ingest_runs
  await runAndLog(env, "buildings", ingestBuildings);
  await runAndLog(env, "listings", ingestListings);
  await runAndLog(env, "intel_current", ingestIntel);
}

/** -----------------------------
 *  Runner + logging
 * ----------------------------- */
async function runAndLog(env, source, fn) {
  const started = new Date().toISOString();

  try {
    const result = await fn(env);

    await logRun(env, {
      source,
      status: "ok",
      detail: `ok @ ${started}`,
      counts: result || {},
    });

    return result;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);

    await logRun(env, {
      source,
      status: "error",
      detail: msg.slice(0, 2000),
      counts: { started },
    });

    throw err; // ensures Cloudflare marks the run as failed too
  }
}

async function logRun(env, { source, status, detail, counts }) {
  await supabaseInsert(env, "ingest_runs", [
    {
      source,
      status,
      detail: detail || null,
      counts: counts || {},
      ran_at: new Date().toISOString(),
    },
  ]);
}

/** -----------------------------
 *  INGEST: BUILDINGS
 *  CSV columns:
 *  neighborhood_id,bbl,address_norm,address_display,lat,lng
 *  Upsert key: bbl
 * ----------------------------- */
async function ingestBuildings(env) {
  const csvText = await fetchText(env.CSV_URL_BUILDINGS);
  const rows = parseCsv(csvText);

  const buildings = rows
    .filter((r) => r.bbl && r.neighborhood_id && r.address_display)
    .map((r) => ({
      neighborhood_id: r.neighborhood_id,
      bbl: String(r.bbl),
      address_norm: r.address_norm || String(r.address_display).toLowerCase(),
      address_display: r.address_display,
      lat: r.lat ? toNumberOrNull(r.lat) : null,
      lng: r.lng ? toNumberOrNull(r.lng) : null,
    }));

  if (!buildings.length) return { upserted_buildings: 0 };

  await supabaseUpsert(env, "buildings", buildings, "bbl");
  return { upserted_buildings: buildings.length };
}

/** -----------------------------
 *  INGEST: LISTINGS
 *  CSV columns:
 *  bbl,source,source_url,status,ask_price,listed_at,raw
 *  Upsert key: source_url
 * ----------------------------- */
async function ingestListings(env) {
  const csvText = await fetchText(env.CSV_URL_LISTINGS);
  const rows = parseCsv(csvText);

  const listings = rows
    .filter((r) => r.bbl && r.source_url)
    .map((r) => ({
      bbl: String(r.bbl),
      source: r.source || "unknown",
      source_url: r.source_url,
      status: r.status || "active",
      ask_price: r.ask_price ? toNumberOrNull(r.ask_price) : null,
      listed_at: r.listed_at ? toIsoOrNull(r.listed_at) : null,
      raw: r.raw ? toJsonOrEmpty(r.raw) : {},
      // optional: keep legacy url in sync if the column exists
      url: r.source_url,
    }));

  if (!listings.length) return { upserted_listings: 0 };

  await supabaseUpsert(env, "listings", listings, "source_url");
  return { upserted_listings: listings.length };
}

/** -----------------------------
 *  INGEST: INTEL_CURRENT
 *  CSV columns:
 *  bbl,overall_score,expansion_score,reno_risk_score,landmark_friction_score,flags,updated_at
 *  Upsert key: bbl
 * ----------------------------- */
async function ingestIntel(env) {
  const csvText = await fetchText(env.CSV_URL_INTEL);
  const rows = parseCsv(csvText);

  const intel = rows
    .filter((r) => r.bbl)
    .map((r) => ({
      bbl: String(r.bbl),
      overall_score: toIntOrNull(r.overall_score),
      expansion_score: toIntOrNull(r.expansion_score),
      reno_risk_score: toIntOrNull(r.reno_risk_score),
      landmark_friction_score: toIntOrNull(r.landmark_friction_score),
      flags: r.flags ? toJsonOrEmpty(r.flags) : [],
      updated_at: r.updated_at ? toIsoOrNull(r.updated_at) : new Date().toISOString(),
    }));

  if (!intel.length) return { upserted_intel: 0 };

  await supabaseUpsert(env, "intel_current", intel, "bbl");
  return { upserted_intel: intel.length };
}

/** -----------------------------
 *  Supabase REST helpers
 * ----------------------------- */
async function supabaseUpsert(env, table, records, onConflict) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}?on_conflict=${encodeURIComponent(
    onConflict
  )}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates,return=minimal",
    },
    body: JSON.stringify(records),
  });

  if (!res.ok) {
    const body = await safeText(res);
    throw new Error(
      `Supabase upsert failed (${table}): ${res.status} ${res.statusText}\n${body}`
    );
  }
}

async function supabaseInsert(env, table, records) {
  const url = `${env.SUPABASE_URL}/rest/v1/${table}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(records),
  });

  if (!res.ok) {
    const body = await safeText(res);
    throw new Error(
      `Supabase insert failed (${table}): ${res.status} ${res.statusText}\n${body}`
    );
  }
}

/** -----------------------------
 *  Fetch + CSV parsing
 * ----------------------------- */
async function fetchText(url) {
  const res = await fetch(url, { headers: { "user-agent": "stoopr-ingest/1.0" } });
  if (!res.ok) throw new Error(`CSV fetch failed: ${res.status} ${res.statusText}`);
  return await res.text();
}

function parseCsv(text) {
  const lines = text
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .filter((l) => l.trim().length > 0);

  if (lines.length < 2) return [];

  const header = splitCsvLine(lines[0]).map((h) => h.trim());
  const out = [];

  for (let i = 1; i < lines.length; i++) {
    const vals = splitCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < header.length; j++) {
      row[header[j]] = (vals[j] ?? "").trim();
    }
    out.push(row);
  }

  return out;
}

// Handles quoted CSV values with commas.
function splitCsvLine(line) {
  const result = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const c = line[i];

    if (c === '"' && line[i + 1] === '"') {
      cur += '"';
      i++;
      continue;
    }
    if (c === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (c === "," && !inQuotes) {
      result.push(cur);
      cur = "";
      continue;
    }
    cur += c;
  }

  result.push(cur);
  return result;
}

/** -----------------------------
 *  Coercion helpers
 * ----------------------------- */
function toNumberOrNull(v) {
  const n = Number(String(v).replace(/[$,]/g, ""));
  return Number.isFinite(n) ? n : null;
}

function toIntOrNull(v) {
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

function toIsoOrNull(v) {
  const d = new Date(v);
  return Number.isFinite(d.getTime()) ? d.toISOString() : null;
}

function toJsonOrEmpty(v) {
  try {
    return JSON.parse(v) ?? {};
  } catch {
    return {};
  }
}

async function safeText(res) {
  try {
    return await res.text();
  } catch {
    return "";
  }
}
