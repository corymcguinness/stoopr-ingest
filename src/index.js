export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(main(env));
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    const token = url.searchParams.get("token");

    if (!env.INGEST_TOKEN || token !== env.INGEST_TOKEN) {
      return new Response("Unauthorized", { status: 401 });
    }

    try {
      const result = await main(env);
      return new Response(JSON.stringify(result, null, 2), {
        status: 200,
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    } catch (e) {
      return new Response(e instanceof Error ? e.message : String(e), {
        status: 500,
      });
    }
  },
};

async function main(env) {
  const required = [
    "SUPABASE_URL",
    "SUPABASE_SERVICE_ROLE_KEY",
    "PLUTO_URL",
    "DOB_PERMITS_URL",
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

  const out = {};

  // 0) PLUTO (Brooklyn) staged into pluto_raw (paged + resumable)
  out.pluto_raw = await runAndLog(env, "pluto_raw", ingestPlutoBrooklyn);

  // 2) DOB permits (Brooklyn) into dob_permits (paged + resumable)
  out.dob_permits = await runAndLog(env, "dob_permits", ingestDobPermitsBrooklyn);

  return { ok: true, ...out };
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
 *  INGEST: PLUTO (BROOKLYN)
 *  Writes into pluto_raw
 *  Resumes using ingest_state.cursor = { offset: number }
 * ----------------------------- */
async function ingestPlutoBrooklyn(env) {
  const state = await getIngestState(env, "pluto_raw");
  let offset = Number(state?.cursor?.offset || 0);

  const limit = 5000;
  const maxPagesThisRun = 5;

  let pages = 0;
  let total = 0;

  while (pages < maxPagesThisRun) {
    const url =
      `${env.PLUTO_URL}` +
      `?$select=bbl,borough,zipcode,bldgclass,landuse,yearbuilt,numfloors,unitsres,address` +
      `&$where=borough='BK'` +
      `&$limit=${limit}` +
      `&$offset=${offset}`;

    const rows = await fetchJson(url, env);

    if (!Array.isArray(rows) || rows.length === 0) {
      await setIngestState(env, "pluto_raw", { offset }, true);
      return { upserted_pluto_rows: total, pages, offset, done: true };
    }

    const records = rows
      .filter((r) => r.bbl)
      .map((r) => ({
        bbl: String(r.bbl),
        borough: r.borough || null,
        zipcode: r.zipcode || null,
        bldgclass: r.bldgclass || null,
        landuse: r.landuse != null ? parseInt(String(r.landuse), 10) : null,
        yearbuilt: r.yearbuilt != null ? parseInt(String(r.yearbuilt), 10) : null,
        numfloors: r.numfloors != null ? Number(r.numfloors) : null,
        unitsres: r.unitsres != null ? parseInt(String(r.unitsres), 10) : null,
        address: r.address || null,
        raw: r,
        ingested_at: new Date().toISOString(),
      }));

    if (records.length) {
      await supabaseUpsert(env, "pluto_raw", records, "bbl");
      total += records.length;
    }

    offset += limit;
    pages += 1;

    await setIngestState(env, "pluto_raw", { offset }, false);
  }

  return { upserted_pluto_rows: total, pages, offset, done: false };
}

/** -----------------------------
 *  INGEST: DOB PERMITS (BROOKLYN)
 *  Writes into dob_permits
 *  Resumes using ingest_state.cursor = { offset: number }
 *
 *  DOB_PERMITS_URL should be:
 *   https://data.cityofnewyork.us/resource/ic3t-wcy2.json
 * ----------------------------- */
async function ingestDobPermitsBrooklyn(env) {
  const state = await getIngestState(env, "dob_permits");
  let offset = Number(state?.cursor?.offset || 0);

  const limit = 5000;
  const maxPagesThisRun = 5;

  let pages = 0;
  let total = 0;

  while (pages < maxPagesThisRun) {
    const url =
      `${env.DOB_PERMITS_URL}` +
      `?$select=:id,bbl,borough,block,lot,job_type,job_status,latest_action_date` +
      `&$where=borough='BROOKLYN'` +
      `&$limit=${limit}` +
      `&$offset=${offset}`;

    const rows = await fetchJson(url, env);

    if (!Array.isArray(rows) || rows.length === 0) {
      await setIngestState(env, "dob_permits", { offset }, true);
      return { upserted_dob_rows: total, pages, offset, done: true };
    }

    const records = rows
      .map((r) => {
        const bbl =
          r.bbl
            ? String(r.bbl)
            : r.block && r.lot
              ? `3${String(r.block).padStart(5, "0")}${String(r.lot).padStart(4, "0")}`
              : null;

        const sourceId = r[":id"] ? `ic3t-wcy2:${r[":id"]}` : null;

        return {
          source_id: sourceId,
          bbl,
          filed_date: r.latest_action_date ? String(r.latest_action_date).slice(0, 10) : null,
          job_type: r.job_type || null,
          status: r.job_status || null,
          source: "dob_job_applications",
          raw: r,
          ingested_at: new Date().toISOString(),
        };
      })
      .filter((x) => x.source_id && x.bbl);

    if (records.length) {
      await supabaseUpsert(env, "dob_permits", records, "source_id");
      total += records.length;
    }

    offset += limit;
    pages += 1;

    await setIngestState(env, "dob_permits", { offset }, false);
  }

  return { upserted_dob_rows: total, pages, offset, done: false };
}

/** -----------------------------
 *  Fetch JSON (Socrata token support)
 * ----------------------------- */
async function fetchJson(url, env) {
  const headers = { "user-agent": "stoopr-ingest/1.0" };
  if (env.SOCRATA_APP_TOKEN) headers["X-App-Token"] = env.SOCRATA_APP_TOKEN;

  const res = await fetch(url, { headers });
  if (!res.ok) {
    throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
  }
  return await res.json();
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
 *  ingest_state helpers (cursor)
 * ----------------------------- */
async function getIngestState(env, source) {
  const url = `${env.SUPABASE_URL}/rest/v1/ingest_state?source=eq.${encodeURIComponent(
    source
  )}&select=source,cursor,updated_at`;

  const res = await fetch(url, {
    headers: {
      apikey: env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    },
  });

  if (!res.ok) {
    const body = await safeText(res);
    throw new Error(
      `Supabase read failed (ingest_state): ${res.status} ${res.statusText}\n${body}`
    );
  }

  const rows = await res.json();
  return rows?.[0] || null;
}

async function setIngestState(env, source, cursor, reset) {
  const payload = [
    {
      source,
      cursor: reset ? {} : cursor,
      updated_at: new Date().toISOString(),
    },
  ];

  await supabaseUpsert(env, "ingest_state", payload, "source");
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
