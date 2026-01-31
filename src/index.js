export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runIngest(env));
  },
};

async function runIngest(env) {
  const required = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "CSV_URL_BUILDINGS"];
  for (const k of required) {
    if (!env[k]) throw new Error(`Missing env var: ${k}`);
  }

  const csvText = await fetchText(env.CSV_URL_BUILDINGS);
  const rows = parseCsv(csvText);

  // Expected CSV columns:
  // neighborhood_id,bbl,address_norm,address_display,lat,lng
  const buildings = rows
    .filter((r) => r.bbl && r.neighborhood_id && r.address_display)
    .map((r) => ({
      neighborhood_id: r.neighborhood_id,
      bbl: String(r.bbl),
      address_norm: r.address_norm || r.address_display.toLowerCase(),
      address_display: r.address_display,
      lat: r.lat ? Number(r.lat) : null,
      lng: r.lng ? Number(r.lng) : null,
    }));

  if (!buildings.length) {
    return {
      ok: true,
      message: "No buildings found in CSV (nothing to upsert).",
    };
  }

  // Upsert buildings by bbl
  await supabaseUpsert(env, "buildings", buildings, "bbl");

  return {
    ok: true,
    upserted_buildings: buildings.length,
  };
}

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
    throw new Error(`Supabase upsert failed (${table}): ${res.status} ${res.statusText}\n${body}`);
  }
}

async function safeText(res) {
  try {
    return await res.text();
  } catch {
    return "";
  }
}
