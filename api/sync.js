// ================================================================
// bexio → Supabase Auto-Sync (Vercel Cron Job)
// ================================================================
// Optimiert für Vercel Hobby-Plan (60s Timeout)
// - Standard-Sync: contacts, invoices, quotes, orders, bills (~45s)
// - Full-Sync: + Stammdaten (mode=full)
// - Incoming Payments übersprungen (braucht 8000+ API calls)
// ================================================================

const BEXIO_API = "https://api.bexio.com";
const RATE_LIMIT_DELAY_MS = 400;
const MAX_LIMIT = 500;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function bexioGet(endpoint, pat) {
  const allResults = [];
  let offset = 0;
  while (true) {
    const url = new URL(`${BEXIO_API}${endpoint}`);
    url.searchParams.set("limit", String(MAX_LIMIT));
    url.searchParams.set("offset", String(offset));
    await sleep(RATE_LIMIT_DELAY_MS);
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json", Authorization: `Bearer ${pat}` },
    });
    if (!res.ok) {
      if (res.status === 404 || res.status === 500) return [];
      const body = await res.text();
      throw new Error(`bexio ${endpoint} → ${res.status}: ${body.substring(0, 150)}`);
    }
    const data = await res.json();
    if (!Array.isArray(data)) {
      if (data && Array.isArray(data.data)) return data.data;
      return [];
    }
    allResults.push(...data);
    if (data.length < MAX_LIMIT) break;
    offset += MAX_LIMIT;
  }
  return allResults;
}

async function supabaseUpsert(table, rows, ctx) {
  if (rows.length === 0) return 0;
  let upserted = 0;
  for (let i = 0; i < rows.length; i += 500) {
    const chunk = rows.slice(i, i + 500);
    const res = await fetch(`${ctx.url}/rest/v1/${table}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: ctx.key, Authorization: `Bearer ${ctx.key}`,
        Prefer: "resolution=merge-duplicates",
      },
      body: JSON.stringify(chunk),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Supabase ${table} → ${res.status}: ${body.substring(0, 150)}`);
    }
    upserted += chunk.length;
  }
  return upserted;
}

async function logSync(entity, status, n, startMs, ctx, error) {
  try {
    await supabaseUpsert("sync_log", [{
      entity, status, records_fetched: n, records_upserted: n,
      duration_ms: Date.now() - startMs, error_message: error || null,
      started_at: new Date(startMs).toISOString(), finished_at: new Date().toISOString(),
    }], ctx);
  } catch {}
}

function mapInvStatus(id) { return {7:"draft",8:"pending",9:"paid",16:"partial",19:"cancelled"}[id] || "unknown"; }
function mapQuoteStatus(id) { return {1:"draft",2:"pending",3:"accepted",4:"declined"}[id] || "unknown"; }

// ── Sync Functions ──────────────────────────────────────────

async function syncContacts(ctx) {
  const data = await bexioGet("/2.0/contact", ctx.pat);
  return await supabaseUpsert("contacts", data.map(d => ({
    id: d.id, nr: d.nr ?? null, contact_type: d.contact_type_id ?? null,
    name_1: d.name_1, name_2: d.name_2 ?? null, email: d.mail ?? null,
    phone: d.phone_fixed ?? null, address: d.address ?? null,
    postcode: d.postcode ?? null, city: d.city ?? null, country_id: d.country_id ?? null,
    is_customer: d.contact_group_ids?.includes?.(1) ?? false,
    is_supplier: d.contact_group_ids?.includes?.(2) ?? false,
    owner_id: d.user_id ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncInvoices(ctx) {
  const data = await bexioGet("/2.0/kb_invoice", ctx.pat);
  return await supabaseUpsert("invoices", data.map(d => ({
    id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
    status: mapInvStatus(d.kb_item_status_id),
    is_valid_from: d.is_valid_from ?? null, is_valid_to: d.is_valid_to ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    total_received: parseFloat(d.total_received_payments ?? "0"),
    currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
    kb_item_status: d.kb_item_status_id ?? null, api_reference: d.api_reference ?? null,
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncQuotes(ctx) {
  const data = await bexioGet("/2.0/kb_offer", ctx.pat);
  return await supabaseUpsert("quotes", data.map(d => ({
    id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
    status: mapQuoteStatus(d.kb_item_status_id),
    is_valid_from: d.is_valid_from ?? null, is_valid_to: d.is_valid_to ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    currency_id: d.currency_id ?? null, kb_item_status: d.kb_item_status_id ?? null,
    api_reference: d.api_reference ?? null,
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncOrders(ctx) {
  const data = await bexioGet("/2.0/kb_order", ctx.pat);
  return await supabaseUpsert("orders", data.map(d => ({
    id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
    status: d.kb_item_status_id === 6 ? "completed" : "open",
    is_valid_from: d.is_valid_from ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    currency_id: d.currency_id ?? null, kb_item_status: d.kb_item_status_id ?? null,
    api_reference: d.api_reference ?? null,
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncBills(ctx) {
  const data = await bexioGet("/4.0/purchase/bills", ctx.pat);
  return await supabaseUpsert("bills", data.map(d => ({
    id: String(d.id), document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.vendor_ref ?? null, status: d.status ?? "draft",
    bill_date: d.bill_date ?? null, due_date: d.due_date ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"), total_paid: parseFloat(d.amount_paid ?? "0"),
    currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
    pending_amount: parseFloat(d.pending_amount ?? "0"),
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncCurrencies(ctx) {
  const data = await bexioGet("/3.0/currencies", ctx.pat);
  return await supabaseUpsert("currencies", data.map(d => ({
    id: d.id, name: d.name, round_factor: d.round_factor ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncTaxes(ctx) {
  const data = await bexioGet("/3.0/taxes", ctx.pat);
  return await supabaseUpsert("taxes", data.map(d => ({
    id: d.id, uuid: d.uuid ?? null, name: d.name ?? d.code ?? `tax_${d.id}`,
    code: d.code ?? null, digit: d.digit ?? null, tax_type: d.type ?? null,
    tax_rate: d.value ?? 0, start_year: d.start_year ?? null, end_year: d.end_year ?? null,
    is_active: d.is_active ?? true, account_id: d.account_id ?? null,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncAccountGroups(ctx) {
  const data = await bexioGet("/2.0/account_groups", ctx.pat);
  return await supabaseUpsert("account_groups", data.map(d => ({
    id: d.id, account_no: d.account_no, name: d.name,
    parent_id: d.parent_id ?? null, is_active: d.is_active ?? true,
    synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncAccounts(ctx) {
  const data = await bexioGet("/2.0/accounts", ctx.pat);
  return await supabaseUpsert("accounts", data.map(d => ({
    id: d.id, account_no: d.account_no, name: d.name,
    account_group_id: d.account_group_id ?? null, account_class: d.account_class ?? 0,
    account_type: d.account_type ?? 0, tax_id: d.tax_id ?? null,
    is_active: d.is_active ?? true, synced_at: new Date().toISOString(),
  })), ctx);
}

async function syncBankAccounts(ctx) {
  const data = await bexioGet("/3.0/banking/accounts", ctx.pat);
  return await supabaseUpsert("bank_accounts", data.map(d => ({
    id: d.id, name: d.name, account_no: d.account_no ?? null,
    iban: d.iban ?? null, bic: d.bic ?? null, bc_nr: d.bc_nr ?? null,
    bank_name: d.bank_name ?? null, currency_id: d.currency_id ?? null,
    account_id: d.account_id ?? null, is_default: d.is_default ?? false,
    synced_at: new Date().toISOString(),
  })), ctx);
}

// ── Orchestrator ────────────────────────────────────────────

async function runSync(ctx, mode) {
  const results = [];
  const t0 = Date.now();

  const tasks = [
    ["contacts", syncContacts],
    ["invoices", syncInvoices],
    ["quotes", syncQuotes],
    ["orders", syncOrders],
    ["bills", syncBills],
  ];

  if (mode === "full") {
    tasks.unshift(
      ["currencies", syncCurrencies], ["taxes", syncTaxes],
      ["account_groups", syncAccountGroups], ["accounts", syncAccounts],
      ["bank_accounts", syncBankAccounts],
    );
  }

  for (const [name, fn] of tasks) {
    const start = Date.now();
    try {
      const n = await fn(ctx);
      await logSync(name, "success", n, start, ctx);
      results.push({ entity: name, status: "success", records: n });
      console.log(`  ✅ ${name}: ${n} (${((Date.now()-start)/1000).toFixed(1)}s)`);
    } catch (e) {
      await logSync(name, "error", 0, start, ctx, e.message);
      results.push({ entity: name, status: "error", records: 0, error: e.message });
      console.error(`  ❌ ${name}: ${e.message}`);
    }
  }

  const total = results.reduce((s, r) => s + r.records, 0);
  const errs = results.some(r => r.status === "error");
  const dur = ((Date.now() - t0) / 1000).toFixed(1);
  await logSync("sync_" + mode, errs ? "partial" : "success", total, t0, ctx,
    errs ? results.filter(r => r.status === "error").map(r => r.entity).join(", ") : undefined);
  return { results, totalRecords: total, duration: dur, hasErrors: errs };
}

// ── Vercel Handler ──────────────────────────────────────────

export default async function handler(req, res) {
  const secret = process.env.CRON_SECRET;
  if (req.headers["authorization"] !== `Bearer ${secret}` && req.query?.key !== secret) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { BEXIO_PAT: pat, SUPABASE_URL: url, SUPABASE_SERVICE_KEY: key } = process.env;
  if (!pat || !url || !key) return res.status(500).json({ error: "Missing env vars" });

  const ctx = { pat, url, key };
  const mode = req.query?.mode || "standard";

  try {
    const r = await runSync(ctx, mode);
    return res.status(200).json({
      status: r.hasErrors ? "partial" : "success", mode,
      totalRecords: r.totalRecords, duration: `${r.duration}s`,
      timestamp: new Date().toISOString(), details: r.results,
    });
  } catch (e) {
    return res.status(500).json({ status: "error", error: e.message });
  }
}
