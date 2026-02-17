// ================================================================
// bexio → Supabase Auto-Sync (Vercel Cron Job)
// ================================================================
// Läuft täglich um 05:00 UTC (06:00 Zürich) via Vercel Cron
// Manuell aufrufbar via GET /api/sync?key=CRON_SECRET
// ================================================================

const BEXIO_API = "https://api.bexio.com";
const RATE_LIMIT_DELAY_MS = 650;
const MAX_LIMIT = 500;

// ── Helpers ──────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function bexioGet(endpoint, pat, params) {
  const allResults = [];
  let offset = 0;

  while (true) {
    const url = new URL(`${BEXIO_API}${endpoint}`);
    url.searchParams.set("limit", String(MAX_LIMIT));
    url.searchParams.set("offset", String(offset));
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(k, v);
      }
    }

    await sleep(RATE_LIMIT_DELAY_MS);

    const res = await fetch(url.toString(), {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${pat}`,
      },
    });

    if (!res.ok) {
      const body = await res.text();
      if (res.status === 404) {
        console.warn(`  ⚠️  ${endpoint} → 404 (übersprungen)`);
        return [];
      }
      if (res.status === 500) {
        console.warn(`  ⚠️  ${endpoint} → 500 (bexio Server-Fehler, übersprungen)`);
        return [];
      }
      throw new Error(`bexio ${endpoint} → ${res.status}: ${body.substring(0, 200)}`);
    }

    const data = await res.json();

    if (!Array.isArray(data)) {
      if (data && Array.isArray(data.data)) {
        return data.data;
      }
      return [];
    }

    allResults.push(...data);
    if (data.length < MAX_LIMIT) break;
    offset += MAX_LIMIT;
  }

  return allResults;
}

async function supabaseUpsert(table, rows, supabaseUrl, supabaseKey) {
  if (rows.length === 0) return 0;

  const BATCH = 500;
  let upserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    const url = `${supabaseUrl}/rest/v1/${table}`;

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: supabaseKey,
        Authorization: `Bearer ${supabaseKey}`,
        Prefer: "resolution=merge-duplicates",
      },
      body: JSON.stringify(chunk),
    });

    if (!res.ok) {
      const body = await res.text();
      throw new Error(`Supabase ${table} → ${res.status}: ${body.substring(0, 200)}`);
    }
    upserted += chunk.length;
  }

  return upserted;
}

async function logSync(entity, status, fetched, upserted, startMs, supabaseUrl, supabaseKey, error) {
  try {
    await supabaseUpsert("sync_log", [{
      entity,
      status,
      records_fetched: fetched,
      records_upserted: upserted,
      duration_ms: Date.now() - startMs,
      error_message: error || null,
      started_at: new Date(startMs).toISOString(),
      finished_at: new Date().toISOString(),
    }], supabaseUrl, supabaseKey);
  } catch (e) {
    console.error(`  ⚠️ logSync failed for ${entity}: ${e.message}`);
  }
}


// ████████████████████████████████████████████████████████████████
// SYNC MODULES
// ████████████████████████████████████████████████████████████████

function mapInvoiceStatus(statusId) {
  switch (statusId) {
    case 7: return "draft";
    case 8: return "pending";
    case 9: return "paid";
    case 16: return "partial";
    case 19: return "cancelled";
    default: return "unknown";
  }
}

function mapQuoteStatus(statusId) {
  switch (statusId) {
    case 1: return "draft";
    case 2: return "pending";
    case 3: return "accepted";
    case 4: return "declined";
    default: return "unknown";
  }
}

// Each sync function returns { entity, status, records, error? }
async function syncEntity(name, fn, ctx) {
  const start = Date.now();
  try {
    const n = await fn(ctx, start);
    console.log(`  ✅ ${name}: ${n} records`);
    return { entity: name, status: "success", records: n };
  } catch (e) {
    console.error(`  ❌ ${name}: ${e.message}`);
    return { entity: name, status: "error", records: 0, error: e.message };
  }
}

// ── Individual sync functions ──

async function syncCurrencies(ctx, start) {
  const data = await bexioGet("/3.0/currencies", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, name: d.name, round_factor: d.round_factor ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("currencies", rows, ctx.url, ctx.key);
  await logSync("currencies", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncTaxes(ctx, start) {
  const data = await bexioGet("/3.0/taxes", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, uuid: d.uuid ?? null, name: d.name ?? d.code ?? `tax_${d.id}`,
    code: d.code ?? null, digit: d.digit ?? null, tax_type: d.type ?? null,
    tax_rate: d.value ?? 0, start_year: d.start_year ?? null, end_year: d.end_year ?? null,
    is_active: d.is_active ?? true, account_id: d.account_id ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("taxes", rows, ctx.url, ctx.key);
  await logSync("taxes", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncAccountGroups(ctx, start) {
  const data = await bexioGet("/2.0/account_groups", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, account_no: d.account_no, name: d.name,
    parent_id: d.parent_id ?? null, is_active: d.is_active ?? true,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("account_groups", rows, ctx.url, ctx.key);
  await logSync("account_groups", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncAccounts(ctx, start) {
  const data = await bexioGet("/2.0/accounts", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, account_no: d.account_no, name: d.name,
    account_group_id: d.account_group_id ?? null, account_class: d.account_class ?? 0,
    account_type: d.account_type ?? 0, tax_id: d.tax_id ?? null,
    is_active: d.is_active ?? true, synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("accounts", rows, ctx.url, ctx.key);
  await logSync("accounts", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncContacts(ctx, start) {
  const data = await bexioGet("/2.0/contact", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, nr: d.nr ?? null, contact_type: d.contact_type_id ?? null,
    name_1: d.name_1, name_2: d.name_2 ?? null, email: d.mail ?? null,
    phone: d.phone_fixed ?? null, address: d.address ?? null,
    postcode: d.postcode ?? null, city: d.city ?? null,
    country_id: d.country_id ?? null,
    is_customer: d.contact_group_ids?.includes?.(1) ?? false,
    is_supplier: d.contact_group_ids?.includes?.(2) ?? false,
    owner_id: d.user_id ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("contacts", rows, ctx.url, ctx.key);
  await logSync("contacts", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncInvoices(ctx, start) {
  const data = await bexioGet("/2.0/kb_invoice", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
    status: mapInvoiceStatus(d.kb_item_status_id),
    is_valid_from: d.is_valid_from ?? null, is_valid_to: d.is_valid_to ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    total_received: parseFloat(d.total_received_payments ?? "0"),
    currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
    kb_item_status: d.kb_item_status_id ?? null, api_reference: d.api_reference ?? null,
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("invoices", rows, ctx.url, ctx.key);
  await logSync("invoices", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncQuotes(ctx, start) {
  const data = await bexioGet("/2.0/kb_offer", ctx.pat);
  const rows = data.map(d => ({
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
  }));
  const n = await supabaseUpsert("quotes", rows, ctx.url, ctx.key);
  await logSync("quotes", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncOrders(ctx, start) {
  const data = await bexioGet("/2.0/kb_order", ctx.pat);
  const rows = data.map(d => ({
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
  }));
  const n = await supabaseUpsert("orders", rows, ctx.url, ctx.key);
  await logSync("orders", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncBills(ctx, start) {
  const data = await bexioGet("/4.0/purchase/bills", ctx.pat);
  const rows = data.map(d => ({
    id: String(d.id), document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.vendor_ref ?? null, status: d.status ?? "draft",
    bill_date: d.bill_date ?? null, due_date: d.due_date ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    total_paid: parseFloat(d.amount_paid ?? "0"),
    currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
    pending_amount: parseFloat(d.pending_amount ?? "0"),
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("bills", rows, ctx.url, ctx.key);
  await logSync("bills", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncExpenses(ctx, start) {
  const data = await bexioGet("/4.0/purchase/expenses", ctx.pat);
  const rows = data.map(d => ({
    id: String(d.id), document_nr: d.document_nr ?? null, title: d.title ?? null,
    contact_id: d.vendor_ref ?? null, status: d.status ?? "draft",
    expense_date: d.expense_date ?? null, due_date: d.due_date ?? null,
    total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
    total_taxes: parseFloat(d.total_taxes ?? "0"),
    total_paid: parseFloat(d.amount_paid ?? "0"),
    currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
    created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("expenses", rows, ctx.url, ctx.key);
  await logSync("expenses", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncBankAccounts(ctx, start) {
  const data = await bexioGet("/3.0/banking/accounts", ctx.pat);
  const rows = data.map(d => ({
    id: d.id, name: d.name, account_no: d.account_no ?? null,
    iban: d.iban ?? null, bic: d.bic ?? null, bc_nr: d.bc_nr ?? null,
    bank_name: d.bank_name ?? null, currency_id: d.currency_id ?? null,
    account_id: d.account_id ?? null, is_default: d.is_default ?? false,
    synced_at: new Date().toISOString(),
  }));
  const n = await supabaseUpsert("bank_accounts", rows, ctx.url, ctx.key);
  await logSync("bank_accounts", "success", data.length, n, start, ctx.url, ctx.key);
  return n;
}

async function syncIncomingPayments(ctx, start) {
  let totalFetched = 0;
  let totalUpserted = 0;
  // Erst alle Rechnungen holen
  const invoices = await bexioGet("/2.0/kb_invoice", ctx.pat);
  const activeInvoices = invoices.filter(inv =>
    [8, 9, 16].includes(inv.kb_item_status_id)
  );

  for (const inv of activeInvoices) {
    await sleep(RATE_LIMIT_DELAY_MS);
    try {
      const payments = await bexioGet(`/2.0/kb_invoice/${inv.id}/payment`, ctx.pat);
      if (payments.length > 0) {
        const rows = payments.map(p => ({
          id: p.id, invoice_id: inv.id, payment_date: p.date ?? null,
          amount: parseFloat(p.value ?? "0"), bank_account_id: p.bank_account_id ?? null,
          title: p.title ?? null, synced_at: new Date().toISOString(),
        }));
        const n = await supabaseUpsert("incoming_payments", rows, ctx.url, ctx.key);
        totalFetched += payments.length;
        totalUpserted += n;
      }
    } catch {
      // Skip individual invoice errors
    }
  }
  await logSync("incoming_payments", "success", totalFetched, totalUpserted, start, ctx.url, ctx.key);
  return totalUpserted;
}


// ████████████████████████████████████████████████████████████████
// ORCHESTRATOR
// ████████████████████████████████████████████████████████████████

async function runFullSync(ctx) {
  const results = [];
  const startFull = Date.now();
  console.log("🔄 Starting full bexio → Supabase sync...\n");

  // Phase 1: Stammdaten
  for (const [name, fn] of [
    ["currencies", syncCurrencies],
    ["taxes", syncTaxes],
  ]) {
    results.push(await syncEntity(name, fn, ctx));
  }

  // Phase 2: Abhängig von Stammdaten
  for (const [name, fn] of [
    ["account_groups", syncAccountGroups],
    ["contacts", syncContacts],
  ]) {
    results.push(await syncEntity(name, fn, ctx));
  }

  // Phase 3: Geschäftsdaten
  for (const [name, fn] of [
    ["accounts", syncAccounts],
    ["invoices", syncInvoices],
    ["quotes", syncQuotes],
    ["orders", syncOrders],
    ["bills", syncBills],
    ["expenses", syncExpenses],
    ["bank_accounts", syncBankAccounts],
  ]) {
    results.push(await syncEntity(name, fn, ctx));
  }

  // Phase 4: Abhängig von Rechnungen
  for (const [name, fn] of [
    ["incoming_payments", syncIncomingPayments],
  ]) {
    results.push(await syncEntity(name, fn, ctx));
  }

  // Log full sync
  const totalRecords = results.reduce((sum, r) => sum + r.records, 0);
  const hasErrors = results.some(r => r.status === "error");
  await logSync("full_sync", hasErrors ? "partial" : "success",
    totalRecords, totalRecords, startFull, ctx.url, ctx.key,
    hasErrors ? results.filter(r => r.status === "error").map(r => r.entity).join(", ") : undefined);

  const duration = ((Date.now() - startFull) / 1000).toFixed(1);
  console.log(`\n${hasErrors ? "⚠️" : "✅"} Sync finished in ${duration}s – ${totalRecords} records (${results.filter(r => r.status === "error").length} errors)`);

  return { results, totalRecords, duration, hasErrors };
}


// ████████████████████████████████████████████████████████████████
// VERCEL HANDLER
// ████████████████████████████████████████████████████████████████

export default async function handler(req, res) {
  // ── Auth: Cron Secret oder manuelle Auth ──
  const cronSecret = process.env.CRON_SECRET;

  // Vercel Cron sendet automatisch Authorization header
  const authHeader = req.headers["authorization"];
  const queryKey = req.query?.key;

  const isVercelCron = authHeader === `Bearer ${cronSecret}`;
  const isManualTrigger = queryKey === cronSecret;

  if (!isVercelCron && !isManualTrigger) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  // ── Env Check ──
  const pat = process.env.BEXIO_PAT;
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_KEY;

  if (!pat || !supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Missing environment variables" });
  }

  const ctx = { pat, url: supabaseUrl, key: supabaseKey };

  try {
    console.log(`\n${"═".repeat(50)}`);
    console.log(`  bexio → Supabase Sync (${new Date().toISOString()})`);
    console.log(`${"═".repeat(50)}\n`);

    const result = await runFullSync(ctx);

    return res.status(200).json({
      status: result.hasErrors ? "partial" : "success",
      totalRecords: result.totalRecords,
      duration: `${result.duration}s`,
      timestamp: new Date().toISOString(),
      details: result.results,
    });
  } catch (e) {
    console.error("💥 Fatal sync error:", e.message);
    return res.status(500).json({
      status: "error",
      error: e.message,
      timestamp: new Date().toISOString(),
    });
  }
}
