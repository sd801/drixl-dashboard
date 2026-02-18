// ================================================================
// bexio → Supabase Auto-Sync (Vercel Cron Job)
// ================================================================
// Einzelne Entity pro Aufruf um 60s Timeout einzuhalten.
//
// Cron (täglich 05:00 UTC):
//   /api/sync → synchronisiert alle Entities nacheinander
//
// Manuell einzelne Entity:
//   /api/sync?key=SECRET&entity=invoices
//
// Manuell alle:
//   /api/sync?key=SECRET
// ================================================================

const BEXIO_API = "https://api.bexio.com";
const DELAY = 200; // ms zwischen bexio API calls
const LIMIT = 500;

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function bexioGet(endpoint, pat) {
  const all = [];
  let offset = 0;
  while (true) {
    const url = new URL(`${BEXIO_API}${endpoint}`);
    url.searchParams.set("limit", String(LIMIT));
    url.searchParams.set("offset", String(offset));
    await sleep(DELAY);
    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json", Authorization: `Bearer ${pat}` },
    });
    if (!res.ok) {
      if (res.status === 404 || res.status === 500) return [];
      const body = await res.text();
      throw new Error(`bexio ${endpoint} → ${res.status}`);
    }
    const data = await res.json();
    if (!Array.isArray(data)) {
      return (data && Array.isArray(data.data)) ? data.data : [];
    }
    all.push(...data);
    if (data.length < LIMIT) break;
    offset += LIMIT;
  }
  return all;
}

async function upsert(table, rows, ctx) {
  if (!rows.length) return 0;
  let n = 0;
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
    if (!res.ok) throw new Error(`if (!res.ok) throw new Error(`Supabase ${table} → ${res.status}`); → ${res.status}`);
    n += chunk.length;
  }
  return n;
}

async function log(entity, status, n, t0, ctx, err) {
  try {
    await upsert("sync_log", [{
      entity, status, records_fetched: n, records_upserted: n,
      duration_ms: Date.now() - t0, error_message: err || null,
      started_at: new Date(t0).toISOString(), finished_at: new Date().toISOString(),
    }], ctx);
  } catch {}
}

// ── Entity Sync Functions ───────────────────────────────────

const SYNC = {
  contacts: async (ctx) => {
    const data = await bexioGet("/2.0/contact", ctx.pat);
    return upsert("contacts", data.map(d => ({
      id: d.id, nr: d.nr ?? null, contact_type: d.contact_type_id ?? null,
      name_1: d.name_1, name_2: d.name_2 ?? null, email: d.mail ?? null,
      phone: d.phone_fixed ?? null, address: d.address ?? null,
      postcode: d.postcode ?? null, city: d.city ?? null, country_id: d.country_id ?? null,
      is_customer: d.contact_group_ids?.includes?.(1) ?? false,
      is_supplier: d.contact_group_ids?.includes?.(2) ?? false,
      owner_id: d.user_id ?? null, updated_at: d.updated_at ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  invoices: async (ctx) => {
    const IS = {7:"draft",8:"pending",9:"paid",16:"partial",19:"cancelled"};
    const data = await bexioGet("/2.0/kb_invoice", ctx.pat);
    return upsert("invoices", data.map(d => ({
      id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
      contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
      status: IS[d.kb_item_status_id] || "unknown",
      is_valid_from: d.is_valid_from ?? null, is_valid_to: d.is_valid_to ?? null,
      total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
      total_taxes: parseFloat(d.total_taxes ?? "0"),
      total_received: parseFloat(d.total_received_payments ?? "0"),
      currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
      kb_item_status: d.kb_item_status_id ?? null, api_reference: d.api_reference ?? null,
      created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  quotes: async (ctx) => {
    const QS = {1:"draft",2:"pending",3:"accepted",4:"declined"};
    const data = await bexioGet("/2.0/kb_offer", ctx.pat);
    return upsert("quotes", data.map(d => ({
      id: d.id, document_nr: d.document_nr ?? null, title: d.title ?? null,
      contact_id: d.contact_id ?? null, user_id: d.user_id ?? null,
      status: QS[d.kb_item_status_id] || "unknown",
      is_valid_from: d.is_valid_from ?? null, is_valid_to: d.is_valid_to ?? null,
      total_gross: parseFloat(d.total_gross ?? "0"), total_net: parseFloat(d.total_net ?? "0"),
      total_taxes: parseFloat(d.total_taxes ?? "0"),
      currency_id: d.currency_id ?? null, kb_item_status: d.kb_item_status_id ?? null,
      api_reference: d.api_reference ?? null,
      created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  orders: async (ctx) => {
    const data = await bexioGet("/2.0/kb_order", ctx.pat);
    return upsert("orders", data.map(d => ({
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
  },

  bills: async (ctx) => {
    const data = await bexioGet("/4.0/purchase/bills", ctx.pat);
    return upsert("bills", data.map(d => ({
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
  },

  // ── Accounting Endpoints (korrigiert: underscores statt hyphens) ──

  accounts: async (ctx) => {
    const data = await bexioGet("/2.0/accounts", ctx.pat);
    return upsert("accounts", data.map(d => ({
      id: d.id, account_no: d.account_no ?? null, name: d.name ?? null,
      account_group_id: d.account_group_id ?? null,
      account_type: d.account_type ?? null,
      tax_id: d.tax_id ?? null, is_active: d.is_active ?? true,
      is_locked: d.is_locked ?? false,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  account_groups: async (ctx) => {
    // KORREKTUR: /2.0/account_groups (nicht account-groups!)
    const data = await bexioGet("/2.0/account_groups", ctx.pat);
    return upsert("account_groups", data.map(d => ({
      id: d.id, account_no: d.account_no ?? null, name: d.name ?? null,
      parent_id: d.parent_id ?? null,
      is_active: d.is_active ?? true, is_locked: d.is_locked ?? false,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  manual_entries: async (ctx) => {
    // KORREKTUR: /3.0/accounting/manual_entries (nicht manual-entries!)
    const data = await bexioGet("/3.0/accounting/manual_entries", ctx.pat);
    return upsert("manual_entries", data.map(d => ({
      id: d.id, type: d.type ?? null, date: d.date ?? null,
      reference_nr: d.reference_nr ?? null,
      total_debit: parseFloat(d.total_debit ?? "0"),
      total_credit: parseFloat(d.total_credit ?? "0"),
      currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
      is_locked: d.is_locked ?? false,
      entries: JSON.stringify(d.entries ?? []),
      created_at: d.created_at ?? null, updated_at: d.updated_at ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  business_years: async (ctx) => {
    // KORREKTUR: /3.0/accounting/business_years (nicht business-years!)
    const data = await bexioGet("/3.0/accounting/business_years", ctx.pat);
    return upsert("business_years", data.map(d => ({
      id: d.id, start: d.start ?? null, end: d.end ?? null,
      is_vat_subject: d.is_vat_subject ?? false,
      is_closed: d.is_closed ?? false,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  calendar_years: async (ctx) => {
    // KORREKTUR: /3.0/accounting/calendar_years (nicht calendar-years!)
    const data = await bexioGet("/3.0/accounting/calendar_years", ctx.pat);
    return upsert("calendar_years", data.map(d => ({
      id: d.id, start: d.start ?? null, end: d.end ?? null,
      is_vat_subject: d.is_vat_subject ?? false,
      is_annual_reporting: d.is_annual_reporting ?? false,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  vat_periods: async (ctx) => {
    // KORREKTUR: /3.0/accounting/vat_periods (nicht vat-periods!)
    const data = await bexioGet("/3.0/accounting/vat_periods", ctx.pat);
    return upsert("vat_periods", data.map(d => ({
      id: d.id, start: d.start ?? null, end: d.end ?? null,
      calendar_year_id: d.calendar_year_id ?? null,
      is_closed: d.is_closed ?? false,
      annual: d.annual ?? false,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  taxes: async (ctx) => {
    const data = await bexioGet("/2.0/taxes", ctx.pat);
    return upsert("taxes", data.map(d => ({
      id: d.id, uuid: d.uuid ?? null, name: d.name ?? null, code: d.code ?? null,
      digit: d.digit ?? null, type: d.type ?? null,
      account_id: d.account_id ?? null, tax_settlement_type: d.tax_settlement_type ?? null,
      value: parseFloat(d.value ?? "0"),
      net_tax_value: d.net_tax_value != null ? parseFloat(d.net_tax_value) : null,
      start_year: d.start_year ?? null, end_year: d.end_year ?? null,
      is_active: d.is_active ?? true, display_name: d.display_name ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  journal: async (ctx) => {
    const data = await bexioGet("/3.0/accounting/journal", ctx.pat);
    return upsert("journal_entries", data.map(d => ({
      id: d.id, date: d.date ?? null, reference_nr: d.reference_nr ?? null,
      description: d.description ?? null,
      debit_account_id: d.debit_account_id ?? null,
      credit_account_id: d.credit_account_id ?? null,
      tax_id: d.tax_id ?? null, tax_account_id: d.tax_account_id ?? null,
      amount: parseFloat(d.amount ?? "0"),
      tax_amount: d.tax_amount != null ? parseFloat(d.tax_amount) : null,
      currency_id: d.currency_id ?? null, currency_code: d.currency_code ?? "CHF",
      base_currency_id: d.base_currency_id ?? null,
      currency_factor: d.currency_factor != null ? parseFloat(d.currency_factor) : null,
      base_currency_amount: d.base_currency_amount != null ? parseFloat(d.base_currency_amount) : null,
      source: d.source ?? null, ref_uuid: d.ref_uuid ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  // ── Payroll Endpoints ──

  employees: async (ctx) => {
    const data = await bexioGet("/4.0/payroll/employees", ctx.pat);
    return upsert("employees", data.map(d => ({
      id: String(d.id), first_name: d.first_name ?? null, last_name: d.last_name ?? null,
      email: d.email ?? null, department: d.department ?? null,
      employment_type: d.employment_type ?? null,
      salary: d.salary != null ? parseFloat(d.salary) : null,
      entry_date: d.entry_date ?? null, exit_date: d.exit_date ?? null,
      is_active: d.exit_date == null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },

  bank_accounts: async (ctx) => {
    const data = await bexioGet("/3.0/banking/accounts", ctx.pat);
    return upsert("bank_accounts", data.map(d => ({
      id: d.id, name: d.name ?? null, iban: d.iban ?? null,
      account_no: d.account_no ?? null, bc_nr: d.bc_nr ?? null,
      currency_id: d.currency_id ?? null, account_id: d.account_id ?? null,
      is_default: d.is_default ?? false, type: d.type ?? null,
      synced_at: new Date().toISOString(),
    })), ctx);
  },
};

// ── Handler ─────────────────────────────────────────────────

export default async function handler(req, res) {
  const secret = process.env.CRON_SECRET;
  if (req.headers["authorization"] !== `Bearer ${secret}` && req.query?.key !== secret) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { BEXIO_PAT: pat, SUPABASE_URL: url, SUPABASE_SERVICE_KEY: key } = process.env;
  if (!pat || !url || !key) return res.status(500).json({ error: "Missing env vars" });
  const ctx = { pat, url, key };

  const entity = req.query?.entity;

  // ── Einzelne Entity ──
  if (entity && SYNC[entity]) {
    const t0 = Date.now();
    try {
      const n = await SYNC[entity](ctx);
      await log(entity, "success", n, t0, ctx);
      return res.status(200).json({ status: "success", entity, records: n, duration: `${((Date.now()-t0)/1000).toFixed(1)}s` });
    } catch (e) {
      await log(entity, "error", 0, t0, ctx, e.message);
      return res.status(500).json({ status: "error", entity, error: e.message });
    }
  }

  // ── Alle Entities nacheinander (Cron oder manuell) ──
  const results = [];
  const t0 = Date.now();
  const entities = ["invoices", "quotes", "orders", "bills", "accounts", "account_groups", "manual_entries", "business_years", "calendar_years", "vat_periods", "taxes", "journal", "bank_accounts"];
  // contacts & employees excluded from daily sync (rarely change)
  // sync manually: /api/sync?key=SECRET&entity=contacts
  //                /api/sync?key=SECRET&entity=employees

  for (const name of entities) {
    const start = Date.now();
    try {
      const n = await SYNC[name](ctx);
      await log(name, "success", n, start, ctx);
      results.push({ entity: name, status: "success", records: n, duration: `${((Date.now()-start)/1000).toFixed(1)}s` });
      console.log(`✅ ${name}: ${n} (${((Date.now()-start)/1000).toFixed(1)}s)`);
    } catch (e) {
      await log(name, "error", 0, start, ctx, e.message);
      results.push({ entity: name, status: "error", error: e.message });
      console.error(`❌ ${name}: ${e.message}`);
    }
  }

  const total = results.reduce((s, r) => s + (r.records || 0), 0);
  const dur = ((Date.now() - t0) / 1000).toFixed(1);
  await log("full_sync", results.some(r => r.status === "error") ? "partial" : "success", total, t0, ctx);

  return res.status(200).json({
    status: results.some(r => r.status === "error") ? "partial" : "success",
    totalRecords: total, duration: `${dur}s`,
    timestamp: new Date().toISOString(), details: results,
  });
}
