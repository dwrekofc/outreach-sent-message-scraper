// ==UserScript==
// @name         Outreach V2 - One Go Links + Bodies Export
// @namespace    https://web.outreach.io/
// @version      2.0.0
// @description  Collect message links by scrolling/pagination, download links CSV, then auto-fetch message bodies and download final CSV.
// @match        https://web.outreach.io/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
  "use strict";

  const UI_ID = "or-v2-ui";
  const SCRIPT_VERSION = "2.0.0";
  const DEBUG = true;

  // Outreach GraphQL endpoint used by the existing exporter.
  const GQL_URL =
    "https://app2b.outreach.io/graphql/Messages_GetThreadMessages";
  const OP_NAME = "Messages_GetThreadMessages";
  const SHA256 =
    "78b5e932965708618acbc1e64a0e6e4a4ff175ed5124adcbaba66b82f2154960";

  const REQUIRED_LINK_COLS = [
    "sequence_id",
    "prospect_id",
    "message_id_encoded",
    "message_url",
  ];

  const state = {
    running: false,
    stopRequested: false,
    bearer: null,
    linksMap: new Map(),
    linksRows: [],
    bodyRows: [],
    generatedLinksCsv: "",
    viewStats: new Map(),
    lastPageSignature: "",
  };

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function ts() {
    return new Date().toISOString();
  }

  function safeStringify(obj) {
    try {
      return JSON.stringify(obj);
    } catch {
      return String(obj);
    }
  }

  function log(msg, obj) {
    const line =
      `[OR-V2][${ts()}] ${msg}` + (obj ? ` | ${safeStringify(obj)}` : "");
    if (DEBUG) console.log(line);
    const box = document.querySelector("#or-v2-logbox");
    if (box) {
      box.value += line + "\n";
      box.scrollTop = box.scrollHeight;
    }
  }

  function setStatus(msg) {
    const el = document.querySelector("#or-v2-status");
    if (el) el.textContent = msg;
    log(`STATUS: ${msg}`);
  }

  function setProgress(msg) {
    const el = document.querySelector("#or-v2-progress");
    if (el) el.textContent = msg;
    log(`PROGRESS: ${msg}`);
  }

  function nowStamp() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(
      d.getHours(),
    )}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
  }

  function csvEscape(value) {
    const s = String(value ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  function downloadText(filename, text, mime = "text/csv;charset=utf-8") {
    const blob = new Blob([text], { type: mime });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    log("Downloaded file", { filename, chars: text.length });
  }

  function parseCsv(text) {
    const rows = [];
    let i = 0;
    let field = "";
    let row = [];
    let inQuotes = false;

    while (i < text.length) {
      const c = text[i];
      if (inQuotes) {
        if (c === '"') {
          if (text[i + 1] === '"') {
            field += '"';
            i += 2;
            continue;
          }
          inQuotes = false;
          i += 1;
          continue;
        }
        field += c;
        i += 1;
        continue;
      }

      if (c === '"') {
        inQuotes = true;
        i += 1;
        continue;
      }
      if (c === ",") {
        row.push(field);
        field = "";
        i += 1;
        continue;
      }
      if (c === "\n") {
        row.push(field);
        rows.push(row);
        row = [];
        field = "";
        i += 1;
        continue;
      }
      if (c === "\r") {
        i += 1;
        continue;
      }
      field += c;
      i += 1;
    }

    row.push(field);
    rows.push(row);

    const header = (rows.shift() || []).map((h) => String(h || "").trim());
    const outRows = rows
      .filter((r) => r.some((x) => String(x || "").trim() !== ""))
      .map((r) => {
        const obj = {};
        for (let j = 0; j < header.length; j += 1) obj[header[j]] = r[j] ?? "";
        return obj;
      });

    return { header, rows: outRows };
  }

  function normalizeMessageIdForCompare(v) {
    const s = String(v || "").trim();
    if (!s) return "";
    try {
      return decodeURIComponent(s).trim().toLowerCase();
    } catch {
      return s.toLowerCase();
    }
  }

  function urlDecodeMessageId(messageIdEncoded) {
    try {
      return decodeURIComponent(String(messageIdEncoded || ""));
    } catch {
      return String(messageIdEncoded || "");
    }
  }

  function validateLinksCsv(header, rows) {
    for (const col of REQUIRED_LINK_COLS) {
      if (!header.includes(col)) return `Missing required column: ${col}`;
    }
    if (!rows.length) return "No rows found in links CSV.";
    return null;
  }

  function getSeqIdFromUrl(urlObj) {
    const path = urlObj.pathname || "";
    const m = path.match(/\/sequences\/(\d+)/i);
    if (m) return m[1];

    const qp =
      urlObj.searchParams.get("sequence_id") ||
      urlObj.searchParams.get("sequenceId") ||
      "";
    return String(qp || "").trim();
  }

  function getProspectIdFromUrl(urlObj) {
    const path = decodeURIComponent(urlObj.pathname || "");
    const m = path.match(/\/prospects\/(\d+)/i);
    if (m) return m[1];

    const qp =
      urlObj.searchParams.get("prospect_id") ||
      urlObj.searchParams.get("prospectId") ||
      "";
    return String(qp || "").trim();
  }

  function getMessageIdEncodedFromUrl(urlObj) {
    const keys = [
      "message_id_encoded",
      "messageId",
      "message_id",
      "mid",
      "thread_message_id",
      "threadMessageId",
    ];

    for (const key of keys) {
      const v = urlObj.searchParams.get(key);
      if (v && String(v).trim()) return String(v).trim();
    }

    const pathRaw = String(urlObj.pathname || "");
    const regexes = [
      /\/messages?\/([^/?#]+)/i,
      /\/mail\/([^/?#]+)/i,
      /\/thread(?:s)?\/([^/?#]+)/i,
      /\/emails?\/([^/?#]+)/i,
    ];

    for (const re of regexes) {
      const m = pathRaw.match(re);
      if (!m || !m[1]) continue;
      const raw = String(m[1]).trim();
      if (!raw) continue;
      return raw.includes("%") ? raw : encodeURIComponent(raw);
    }

    return "";
  }

  function normalizeAbsoluteMessageUrl(urlObj) {
    const u = new URL(urlObj.toString());
    u.hash = "";
    return `${u.origin}${u.pathname}${u.search}`;
  }

  function parseMessageLink(href) {
    let urlObj;
    try {
      urlObj = new URL(href, location.origin);
    } catch {
      return null;
    }

    if (!/outreach\.io$/i.test(urlObj.hostname)) return null;

    const decodedPath = decodeURIComponent(urlObj.pathname || "").toLowerCase();
    const hasProspectPath = decodedPath.includes("/prospects/");
    const hasMessageHint =
      decodedPath.includes("/message") ||
      decodedPath.includes("/mail") ||
      decodedPath.includes("/thread") ||
      urlObj.searchParams.has("messageId") ||
      urlObj.searchParams.has("message_id") ||
      urlObj.searchParams.has("mid");

    if (!hasProspectPath || !hasMessageHint) return null;

    const prospectId = getProspectIdFromUrl(urlObj);
    const messageIdEncoded = getMessageIdEncodedFromUrl(urlObj);

    if (!prospectId || !messageIdEncoded) return null;

    const seqId = getSeqIdFromUrl(urlObj) || getSeqIdFromUrl(new URL(location.href));
    return {
      sequence_id: seqId,
      prospect_id: prospectId,
      message_id_encoded: messageIdEncoded,
      message_url: normalizeAbsoluteMessageUrl(urlObj),
    };
  }

  function addLinkRow(row, source) {
    const key =
      `${row.prospect_id}|${normalizeMessageIdForCompare(row.message_id_encoded)}`;
    if (!key.trim() || state.linksMap.has(key)) return false;
    state.linksMap.set(key, row);
    log("Collected link", {
      source,
      total: state.linksMap.size,
      prospect_id: row.prospect_id,
      message_id_prefix: row.message_id_encoded.slice(0, 40),
    });
    return true;
  }

  function collectLinksFromDom(source = "scan") {
    let added = 0;
    const anchors = document.querySelectorAll("a[href]");
    for (const a of anchors) {
      const row = parseMessageLink(a.getAttribute("href") || a.href || "");
      if (!row) continue;
      if (addLinkRow(row, source)) added += 1;
    }

    if (added) {
      setProgress(`Links collected: ${state.linksMap.size}`);
    }

    return added;
  }

  function isVisible(el) {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    if (style.display === "none" || style.visibility === "hidden") return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  }

  function detectPageInfo() {
    let currentPage = null;
    let totalPages = null;

    const currentEl = document.querySelector(
      '[aria-current="page"], [data-selected="true"], .Mui-selected',
    );
    if (currentEl) {
      const n = parseInt(String(currentEl.textContent || "").trim(), 10);
      if (Number.isFinite(n) && n > 0) currentPage = n;
    }

    const pagerContainers = Array.from(
      document.querySelectorAll(
        [
          'nav[aria-label*="pagination" i]',
          '[class*="pagination" i]',
          '[data-testid*="pagination" i]',
          '[aria-label*="page" i]',
        ].join(","),
      ),
    );

    for (const container of pagerContainers) {
      const text = String(container.textContent || " ").replace(/\s+/g, " ");
      const m = text.match(/page\s*(\d+)\s*of\s*(\d+)/i);
      if (m) {
        const c = parseInt(m[1], 10);
        const t = parseInt(m[2], 10);
        if (Number.isFinite(c) && c > 0) currentPage = c;
        if (Number.isFinite(t) && t > 0) totalPages = Math.max(totalPages || 0, t);
      }

      const pageNums = Array.from(
        container.querySelectorAll("button, a, [role='button']"),
      )
        .filter((el) => isVisible(el))
        .map((el) => parseInt(String(el.textContent || "").trim(), 10))
        .filter((n) => Number.isFinite(n) && n > 0);

      if (pageNums.length) {
        const maxN = Math.max(...pageNums);
        totalPages = Math.max(totalPages || 0, maxN);
      }
    }

    if (!currentPage || !totalPages) {
      const bodyText = String(document.body?.innerText || "").replace(/\s+/g, " ");
      const m = bodyText.match(/page\s*(\d+)\s*of\s*(\d+)/i);
      if (m) {
        const c = parseInt(m[1], 10);
        const t = parseInt(m[2], 10);
        if (!currentPage && Number.isFinite(c) && c > 0) currentPage = c;
        if (!totalPages && Number.isFinite(t) && t > 0) totalPages = t;
      }
    }

    return {
      currentPage,
      totalPages,
    };
  }

  function getViewKey() {
    const u = new URL(location.href);
    const pageKeys = ["page", "p", "offset"];
    for (const k of pageKeys) u.searchParams.delete(k);
    return `${u.origin}${u.pathname}?${u.searchParams.toString()}`;
  }

  function updatePageStats() {
    const info = detectPageInfo();
    const viewKey = getViewKey();
    const prev = state.viewStats.get(viewKey) || {
      seenPages: new Set(),
      totalPages: null,
    };

    if (info.currentPage) prev.seenPages.add(info.currentPage);
    if (info.totalPages)
      prev.totalPages = Math.max(prev.totalPages || 0, info.totalPages);

    state.viewStats.set(viewKey, prev);

    const seen = prev.seenPages.size;
    const totalLabel = prev.totalPages ? String(prev.totalPages) : "?";
    setProgress(`View pages seen: ${seen}/${totalLabel} | links: ${state.linksMap.size}`);

    log("Page info", {
      view: viewKey,
      current_page: info.currentPage,
      total_pages: info.totalPages,
      seen_pages: seen,
    });

    return { ...info, seenPages: seen, viewKey };
  }

  function isDisabled(el) {
    if (!el) return true;
    if (el.disabled) return true;
    if (String(el.getAttribute("aria-disabled") || "").toLowerCase() === "true")
      return true;
    const cls = String(el.className || "").toLowerCase();
    if (cls.includes("disabled")) return true;
    return false;
  }

  function findNextPageButton() {
    const pagerContainers = Array.from(
      document.querySelectorAll(
        [
          'nav[aria-label*="pagination" i]',
          '[class*="pagination" i]',
          '[data-testid*="pagination" i]',
        ].join(","),
      ),
    );

    const scopedSelectors = [
      'button[aria-label*="next" i]',
      'a[aria-label*="next" i]',
      'button[title*="next" i]',
      '[data-testid*="next" i]',
      '[class*="next" i][role="button"]',
    ];

    for (const container of pagerContainers) {
      for (const selector of scopedSelectors) {
        const nodes = container.querySelectorAll(selector);
        for (const node of nodes) {
          if (!isVisible(node) || isDisabled(node)) continue;
          return node;
        }
      }
    }

    const selectors = [
      'button[aria-label*="next" i]',
      'a[aria-label*="next" i]',
      'button[title*="next" i]',
      '[data-testid*="next" i]',
      '[class*="next" i][role="button"]',
    ];

    for (const selector of selectors) {
      const nodes = document.querySelectorAll(selector);
      for (const node of nodes) {
        if (!isVisible(node) || isDisabled(node)) continue;
        return node;
      }
    }

    const generic = Array.from(document.querySelectorAll("button, a, [role='button']"));
    for (const node of generic) {
      if (!isVisible(node) || isDisabled(node)) continue;
      const txt = String(node.textContent || "").trim().toLowerCase();
      if (["next", ">", "›", "→", "»"].includes(txt)) return node;
    }

    return null;
  }

  function getPageSignature() {
    const hrefs = Array.from(document.querySelectorAll("a[href]"))
      .slice(0, 20)
      .map((a) => a.getAttribute("href") || a.href || "")
      .join("|");
    const pageInfo = detectPageInfo();
    return `${location.href}::${pageInfo.currentPage || "?"}/${pageInfo.totalPages || "?"}::${hrefs}`;
  }

  async function clickAndWaitForPageChange(btn) {
    const before = getPageSignature();
    state.lastPageSignature = before;

    btn.dispatchEvent(
      new MouseEvent("click", {
        bubbles: true,
        cancelable: true,
        view: window,
      }),
    );

    const started = Date.now();
    while (Date.now() - started < 12000) {
      if (state.stopRequested) return false;
      await sleep(250);
      const after = getPageSignature();
      if (after !== before) {
        log("Page changed after next click");
        return true;
      }
    }

    log("No page change detected after next click", { timeout_ms: 12000 });
    return false;
  }

  async function autoScrollAndCollectCurrentPage() {
    let stablePasses = 0;
    let lastCount = state.linksMap.size;

    for (let i = 0; i < 90; i += 1) {
      if (state.stopRequested) return;

      collectLinksFromDom("scroll-pass");
      window.scrollBy(0, Math.max(480, Math.floor(window.innerHeight * 0.92)));
      await sleep(360);
      collectLinksFromDom("scroll-pass-post");

      const count = state.linksMap.size;
      if (count === lastCount) stablePasses += 1;
      else stablePasses = 0;
      lastCount = count;

      const nearBottom =
        window.scrollY + window.innerHeight >=
        Math.max(document.body?.scrollHeight || 0, document.documentElement?.scrollHeight || 0) -
          40;

      if (stablePasses >= 6 || (nearBottom && stablePasses >= 3)) break;
    }

    window.scrollTo(0, 0);
    await sleep(250);
    collectLinksFromDom("post-scroll");
  }

  function buildLinksCsv(rows) {
    const header = REQUIRED_LINK_COLS;
    const lines = [header.join(",")];
    for (const r of rows) lines.push(header.map((k) => csvEscape(r[k] ?? "")).join(","));
    return lines.join("\n");
  }

  async function collectLinksAcrossPages() {
    const seenPageSignatures = new Set();

    for (let cycle = 0; cycle < 300; cycle += 1) {
      if (state.stopRequested) return;

      const signature = getPageSignature();
      if (seenPageSignatures.has(signature)) {
        log("Page signature already visited; stopping page traversal", {
          cycle,
        });
        break;
      }
      seenPageSignatures.add(signature);

      const pageInfo = updatePageStats();
      setStatus(
        `Phase 1/2: Collecting links${
          pageInfo.currentPage
            ? ` (page ${pageInfo.currentPage}${
                pageInfo.totalPages ? `/${pageInfo.totalPages}` : ""
              })`
            : ""
        }`,
      );

      await autoScrollAndCollectCurrentPage();

      const nextBtn = findNextPageButton();
      if (!nextBtn) {
        log("No enabled next-page button found; collection finished.");
        break;
      }

      const moved = await clickAndWaitForPageChange(nextBtn);
      if (!moved) break;
      await sleep(900);
      collectLinksFromDom("after-next-page");
    }

    state.linksRows = Array.from(state.linksMap.values());
    state.linksRows.sort((a, b) => {
      if (a.sequence_id !== b.sequence_id)
        return String(a.sequence_id).localeCompare(String(b.sequence_id));
      if (a.prospect_id !== b.prospect_id)
        return String(a.prospect_id).localeCompare(String(b.prospect_id));
      return normalizeMessageIdForCompare(a.message_id_encoded).localeCompare(
        normalizeMessageIdForCompare(b.message_id_encoded),
      );
    });

    log("Collection finished", {
      total_links: state.linksRows.length,
      views_seen: state.viewStats.size,
    });
  }

  function looksLikeJwt(s) {
    return (
      typeof s === "string" &&
      s.startsWith("eyJ") &&
      s.split(".").length >= 3 &&
      s.length > 80
    );
  }

  function findBearerInStorage() {
    function scanOidc(storage, label) {
      if (!storage) return null;
      for (let i = 0; i < storage.length; i += 1) {
        const key = storage.key(i);
        if (!key || !key.startsWith("oidc.user:")) continue;
        const raw = storage.getItem(key);
        if (!raw) continue;

        try {
          const obj = JSON.parse(raw);
          if (obj && typeof obj.access_token === "string" && obj.access_token.length > 20) {
            return { token: obj.access_token, hint: `${label}:${key} (access_token)` };
          }
          if (obj && typeof obj.accessToken === "string" && obj.accessToken.length > 20) {
            return { token: obj.accessToken, hint: `${label}:${key} (accessToken)` };
          }
        } catch {
          // ignore broken entries
        }
      }
      return null;
    }

    const oidcSession = scanOidc(window.sessionStorage, "sessionStorage");
    if (oidcSession) return oidcSession;

    const oidcLocal = scanOidc(window.localStorage, "localStorage");
    if (oidcLocal) return oidcLocal;

    const candidates = [];

    function scanStorage(storage, label) {
      if (!storage) return;
      for (let i = 0; i < storage.length; i += 1) {
        const key = storage.key(i);
        if (!key) continue;
        const value = storage.getItem(key);
        if (!value) continue;

        if (looksLikeJwt(value)) candidates.push({ token: value, hint: `${label}:${key}` });

        if (value.trim().startsWith("{") || value.trim().startsWith("[")) {
          try {
            const parsed = JSON.parse(value);
            const stack = [parsed];
            while (stack.length) {
              const cur = stack.pop();
              if (!cur) continue;
              if (typeof cur === "string" && looksLikeJwt(cur)) {
                candidates.push({ token: cur, hint: `${label}:${key} (nested)` });
              } else if (typeof cur === "object") {
                for (const val of Object.values(cur)) stack.push(val);
              }
            }
          } catch {
            // ignore non-json
          }
        }
      }
    }

    scanStorage(window.sessionStorage, "sessionStorage");
    scanStorage(window.localStorage, "localStorage");

    candidates.sort((a, b) => b.token.length - a.token.length);
    const best = candidates[0];
    if (!best) return { token: null, hint: null };
    return best;
  }

  function isPersistedQueryNotFound(json) {
    const msg = json?.errors?.[0]?.message;
    return (
      typeof msg === "string" &&
      msg.toLowerCase().includes("persistedquerynotfound")
    );
  }

  async function gqlPost({ bearer, payload, attempt }) {
    log("GraphQL request", {
      attempt,
      prospect_id: Number(payload?.variables?.prospectId),
      message_id_prefix: String(payload?.variables?.messageId || "").slice(0, 32),
    });

    const t0 = performance.now();
    const resp = await fetch(GQL_URL, {
      method: "POST",
      mode: "cors",
      credentials: "include",
      headers: {
        accept: "*/*",
        "content-type": "application/json",
        "apollographql-client-name": "giraffe",
        authorization: `Bearer ${bearer}`,
      },
      body: JSON.stringify(payload),
    });

    const ms = Math.round(performance.now() - t0);
    const json = await resp.json().catch(() => null);

    log("GraphQL response", {
      attempt,
      http_status: resp.status,
      ok: resp.ok,
      ms,
      has_data: !!json?.data,
      has_errors: Array.isArray(json?.errors) && json.errors.length > 0,
      error0: json?.errors?.[0]?.message
        ? String(json.errors[0].message).slice(0, 180)
        : "",
    });

    return { ok: resp.ok, status: resp.status, json };
  }

  async function gqlGetThreadMessage({ bearer, prospectId, messageId }) {
    const payload = {
      operationName: OP_NAME,
      variables: {
        duplicateEnabled: false,
        messageId: messageId,
        prospectId: Number(prospectId),
        shouldRequestOldMailing: false,
        loadBodyHtmlDiff: true,
      },
      extensions: { persistedQuery: { version: 1, sha256Hash: SHA256 } },
    };

    let resp = await gqlPost({ bearer, payload, attempt: "hash_only" });
    if (!resp.ok || !resp.json) return resp;

    if (isPersistedQueryNotFound(resp.json)) {
      const noPersisted = { ...payload };
      delete noPersisted.extensions;
      log("Persisted query missing; retry without extension");
      resp = await gqlPost({
        bearer,
        payload: noPersisted,
        attempt: "no_persisted_extension",
      });

      if (resp.ok && resp.json && isPersistedQueryNotFound(resp.json)) {
        log("Persisted query still missing; retry hash once");
        await sleep(220);
        resp = await gqlPost({ bearer, payload, attempt: "hash_retry_once" });
      }
    }

    return resp;
  }

  function pickThreadEntryByMessageId(collection, requestedMessageId) {
    if (!Array.isArray(collection) || !collection.length) return null;

    const target = normalizeMessageIdForCompare(requestedMessageId);
    if (!target) return collection[0];

    const exact = collection.find((item) => {
      const id = normalizeMessageIdForCompare(item?.message?.id);
      return id && id === target;
    });
    if (exact) return exact;

    const targetLoose = target.replace(/[<>]/g, "");
    const loose = collection.find((item) => {
      const id = normalizeMessageIdForCompare(item?.message?.id).replace(/[<>]/g, "");
      return id && id === targetLoose;
    });

    return loose || collection[0];
  }

  function extractResult(linkRow, gqlJson, requestedMessageId) {
    const data = gqlJson?.data ?? null;
    const collection = data?.threadMessages?.collection ?? [];
    const first = pickThreadEntryByMessageId(collection, requestedMessageId);

    const msg = first?.message ?? {};
    const outbox = first?.outboxMailingReduced ?? null;

    const bodyText = (
      first?.bodyText ||
      msg.bodyText ||
      first?.messageBodyText ||
      msg.messageBodyText ||
      ""
    ).toString();

    const bodyHtml = (
      first?.bodyHtml ||
      msg.bodyHtml ||
      first?.messageBodyHtml ||
      msg.messageBodyHtml ||
      ""
    ).toString();

    return {
      sequence_id: (linkRow.sequence_id || "").toString(),
      prospect_id: (linkRow.prospect_id || "").toString(),
      message_id: (msg.id || requestedMessageId || "").toString(),
      subject: (msg.subject || "").toString(),
      body_text: bodyText,
      body_html: bodyHtml,
      delivered_at: (outbox?.deliveredAt || msg.deliveredAt || "").toString(),
      opened_at: (outbox?.openedAt || msg.openedAt || "").toString(),
      replied_at: (outbox?.repliedAt || msg.repliedAt || "").toString(),
      open_count: (outbox?.openCount ?? "").toString(),
      click_count: (outbox?.clickCount ?? "").toString(),
      state: (outbox?.state || msg.state || "").toString(),
      message_url: (linkRow.message_url || "").toString(),
      error: "",
    };
  }

  function buildBodiesCsv(rows) {
    const header = [
      "sequence_id",
      "prospect_id",
      "message_id",
      "subject",
      "body_text",
      "body_html",
      "delivered_at",
      "opened_at",
      "replied_at",
      "open_count",
      "click_count",
      "state",
      "message_url",
      "error",
    ];

    const lines = [header.join(",")];
    for (const row of rows) {
      lines.push(header.map((key) => csvEscape(row[key] ?? "")).join(","));
    }
    return lines.join("\n");
  }

  async function runFetchBodiesFromLinksCsv(csvText) {
    const parsed = parseCsv(csvText);
    const validationError = validateLinksCsv(parsed.header, parsed.rows);
    if (validationError) {
      throw new Error(`Generated links CSV invalid: ${validationError}`);
    }

    state.linksRows = parsed.rows.map((row) => ({
      sequence_id: row.sequence_id || "",
      prospect_id: row.prospect_id || "",
      message_id_encoded: row.message_id_encoded || "",
      message_url: row.message_url || "",
    }));

    const bearerFound = state.bearer || findBearerInStorage().token;
    state.bearer = bearerFound;

    if (!state.bearer) {
      throw new Error(
        "Could not find Bearer token in browser storage. Open a message detail page in Outreach and retry.",
      );
    }

    state.bodyRows = [];
    const delayMs = 450;

    setStatus(`Phase 2/2: Fetching message bodies (${state.linksRows.length} rows)`);

    for (let i = 0; i < state.linksRows.length; i += 1) {
      if (state.stopRequested) return;

      const row = state.linksRows[i];
      const prospectId = String(row.prospect_id || "").trim();
      const messageIdEnc = String(row.message_id_encoded || "").trim();
      const messageId = urlDecodeMessageId(messageIdEnc);

      setProgress(`Bodies: row ${i + 1}/${state.linksRows.length} (prospect ${prospectId || "?"})`);

      if (!prospectId || !messageId) {
        state.bodyRows.push({
          sequence_id: row.sequence_id || "",
          prospect_id: prospectId,
          message_id: "",
          subject: "",
          body_text: "",
          body_html: "",
          delivered_at: "",
          opened_at: "",
          replied_at: "",
          open_count: "",
          click_count: "",
          state: "",
          message_url: row.message_url || "",
          error: "missing prospect_id or message_id",
        });
        log("Body fetch skipped", {
          row: i + 1,
          reason: "missing prospect_id or message_id",
        });
        continue;
      }

      const resp = await gqlGetThreadMessage({
        bearer: state.bearer,
        prospectId,
        messageId,
      });

      if (!resp.ok || !resp.json) {
        state.bodyRows.push({
          sequence_id: row.sequence_id || "",
          prospect_id: prospectId,
          message_id: "",
          subject: "",
          body_text: "",
          body_html: "",
          delivered_at: "",
          opened_at: "",
          replied_at: "",
          open_count: "",
          click_count: "",
          state: "",
          message_url: row.message_url || "",
          error: `HTTP ${resp.status}`,
        });
        log("Body fetch failed", { row: i + 1, http_status: resp.status });
      } else {
        const errors = resp.json.errors;
        if (Array.isArray(errors) && errors.length) {
          const err = String(errors[0]?.message || "GraphQL error");
          state.bodyRows.push({
            sequence_id: row.sequence_id || "",
            prospect_id: prospectId,
            message_id: "",
            subject: "",
            body_text: "",
            body_html: "",
            delivered_at: "",
            opened_at: "",
            replied_at: "",
            open_count: "",
            click_count: "",
            state: "",
            message_url: row.message_url || "",
            error: err,
          });
          log("Body fetch error", { row: i + 1, error: err.slice(0, 180) });
        } else {
          const out = extractResult(row, resp.json, messageId);
          state.bodyRows.push(out);
          log("Body extracted", {
            row: i + 1,
            subject_prefix: String(out.subject || "").slice(0, 80),
            body_text_len: String(out.body_text || "").length,
            body_html_len: String(out.body_html || "").length,
          });
        }
      }

      await sleep(delayMs);
    }
  }

  async function runV2() {
    if (state.running) return;
    state.running = true;
    state.stopRequested = false;
    state.linksMap.clear();
    state.linksRows = [];
    state.bodyRows = [];
    state.generatedLinksCsv = "";
    state.viewStats.clear();

    const btnStart = document.querySelector("#or-v2-start");
    const btnStop = document.querySelector("#or-v2-stop");
    if (btnStart) btnStart.disabled = true;
    if (btnStop) btnStop.disabled = false;

    try {
      setStatus("Phase 1/2: Collecting message links (scroll + pages)");
      setProgress("Starting collection scan...");

      collectLinksFromDom("initial");
      await collectLinksAcrossPages();
      collectLinksFromDom("final");

      if (!state.linksRows.length) {
        throw new Error(
          "No message links were collected. Open a messages list view in Outreach and retry.",
        );
      }

      const linksCsv = buildLinksCsv(state.linksRows);
      state.generatedLinksCsv = linksCsv;

      const seqId =
        state.linksRows.find((r) => String(r.sequence_id || "").trim())?.sequence_id ||
        "unknown";
      const linksFilename = `outreach_message_links_v2_seq_${seqId}_${nowStamp()}.csv`;

      setStatus(`Phase 1/2 complete: ${state.linksRows.length} links`);
      setProgress("Downloading links CSV...");
      downloadText(linksFilename, linksCsv);

      setProgress(
        "Using generated links CSV for body scrape (browser cannot directly read Downloads folder).",
      );
      await runFetchBodiesFromLinksCsv(state.generatedLinksCsv);

      if (state.stopRequested) {
        setStatus("Stopped by user.");
        return;
      }

      const bodiesCsv = buildBodiesCsv(state.bodyRows);
      const bodiesFilename = `outreach_message_bodies_v2_seq_${seqId}_${nowStamp()}.csv`;
      downloadText(bodiesFilename, bodiesCsv);

      setStatus(
        `Done. Exported ${state.linksRows.length} links and ${state.bodyRows.length} body rows.`,
      );
      setProgress("Both CSV files were downloaded automatically.");
      log("V2 run complete", {
        links_rows: state.linksRows.length,
        body_rows: state.bodyRows.length,
        links_filename: linksFilename,
        bodies_filename: bodiesFilename,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setStatus(`Error: ${msg}`);
      setProgress("See logs for details.");
      log("Run failed", { error: msg });
    } finally {
      state.running = false;
      if (btnStart) btnStart.disabled = false;
      if (btnStop) btnStop.disabled = true;
    }
  }

  function renderUI() {
    if (document.getElementById(UI_ID)) return;

    const wrap = document.createElement("div");
    wrap.id = UI_ID;
    wrap.style.position = "fixed";
    wrap.style.right = "16px";
    wrap.style.bottom = "16px";
    wrap.style.zIndex = "999999";
    wrap.style.background = "white";
    wrap.style.border = "1px solid #cfcfcf";
    wrap.style.borderRadius = "10px";
    wrap.style.padding = "10px";
    wrap.style.boxShadow = "0 8px 20px rgba(0,0,0,0.15)";
    wrap.style.font = "12px/1.35 system-ui, -apple-system, Segoe UI, Roboto, Arial";
    wrap.style.width = "520px";

    wrap.innerHTML = `
      <div style="font-weight:600; margin-bottom:6px;">Outreach V2 one-go exporter v${SCRIPT_VERSION}</div>
      <div id="or-v2-status" style="margin-bottom:4px; color:#333;">Ready.</div>
      <div id="or-v2-progress" style="margin-bottom:10px; color:#666;"></div>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px;">
        <button id="or-v2-start" style="padding:6px 10px; cursor:pointer;">Start V2</button>
        <button id="or-v2-stop" style="padding:6px 10px; cursor:pointer;" disabled>Stop</button>
      </div>
      <div style="margin-bottom:6px; color:#333; font-weight:600;">Log</div>
      <textarea id="or-v2-logbox" style="width:100%; height:180px; font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:11px; padding:6px;"></textarea>
      <div style="margin-top:8px; color:#666;">
        Flow: collect links (scroll + next page) -> download links CSV -> fetch bodies -> download final CSV.
      </div>
    `;

    document.body.appendChild(wrap);

    const btnStart = wrap.querySelector("#or-v2-start");
    const btnStop = wrap.querySelector("#or-v2-stop");

    btnStart.addEventListener("click", () => {
      log("Button click: Start V2");
      runV2();
    });

    btnStop.addEventListener("click", () => {
      log("Button click: Stop");
      state.stopRequested = true;
      setStatus("Stopping after current step...");
      setProgress("");
    });

    log("UI ready", { version: SCRIPT_VERSION });
  }

  function boot() {
    const start = () => {
      if (!document.body) {
        requestAnimationFrame(start);
        return;
      }
      renderUI();
    };
    start();
  }

  boot();
})();
