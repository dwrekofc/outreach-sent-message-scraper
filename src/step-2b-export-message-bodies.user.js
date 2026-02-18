// ==UserScript==
// @name         Outreach Step 2B - Export Message Bodies (Test B)
// @namespace    https://web.outreach.io/
// @version      1.0
// @description  Step 2B: Upload message-links CSV and export message bodies with DOM-scraped context (sequence, persona, title, markdown).
// @match        https://web.outreach.io/*
// @run-at       document-idle
// @grant        none
// ==/UserScript==

(function () {
  "use strict";

  const UI_ID = "or-export-bodies-ui";
  const SCRIPT_VERSION = "2B-1.0";

  // GraphQL endpoint
  const GQL_URL =
    "https://app2b.outreach.io/graphql/Messages_GetThreadMessages";
  const OP_NAME = "Messages_GetThreadMessages";
  const SHA256 =
    "78b5e932965708618acbc1e64a0e6e4a4ff175ed5124adcbaba66b82f2154960";

  // Expected columns from your link scrape
  const REQUIRED_LINK_COLS = [
    "prospect_id",
    "message_id_encoded",
    "message_url",
  ];

  const DEBUG = true;

  // Limit what we print from bodies (so logs stay readable)
  const BODY_PREVIEW_CHARS = 220;
  const UI_JOB_KEY = "or-ui-capture-job-2b-v1";
  const UI_HIDDEN_BODY_RE = /^\s*\[body hidden\]\s*$/i;
  const UI_CAPTURE_TIMEOUT_MS = 30000;
  const UI_CAPTURE_POLL_MS = 500;
  const UI_CAPTURE_MIN_GOOD_CHARS = 60;
  const FETCH_CAPTURE_MAX = 240;
  const STRICT_PAYLOAD_ONLY = true;
  const TARGET_STATE_ROOT_KEY = "OrGlobalGiraffeCache";
  const TARGET_OP_HINTS = [
    "Messages_GetThreadMessages",
    "Messages_GetMessage",
    "Messages",
  ];

  const state = {
    running: false,
    bearer: null,
    tokenHint: null,
    linksRows: [],
    results: [],
    uiProcessing: false,
    interceptedCandidates: [],
    lastInterceptLogAt: 0,
    lastCapturePulseAt: 0,
    lastStateScanAt: 0,
    lastStateCandidate: null,
    networkSeq: 0,
    lastTargetedLogAt: 0,
  };

  function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }
  function ts() {
    return new Date().toISOString();
  }

  function safeStringify(obj) {
    try {
      const replacer = (k, v) => {
        if (k.toLowerCase().includes("authorization")) return "[redacted]";
        if (k.toLowerCase().includes("bearer")) return "[redacted]";
        if (
          typeof v === "string" &&
          v.startsWith("eyJ") &&
          v.split(".").length >= 3
        )
          return "[jwt-redacted]";
        return v;
      };
      return JSON.stringify(obj, replacer);
    } catch {
      return String(obj);
    }
  }

  function log(msg, obj) {
    const line =
      `[OR-2B][${ts()}] ${msg}` + (obj ? ` | ${safeStringify(obj)}` : "");
    if (DEBUG) console.log(line);
    const box = document.querySelector("#or-logbox");
    if (box) {
      box.value += line + "\n";
      box.scrollTop = box.scrollHeight;
    }
  }

  function setStatus(msg) {
    const el = document.querySelector("#or-body-status");
    if (el) el.textContent = msg;
    log(`STATUS: ${msg}`);
  }

  function setProgress(msg) {
    const el = document.querySelector("#or-body-progress");
    if (el) el.textContent = msg;
    log(`PROGRESS: ${msg}`);
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
    log(`Downloaded file: ${filename} (${text.length} chars)`);
  }

  function nowStamp() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
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
      for (let i = 0; i < storage.length; i++) {
        const k = storage.key(i);
        if (!k || !k.startsWith("oidc.user:")) continue;
        const v = storage.getItem(k);
        if (!v) continue;
        try {
          const obj = JSON.parse(v);
          if (obj && typeof obj.access_token === "string" && obj.access_token.length > 20) {
            return { token: obj.access_token, hint: `${label}:${k} (access_token)` };
          }
          if (obj && typeof obj.accessToken === "string" && obj.accessToken.length > 20) {
            return { token: obj.accessToken, hint: `${label}:${k} (accessToken)` };
          }
        } catch { /* ignore */ }
      }
      return null;
    }

    const oidc1 = scanOidc(window.sessionStorage, "sessionStorage");
    if (oidc1) return oidc1;
    const oidc2 = scanOidc(window.localStorage, "localStorage");
    if (oidc2) return oidc2;

    const candidates = [];
    function pushCandidate(where, key, token) { candidates.push({ where, key, token }); }
    function scanStorage(storage, label) {
      if (!storage) return;
      for (let i = 0; i < storage.length; i++) {
        const k = storage.key(i);
        if (!k) continue;
        const v = storage.getItem(k);
        if (!v) continue;
        if (looksLikeJwt(v)) pushCandidate(label, k, v);
        if (v.trim().startsWith("{") || v.trim().startsWith("[")) {
          try {
            const obj = JSON.parse(v);
            const stack = [obj];
            while (stack.length) {
              const cur = stack.pop();
              if (!cur) continue;
              if (typeof cur === "string" && looksLikeJwt(cur)) pushCandidate(label, k, cur);
              else if (typeof cur === "object") for (const val of Object.values(cur)) stack.push(val);
            }
          } catch { /* ignore */ }
        }
      }
    }
    scanStorage(window.localStorage, "localStorage");
    scanStorage(window.sessionStorage, "sessionStorage");
    candidates.sort((a, b) => b.token.length - a.token.length);
    const best = candidates[0];
    if (!best) return { token: null, hint: null };
    return { token: best.token, hint: `${best.where}:${best.key} (heuristic)` };
  }

  function parseCsv(text) {
    const rows = [];
    let i = 0, field = "", row = [], inQuotes = false;
    while (i < text.length) {
      const c = text[i];
      if (inQuotes) {
        if (c === '"') {
          if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
          inQuotes = false; i++; continue;
        } else { field += c; i++; continue; }
      } else {
        if (c === '"') { inQuotes = true; i++; continue; }
        if (c === ",") { row.push(field); field = ""; i++; continue; }
        if (c === "\n") { row.push(field); rows.push(row); row = []; field = ""; i++; continue; }
        if (c === "\r") { i++; continue; }
        field += c; i++;
      }
    }
    row.push(field);
    rows.push(row);
    const header = rows.shift().map((h) => (h || "").trim());
    const out = rows
      .filter((r) => r.some((x) => (x || "").trim() !== ""))
      .map((r) => {
        const obj = {};
        for (let j = 0; j < header.length; j++) obj[header[j]] = r[j] ?? "";
        return obj;
      });
    return { header, rows: out };
  }

  async function readFileAsText(file) {
    return new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(String(fr.result || ""));
      fr.onerror = reject;
      fr.readAsText(file);
    });
  }

  function urlDecodeMessageId(message_id_encoded) {
    try { return decodeURIComponent(message_id_encoded); } catch { return message_id_encoded; }
  }

  function getSeqIdFromLinks(rows) {
    for (const r of rows) if (r.sequence_id) return String(r.sequence_id).trim();
    return "";
  }

  function validateLinksCsv(header, rows) {
    for (const c of REQUIRED_LINK_COLS) if (!header.includes(c)) return `Missing required column: ${c}`;
    if (!rows.length) return "No rows found in links CSV.";
    return null;
  }

  function normalizeTextPreview(s) {
    const t = String(s || "").replace(/\s+/g, " ").replace(/\u00a0/g, " ").trim();
    return t.length > BODY_PREVIEW_CHARS ? t.slice(0, BODY_PREVIEW_CHARS) + "…" : t;
  }

  function htmlToTextPreview(html) {
    if (!html) return "";
    const tmp = document.createElement("div");
    tmp.innerHTML = html;
    return normalizeTextPreview(tmp.innerText || tmp.textContent || "");
  }

  function normalizeMessagePath(u) {
    try {
      const url = new URL(String(u || ""), location.origin);
      return decodeURIComponent(url.pathname).replace(/\/+$/, "");
    } catch { return ""; }
  }

  function getTargetMessageIdForRow(row) {
    return normalizeMessageIdForCompare(urlDecodeMessageId(row?.message_id_encoded || ""));
  }

  function messageIdsMatchLoose(a, b) {
    const x = normalizeMessageIdForCompare(a).replace(/[<>]/g, "");
    const y = normalizeMessageIdForCompare(b).replace(/[<>]/g, "");
    return !!x && !!y && x === y;
  }

  function normalizeMessageUrl(u) {
    try {
      const url = new URL(String(u || ""), location.origin);
      return `${url.origin}${decodeURIComponent(url.pathname)}`.replace(/\/+$/, "");
    } catch { return ""; }
  }

  function isBodyHiddenMarker(bodyText, bodyHtml) {
    return UI_HIDDEN_BODY_RE.test(String(bodyText || bodyHtml || "").trim());
  }

  function bodyChars(bodyText, bodyHtml) {
    return String(bodyText || "").length + String(bodyHtml || "").length;
  }

  function isLikelySupportWidgetContent({ body_text, body_html, subject, capture_source }) {
    const text = String(body_text || "").toLowerCase();
    const html = String(body_html || "").toLowerCase();
    const subj = String(subject || "").toLowerCase();
    const source = String(capture_source || "").toLowerCase();
    if (html.includes("smooch.io") || html.includes("zendesk sunshine")) return true;
    if (html.includes("messenger-button") || html.includes("type a message")) return true;
    if (text.includes("you're back online") && text.includes("outreach support")) return true;
    if (subj === "outreach support") return true;
    if (source.startsWith("iframe:") && text.includes("outreach support")) return true;
    return false;
  }

  function looksLikeMessageContent({ body_text, body_html }) {
    const text = String(body_text || "").toLowerCase();
    const html = String(body_html || "").toLowerCase();
    if (!text && !html) return false;
    if (isBodyHiddenMarker(body_text, body_html)) return true;
    if (
      /on\s+\w{3},\s+\w{3}\s+\d{1,2},\s+\d{4}\s+at/i.test(text) ||
      text.includes(" wrote:") || text.includes("unsubscribe") ||
      text.includes("best,") || text.includes("regards,")
    ) return true;
    if (
      html.includes("outreach-signature") || html.includes("outreach-quote") ||
      html.includes("mailto:")
    ) return true;
    return bodyChars(body_text, body_html) > 1200;
  }

  function scoreBodyCandidate({ body_text, body_html, body_masked, subject, message_id, capture_source }) {
    const total = bodyChars(body_text, body_html);
    const hidden = !!body_masked || isBodyHiddenMarker(body_text, body_html);
    const supportWidget = isLikelySupportWidgetContent({ body_text, body_html, subject, capture_source });
    const msgLike = looksLikeMessageContent({ body_text, body_html });
    let score = Math.min(total, 20000) / 100;
    if (!hidden && total > 30) score += 250;
    if (hidden) score -= 50;
    if (subject) score += 8;
    if (message_id) score += 6;
    if (String(capture_source || "").includes("generic-div-scan")) score -= 40;
    if (msgLike) score += 180;
    if (supportWidget) score -= 700;
    return score;
  }

  function createBlankResult(linkRow, error = "") {
    return {
      sequence_id: (linkRow.sequence_id || "").toString(),
      prospect_id: (linkRow.prospect_id || "").toString(),
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
      message_url: (linkRow.message_url || "").toString(),
      capture_source: "",
      captured_at: "",
      url_at_capture: "",
      req_operation_name: "",
      req_message_id: "",
      req_prospect_id: "",
      persona: "",
      title: "",
      sequence_name: "",
      sequence_link: "",
      step_info: "",
      context_json: "",
      markdown_output: "",
      error,
    };
  }

  function loadUiJob() {
    const raw = localStorage.getItem(UI_JOB_KEY);
    if (!raw) return null;
    try {
      const job = JSON.parse(raw);
      if (!job || !Array.isArray(job.rows) || !Array.isArray(job.results)) return null;
      return job;
    } catch { return null; }
  }

  function saveUiJob(job) {
    if (!job) return;
    job.updated_at = ts();
    localStorage.setItem(UI_JOB_KEY, JSON.stringify(job));
  }

  function clearUiJob() {
    localStorage.removeItem(UI_JOB_KEY);
  }

  function uiJobSummary(job) {
    if (!job) return "No UI runner job.";
    const total = Number(job.rows?.length || 0);
    const done = Math.min(Number(job.index || 0), total);
    const mode = job.active ? "running" : done >= total ? "done" : "paused";
    return `UI runner ${mode}: ${done}/${total}`;
  }

  function collectBodyCandidates(node, out, depth = 0, seen = new WeakSet()) {
    if (!node || depth > 12 || out.length > 60) return;
    if (Array.isArray(node)) {
      for (const item of node) collectBodyCandidates(item, out, depth + 1, seen);
      return;
    }
    if (typeof node !== "object") return;
    if (seen.has(node)) return;
    seen.add(node);

    const bodyText = (node.bodyText || node.messageBodyText || node.body_text || "").toString();
    const bodyHtml = (node.bodyHtml || node.messageBodyHtml || node.body_html || "").toString();
    if (bodyText || bodyHtml) {
      out.push({
        message_id: (node.messageId || node.id || node.message?.id || node.message?.messageId || "").toString(),
        prospect_id: (node.prospectId || node.prospect?.id || node.message?.prospectId || "").toString(),
        subject: (node.subject || node.message?.subject || "").toString(),
        body_text: bodyText,
        body_html: bodyHtml,
        body_masked: !!node.bodyMasked || !!node.message?.bodyMasked || isBodyHiddenMarker(bodyText, bodyHtml),
      });
    }
    for (const v of Object.values(node)) collectBodyCandidates(v, out, depth + 1, seen);
  }

  function getFetchUrl(input) {
    if (typeof input === "string") return input;
    if (input && typeof input.url === "string") return input.url;
    return "";
  }

  function parseJsonSafe(v) {
    if (typeof v !== "string") return null;
    const t = v.trim();
    if (!t) return null;
    if (!(t.startsWith("{") || t.startsWith("["))) return null;
    try { return JSON.parse(t); } catch { return null; }
  }

  function findByKeyRegex(node, regex, depth = 0, seen = new WeakSet()) {
    if (!node || depth > 8) return null;
    if (Array.isArray(node)) {
      for (const item of node) {
        const hit = findByKeyRegex(item, regex, depth + 1, seen);
        if (hit != null) return hit;
      }
      return null;
    }
    if (typeof node !== "object") return null;
    if (seen.has(node)) return null;
    seen.add(node);
    for (const [k, v] of Object.entries(node)) {
      if (regex.test(k) && v != null && v !== "") return v;
    }
    for (const v of Object.values(node)) {
      const hit = findByKeyRegex(v, regex, depth + 1, seen);
      if (hit != null) return hit;
    }
    return null;
  }

  function opLooksRelevant(opName) {
    const op = String(opName || "");
    if (!op) return false;
    return TARGET_OP_HINTS.some((hint) => op.includes(hint));
  }

  function isThreadMessagesRequestMeta(reqMeta) {
    const op = String(reqMeta?.operation_name || reqMeta?.req_operation_name || "");
    return op === "Messages_GetThreadMessages";
  }

  function isThreadMessagesUrl(url) {
    return String(url || "").includes("/graphql/Messages_GetThreadMessages");
  }

  function extractThreadMessagesCandidates(url, json, reqMeta = {}) {
    if (!isThreadMessagesUrl(url) && !isThreadMessagesRequestMeta(reqMeta)) return [];
    const col = json?.data?.threadMessages?.collection;
    if (!Array.isArray(col) || !col.length) return [];

    const reqMessageId = String(reqMeta.message_id || "");
    const reqProspectId = String(reqMeta.prospect_id || "");
    const out = [];

    for (const item of col) {
      const messageId = String(item?.id || item?.message?.id || "");
      const prospectId = String(item?.prospectId || item?.message?.prospectId || "");
      const bodyText = String(item?.bodyText || item?.message?.bodyText || "");
      const bodyHtml = String(item?.bodyHtml || item?.message?.bodyHtml || "");
      const subject = String(item?.subject || item?.message?.subject || "");
      const bodyMasked = !!item?.bodyMasked || isBodyHiddenMarker(bodyText, bodyHtml);
      if (!messageId && !bodyText && !bodyHtml) continue;
      if (reqMessageId && !messageIdsMatchLoose(messageId, reqMessageId)) continue;
      if (reqProspectId && prospectId && prospectId !== reqProspectId) continue;

      out.push({
        message_id: messageId,
        prospect_id: prospectId || reqProspectId,
        subject,
        body_text: bodyText,
        body_html: bodyHtml,
        body_masked: bodyMasked,
        capture_source: "payload:Messages_GetThreadMessages",
      });
    }
    return out;
  }

  function extractRequestMeta({ method, url, bodyText }) {
    const bodyJson = parseJsonSafe(bodyText);
    const payloads = Array.isArray(bodyJson) ? bodyJson : bodyJson && typeof bodyJson === "object" ? [bodyJson] : [];

    const metas = payloads.map((p) => {
      const opName = p?.operationName || findByKeyRegex(p, /^operationname$/i) || findByKeyRegex(p, /operation/i) || "";
      const msgId = findByKeyRegex(p?.variables || p, /^messageid$/i) || findByKeyRegex(p?.variables || p, /message.?id/i) || "";
      const prospectId = findByKeyRegex(p?.variables || p, /^prospectid$/i) || findByKeyRegex(p?.variables || p, /prospect.?id/i) || "";
      const sha = p?.extensions?.persistedQuery?.sha256Hash || findByKeyRegex(p, /sha256/i) || "";
      const hasQueryText = typeof p?.query === "string" && p.query.length > 10;
      return {
        operation_name: String(opName || ""),
        message_id: String(msgId || ""),
        prospect_id: String(prospectId || ""),
        sha256: String(sha || ""),
        has_query_text: !!hasQueryText,
      };
    });

    metas.sort((a, b) => {
      const score = (m) =>
        (m.message_id ? 4 : 0) + (m.prospect_id ? 2 : 0) +
        (opLooksRelevant(m.operation_name) ? 3 : 0) +
        (m.operation_name ? 1 : 0) + (m.has_query_text ? 1 : 0);
      return score(b) - score(a);
    });

    const top = metas[0] || {};
    return {
      method: String(method || "GET").toUpperCase(),
      url: String(url || ""),
      operation_name: top.operation_name || "",
      message_id: top.message_id || "",
      prospect_id: top.prospect_id || "",
      sha256: top.sha256 || "",
      has_query_text: !!top.has_query_text,
    };
  }

  function shouldInspectJsonResponse(url, contentType) {
    const ct = String(contentType || "").toLowerCase();
    if (!ct.includes("json")) return false;
    const u = String(url || "");
    return u.includes("outreach.io") || /graphql|message|thread|mail|prospect/i.test(u);
  }

  function keepLatestCandidates() {
    if (state.interceptedCandidates.length <= FETCH_CAPTURE_MAX) return;
    state.interceptedCandidates = state.interceptedCandidates.slice(-FETCH_CAPTURE_MAX);
  }

  function ingestInterceptedJson(url, json, reqMeta = {}) {
    const candidates = extractThreadMessagesCandidates(url, json, reqMeta);
    if (!candidates.length) return;

    for (const c of candidates) {
      state.interceptedCandidates.push({
        ...c,
        fetch_url: String(url || ""),
        page_url: location.href,
        seen_at_ms: Date.now(),
        req_operation_name: String(reqMeta.operation_name || ""),
        req_message_id: String(reqMeta.message_id || ""),
        req_prospect_id: String(reqMeta.prospect_id || ""),
        req_sha256: String(reqMeta.sha256 || ""),
        req_method: String(reqMeta.method || ""),
      });
    }
    keepLatestCandidates();

    const nowMs = Date.now();
    if (nowMs - state.lastInterceptLogAt > 800) {
      const best = candidates.slice().sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a))[0];
      log("Intercepted candidate(s)", {
        count: candidates.length,
        best_chars: bodyChars(best?.body_text, best?.body_html),
        best_hidden: isBodyHiddenMarker(best?.body_text, best?.body_html),
        req_operation_name: reqMeta.operation_name || "",
        req_message_id_prefix: String(reqMeta.message_id || "").slice(0, 28),
      });
      state.lastInterceptLogAt = nowMs;
    }

    if (reqMeta.message_id || isThreadMessagesRequestMeta(reqMeta)) {
      if (nowMs - state.lastTargetedLogAt > 1000) {
        log("Targeted request seen", {
          op: reqMeta.operation_name || "",
          req_message_id_prefix: String(reqMeta.message_id || "").slice(0, 28),
          req_prospect_id: reqMeta.prospect_id || "",
          candidates: candidates.length,
        });
        state.lastTargetedLogAt = nowMs;
      }
    }
  }

  function installFetchInterceptor() {
    if (typeof window.fetch !== "function") return;
    if (window.fetch.__orWrappedForOrCapture) return;

    const previousFetch = window.fetch.bind(window);
    const wrappedFetch = async function (...args) {
      const reqUrl = getFetchUrl(args[0]);
      const init = args[1] || {};
      let reqMethod = init?.method || (args[0] && typeof args[0] === "object" ? args[0].method : "") || "GET";
      let reqBodyText = "";
      const rawBody = init?.body;
      if (typeof rawBody === "string") reqBodyText = rawBody;
      else if (rawBody && typeof rawBody.toString === "function") {
        if (rawBody instanceof URLSearchParams) reqBodyText = rawBody.toString();
      }
      if (!reqBodyText && args[0] && typeof args[0] === "object" && typeof args[0].clone === "function" && typeof args[0].text === "function") {
        try { reqBodyText = await args[0].clone().text(); } catch { /* ignore */ }
      }
      const reqMeta = extractRequestMeta({ method: reqMethod, url: reqUrl, bodyText: reqBodyText });
      const resp = await previousFetch(...args);
      try {
        const url = reqUrl;
        const contentType = resp.headers?.get("content-type");
        if (shouldInspectJsonResponse(url, contentType)) {
          resp.clone().json().then((json) => ingestInterceptedJson(url, json, reqMeta)).catch(() => {});
        }
      } catch { /* ignore */ }
      return resp;
    };
    wrappedFetch.__orWrappedForOrCapture = true;
    wrappedFetch.__orPrevFetch = previousFetch;
    window.fetch = wrappedFetch;
    window.__orFetchInterceptInstalled = true;
    log("Fetch interceptor installed");
  }

  function installXhrInterceptor() {
    if (XMLHttpRequest.prototype.send.__orWrappedForOrCapture) return;

    const originalOpen = XMLHttpRequest.prototype.open;
    const originalSend = XMLHttpRequest.prototype.send;

    XMLHttpRequest.prototype.open = function (method, url, ...rest) {
      this.__orUrl = String(url || "");
      this.__orMethod = String(method || "GET").toUpperCase();
      return originalOpen.call(this, method, url, ...rest);
    };

    XMLHttpRequest.prototype.send = function (...args) {
      let reqBodyText = "";
      if (typeof args[0] === "string") reqBodyText = args[0];
      else if (args[0] && typeof args[0].toString === "function" && args[0] instanceof URLSearchParams) {
        reqBodyText = args[0].toString();
      }
      this.__orReqMeta = extractRequestMeta({ method: this.__orMethod || "GET", url: this.__orUrl || "", bodyText: reqBodyText });

      this.addEventListener("load", function () {
        try {
          const url = this.__orUrl || "";
          const contentType = this.getResponseHeader("content-type") || "";
          if (!shouldInspectJsonResponse(url, contentType)) return;
          const text = this.responseText;
          if (!text || typeof text !== "string") return;
          const json = JSON.parse(text);
          ingestInterceptedJson(url, json, this.__orReqMeta || {});
        } catch { /* ignore */ }
      });
      return originalSend.apply(this, args);
    };
    XMLHttpRequest.prototype.open.__orWrappedForOrCapture = true;
    XMLHttpRequest.prototype.send.__orWrappedForOrCapture = true;
    window.__orXhrInterceptInstalled = true;
    log("XHR interceptor installed");
  }

  // ───────────────────────────────────────────────
  // DOM Scraping Layer (Test B additions)
  // ───────────────────────────────────────────────

  /**
   * Parse the rich aria-label string from the send icon.
   * Example: "Step #3 (Auto email) of Unified HR Benefits-..., delivered on February 18, 2026 at 7:54 AM MST"
   */
  function parseAriaLabel(label) {
    const out = { step: "", sequence_name: "", delivered_at: "" };
    if (!label) return out;

    // Extract delivered timestamp
    const deliveredMatch = label.match(/,\s*delivered on\s+(.+)$/i);
    if (deliveredMatch) out.delivered_at = deliveredMatch[1].trim();

    // Extract step and sequence: "Step #3 (Auto email) of <sequence name>, delivered on ..."
    const stepSeqMatch = label.match(/^(.+?)\s+of\s+(.+?)(?:,\s*delivered|$)/i);
    if (stepSeqMatch) {
      out.step = stepSeqMatch[1].trim();
      out.sequence_name = stepSeqMatch[2].trim();
    }

    return out;
  }

  /**
   * Parse step info text like "Step #3 (Auto email)" into components.
   */
  function parseStepInfo(stepText) {
    const out = { step_number: null, step_type: "" };
    if (!stepText) return out;
    const numMatch = stepText.match(/#(\d+)/);
    if (numMatch) out.step_number = parseInt(numMatch[1], 10);
    const typeMatch = stepText.match(/\(([^)]+)\)/);
    if (typeMatch) out.step_type = typeMatch[1].trim();
    return out;
  }

  /**
   * Scrape sequence info from a single MuiAlert-standardInfo block.
   * Returns { sequence_name, sequence_link, step, step_number, step_type, delivered_at }
   */
  function scrapeSequenceInfo(alertEl) {
    const out = {
      sequence_name: "",
      sequence_link: "",
      step: "",
      step_number: null,
      step_type: "",
      delivered_at: "",
    };
    if (!alertEl) return out;

    const messageDiv = alertEl.querySelector(".MuiAlert-message");
    if (messageDiv) {
      const links = messageDiv.querySelectorAll("a");
      // First <a> = step link (e.g. "Step #3 (Auto email)")
      // Second <a> = sequence name link
      if (links.length >= 1) {
        out.step = (links[0].textContent || "").trim();
        out.sequence_link = links[0].getAttribute("href") || "";
      }
      if (links.length >= 2) {
        out.sequence_name = (links[1].textContent || "").trim();
        // Prefer the sequence link from the second anchor if present
        const seqHref = links[1].getAttribute("href") || "";
        if (seqHref) out.sequence_link = seqHref;
      }
    }

    // Parse step number and type
    const stepParsed = parseStepInfo(out.step);
    out.step_number = stepParsed.step_number;
    out.step_type = stepParsed.step_type;

    // Try to get delivered timestamp from aria-label on send icon container
    const ariaEls = alertEl.querySelectorAll("[aria-label]");
    for (const el of ariaEls) {
      const label = el.getAttribute("aria-label") || "";
      if (/delivered on/i.test(label)) {
        const parsed = parseAriaLabel(label);
        if (parsed.delivered_at) out.delivered_at = parsed.delivered_at;
        // Also fill in sequence_name from aria-label if not yet populated
        if (!out.sequence_name && parsed.sequence_name) out.sequence_name = parsed.sequence_name;
        if (!out.step && parsed.step) {
          out.step = parsed.step;
          const sp = parseStepInfo(parsed.step);
          out.step_number = sp.step_number;
          out.step_type = sp.step_type;
        }
        break;
      }
    }

    return out;
  }

  /**
   * Scrape persona chip and title from the prospect sidebar panel.
   */
  function scrapeProspectSidebar() {
    const out = { persona: "", title: "" };

    // Persona: look for MuiChip-label inside the sidebar
    const chips = document.querySelectorAll("span.MuiChip-label.MuiChip-labelSmall");
    for (const chip of chips) {
      const text = (chip.textContent || "").trim();
      if (text && text.length > 3) {
        out.persona = text;
        break;
      }
    }

    // Title: look for p.MuiTypography-body1 in the right sidebar panel.
    // The sidebar is typically the last major column in the layout.
    // We look for the prospect detail section which contains title info.
    // Strategy: find all p.MuiTypography-body1 elements that are NOT inside the
    // message thread area (which is in the main content). The sidebar is usually
    // in a separate panel div.
    const mainContent = document.querySelector('[class*="v5-jss"][class*="MuiBox-root"]');
    const allBody1 = document.querySelectorAll("p.MuiTypography-body1");
    for (const el of allBody1) {
      const text = (el.textContent || "").trim();
      // Skip very short or known non-title texts
      if (!text || text.length < 3) continue;
      if (/^(to|from|cc|bcc|sequenced via|of|using template)$/i.test(text)) continue;
      // Check if this is in the sidebar area (right side panel)
      // The sidebar typically contains prospect info and is not inside the message alerts
      const closestAlert = el.closest(".MuiAlert-root");
      const closestMessageWrapper = el.closest("[class*='message-wrapper']");
      if (closestAlert || closestMessageWrapper) continue;
      // Check if it's in a panel/sidebar-like container
      const parent = el.parentElement;
      if (!parent) continue;
      // Look for title-like context: nearby elements that suggest this is a title field
      // The title is often near the prospect name in the sidebar
      const closestPanel = el.closest('[class*="v5-jss"]');
      if (closestPanel) {
        // Check if this panel also contains a prospect link or chip
        const hasChip = closestPanel.querySelector("span.MuiChip-label");
        const hasProspectLink = closestPanel.querySelector('a[href*="/prospects/"]');
        if (hasChip || hasProspectLink) {
          out.title = text;
          break;
        }
      }
    }

    return out;
  }

  /**
   * Scrape email header info (sender, recipient, time) from the message section
   * associated with a given alert block.
   */
  function scrapeEmailHeader(sectionEl) {
    const out = { sender_name: "", sender_email: "", recipient_name: "", time_sent: "" };
    if (!sectionEl) return out;

    // Sender name: p.MuiTypography-body2 with noWrap
    const senderNameEl = sectionEl.querySelector("p.MuiTypography-body2.MuiTypography-noWrap");
    if (senderNameEl) out.sender_name = (senderNameEl.textContent || "").trim();

    // Sender email: p.MuiTypography-body1 with noWrap that contains an email pattern
    const body1Els = sectionEl.querySelectorAll("p.MuiTypography-body1.MuiTypography-noWrap");
    for (const el of body1Els) {
      const text = (el.textContent || "").trim();
      if (/@/.test(text)) {
        out.sender_email = text;
        break;
      }
    }

    // Recipient: look for prospect link in the "to" section
    const prospectLinks = sectionEl.querySelectorAll('a[href*="/prospects/"]');
    for (const link of prospectLinks) {
      const text = (link.textContent || "").trim();
      if (text) {
        out.recipient_name = text;
        break;
      }
    }

    // Time sent: look for the time display div (contains text like "7:54 AM (5 hours ago)")
    const allDivs = sectionEl.querySelectorAll("div");
    for (const div of allDivs) {
      const text = (div.textContent || "").trim();
      if (/^\d{1,2}:\d{2}\s*(AM|PM)/i.test(text) && div.children.length === 0) {
        out.time_sent = text;
        break;
      }
    }

    return out;
  }

  /**
   * Scrape the subject line from the page header.
   */
  function scrapeSubject() {
    const h4 = document.querySelector("h4.MuiTypography-h4");
    if (h4) return (h4.textContent || "").trim();
    return "";
  }

  /**
   * Main DOM scraper: iterates all alert blocks and their corresponding message bodies.
   * Returns an array of message context objects.
   */
  function scrapePageContext() {
    const results = [];
    const subject = scrapeSubject();
    const sidebar = scrapeProspectSidebar();

    const alertBlocks = document.querySelectorAll(".MuiAlert-standardInfo");
    if (!alertBlocks.length) {
      log("scrapePageContext: no MuiAlert-standardInfo blocks found");
      return results;
    }

    log("scrapePageContext: found alert blocks", { count: alertBlocks.length });

    for (const alertEl of alertBlocks) {
      const seqInfo = scrapeSequenceInfo(alertEl);

      // Find the parent container that holds both the alert and the message body
      // Walk up to find the section-level container, then look for the message body below
      let sectionEl = alertEl.closest('[class*="v5-jss"][class*="MuiBox-root"]');
      // Walk up a few levels to find the wrapping section for the entire message block
      for (let i = 0; i < 5 && sectionEl; i++) {
        const parent = sectionEl.parentElement;
        if (!parent) break;
        // Check if this parent has both the alert and a message-wrapper or native-html
        if (parent.querySelector(".native-html") || parent.querySelector("[class*='message-wrapper']")) {
          sectionEl = parent;
          break;
        }
        sectionEl = parent;
      }

      // Scrape email header from section
      const emailHeader = scrapeEmailHeader(sectionEl);

      // Find message body: native-html or message-wrapper div within the section
      let bodyText = "";
      let bodyHtml = "";
      const nativeHtml = sectionEl ? sectionEl.querySelector(".native-html") : null;
      if (nativeHtml) {
        bodyText = (nativeHtml.innerText || "").trim();
        bodyHtml = (nativeHtml.innerHTML || "").trim();
      } else {
        const msgWrapper = sectionEl ? sectionEl.querySelector("[class*='message-wrapper']") : null;
        if (msgWrapper) {
          bodyText = (msgWrapper.innerText || "").trim();
          bodyHtml = (msgWrapper.innerHTML || "").trim();
        }
      }

      results.push({
        sequence_name: seqInfo.sequence_name,
        sequence_link: seqInfo.sequence_link,
        step: seqInfo.step,
        step_number: seqInfo.step_number,
        step_type: seqInfo.step_type,
        subject: subject,
        body_text: bodyText,
        body_html: bodyHtml,
        delivered_at: seqInfo.delivered_at,
        sender_name: emailHeader.sender_name,
        sender_email: emailHeader.sender_email,
        recipient_name: emailHeader.recipient_name,
        time_sent: emailHeader.time_sent,
        persona: sidebar.persona,
        title: sidebar.title,
      });
    }

    log("scrapePageContext: scraped messages", { count: results.length });
    return results;
  }

  /**
   * Build context JSON object merging DOM-scraped data with API result data.
   */
  function buildContextJson(scraped, apiResult) {
    const ctx = {
      sequence_name: scraped?.sequence_name || "",
      sequence_link: scraped?.sequence_link || "",
      step: scraped?.step || "",
      step_number: scraped?.step_number || null,
      step_type: scraped?.step_type || "",
      subject: scraped?.subject || apiResult?.subject || "",
      body_text: scraped?.body_text || apiResult?.body_text || "",
      delivered_at: scraped?.delivered_at || apiResult?.delivered_at || "",
      sender_name: scraped?.sender_name || "",
      sender_email: scraped?.sender_email || "",
      recipient_name: scraped?.recipient_name || "",
      persona: scraped?.persona || "",
      title: scraped?.title || "",
    };
    return ctx;
  }

  /**
   * Format context JSON into a markdown string matching the expected output format.
   */
  function buildMarkdownOutput(ctx) {
    const lines = [];

    if (ctx.sequence_name) {
      lines.push("**SEQUENCE**");
      lines.push(ctx.sequence_name);
      lines.push("");
    }

    if (ctx.step) {
      lines.push("**STEP**");
      lines.push(ctx.step);
      lines.push("");
    }

    if (ctx.subject) {
      lines.push("**SUBJ:**");
      lines.push(ctx.subject);
      lines.push("");
    }

    if (ctx.persona) {
      lines.push(`**PERSONA:** ${ctx.persona}`);
    }
    if (ctx.title) {
      lines.push(`**TITLE:** ${ctx.title}`);
    }
    if (ctx.persona || ctx.title) {
      lines.push("");
    }

    if (ctx.body_text) {
      lines.push("**MESSAGE**");
      lines.push(ctx.body_text);
    }

    return lines.join("\n");
  }

  /**
   * Build enriched CSV with all original + new columns.
   */
  function buildEnrichedResultsCsv(results) {
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
      "capture_source",
      "captured_at",
      "url_at_capture",
      "req_operation_name",
      "req_message_id",
      "req_prospect_id",
      "persona",
      "title",
      "sequence_name",
      "sequence_link",
      "step_info",
      "context_json",
      "markdown_output",
      "error",
    ];

    const lines = [header.join(",")];
    for (const r of results || []) {
      lines.push(header.map((k) => csvEscape(r?.[k] ?? "")).join(","));
    }
    return lines.join("\n");
  }

  // ───────────────────────────────────────────────
  // Candidate selection (same as original)
  // ───────────────────────────────────────────────

  function normalizeCandidate(c) {
    if (!c) return null;
    return {
      message_id: (c.message_id || "").toString(),
      prospect_id: (c.prospect_id || "").toString(),
      subject: (c.subject || "").toString(),
      body_text: (c.body_text || "").toString(),
      body_html: (c.body_html || "").toString(),
      body_masked: !!c.body_masked,
      capture_source: (c.capture_source || "").toString(),
      seen_at_ms: Number(c.seen_at_ms || Date.now()),
      page_url: (c.page_url || location.href).toString(),
      fetch_url: (c.fetch_url || "").toString(),
      req_operation_name: (c.req_operation_name || "").toString(),
      req_message_id: (c.req_message_id || "").toString(),
      req_prospect_id: (c.req_prospect_id || "").toString(),
      req_sha256: (c.req_sha256 || "").toString(),
      req_method: (c.req_method || "").toString(),
    };
  }

  function chooseBestCandidateForRow(row, rawCandidates) {
    const targetMessageId = getTargetMessageIdForRow(row);
    const targetProspectId = String(row?.prospect_id || "").trim();

    function enrich(candidate, defaultSource) {
      const c = normalizeCandidate(candidate);
      if (!c) return null;
      if (!c.capture_source) c.capture_source = defaultSource;
      c.hidden = !!c.body_masked || isBodyHiddenMarker(c.body_text, c.body_html);
      c.score = scoreBodyCandidate(c);
      c.junk = isLikelySupportWidgetContent(c);
      const mid = normalizeMessageIdForCompare(c.message_id);
      if (targetMessageId && mid === targetMessageId) c.score += 140;
      if (targetMessageId && mid.replace(/[<>]/g, "") === targetMessageId.replace(/[<>]/g, "")) c.score += 90;
      if (targetMessageId && messageIdsMatchLoose(c.req_message_id, targetMessageId)) c.score += 480;
      if (targetMessageId && messageIdsMatchLoose(c.message_id, targetMessageId)) c.score += 140;
      if (targetProspectId && (String(c.req_prospect_id || "").trim() === targetProspectId || String(c.prospect_id || "").trim() === targetProspectId)) c.score += 55;
      if (opLooksRelevant(c.req_operation_name)) c.score += 35;
      return c;
    }

    const options = (Array.isArray(rawCandidates) ? rawCandidates : [])
      .map((c) => enrich(c, "candidate"))
      .filter(Boolean);
    if (!options.length) return null;
    options.sort((a, b) => b.score - a.score);
    return options[0];
  }

  function getTargetedNetworkCandidateForRow(row) {
    const targetPath = normalizeMessagePath(row.message_url);
    const targetMessageId = getTargetMessageIdForRow(row);
    const targetProspectId = String(row?.prospect_id || "").trim();
    const nowMs = Date.now();

    const alive = state.interceptedCandidates.filter((c) => nowMs - Number(c.seen_at_ms || 0) <= 120000);
    state.interceptedCandidates = alive;
    if (!alive.length) return null;

    const exact = alive.filter((c) => {
      if (!targetMessageId) return false;
      if (c.capture_source !== "payload:Messages_GetThreadMessages") return false;
      if (!isThreadMessagesRequestMeta({ operation_name: c.req_operation_name })) return false;
      const reqMsg = c.req_message_id || "";
      const msgId = c.message_id || "";
      const byReq = reqMsg && messageIdsMatchLoose(reqMsg, targetMessageId);
      const byBody = msgId && messageIdsMatchLoose(msgId, targetMessageId);
      return byReq || byBody;
    });
    const pool = exact;
    if (!pool.length) return null;
    const samePage = targetPath ? pool.filter((c) => normalizeMessagePath(c.page_url) === targetPath) : [];
    const finalPool = samePage.length ? samePage : pool;

    const scored = finalPool.map((c) => {
      const candidate = normalizeCandidate(c);
      candidate.score = scoreBodyCandidate(candidate);
      if (targetMessageId && messageIdsMatchLoose(candidate.req_message_id, targetMessageId)) candidate.score += 600;
      if (targetMessageId && messageIdsMatchLoose(candidate.message_id, targetMessageId)) candidate.score += 220;
      if (targetProspectId && (String(candidate.req_prospect_id || "").trim() === targetProspectId || String(candidate.prospect_id || "").trim() === targetProspectId)) candidate.score += 70;
      if (targetPath && normalizeMessagePath(candidate.page_url) === targetPath) candidate.score += 30;
      if (isThreadMessagesRequestMeta(candidate)) candidate.score += 100;
      if (candidate.capture_source === "payload:Messages_GetThreadMessages") candidate.score += 120;
      return candidate;
    });

    scored.sort((a, b) => b.score - a.score);
    return scored[0] || null;
  }

  function getDeterministicStateCandidateForRow(row) {
    const root = window[TARGET_STATE_ROOT_KEY];
    if (!root || typeof root !== "object") return null;

    const targetMessageId = getTargetMessageIdForRow(row);
    const targetProspectId = String(row?.prospect_id || "").trim();
    if (!targetMessageId) return null;

    const all = [];
    collectBodyCandidates(root, all);
    if (!all.length) return null;

    const matched = all
      .map((c) => ({
        ...c,
        capture_source: `state:window.${TARGET_STATE_ROOT_KEY}`,
        seen_at_ms: Date.now(),
        page_url: location.href,
      }))
      .filter((c) => {
        if (!messageIdsMatchLoose(c.message_id, targetMessageId)) return false;
        if (targetProspectId && String(c.prospect_id || "").trim() && String(c.prospect_id || "").trim() !== targetProspectId) return false;
        return true;
      });

    if (!matched.length) return null;
    matched.sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a));
    return matched[0];
  }

  function normalizeMessageIdForCompare(v) {
    const s = String(v || "").trim();
    if (!s) return "";
    try { return decodeURIComponent(s).trim().toLowerCase(); } catch { return s.toLowerCase(); }
  }

  function isCurrentPageForRow(row) {
    const now = normalizeMessagePath(location.href);
    const target = normalizeMessagePath(row.message_url);
    return !!now && !!target && now === target;
  }

  // ───────────────────────────────────────────────
  // UI capture with DOM scraping integration
  // ───────────────────────────────────────────────

  async function captureRowBodyViaUi(row, timeoutMs = UI_CAPTURE_TIMEOUT_MS) {
    const started = Date.now();
    const targetMessageId = getTargetMessageIdForRow(row);

    while (Date.now() - started < timeoutMs) {
      const targetedCandidate = getTargetedNetworkCandidateForRow(row);
      const strictStateCandidate = getDeterministicStateCandidateForRow(row);
      const best = chooseBestCandidateForRow(row, [targetedCandidate, strictStateCandidate]);

      const bestMessageIdMatch =
        !!best && !!targetMessageId &&
        (messageIdsMatchLoose(best.message_id, targetMessageId) ||
          messageIdsMatchLoose(best.req_message_id, targetMessageId));
      const deterministicSourceOk =
        best?.capture_source === "payload:Messages_GetThreadMessages" ||
        best?.capture_source === `state:window.${TARGET_STATE_ROOT_KEY}`;

      if (best) {
        if (deterministicSourceOk && bestMessageIdMatch && best.hidden) return best;
        if (
          !best.hidden && !best.junk &&
          looksLikeMessageContent(best) &&
          bodyChars(best.body_text, best.body_html) >= UI_CAPTURE_MIN_GOOD_CHARS &&
          bestMessageIdMatch && deterministicSourceOk
        ) return best;
      }

      const nowMs = Date.now();
      if (nowMs - state.lastCapturePulseAt > 4000) {
        log("UI capture pulse", {
          elapsed_ms: nowMs - started,
          strict_mode: true,
          has_targeted: !!targetedCandidate,
          has_strict_state: !!strictStateCandidate,
          best_chars: best ? bodyChars(best.body_text, best.body_html) : 0,
          best_source: best?.capture_source || "",
          best_junk: !!best?.junk,
          best_msg_match: !!bestMessageIdMatch,
          best_req_op: best?.req_operation_name || "",
          best_req_msgid_prefix: String(best?.req_message_id || "").slice(0, 24),
        });
        state.lastCapturePulseAt = nowMs;
      }
      await sleep(UI_CAPTURE_POLL_MS);
    }
    return null;
  }

  function createUiJob(rows, delayMs, limit) {
    const total = limit ? Math.min(limit, rows.length) : rows.length;
    const subset = rows.slice(0, total).map((r) => ({
      sequence_id: r.sequence_id || "",
      prospect_id: r.prospect_id || "",
      message_id_encoded: r.message_id_encoded || "",
      message_url: r.message_url || "",
    }));
    return {
      kind: "ui_capture_2b",
      created_at: ts(),
      updated_at: ts(),
      active: true,
      delay_ms: Number.isFinite(delayMs) ? Math.max(0, delayMs) : 900,
      index: 0,
      rows: subset,
      results: subset.map((row) => createBlankResult(row)),
    };
  }

  async function processUiJobTick() {
    if (state.uiProcessing) return;

    const job = loadUiJob();
    if (!job || !job.active) return;
    if (!Array.isArray(job.rows) || !Array.isArray(job.results)) return;

    state.uiProcessing = true;
    try {
      const total = job.rows.length;
      const idx = Number(job.index || 0);

      if (idx >= total) {
        job.active = false;
        saveUiJob(job);
        setStatus(`UI runner complete (${total}/${total}).`);
        setProgress("Click Download UI CSV.");
        return;
      }

      const row = job.rows[idx];
      setStatus(`UI runner active (${idx + 1}/${total})`);

      if (!row?.message_url) {
        job.results[idx] = createBlankResult(row, "missing message_url");
        job.index = idx + 1;
        saveUiJob(job);
        return;
      }

      if (!isCurrentPageForRow(row)) {
        setProgress(`Navigating to row ${idx + 1}/${total}…`);
        log("UI navigate", { row: idx + 1, total, url: normalizeMessageUrl(row.message_url) });
        location.assign(row.message_url);
        return;
      }

      setProgress(`Capturing row ${idx + 1}/${total} on page…`);
      log("UI capture start", { row: idx + 1, total, prospect_id: row.prospect_id });

      const best = await captureRowBodyViaUi(row, UI_CAPTURE_TIMEOUT_MS);
      const out = createBlankResult(row);

      // DOM scraping: run after page has stabilized (we waited for API payload already)
      const scrapedMessages = scrapePageContext();
      const scraped = scrapedMessages.length > 0 ? scrapedMessages[0] : null;
      const sidebar = scrapeProspectSidebar();

      if (best) {
        out.message_id = best.message_id || urlDecodeMessageId(row.message_id_encoded);
        out.subject = best.subject || scraped?.subject || "";
        out.body_text = best.body_text || "";
        out.body_html = best.body_html || "";
        out.capture_source = best.capture_source || "";
        out.captured_at = ts();
        out.url_at_capture = location.href;
        out.req_operation_name = best.req_operation_name || "";
        out.req_message_id = best.req_message_id || "";
        out.req_prospect_id = best.req_prospect_id || "";
        if (isBodyHiddenMarker(out.body_text, out.body_html)) {
          out.error = "body hidden";
        }
        if (!out.error && out.capture_source === "page-snapshot") {
          out.error = "low-confidence snapshot";
        }
      } else {
        out.error = STRICT_PAYLOAD_ONLY
          ? `no targeted payload match (${UI_CAPTURE_TIMEOUT_MS}ms)`
          : `capture timeout (${UI_CAPTURE_TIMEOUT_MS}ms)`;
      }

      // Enrich with DOM-scraped metadata
      if (scraped) {
        out.persona = scraped.persona || sidebar.persona || "";
        out.title = scraped.title || sidebar.title || "";
        out.sequence_name = scraped.sequence_name || "";
        out.sequence_link = scraped.sequence_link || "";
        out.step_info = scraped.step || "";
        out.delivered_at = out.delivered_at || scraped.delivered_at || "";
        if (!out.subject && scraped.subject) out.subject = scraped.subject;
      } else {
        out.persona = sidebar.persona || "";
        out.title = sidebar.title || "";
      }

      // Build context_json and markdown_output
      const contextJson = buildContextJson(scraped, out);
      out.context_json = JSON.stringify(contextJson);
      out.markdown_output = buildMarkdownOutput({
        ...contextJson,
        body_text: out.body_text || contextJson.body_text,
      });

      // For multi-message pages, build combined markdown with all steps
      if (scrapedMessages.length > 1) {
        const allMarkdown = scrapedMessages.map((msg) => {
          const ctx = buildContextJson(msg, out);
          return buildMarkdownOutput({
            ...ctx,
            body_text: msg.body_text || out.body_text,
          });
        }).join("\n\n---\n\n");
        out.markdown_output = allMarkdown;

        // context_json gets all messages as an array
        const allContexts = scrapedMessages.map((msg) => buildContextJson(msg, out));
        out.context_json = JSON.stringify(allContexts);
      }

      job.results[idx] = out;
      job.index = idx + 1;
      saveUiJob(job);

      log("UI capture result", {
        row: idx + 1,
        total,
        source: out.capture_source || "",
        body_text_len: out.body_text.length,
        body_html_len: out.body_html.length,
        hidden: isBodyHiddenMarker(out.body_text, out.body_html),
        persona: out.persona || "",
        title: out.title || "",
        sequence_name: out.sequence_name || "",
        step_info: out.step_info || "",
        scraped_messages: scrapedMessages.length,
        error: out.error || "",
      });

      if (job.index >= total) {
        job.active = false;
        saveUiJob(job);
        setStatus(`UI runner done (${total}/${total}).`);
        setProgress("Click Download UI CSV.");
        return;
      }

      const next = job.rows[job.index];
      await sleep(Number(job.delay_ms || 900));
      if (next?.message_url) {
        setProgress(`Moving to next row ${job.index + 1}/${total}…`);
        location.assign(next.message_url);
      } else {
        setProgress(`Skipping row ${job.index + 1}/${total} (missing URL).`);
      }
    } finally {
      state.uiProcessing = false;
    }
  }

  // ───────────────────────────────────────────────
  // UI rendering
  // ───────────────────────────────────────────────

  function renderUI() {
    if (document.getElementById(UI_ID)) return;

    const wrap = document.createElement("div");
    wrap.id = UI_ID;
    wrap.style.position = "fixed";
    wrap.style.right = "16px";
    wrap.style.bottom = "16px";
    wrap.style.zIndex = 999999;
    wrap.style.background = "white";
    wrap.style.border = "1px solid #ccc";
    wrap.style.borderRadius = "10px";
    wrap.style.padding = "10px";
    wrap.style.boxShadow = "0 8px 20px rgba(0,0,0,0.15)";
    wrap.style.font = "12px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Arial";
    wrap.style.minWidth = "520px";

    wrap.innerHTML = `
      <div style="font-weight:600; margin-bottom:6px;">
        Outreach export bodies <span style="background:#2196F3;color:white;padding:1px 6px;border-radius:4px;font-size:11px;">Test B</span>
        (API + UI runner + DOM scrape) v${SCRIPT_VERSION}
      </div>
      <div id="or-body-status" style="margin-bottom:4px; color:#333;">Ready.</div>
      <div id="or-body-progress" style="margin-bottom:10px; color:#666;"></div>

      <div style="margin-bottom:10px;">
        <div style="margin-bottom:4px; color:#333;">1) Upload links CSV (required)</div>
        <input id="or-links-file" type="file" accept=".csv" />
      </div>

      <div style="margin-bottom:8px; color:#333; font-weight:600;">UI runner (recommended)</div>
      <div id="or-ui-job-summary" style="margin-bottom:8px; color:#555;">No UI runner job.</div>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
        <button id="or-ui-start" style="padding:6px 8px; cursor:pointer;">Start UI run</button>
        <button id="or-ui-resume" style="padding:6px 8px; cursor:pointer;">Resume UI run</button>
        <button id="or-ui-pause" style="padding:6px 8px; cursor:pointer;">Pause UI run</button>
        <button id="or-ui-download" style="padding:6px 8px; cursor:pointer;">Download UI CSV</button>
        <button id="or-ui-reset" style="padding:6px 8px; cursor:pointer;">Reset UI job</button>
      </div>

      <div style="margin-bottom:8px; color:#333; font-weight:600;">DOM scrape test</div>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
        <button id="or-scrape-test" style="padding:6px 8px; cursor:pointer;">Scrape this page</button>
        <button id="or-scrape-download" style="padding:6px 8px; cursor:pointer;">Download scraped MD</button>
      </div>

      <div style="display:flex; gap:10px; align-items:center; margin-bottom:10px;">
        <div>
          <label style="color:#333;">Delay (ms)</label><br/>
          <input id="or-delay" type="number" min="0" step="50" value="450" style="width:110px; padding:4px 6px;" />
        </div>
        <div>
          <label style="color:#333;">Limit (rows)</label><br/>
          <input id="or-limit" type="number" min="0" step="1" placeholder="(all)" style="width:110px; padding:4px 6px;" />
        </div>
        <div>
          <label style="color:#333;">Preview chars</label><br/>
          <input id="or-preview-chars" type="number" min="50" step="50" value="${BODY_PREVIEW_CHARS}"
                 style="width:110px; padding:4px 6px;" disabled />
        </div>
      </div>

      <div style="margin-bottom:6px; color:#333; font-weight:600;">Log (shows subject + body preview)</div>
      <textarea id="or-logbox" style="width:100%; height:160px; font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:11px; padding:6px;"></textarea>

      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px; margin-bottom:8px;">
        <button id="or-copy-logs" style="padding:6px 8px; cursor:pointer;">Copy logs</button>
        <button id="or-clear-logs" style="padding:6px 8px; cursor:pointer;">Clear logs</button>
      </div>

      <div style="margin-top:6px; color:#666;">
        Test B: DOM scraping for sequence/step/persona/title metadata + context_json + markdown_output columns. Strict payload mode.
      </div>
    `;

    document.body.appendChild(wrap);

    const linksFile = wrap.querySelector("#or-links-file");
    const btnUiStart = wrap.querySelector("#or-ui-start");
    const btnUiResume = wrap.querySelector("#or-ui-resume");
    const btnUiPause = wrap.querySelector("#or-ui-pause");
    const btnUiDownload = wrap.querySelector("#or-ui-download");
    const btnUiReset = wrap.querySelector("#or-ui-reset");
    const btnScrapeTest = wrap.querySelector("#or-scrape-test");
    const btnScrapeDownload = wrap.querySelector("#or-scrape-download");
    const uiSummary = wrap.querySelector("#or-ui-job-summary");
    const delayInput = wrap.querySelector("#or-delay");
    const limitInput = wrap.querySelector("#or-limit");
    const btnCopyLogs = wrap.querySelector("#or-copy-logs");
    const btnClearLogs = wrap.querySelector("#or-clear-logs");

    // Store last scrape result for download
    let lastScrapeResult = null;

    function refreshUiSummary() {
      if (!uiSummary) return;
      uiSummary.textContent = uiJobSummary(loadUiJob());
    }

    linksFile.addEventListener("change", async () => {
      const f = linksFile.files?.[0];
      if (!f) return;
      setStatus("Reading links CSV…");
      setProgress("");
      log("Links CSV selected", { name: f.name, size: f.size });

      const text = await readFileAsText(f);
      const parsed = parseCsv(text);
      log("Parsed CSV", { header_cols: parsed.header.length, rows: parsed.rows.length });

      const err = validateLinksCsv(parsed.header, parsed.rows);
      if (err) {
        setStatus(`Links CSV error: ${err}`);
        state.linksRows = [];
        return;
      }

      state.linksRows = parsed.rows.map((r) => ({
        sequence_id: r.sequence_id || "",
        prospect_id: r.prospect_id,
        message_id_encoded: r.message_id_encoded,
        message_url: r.message_url,
      }));

      const seqId = getSeqIdFromLinks(state.linksRows);
      setStatus(`Loaded ${state.linksRows.length} link rows` + (seqId ? ` (seq ${seqId})` : "") + ".");
      log("Links rows loaded", { seqId, count: state.linksRows.length });
      refreshUiSummary();
    });

    btnUiStart.addEventListener("click", async () => {
      log("Button click: Start UI run");
      if (!state.linksRows.length) {
        setStatus("Upload the links CSV first.");
        return;
      }
      const delayMs = parseInt(delayInput.value, 10);
      const limit = parseInt(limitInput.value, 10);
      const limitVal = Number.isFinite(limit) && limit > 0 ? limit : null;
      const job = createUiJob(state.linksRows, Number.isFinite(delayMs) ? delayMs : 900, limitVal);
      saveUiJob(job);
      refreshUiSummary();

      setStatus(`UI runner started (${job.rows.length} rows).`);
      setProgress("Auto-navigation is active. Keep this tab open.");
      log("UI job start", { total: job.rows.length, delay_ms: job.delay_ms });

      await processUiJobTick();
      refreshUiSummary();
    });

    btnUiResume.addEventListener("click", async () => {
      log("Button click: Resume UI run");
      if (state.uiProcessing) {
        setStatus("UI runner is already capturing on this page.");
        setProgress("Wait for current row to finish.");
        return;
      }
      const job = loadUiJob();
      if (!job) {
        setStatus("No saved UI job to resume.");
        return;
      }
      if (job.index >= job.rows.length) {
        setStatus("UI job already completed.");
        setProgress("Download UI CSV or click Reset UI job.");
        refreshUiSummary();
        return;
      }
      job.active = true;
      saveUiJob(job);
      refreshUiSummary();
      setStatus(`UI runner resumed (${job.index + 1}/${job.rows.length}).`);
      setProgress("Auto-navigation is active.");
      await processUiJobTick();
      refreshUiSummary();
    });

    btnUiPause.addEventListener("click", () => {
      log("Button click: Pause UI run");
      const job = loadUiJob();
      if (!job) {
        setStatus("No saved UI job.");
        return;
      }
      job.active = false;
      saveUiJob(job);
      refreshUiSummary();
      setStatus(`UI runner paused (${job.index}/${job.rows.length}).`);
      setProgress("Use Resume UI run to continue.");
    });

    btnUiDownload.addEventListener("click", () => {
      log("Button click: Download UI CSV");
      const job = loadUiJob();
      if (!job || !Array.isArray(job.results) || !job.results.length) {
        setStatus("No UI results yet.");
        return;
      }
      const seqId = getSeqIdFromLinks(job.rows || []) || "unknown";
      const csv = buildEnrichedResultsCsv(job.results);
      downloadText(`outreach_message_bodies_2b_seq_${seqId}_${nowStamp()}.csv`, csv);
      setStatus(`Downloaded UI CSV (${job.results.length} rows).`);
      setProgress("");
      refreshUiSummary();
    });

    btnUiReset.addEventListener("click", () => {
      log("Button click: Reset UI job");
      clearUiJob();
      state.interceptedCandidates = [];
      refreshUiSummary();
      setStatus("Cleared saved UI job.");
      setProgress("");
    });

    btnScrapeTest.addEventListener("click", () => {
      log("Button click: Scrape this page");
      const scraped = scrapePageContext();
      lastScrapeResult = scraped;
      if (!scraped.length) {
        setStatus("No messages scraped from this page.");
        setProgress("Navigate to an Outreach message thread page.");
        return;
      }
      setStatus(`Scraped ${scraped.length} message(s) from page.`);
      for (let i = 0; i < scraped.length; i++) {
        const msg = scraped[i];
        log(`Scraped message ${i + 1}`, {
          step: msg.step,
          sequence_name: (msg.sequence_name || "").slice(0, 60),
          delivered_at: msg.delivered_at,
          sender: msg.sender_name,
          recipient: msg.recipient_name,
          persona: msg.persona,
          title: msg.title,
          body_text_len: (msg.body_text || "").length,
        });
      }
      setProgress("Check logs. Use Download scraped MD to save.");
    });

    btnScrapeDownload.addEventListener("click", () => {
      log("Button click: Download scraped MD");
      const scraped = lastScrapeResult || scrapePageContext();
      if (!scraped.length) {
        setStatus("No messages to download.");
        return;
      }
      const allMarkdown = scraped.map((msg) => {
        const ctx = buildContextJson(msg, {});
        return buildMarkdownOutput({ ...ctx, body_text: msg.body_text });
      }).join("\n\n---\n\n");
      downloadText(`outreach_scraped_${nowStamp()}.md`, allMarkdown, "text/markdown;charset=utf-8");
      setStatus(`Downloaded markdown (${scraped.length} messages).`);
    });

    btnCopyLogs.addEventListener("click", async () => {
      log("Button click: Copy logs");
      const box = document.querySelector("#or-logbox");
      if (!box) return;
      await navigator.clipboard.writeText(box.value);
      setStatus("Logs copied to clipboard.");
      setProgress("");
    });

    btnClearLogs.addEventListener("click", () => {
      log("Button click: Clear logs");
      const box = document.querySelector("#or-logbox");
      if (box) box.value = "";
      setStatus("Logs cleared.");
      setProgress("");
    });

    refreshUiSummary();
    setInterval(refreshUiSummary, 1500);
    const startupJob = loadUiJob();
    if (startupJob?.active) {
      const total = startupJob.rows?.length || 0;
      const idx = Number(startupJob.index || 0);
      setStatus(`Resuming saved UI job (${Math.min(idx + 1, total)}/${total})…`);
      setProgress("Auto-navigation is active.");
    }
    log("UI initialized", {
      page: location.href,
      script_version: SCRIPT_VERSION,
      strict_payload_only: STRICT_PAYLOAD_ONLY,
      mode: "Test B (DOM scrape)",
    });
  }

  // ───────────────────────────────────────────────
  // Bootstrap: install interceptors early, render UI after DOM ready
  // ───────────────────────────────────────────────

  installFetchInterceptor();
  installXhrInterceptor();
  setInterval(() => {
    installFetchInterceptor();
    installXhrInterceptor();
    processUiJobTick().catch((err) => {
      log("UI job tick error", { err: String(err || "unknown") });
    });
  }, 1800);

  // Since @run-at is document-idle, DOM is ready - render immediately
  renderUI();
  processUiJobTick().catch((err) => {
    log("UI job startup error", { err: String(err || "unknown") });
  });
})();
