// ==UserScript==
// @name         Outreach - Export email bodies (API + UI runner)
// @namespace    https://web.outreach.io/
// @version      1.3
// @description  Upload link CSV and export message bodies. Supports API replay mode and UI-driven auto-navigation capture mode.
// @match        https://web.outreach.io/*
// @run-at       document-start
// @grant        none
// ==/UserScript==

(function () {
  "use strict";

  const UI_ID = "or-export-bodies-ui";
  const SCRIPT_VERSION = "1.3";

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
  const UI_JOB_KEY = "or-ui-capture-job-v1";
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
      `[OR][${ts()}] ${msg}` + (obj ? ` | ${safeStringify(obj)}` : "");
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
    // 1) Prefer OIDC user objects: keys like "oidc.user:https://id.outreach.io/:<clientid>"
    function scanOidc(storage, label) {
      if (!storage) return null;

      for (let i = 0; i < storage.length; i++) {
        const k = storage.key(i);
        if (!k || !k.startsWith("oidc.user:")) continue;

        const v = storage.getItem(k);
        if (!v) continue;

        try {
          const obj = JSON.parse(v);
          // Prefer access_token for API calls
          if (
            obj &&
            typeof obj.access_token === "string" &&
            obj.access_token.length > 20
          ) {
            return {
              token: obj.access_token,
              hint: `${label}:${k} (access_token)`,
            };
          }
          // Fallback: sometimes it's named differently
          if (
            obj &&
            typeof obj.accessToken === "string" &&
            obj.accessToken.length > 20
          ) {
            return {
              token: obj.accessToken,
              hint: `${label}:${k} (accessToken)`,
            };
          }
        } catch {
          // ignore
        }
      }
      return null;
    }

    const oidc1 = scanOidc(window.sessionStorage, "sessionStorage");
    if (oidc1) return oidc1;

    const oidc2 = scanOidc(window.localStorage, "localStorage");
    if (oidc2) return oidc2;

    // 2) Fallback: original heuristic scan for JWT-ish strings anywhere
    const candidates = [];

    function pushCandidate(where, key, token) {
      candidates.push({ where, key, token });
    }

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
              if (typeof cur === "string" && looksLikeJwt(cur)) {
                pushCandidate(label, k, cur);
              } else if (typeof cur === "object") {
                for (const val of Object.values(cur)) stack.push(val);
              }
            }
          } catch {
            // ignore
          }
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
    let i = 0,
      field = "",
      row = [],
      inQuotes = false;

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
          i++;
          continue;
        } else {
          field += c;
          i++;
          continue;
        }
      } else {
        if (c === '"') {
          inQuotes = true;
          i++;
          continue;
        }
        if (c === ",") {
          row.push(field);
          field = "";
          i++;
          continue;
        }
        if (c === "\n") {
          row.push(field);
          rows.push(row);
          row = [];
          field = "";
          i++;
          continue;
        }
        if (c === "\r") {
          i++;
          continue;
        }
        field += c;
        i++;
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
    try {
      return decodeURIComponent(message_id_encoded);
    } catch {
      return message_id_encoded;
    }
  }

  function getSeqIdFromLinks(rows) {
    for (const r of rows)
      if (r.sequence_id) return String(r.sequence_id).trim();
    return "";
  }

  function validateLinksCsv(header, rows) {
    for (const c of REQUIRED_LINK_COLS)
      if (!header.includes(c)) return `Missing required column: ${c}`;
    if (!rows.length) return "No rows found in links CSV.";
    return null;
  }

  function normalizeTextPreview(s) {
    const t = String(s || "")
      .replace(/\s+/g, " ")
      .replace(/\u00a0/g, " ")
      .trim();
    return t.length > BODY_PREVIEW_CHARS
      ? t.slice(0, BODY_PREVIEW_CHARS) + "…"
      : t;
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
    } catch {
      return "";
    }
  }

  function getTargetMessageIdForRow(row) {
    return normalizeMessageIdForCompare(
      urlDecodeMessageId(row?.message_id_encoded || ""),
    );
  }

  function messageIdsMatchLoose(a, b) {
    const x = normalizeMessageIdForCompare(a).replace(/[<>]/g, "");
    const y = normalizeMessageIdForCompare(b).replace(/[<>]/g, "");
    return !!x && !!y && x === y;
  }

  function normalizeMessageUrl(u) {
    try {
      const url = new URL(String(u || ""), location.origin);
      return `${url.origin}${decodeURIComponent(url.pathname)}`.replace(
        /\/+$/,
        "",
      );
    } catch {
      return "";
    }
  }

  function isBodyHiddenMarker(bodyText, bodyHtml) {
    return UI_HIDDEN_BODY_RE.test(String(bodyText || bodyHtml || "").trim());
  }

  function bodyChars(bodyText, bodyHtml) {
    return String(bodyText || "").length + String(bodyHtml || "").length;
  }

  function isLikelySupportWidgetContent({
    body_text,
    body_html,
    subject,
    capture_source,
  }) {
    const text = String(body_text || "").toLowerCase();
    const html = String(body_html || "").toLowerCase();
    const subj = String(subject || "").toLowerCase();
    const source = String(capture_source || "").toLowerCase();

    if (html.includes("smooch.io") || html.includes("zendesk sunshine"))
      return true;
    if (html.includes("messenger-button") || html.includes("type a message"))
      return true;
    if (text.includes("you're back online") && text.includes("outreach support"))
      return true;
    if (subj === "outreach support") return true;
    if (source.startsWith("iframe:") && text.includes("outreach support"))
      return true;

    return false;
  }

  function looksLikeMessageContent({ body_text, body_html }) {
    const text = String(body_text || "").toLowerCase();
    const html = String(body_html || "").toLowerCase();

    if (!text && !html) return false;
    if (isBodyHiddenMarker(body_text, body_html)) return true;
    if (
      /on\s+\w{3},\s+\w{3}\s+\d{1,2},\s+\d{4}\s+at/i.test(text) ||
      text.includes(" wrote:") ||
      text.includes("unsubscribe") ||
      text.includes("best,") ||
      text.includes("regards,")
    ) {
      return true;
    }
    if (
      html.includes("outreach-signature") ||
      html.includes("outreach-quote") ||
      html.includes("mailto:")
    ) {
      return true;
    }
    return bodyChars(body_text, body_html) > 1200;
  }

  function scoreBodyCandidate({
    body_text,
    body_html,
    body_masked,
    subject,
    message_id,
    capture_source,
  }) {
    const total = bodyChars(body_text, body_html);
    const hidden = !!body_masked || isBodyHiddenMarker(body_text, body_html);
    const supportWidget = isLikelySupportWidgetContent({
      body_text,
      body_html,
      subject,
      capture_source,
    });
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
      error,
    };
  }

  function loadUiJob() {
    const raw = localStorage.getItem(UI_JOB_KEY);
    if (!raw) return null;
    try {
      const job = JSON.parse(raw);
      if (!job || !Array.isArray(job.rows) || !Array.isArray(job.results))
        return null;
      return job;
    } catch {
      return null;
    }
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
      for (const item of node)
        collectBodyCandidates(item, out, depth + 1, seen);
      return;
    }

    if (typeof node !== "object") return;
    if (seen.has(node)) return;
    seen.add(node);

    const bodyText = (
      node.bodyText ||
      node.messageBodyText ||
      node.body_text ||
      ""
    ).toString();
    const bodyHtml = (
      node.bodyHtml ||
      node.messageBodyHtml ||
      node.body_html ||
      ""
    ).toString();

    if (bodyText || bodyHtml) {
      out.push({
        message_id: (
          node.messageId ||
          node.id ||
          node.message?.id ||
          node.message?.messageId ||
          ""
        ).toString(),
        prospect_id: (
          node.prospectId ||
          node.prospect?.id ||
          node.message?.prospectId ||
          ""
        ).toString(),
        subject: (node.subject || node.message?.subject || "").toString(),
        body_text: bodyText,
        body_html: bodyHtml,
        body_masked:
          !!node.bodyMasked ||
          !!node.message?.bodyMasked ||
          isBodyHiddenMarker(bodyText, bodyHtml),
      });
    }

    for (const v of Object.values(node))
      collectBodyCandidates(v, out, depth + 1, seen);
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
    try {
      return JSON.parse(t);
    } catch {
      return null;
    }
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
    if (!isThreadMessagesUrl(url) && !isThreadMessagesRequestMeta(reqMeta)) {
      return [];
    }
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
    const payloads = Array.isArray(bodyJson)
      ? bodyJson
      : bodyJson && typeof bodyJson === "object"
        ? [bodyJson]
        : [];

    const metas = payloads.map((p) => {
      const opName =
        p?.operationName ||
        findByKeyRegex(p, /^operationname$/i) ||
        findByKeyRegex(p, /operation/i) ||
        "";
      const msgId =
        findByKeyRegex(p?.variables || p, /^messageid$/i) ||
        findByKeyRegex(p?.variables || p, /message.?id/i) ||
        "";
      const prospectId =
        findByKeyRegex(p?.variables || p, /^prospectid$/i) ||
        findByKeyRegex(p?.variables || p, /prospect.?id/i) ||
        "";
      const sha =
        p?.extensions?.persistedQuery?.sha256Hash ||
        findByKeyRegex(p, /sha256/i) ||
        "";
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
        (m.message_id ? 4 : 0) +
        (m.prospect_id ? 2 : 0) +
        (opLooksRelevant(m.operation_name) ? 3 : 0) +
        (m.operation_name ? 1 : 0) +
        (m.has_query_text ? 1 : 0);
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
    return (
      u.includes("outreach.io") ||
      /graphql|message|thread|mail|prospect/i.test(u)
    );
  }

  function keepLatestCandidates() {
    if (state.interceptedCandidates.length <= FETCH_CAPTURE_MAX) return;
    state.interceptedCandidates =
      state.interceptedCandidates.slice(-FETCH_CAPTURE_MAX);
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
      const best = candidates
        .slice()
        .sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a))[0];
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
      let reqMethod =
        init?.method ||
        (args[0] && typeof args[0] === "object" ? args[0].method : "") ||
        "GET";
      let reqBodyText = "";
      const rawBody = init?.body;
      if (typeof rawBody === "string") reqBodyText = rawBody;
      else if (rawBody && typeof rawBody.toString === "function") {
        if (rawBody instanceof URLSearchParams) reqBodyText = rawBody.toString();
      }
      if (
        !reqBodyText &&
        args[0] &&
        typeof args[0] === "object" &&
        typeof args[0].clone === "function" &&
        typeof args[0].text === "function"
      ) {
        try {
          reqBodyText = await args[0].clone().text();
        } catch {
          // ignore request body read failures
        }
      }
      const reqMeta = extractRequestMeta({
        method: reqMethod,
        url: reqUrl,
        bodyText: reqBodyText,
      });

      const resp = await previousFetch(...args);

      try {
        const url = reqUrl;
        const contentType = resp.headers?.get("content-type");
        if (shouldInspectJsonResponse(url, contentType)) {
          resp
            .clone()
            .json()
            .then((json) => ingestInterceptedJson(url, json, reqMeta))
            .catch(() => {});
        }
      } catch {
        // ignore intercept errors
      }

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
      else if (
        args[0] &&
        typeof args[0].toString === "function" &&
        args[0] instanceof URLSearchParams
      ) {
        reqBodyText = args[0].toString();
      }
      this.__orReqMeta = extractRequestMeta({
        method: this.__orMethod || "GET",
        url: this.__orUrl || "",
        bodyText: reqBodyText,
      });

      this.addEventListener("load", function () {
        try {
          const url = this.__orUrl || "";
          const contentType = this.getResponseHeader("content-type") || "";
          if (!shouldInspectJsonResponse(url, contentType)) return;
          const text = this.responseText;
          if (!text || typeof text !== "string") return;
          const json = JSON.parse(text);
          ingestInterceptedJson(url, json, this.__orReqMeta || {});
        } catch {
          // ignore intercept errors
        }
      });
      return originalSend.apply(this, args);
    };
    XMLHttpRequest.prototype.open.__orWrappedForOrCapture = true;
    XMLHttpRequest.prototype.send.__orWrappedForOrCapture = true;

    window.__orXhrInterceptInstalled = true;
    log("XHR interceptor installed");
  }

  function bestSubjectFromPage() {
    const title = (document.title || "")
      .replace(/\s*-\s*Outreach.*$/i, "")
      .trim();
    const subjectSelectors = [
      '[data-testid*="subject"]',
      '[data-test*="subject"]',
      '[class*="subject"]',
      "h1",
    ];
    for (const sel of subjectSelectors) {
      const el = document.querySelector(sel);
      const t = (el?.textContent || "").replace(/\s+/g, " ").trim();
      if (t && t.length > 2 && t.length < 280) return t;
    }
    return title;
  }

  function bestBodyElementFromDom() {
    const selectors = [
      '[data-testid*="message-body"]',
      '[data-testid*="email-body"]',
      '[data-test*="message-body"]',
      '[data-test*="email-body"]',
      '[class*="message-body"]',
      '[class*="email-body"]',
      '[class*="mailing-body"]',
      '[class*="messageBody"]',
      '[class*="emailBody"]',
      ".outreach-quote",
    ];

    const seen = new Set();
    const candidates = [];

    function addEl(el, source) {
      if (!el || seen.has(el)) return;
      if (el.closest(`#${UI_ID}`)) return;
      seen.add(el);

      const text = (el.innerText || "").trim();
      const html = (el.innerHTML || "").trim();
      const chars = bodyChars(text, html);
      if (chars < 80) return;
      candidates.push({ el, source, chars });
    }

    for (const sel of selectors) {
      const list = document.querySelectorAll(sel);
      for (const el of list) addEl(el, sel);
    }

    if (!candidates.length) {
      const generic = document.querySelectorAll(
        "main div, article div, section div",
      );
      for (const el of generic) addEl(el, "generic-div-scan");
    }

    if (!candidates.length) return null;
    candidates.sort((a, b) => b.chars - a.chars);
    return candidates[0];
  }

  function extractFromDom() {
    const best = bestBodyElementFromDom();
    if (!best?.el) return null;

    const bodyText = (best.el.innerText || "").trim();
    const bodyHtml = (best.el.innerHTML || "").trim();
    if (!bodyText && !bodyHtml) return null;

    return {
      message_id: "",
      subject: bestSubjectFromPage(),
      body_text: bodyText,
      body_html: bodyHtml,
      capture_source: `dom:${best.source}`,
      seen_at_ms: Date.now(),
      page_url: location.href,
    };
  }

  function extractFromIframes() {
    const iframes = Array.from(document.querySelectorAll("iframe")).slice(
      0,
      10,
    );
    const candidates = [];

    for (let i = 0; i < iframes.length; i++) {
      try {
        const doc = iframes[i].contentDocument;
        const body = doc?.body;
        if (!body) continue;
        const bodyText = (body.innerText || "").trim();
        const bodyHtml = (body.innerHTML || "").trim();
        if (bodyChars(bodyText, bodyHtml) < 80) continue;

        candidates.push({
          message_id: "",
          subject: bestSubjectFromPage(),
          body_text: bodyText,
          body_html: bodyHtml,
          capture_source: `iframe:${i}`,
          seen_at_ms: Date.now(),
          page_url: location.href,
        });
      } catch {
        // cross-origin iframe; ignore
      }
    }

    if (!candidates.length) return null;
    candidates.sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a));
    return candidates[0];
  }

  function getWindowStateRoots() {
    const out = [];
    const pushRoot = (name, value) => {
      if (!value || typeof value !== "object") return;
      out.push({ name, value });
    };

    const directKeys = [
      "__APOLLO_STATE__",
      "__INITIAL_STATE__",
      "__PRELOADED_STATE__",
      "__NEXT_DATA__",
      "__NUXT__",
      "__REDUX_STATE__",
      "__STORE__",
      "apolloState",
      "initialState",
    ];
    for (const key of directKeys) {
      try {
        pushRoot(`window.${key}`, window[key]);
      } catch {
        // ignore inaccessible globals
      }
    }

    const rx = /(apollo|redux|store|state|cache)/i;
    const keys = Object.keys(window).slice(0, 1200);
    for (const key of keys) {
      if (out.length >= 30) break;
      if (!rx.test(key)) continue;
      try {
        pushRoot(`window.${key}`, window[key]);
      } catch {
        // ignore
      }
    }

    return out;
  }

  function extractFromWindowState() {
    const nowMs = Date.now();
    if (nowMs - state.lastStateScanAt < 2000) {
      return state.lastStateCandidate;
    }
    state.lastStateScanAt = nowMs;

    const roots = getWindowStateRoots();
    const all = [];

    for (const root of roots) {
      const local = [];
      collectBodyCandidates(root.value, local);
      for (const c of local) {
        all.push({
          ...c,
          capture_source: `state:${root.name}`,
          seen_at_ms: nowMs,
          page_url: location.href,
        });
      }
      if (all.length > 120) break;
    }

    if (!all.length) {
      state.lastStateCandidate = null;
      return null;
    }

    all.sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a));
    state.lastStateCandidate = all[0];
    return state.lastStateCandidate;
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
        if (
          targetProspectId &&
          String(c.prospect_id || "").trim() &&
          String(c.prospect_id || "").trim() !== targetProspectId
        ) {
          return false;
        }
        return true;
      });

    if (!matched.length) return null;
    matched.sort((a, b) => scoreBodyCandidate(b) - scoreBodyCandidate(a));
    return matched[0];
  }

  function extractPageSnapshot() {
    try {
      const root = document.querySelector("main") || document.body;
      if (!root) return null;

      const clone = root.cloneNode(true);
      const ui = clone.querySelector(`#${UI_ID}`);
      if (ui) ui.remove();
      for (const el of clone.querySelectorAll("script,style,noscript"))
        el.remove();
      for (const el of clone.querySelectorAll("iframe")) el.remove();
      for (const el of clone.querySelectorAll('[id*="messenger"],[class*="messenger"],[id*="widget"],[class*="widget"],[id*="support"],[class*="support"]')) {
        el.remove();
      }

      const bodyText = (clone.innerText || clone.textContent || "").trim();
      const bodyHtml = (clone.innerHTML || "").trim();
      if (bodyChars(bodyText, bodyHtml) < 120) return null;

      return {
        message_id: "",
        subject: bestSubjectFromPage(),
        body_text: bodyText,
        body_html: bodyHtml,
        capture_source: "page-snapshot",
        seen_at_ms: Date.now(),
        page_url: location.href,
      };
    } catch {
      return null;
    }
  }

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
      if (
        targetMessageId &&
        mid.replace(/[<>]/g, "") === targetMessageId.replace(/[<>]/g, "")
      ) {
        c.score += 90;
      }
      if (targetMessageId && messageIdsMatchLoose(c.req_message_id, targetMessageId))
        c.score += 480;
      if (targetMessageId && messageIdsMatchLoose(c.message_id, targetMessageId))
        c.score += 140;
      if (
        targetProspectId &&
        (String(c.req_prospect_id || "").trim() === targetProspectId ||
          String(c.prospect_id || "").trim() === targetProspectId)
      ) {
        c.score += 55;
      }
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

    const alive = state.interceptedCandidates.filter(
      (c) => nowMs - Number(c.seen_at_ms || 0) <= 120000,
    );
    state.interceptedCandidates = alive;
    if (!alive.length) return null;

    const exact = alive.filter((c) => {
      if (!targetMessageId) return false;
      if (c.capture_source !== "payload:Messages_GetThreadMessages") return false;
      if (!isThreadMessagesRequestMeta({ operation_name: c.req_operation_name }))
        return false;
      const reqMsg = c.req_message_id || "";
      const msgId = c.message_id || "";
      const byReq = reqMsg && messageIdsMatchLoose(reqMsg, targetMessageId);
      const byBody = msgId && messageIdsMatchLoose(msgId, targetMessageId);
      return byReq || byBody;
    });
    const pool = exact;
    if (!pool.length) return null;
    const samePage = targetPath
      ? pool.filter((c) => normalizeMessagePath(c.page_url) === targetPath)
      : [];
    const finalPool = samePage.length ? samePage : pool;

    const scored = finalPool.map((c) => {
      const candidate = normalizeCandidate(c);
      candidate.score = scoreBodyCandidate(candidate);
      if (targetMessageId && messageIdsMatchLoose(candidate.req_message_id, targetMessageId))
        candidate.score += 600;
      if (targetMessageId && messageIdsMatchLoose(candidate.message_id, targetMessageId))
        candidate.score += 220;
      if (
        targetProspectId &&
        (String(candidate.req_prospect_id || "").trim() === targetProspectId ||
          String(candidate.prospect_id || "").trim() === targetProspectId)
      ) {
        candidate.score += 70;
      }
      if (targetPath && normalizeMessagePath(candidate.page_url) === targetPath)
        candidate.score += 30;
      if (isThreadMessagesRequestMeta(candidate)) candidate.score += 100;
      if (candidate.capture_source === "payload:Messages_GetThreadMessages")
        candidate.score += 120;
      return candidate;
    });

    scored.sort((a, b) => b.score - a.score);
    return scored[0] || null;
  }

  function getBestFetchCandidateForRow(row) {
    const targetPath = normalizeMessagePath(row.message_url);
    const nowMs = Date.now();
    const alive = state.interceptedCandidates.filter(
      (c) => nowMs - Number(c.seen_at_ms || 0) <= 120000,
    );
    state.interceptedCandidates = alive;
    if (!alive.length) return null;

    const samePage = targetPath
      ? alive.filter((c) => normalizeMessagePath(c.page_url) === targetPath)
      : [];
    const pool = samePage.length ? samePage : alive;

    const scored = pool.map((c) => {
      const candidate = normalizeCandidate(c);
      candidate.score = scoreBodyCandidate(candidate);
      if (targetPath && normalizeMessagePath(candidate.page_url) === targetPath)
        candidate.score += 65;
      return candidate;
    });

    scored.sort((a, b) => b.score - a.score);
    return scored[0] || null;
  }

  function isCurrentPageForRow(row) {
    const now = normalizeMessagePath(location.href);
    const target = normalizeMessagePath(row.message_url);
    return !!now && !!target && now === target;
  }

  async function captureRowBodyViaUi(row, timeoutMs = UI_CAPTURE_TIMEOUT_MS) {
    const started = Date.now();
    const targetMessageId = getTargetMessageIdForRow(row);

    while (Date.now() - started < timeoutMs) {
      const targetedCandidate = getTargetedNetworkCandidateForRow(row);
      const strictStateCandidate = getDeterministicStateCandidateForRow(row);
      const best = chooseBestCandidateForRow(row, [
        targetedCandidate,
        strictStateCandidate,
      ]);

      const bestMessageIdMatch =
        !!best &&
        !!targetMessageId &&
        (messageIdsMatchLoose(best.message_id, targetMessageId) ||
          messageIdsMatchLoose(best.req_message_id, targetMessageId));
      const deterministicSourceOk =
        best?.capture_source === "payload:Messages_GetThreadMessages" ||
        best?.capture_source === `state:window.${TARGET_STATE_ROOT_KEY}`;

      if (best) {
        if (
          deterministicSourceOk &&
          bestMessageIdMatch &&
          best.hidden
        ) {
          return best;
        }
        if (
          !best.hidden &&
          !best.junk &&
          looksLikeMessageContent(best) &&
          bodyChars(best.body_text, best.body_html) >= UI_CAPTURE_MIN_GOOD_CHARS &&
          bestMessageIdMatch &&
          deterministicSourceOk
        ) {
          return best;
        }
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

  function buildUiResultsCsv(results) {
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
      "error",
    ];

    const lines = [header.join(",")];
    for (const r of results || []) {
      lines.push(header.map((k) => csvEscape(r?.[k] ?? "")).join(","));
    }
    return lines.join("\n");
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
      kind: "ui_capture",
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
        log("UI navigate", {
          row: idx + 1,
          total,
          url: normalizeMessageUrl(row.message_url),
        });
        location.assign(row.message_url);
        return;
      }

      setProgress(`Capturing row ${idx + 1}/${total} on page…`);
      log("UI capture start", {
        row: idx + 1,
        total,
        prospect_id: row.prospect_id,
      });

      const best = await captureRowBodyViaUi(row, UI_CAPTURE_TIMEOUT_MS);
      const out = createBlankResult(row);
      if (best) {
        out.message_id =
          best.message_id || urlDecodeMessageId(row.message_id_encoded);
        out.subject = best.subject || "";
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
        req_op: best?.req_operation_name || "",
        req_msgid_prefix: String(best?.req_message_id || "").slice(0, 24),
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

  function normalizeMessageIdForCompare(v) {
    const s = String(v || "").trim();
    if (!s) return "";
    try {
      return decodeURIComponent(s).trim().toLowerCase();
    } catch {
      return s.toLowerCase();
    }
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
      url: GQL_URL,
      attempt,
      operationName: OP_NAME,
      variables: {
        prospectId: Number(payload?.variables?.prospectId),
        messageId_prefix:
          String(payload?.variables?.messageId || "").slice(0, 24) + "...",
      },
      has_persisted_query: !!payload?.extensions?.persistedQuery,
    });

    const t0 = performance.now();
    const r = await fetch(GQL_URL, {
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
    const json = await r.json().catch(() => null);

    log("GraphQL response", {
      attempt,
      http_status: r.status,
      ok: r.ok,
      ms,
      has_data: !!json?.data,
      has_errors: Array.isArray(json?.errors) && json.errors.length > 0,
      error0: json?.errors?.[0]?.message
        ? String(json.errors[0].message).slice(0, 200)
        : "",
    });

    return { status: r.status, ok: r.ok, json, ms };
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
      const payloadNoPersisted = { ...payload };
      delete payloadNoPersisted.extensions;

      log("Persisted query miss; retrying without persistedQuery extension");
      resp = await gqlPost({
        bearer,
        payload: payloadNoPersisted,
        attempt: "no_persisted_extension",
      });

      if (resp.ok && resp.json && isPersistedQueryNotFound(resp.json)) {
        log("Persisted query still missing; retrying hash request once");
        await sleep(200);
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
      const id = normalizeMessageIdForCompare(item?.message?.id).replace(
        /[<>]/g,
        "",
      );
      return id && id === targetLoose;
    });
    return loose || collection[0];
  }

  function extractResult(linkRow, gqlJson, requestedMessageId) {
    const data = gqlJson?.data ?? null;
    const col = data?.threadMessages?.collection ?? [];
    const first = pickThreadEntryByMessageId(col, requestedMessageId);

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
      click_count: (outbox?.clickCount ?? "").toString(),
      open_count: (outbox?.openCount ?? "").toString(),
      state: (outbox?.state || msg.state || "").toString(),
      message_url: (linkRow.message_url || "").toString(),
      error: "",
    };
  }

  function buildResultsCsv(results) {
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
    for (const r of results)
      lines.push(header.map((k) => csvEscape(r[k])).join(","));
    return lines.join("\n");
  }

  async function runFetchBodies({ delayMs = 450, limit = null } = {}) {
    if (state.running) return;
    state.running = true;
    state.results = [];

    const bearer = state.bearer || findBearerInStorage().token;
    state.bearer = bearer;

    if (!bearer) {
      setStatus(
        "Could not find a Bearer token in local/session storage on this page.",
      );
      setProgress("Open a message detail page in Outreach, then retry.");
      state.running = false;
      return;
    }

    const rows = state.linksRows;
    const total = limit ? Math.min(limit, rows.length) : rows.length;

    setStatus(`Running… (fetching bodies for ${total} rows)`);
    log("Run start", { total, delayMs });

    for (let i = 0; i < total; i++) {
      const lr = rows[i];
      const prospectId = (lr.prospect_id || "").toString().trim();
      const messageIdEnc = (lr.message_id_encoded || "").toString().trim();
      const messageId = urlDecodeMessageId(messageIdEnc);

      if (!prospectId || !messageId) {
        setProgress(
          `Skipping row ${i + 1}/${total} (missing prospect_id or message_id_encoded)`,
        );
        state.results.push({
          sequence_id: lr.sequence_id || "",
          prospect_id: prospectId || "",
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
          message_url: lr.message_url || "",
          error: "missing prospect_id or message_id",
        });
        continue;
      }

      setProgress(`Row ${i + 1}/${total}: prospect ${prospectId}`);

      const resp = await gqlGetThreadMessage({ bearer, prospectId, messageId });

      if (!resp.ok || !resp.json) {
        state.results.push({
          sequence_id: lr.sequence_id || "",
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
          message_url: lr.message_url || "",
          error: `HTTP ${resp.status}`,
        });
        log("Row failed", { row: i + 1, http_status: resp.status });
      } else {
        const errors = resp.json.errors;
        if (Array.isArray(errors) && errors.length) {
          const emsg = (errors[0]?.message || "GraphQL error").toString();
          const isPersistedMiss = emsg
            .toLowerCase()
            .includes("persistedquerynotfound");
          state.results.push({
            sequence_id: lr.sequence_id || "",
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
            message_url: lr.message_url || "",
            error: isPersistedMiss
              ? "PersistedQueryNotFound (hash not recognized; open message detail in UI and retry)"
              : emsg,
          });
          log("Row error", { row: i + 1, error: emsg.slice(0, 240) });
        } else {
          const out = extractResult(lr, resp.json, messageId);
          state.results.push(out);

          const bodyPreview = out.body_text
            ? normalizeTextPreview(out.body_text)
            : out.body_html
              ? htmlToTextPreview(out.body_html)
              : "";

          log("Extracted", {
            row: i + 1,
            subject_prefix: (out.subject || "").slice(0, 80),
            body_preview: bodyPreview,
            body_text_len: out.body_text.length,
            body_html_len: out.body_html.length,
            body_hidden: /^\s*\[body hidden\]\s*$/i.test(
              out.body_text || out.body_html,
            ),
          });
        }
      }

      await sleep(delayMs);
    }

    setStatus(`Done. Fetched ${state.results.length} rows.`);
    setProgress("Click Download results CSV.");
    log("Run end", { results: state.results.length });

    state.running = false;
  }

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
    wrap.style.font =
      "12px/1.3 system-ui, -apple-system, Segoe UI, Roboto, Arial";
    wrap.style.minWidth = "520px";

    wrap.innerHTML = `
      <div style="font-weight:600; margin-bottom:6px;">Outreach export bodies (API + UI runner) v${SCRIPT_VERSION}</div>
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

      <div style="margin-bottom:8px; color:#333; font-weight:600;">API mode (fallback)</div>
      <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px;">
        <button id="or-find-token" style="padding:6px 8px; cursor:pointer;">Find token</button>
        <button id="or-run" style="padding:6px 8px; cursor:pointer;">Fetch via API</button>
        <button id="or-download" style="padding:6px 8px; cursor:pointer;">Download API CSV</button>
        <button id="or-copy-logs" style="padding:6px 8px; cursor:pointer;">Copy logs</button>
        <button id="or-clear-logs" style="padding:6px 8px; cursor:pointer;">Clear logs</button>
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

      <div style="margin-top:10px; color:#666;">
        UI runner strict mode: capture only targeted Messages_GetThreadMessages payloads with matching message_id. Logs also go to DevTools Console.
      </div>
    `;

    document.body.appendChild(wrap);

    const linksFile = wrap.querySelector("#or-links-file");
    const btnFindToken = wrap.querySelector("#or-find-token");
    const btnRun = wrap.querySelector("#or-run");
    const btnDownload = wrap.querySelector("#or-download");
    const btnUiStart = wrap.querySelector("#or-ui-start");
    const btnUiResume = wrap.querySelector("#or-ui-resume");
    const btnUiPause = wrap.querySelector("#or-ui-pause");
    const btnUiDownload = wrap.querySelector("#or-ui-download");
    const btnUiReset = wrap.querySelector("#or-ui-reset");
    const uiSummary = wrap.querySelector("#or-ui-job-summary");
    const delayInput = wrap.querySelector("#or-delay");
    const limitInput = wrap.querySelector("#or-limit");
    const btnCopyLogs = wrap.querySelector("#or-copy-logs");
    const btnClearLogs = wrap.querySelector("#or-clear-logs");

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

      log("Parsed CSV", {
        header_cols: parsed.header.length,
        rows: parsed.rows.length,
      });

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
      setStatus(
        `Loaded ${state.linksRows.length} link rows` +
          (seqId ? ` (seq ${seqId})` : "") +
          ".",
      );
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
      const job = createUiJob(
        state.linksRows,
        Number.isFinite(delayMs) ? delayMs : 900,
        limitVal,
      );
      saveUiJob(job);
      refreshUiSummary();

      setStatus(`UI runner started (${job.rows.length} rows).`);
      setProgress("Auto-navigation is active. Keep this tab open.");
      log("UI job start", {
        total: job.rows.length,
        delay_ms: job.delay_ms,
      });

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
      const csv = buildUiResultsCsv(job.results);
      downloadText(
        `outreach_message_bodies_ui_seq_${seqId}_${nowStamp()}.csv`,
        csv,
      );
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

    btnFindToken.addEventListener("click", () => {
      log("Button click: Find token");
      const found = findBearerInStorage();
      state.bearer = found.token;
      state.tokenHint = found.hint;

      if (found.token) {
        setStatus("Token found in browser storage.");
        setProgress(`Hint: ${found.hint || "unknown key"} (token not printed)`);
      } else {
        setStatus("Token not found in browser storage.");
        setProgress("Open a message detail page in Outreach, then retry.");
      }
      log("Token discovery result", { found: !!found.token, hint: found.hint });
    });

    btnRun.addEventListener("click", async () => {
      log("Button click: Fetch via API");

      if (!state.linksRows.length) {
        setStatus("Upload the links CSV first.");
        return;
      }

      const delayMs = parseInt(delayInput.value, 10);
      const limit = parseInt(limitInput.value, 10);
      const limitVal = Number.isFinite(limit) && limit > 0 ? limit : null;

      btnRun.disabled = true;
      btnDownload.disabled = true;
      btnFindToken.disabled = true;
      linksFile.disabled = true;

      await runFetchBodies({
        delayMs: Number.isFinite(delayMs) ? delayMs : 450,
        limit: limitVal,
      });

      btnRun.disabled = false;
      btnDownload.disabled = false;
      btnFindToken.disabled = false;
      linksFile.disabled = false;
    });

    btnDownload.addEventListener("click", () => {
      log("Button click: Download API CSV");
      if (!state.results.length) {
        setStatus("No API results yet. Run Fetch via API first.");
        return;
      }
      const seqId = getSeqIdFromLinks(state.linksRows) || "unknown";
      const csv = buildResultsCsv(state.results);
      downloadText(
        `outreach_message_bodies_seq_${seqId}_${nowStamp()}.csv`,
        csv,
      );
      setStatus(`Downloaded results CSV (${state.results.length} rows).`);
      setProgress("");
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
      setStatus(
        `Resuming saved UI job (${Math.min(idx + 1, total)}/${total})…`,
      );
      setProgress("Auto-navigation is active.");
    }
    log("UI initialized", {
      page: location.href,
      script_version: SCRIPT_VERSION,
      strict_payload_only: STRICT_PAYLOAD_ONLY,
    });
  }

  installFetchInterceptor();
  installXhrInterceptor();
  setInterval(() => {
    installFetchInterceptor();
    installXhrInterceptor();
    processUiJobTick().catch((err) => {
      log("UI job tick error", { err: String(err || "unknown") });
    });
  }, 1800);

  setTimeout(() => {
    renderUI();
    processUiJobTick().catch((err) => {
      log("UI job startup error", { err: String(err || "unknown") });
    });
  }, 1200);
})();
