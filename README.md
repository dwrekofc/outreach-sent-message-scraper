# outreach-sent-message-scraper

Tampermonkey userscripts for exporting Outreach message content, with both legacy 2-step flows and a V2 one-go flow.

## Repository Intent

This repo exists so team members can run and support Outreach export scripts without reverse-engineering logic from browser DevTools each time.

Primary use case:
1. Gather message links from Outreach views.
2. Resolve message bodies/metadata for those links.
3. Download CSV (and in one variant markdown) outputs for downstream analysis.

## Source Script Inventory (All 4 Scripts)

### `src/step-1-collect-message-links.user.js`

- Script header: `Outreach Step 1 - Collect Message Links` v`1.3`
- Actual behavior today: functionally aligned with Step 2 exporter logic (UI runner + API mode), not a minimal links-only collector.
- Run mode: manual UI panel in Outreach (`document-start`).
- Expected input CSV columns: `prospect_id`, `message_id_encoded`, `message_url`.
- Output files:
  - `outreach_message_bodies_ui_seq_<seq>_<timestamp>.csv`
  - `outreach_message_bodies_seq_<seq>_<timestamp>.csv`
- Persistence key: `localStorage["or-ui-capture-job-v1"]`
- Intended status: legacy compatibility script.

### `src/step-2-export-message-bodies.user.js`

- Script header: `Outreach Step 2 - Export Message Bodies` v`1.3`
- Purpose: upload links CSV and export message bodies.
- Run mode: manual UI panel in Outreach (`document-start`).
- Execution paths:
  - UI runner mode (navigates message URLs, captures targeted payload/body candidates).
  - API mode fallback (GraphQL call to `Messages_GetThreadMessages`).
- Expected input CSV columns: `prospect_id`, `message_id_encoded`, `message_url`.
- Output files:
  - `outreach_message_bodies_ui_seq_<seq>_<timestamp>.csv`
  - `outreach_message_bodies_seq_<seq>_<timestamp>.csv`
- Persistence key: `localStorage["or-ui-capture-job-v1"]`
- Intended status: legacy/manual mainline.

### `src/step-2b-export-message-bodies.user.js`

- Script header: `Outreach Step 2B - Export Message Bodies (Test B)` v`2B-1.0`
- Purpose: Step 2 variant with additional DOM-enriched context extraction.
- Run mode: manual UI panel in Outreach (`document-idle`).
- Expected input CSV columns: `prospect_id`, `message_id_encoded`, `message_url`.
- Output files:
  - `outreach_message_bodies_2b_seq_<seq>_<timestamp>.csv`
  - `outreach_scraped_<timestamp>.md`
- Persistence key: `localStorage["or-ui-capture-job-2b-v1"]`
- Intended status: experimental/enriched export branch.

### `src/v2-outreach-one-go.user.js`

- Script header: `Outreach V2 - One Go Links + Bodies Export` v`2.0.0`
- Purpose: single-run flow replacing manual step handoff.
- Run mode: minimal UI panel in Outreach (`document-idle`), controls: `Start V2`, `Stop`.
- End-to-end flow:
  1. Collect message links by DOM scan + auto-scroll + next-page traversal.
  2. Auto-detect page progression (`current/total` when discoverable).
  3. Download links CSV.
  4. Reuse generated links CSV content in memory for body fetch phase.
  5. Fetch message bodies via GraphQL.
  6. Download final bodies CSV.
- Output files:
  - `outreach_message_links_v2_seq_<seq>_<timestamp>.csv`
  - `outreach_message_bodies_v2_seq_<seq>_<timestamp>.csv`
- Intended status: current preferred automation path.

## LLM-Friendly Operational Summary

```yaml
scripts:
  - path: src/step-1-collect-message-links.user.js
    version: "1.3"
    role: "legacy exporter (name suggests links-only, behavior is full exporter)"
    input_csv_required: true
    output:
      - outreach_message_bodies_ui_seq_<seq>_<timestamp>.csv
      - outreach_message_bodies_seq_<seq>_<timestamp>.csv
    local_storage_keys:
      - or-ui-capture-job-v1
  - path: src/step-2-export-message-bodies.user.js
    version: "1.3"
    role: "legacy step-2 exporter (UI runner + API fallback)"
    input_csv_required: true
    output:
      - outreach_message_bodies_ui_seq_<seq>_<timestamp>.csv
      - outreach_message_bodies_seq_<seq>_<timestamp>.csv
    local_storage_keys:
      - or-ui-capture-job-v1
  - path: src/step-2b-export-message-bodies.user.js
    version: "2B-1.0"
    role: "experimental step-2 with enriched DOM fields + markdown output"
    input_csv_required: true
    output:
      - outreach_message_bodies_2b_seq_<seq>_<timestamp>.csv
      - outreach_scraped_<timestamp>.md
    local_storage_keys:
      - or-ui-capture-job-2b-v1
  - path: src/v2-outreach-one-go.user.js
    version: "2.0.0"
    role: "preferred one-go flow (collect links + fetch bodies in one run)"
    input_csv_required: false
    output:
      - outreach_message_links_v2_seq_<seq>_<timestamp>.csv
      - outreach_message_bodies_v2_seq_<seq>_<timestamp>.csv
```

## CSV Contract

### Links CSV (producer: V2 Phase 1, legacy Step 1 external flows)

- Required columns:
  - `sequence_id`
  - `prospect_id`
  - `message_id_encoded`
  - `message_url`

Note:
- Legacy Step 2/2B validate only `prospect_id`, `message_id_encoded`, `message_url`.
- V2 includes `sequence_id` in generated links CSV for traceability.

### Bodies CSV (producer: Step 2/Step 2B/V2)

- Common fields:
  - `sequence_id`
  - `prospect_id`
  - `message_id`
  - `subject`
  - `body_text`
  - `body_html`
  - `delivered_at`
  - `opened_at`
  - `replied_at`
  - `open_count`
  - `click_count`
  - `state`
  - `message_url`
  - `error`

## Support + Maintenance Notes

### Authentication and GraphQL

- All body exporters rely on a browser session token (Bearer from local/session storage).
- API endpoint currently used by all scripts:
  - `https://app2b.outreach.io/graphql/Messages_GetThreadMessages`
- Operation:
  - `Messages_GetThreadMessages`
- Persisted query hash fallback behavior exists (hash-only, then non-persisted retry).

### High-Risk Change Areas

- Outreach DOM changes:
  - pagination controls
  - message link URL shape
  - message body container signatures
- GraphQL contract changes:
  - operation name
  - sha256 persisted hash
  - payload field names

### Troubleshooting Checklist

1. If no links are collected in V2:
   - Confirm you are in a list view that exposes message/prospect links in anchors.
   - Inspect URL pattern changes and update V2 `parseMessageLink()` heuristics.
2. If bodies fail with auth issues:
   - Open a concrete message detail view in Outreach.
   - Retry so token discovery can find current OIDC/session token.
3. If GraphQL returns persisted query misses:
   - Script retries without persisted extension automatically.
   - If failures persist, update endpoint/hash/operation payload.
4. If UI runner captures junk/support-widget text:
   - Tune support-content filters and candidate scoring.

## Recommended Usage

- Use `src/v2-outreach-one-go.user.js` for normal operations.
- Keep Step 2 and Step 2B as fallback/debug paths when V2 heuristics need adjustment.
- Treat Step 1 as legacy compatibility and not the preferred daily path.
