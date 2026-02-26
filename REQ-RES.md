# API Request & Response Payloads

Complete reference for every enabled endpoint. Use this directly in **Thunder Client** (VS Code extension) or any HTTP client.

---

## Thunder Client Quick Setup

1. Install **Thunder Client** extension in VS Code
2. Create a new **Environment** called `Local`
3. Add variable: `base_url` → `http://localhost:8080/api/v1`
4. In every request, set **Header**: `Content-Type: application/json`
5. If auth is enabled, add **Header**: `Authorization: Bearer <YOUR_API_KEY>`

All requests below use `{{base_url}}` as the base.

---

## Base URL & Headers

| Field | Value |
|---|---|
| Base URL | `http://localhost:8080/api/v1` |
| Content-Type | `application/json` |
| Auth (if enabled) | `Authorization: Bearer <API_KEY>` |

Rate limiting is per-client. If you get `429`, wait for the `Retry-After` seconds shown in the response header.

---

## Response Envelope

Every response — success or error — uses this wrapper:

```json
{
  "success": true,
  "message": "human-readable description",
  "data": {},
  "request_id": "uuid-v4",
  "timestamp": "2026-02-18T12:00:00Z"
}
```

On error, `success` is `false` and an `error` object replaces `data`:

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid import params",
    "details": { "dataset_id": "is required" }
  },
  "request_id": "uuid-v4",
  "timestamp": "2026-02-18T12:00:00Z"
}
```

---

## 1. Health & Probes

### `GET {{base_url}}/health`
No body. No auth required.

**Response:**
```json
{
  "success": true,
  "message": "system healthy",
  "data": {
    "status": "healthy",
    "version": "0.1.0",
    "uptime": "5m10s",
    "timestamp": "2026-02-18T12:26:16Z",
    "checks": {
      "dispatcher":     { "status": "up", "message": "dispatched=4 rejected=0" },
      "job_store":      { "status": "up", "message": "in-memory store operational" },
      "priority_queue": { "status": "up", "message": "total=0 high=0 med=0 low=0" },
      "ram":            { "status": "up", "message": "12.3 MB / 500 MB (2.5%)" },
      "worker_pool":    { "status": "up", "message": "workers=5 in_flight=0 processed=10" }
    }
  }
}
```

### `GET {{base_url}}/ready`
No body. Returns `503` when RAM cap is exceeded.

**Response:**
```json
{ "success": true, "message": "ready", "data": { "status": "ready" } }
```

### `GET {{base_url}}/live`
No body. Basic liveness probe.

**Response:**
```json
{ "success": true, "message": "alive", "data": { "status": "alive" } }
```

### `GET {{base_url}}/stats`
No body. Returns dispatcher, RAM, and worker pool stats.

**Response:**
```json
{
  "success": true,
  "message": "system stats",
  "data": {
    "dispatcher": {
      "dispatched": 10,
      "rejected": 0,
      "rate_limited": 0,
      "pending": 0,
      "queued": 0
    },
    "ram": {
      "alloc_mb": 12.3,
      "cap_mb": 500,
      "usage_pct": 2.5,
      "under_cap": true
    },
    "worker_pool": {
      "active_workers": 5,
      "desired_workers": 5,
      "min_workers": 5,
      "max_workers": 10,
      "jobs_processed": 10,
      "jobs_failed": 0,
      "jobs_timed_out": 0,
      "in_flight": 0
    },
    "uptime": "5m10s"
  }
}
```

---

## 2. Worker Pool

### `GET {{base_url}}/workers`
No body.

**Response:**
```json
{
  "success": true,
  "message": "worker pool stats",
  "data": {
    "active_workers": 5,
    "desired_workers": 5,
    "min_workers": 5,
    "max_workers": 10,
    "jobs_processed": 10,
    "jobs_failed": 0,
    "jobs_timed_out": 0,
    "in_flight": 0
  }
}
```

### `POST {{base_url}}/workers/scale`
Scale the pool between **5 and 10** workers.

**Body:**
```json
{ "workers": 8 }
```

**Response:**
```json
{
  "success": true,
  "message": "worker pool scaled",
  "data": {
    "workers": 8,
    "stats": {
      "active_workers": 8,
      "desired_workers": 8,
      "min_workers": 5,
      "max_workers": 10,
      "jobs_processed": 10,
      "jobs_failed": 0,
      "jobs_timed_out": 0,
      "in_flight": 0
    }
  }
}
```

---

## 3. Datasets

### `GET {{base_url}}/datasets`
No body. Lists all in-memory datasets.

**Response:**
```json
{
  "success": true,
  "message": "datasets listed",
  "data": [
    {
      "id": "movies",
      "columns": ["MOVIES","YEAR","GENRE","RATING","ONE-LINE","STARS","VOTES","RunTime","Gross"],
      "record_count": 9999
    },
    {
      "id": "movies_final",
      "columns": ["MOVIES","YEAR","GENRE","RATING","ONE-LINE","STARS","VOTES","RunTime","Gross"],
      "record_count": 6422
    }
  ]
}
```

### `GET {{base_url}}/datasets/{datasetID}`
No body. Returns first **100 records** only (to avoid huge payloads). Check `truncated` flag.

**Example:** `GET {{base_url}}/datasets/movies_final`

**Response:**
```json
{
  "success": true,
  "message": "dataset retrieved",
  "data": {
    "id": "movies_final",
    "columns": ["MOVIES","YEAR","GENRE","RATING","ONE-LINE","STARS","VOTES","RunTime","Gross"],
    "record_count": 6422,
    "records": [
      { "MOVIES": "Blood Red Sky", "YEAR": "(2021)", "GENRE": "action, horror, thriller", "RATING": "6.1", "VOTES": "21,062", "RunTime": "121", "Gross": "0", ... }
    ],
    "truncated": true
  }
}
```

### `GET {{base_url}}/datasets/{datasetID}/export`
No body. Downloads the **full dataset as a CSV file**. Thunder Client will show the raw CSV in the response body.

**Example:** `GET {{base_url}}/datasets/movies_final/export`

Response headers:
```
Content-Type: text/csv
Content-Disposition: attachment; filename="movies_final_export.csv"
```

Response body (CSV):
```
MOVIES,YEAR,GENRE,RATING,ONE-LINE,STARS,VOTES,RunTime,Gross
Blood Red Sky,(2021),"action, horror, thriller",6.1,...
...
```

> **Tip in Thunder Client:** After the request, click **"Save Response"** → **"Save to File"** to save the CSV to disk.

---

## 4. Jobs (CRUD)

### `GET {{base_url}}/jobs`
No body. Supports query params for filtering and pagination.

**Query params (all optional):**

| Param | Values | Example |
|---|---|---|
| `status` | `pending` `queued` `running` `completed` `failed` `cancelled` | `?status=completed` |
| `type` | `etl.import` `etl.clean` `etl.normalize` `etl.deduplicate` `etl.pipeline` `image.resize` `image.convert` `image.watermark` `image.strip_metadata` `image.pipeline` | `?type=etl.import` |
| `priority` | `1` to `10` | `?priority=1` |
| `tag` | any string | `?tag=movies` |
| `page` | integer | `?page=1` |
| `page_size` | integer (default: 20) | `?page_size=50` |
| `sort_by` | `created_at` `priority` `status` | `?sort_by=created_at` |
| `sort_dir` | `asc` `desc` | `?sort_dir=desc` |

**Full example URL:** `{{base_url}}/jobs?status=completed&type=etl.import&page=1&page_size=20`

**Response:**
```json
{
  "success": true,
  "message": "jobs listed",
  "data": {
    "items": [
      {
        "id": "26515e83-0044-4dae-aa64-53ec355be64a",
        "type": "etl.import",
        "priority": 1,
        "status": "completed",
        "progress": { "total_items": 9999, "processed_items": 9999, "percentage": 100, "message": "Imported 9999 records into dataset 'movies'" },
        "created_at": "2026-02-18T13:05:00Z",
        "completed_at": "2026-02-18T13:05:01Z"
      }
    ],
    "total_count": 1,
    "page": 1,
    "page_size": 20,
    "total_pages": 1
  }
}
```

### `GET {{base_url}}/jobs/{jobID}`
No body.

**Response:**
```json
{
  "success": true,
  "message": "job retrieved",
  "data": {
    "id": "26515e83-0044-4dae-aa64-53ec355be64a",
    "type": "etl.import",
    "priority": 1,
    "status": "completed",
    "progress": {
      "total_items": 9999,
      "processed_items": 9999,
      "failed_items": 0,
      "skipped_items": 0,
      "percentage": 100,
      "current_step": "completed",
      "message": "Imported 9999 records into dataset 'movies'"
    },
    "created_at": "2026-02-18T13:05:00Z",
    "queued_at": "2026-02-18T13:05:00Z",
    "started_at": "2026-02-18T13:05:00Z",
    "completed_at": "2026-02-18T13:05:01Z",
    "params": { "source_file_path": "data/csv/movies.csv", "dataset_id": "movies", ... },
    "result": { "dataset_id": "movies", "total_records": 9999, "processed": 9999, "failed": 0 },
    "retry_count": 0,
    "max_retries": 3
  }
}
```

### `DELETE {{base_url}}/jobs/{jobID}`
No body. Cancels a `pending`, `queued`, or `running` job.

**Response:**
```json
{
  "success": true,
  "message": "job cancelled",
  "data": { "job_id": "26515e83-...", "status": "cancelled" }
}
```

**Error (if already completed):**
```json
{
  "success": false,
  "error": { "code": "INVALID_STATE", "message": "Cannot cancel a job with status: completed", "details": { "current_status": "completed" } }
}
```

### `GET {{base_url}}/jobs/{jobID}/progress`
No body. Lightweight progress poll — use this while job is `running`.

**Response:**
```json
{
  "success": true,
  "message": "progress retrieved",
  "data": {
    "job_id": "26515e83-...",
    "type": "etl.import",
    "status": "running",
    "progress": {
      "total_items": 9999,
      "processed_items": 3000,
      "percentage": 30,
      "current_step": "importing chunk 3/10",
      "message": "Processing chunk 3"
    },
    "elapsed": "1s"
  }
}
```

---

## 5. ETL / Data Migration Jobs

All ETL endpoints accept a **common wrapper** with task-specific `params`:

```json
{
  "name": "optional job name",
  "priority": 1,
  "max_retries": 3,
  "callback_url": "https://example.com/webhook",
  "tags": ["movies", "etl"],
  "metadata": { "env": "dev" },
  "params": { "...task-specific fields..." }
}
```

> **Priority scale:** `1` = highest (HIGH tier), `1–3` = HIGH, `4–7` = MEDIUM, `8–10` = LOW

### `POST {{base_url}}/jobs/etl/import`

Streams a CSV from disk into a named in-memory dataset. Supports chunked batch insertion.

**Body:**
```json
{
  "priority": 1,
  "params": {
    "source_file_path": "data/csv/movies.csv",
    "delimiter": ",",
    "has_header": true,
    "dataset_id": "movies",
    "batch_size": 500
  }
}
```

**All `params` fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `source_file_path` | string | yes* | — | Path on server (relative to working dir) |
| `source_url` | string | yes* | — | Download CSV from URL (alternative to file path) |
| `delimiter` | string | no | `,` | Column separator |
| `quote_char` | string | no | `"` | Quote character |
| `escape_char` | string | no | `\` | Escape character |
| `has_header` | bool | no | `false` | First row is column headers |
| `encoding` | string | no | `utf-8` | `utf-8` / `latin-1` / `utf-16` |
| `skip_rows` | int | no | `0` | Skip N rows from top before header |
| `max_rows` | int | no | `0` | Limit rows imported (0 = no limit) |
| `dataset_id` | string | **yes** | — | Name for the stored dataset |
| `batch_size` | int | no | `5000` | Records per insert batch (1–100000) |
| `column_mapping` | object | no | — | Rename columns: `{"old": "new"}` |
| `columns` | array | no | — | Manual column definitions (if no header) |

**Response:**
```json
{
  "success": true,
  "message": "CSV import job created",
  "data": {
    "job_id": "26515e83-0044-4dae-aa64-53ec355be64a",
    "type": "etl.import",
    "status": "queued",
    "priority": 1,
    "message": "Job accepted and queued for processing",
    "created_at": "2026-02-18T13:05:00Z"
  }
}
```

---

### `POST {{base_url}}/jobs/etl/clean`

Applies rule-based cleaning to an imported dataset.

**Body (movies.csv example — all rules used in testing):**
```json
{
  "priority": 1,
  "params": {
    "dataset_id": "movies",
    "create_copy": true,
    "output_dataset_id": "movies_clean",
    "null_handling": "skip",
    "rules": [
      { "column": "MOVIES",   "operation": "trim_whitespace" },
      { "column": "YEAR",     "operation": "trim_whitespace" },
      { "column": "GENRE",    "operation": "trim_whitespace" },
      { "column": "GENRE",    "operation": "regex_replace", "params": { "pattern": "\\s{2,}", "replacement": " " } },
      { "column": "RATING",   "operation": "trim_whitespace" },
      { "column": "RATING",   "operation": "fill_null",     "params": { "value": "N/A" } },
      { "column": "ONE-LINE", "operation": "trim_whitespace" },
      { "column": "STARS",    "operation": "trim_whitespace" },
      { "column": "STARS",    "operation": "regex_replace", "params": { "pattern": "\\s{2,}", "replacement": " " } },
      { "column": "VOTES",    "operation": "trim_whitespace" },
      { "column": "VOTES",    "operation": "fill_null",     "params": { "value": "0" } },
      { "column": "RunTime",  "operation": "trim_whitespace" },
      { "column": "RunTime",  "operation": "fill_null",     "params": { "value": "0" } },
      { "column": "Gross",    "operation": "trim_whitespace" },
      { "column": "Gross",    "operation": "fill_null",     "params": { "value": "0" } }
    ]
  }
}
```

**Available `operation` values for `CleaningRule`:**

| Operation | Description | `params` keys |
|---|---|---|
| `trim_whitespace` | Strip leading/trailing whitespace and newlines | — |
| `to_lowercase` | Convert value to lowercase | — |
| `to_uppercase` | Convert value to uppercase | — |
| `remove_html` | Strip HTML tags, unescape entities | — |
| `regex_replace` | Replace pattern with replacement string | `pattern`, `replacement` |
| `fill_null` | Replace empty/null value with a constant | `value` |
| `drop_null` | Remove entire record if this column is empty | — |
| `remove_special_chars` | Remove non-alphanumeric characters | — |
| `standardize_date` | Parse and reformat date to `2006-01-02` | — |
| `type_cast` | Cast value to a target type | `type` (`int`/`float`/`bool`) |

`null_handling` global options: `skip` · `drop` · `fill_default` · `fill_mean` · `fill_median`

---

### `POST {{base_url}}/jobs/etl/normalize`

Normalizes column values in an imported or cleaned dataset.

**Body (movies.csv example):**
```json
{
  "priority": 1,
  "params": {
    "dataset_id": "movies_clean",
    "create_copy": true,
    "output_dataset_id": "movies_norm",
    "rules": [
      { "column": "GENRE",  "operation": "to_lowercase" },
      { "column": "MOVIES", "operation": "trim" },
      { "column": "RATING", "operation": "trim" }
    ]
  }
}
```

**Available `operation` values for `NormalizationRule`:**

| Operation | Description | `params` keys |
|---|---|---|
| `min_max_scale` | Scale numeric values to `[0, 1]` | — |
| `z_score` | Standardize: `(x - mean) / std` | — |
| `to_lowercase` | Lowercase string | — |
| `to_uppercase` | Uppercase string | — |
| `trim` | Strip whitespace | — |
| `email_normalize` | Lowercase + trim email | — |
| `phone_format` | Reformat to E.164 (`+1...`) | `country_code` |
| `date_format` | Reformat date string | `input_format`, `output_format` |
| `enum_map` | Map free-text to fixed enum values | `mappings` (JSON obj) |
| `currency_format` | Strip symbols, standardize decimal | — |
| `unit_convert` | Multiply by factor | `factor` |

---

### `POST {{base_url}}/jobs/etl/deduplicate`

Detects and removes duplicate records.

**Body (movies.csv example — dry run first):**
```json
{
  "priority": 1,
  "params": {
    "dataset_id": "movies_norm",
    "match_columns": ["MOVIES"],
    "strategy": "exact",
    "keep_strategy": "first",
    "dry_run": true
  }
}
```

**Body (actual removal, with output copy):**
```json
{
  "priority": 1,
  "params": {
    "dataset_id": "movies_norm",
    "match_columns": ["MOVIES"],
    "strategy": "exact",
    "keep_strategy": "first",
    "dry_run": false,
    "create_copy": true,
    "output_dataset_id": "movies_final"
  }
}
```

**All `params` fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `dataset_id` | string | yes | Source dataset |
| `match_columns` | string[] | yes | Columns to match on (e.g. `["MOVIES"]`) |
| `strategy` | string | yes | `exact` or `fuzzy` |
| `fuzzy_threshold` | float | no | `0.0`–`1.0`, only used when `strategy=fuzzy` (default `0.8`) |
| `keep_strategy` | string | yes | `first` · `last` · `most_complete` |
| `dry_run` | bool | no | `true` = report only, no removal |
| `create_copy` | bool | no | Write result to a new dataset |
| `output_dataset_id` | string | no | Name for new deduplicated dataset |

**Response result (with `dry_run: false`):**
```json
{
  "success": true,
  "message": "Deduplication job created",
  "data": { "job_id": "...", "type": "etl.deduplicate", "status": "queued", ... }
}
```

Check job result via `GET /jobs/{jobID}`:
```json
"result": {
  "dataset_id": "movies_final",
  "total_records": 9999,
  "processed": 9999,
  "duplicates_found": 3577,
  "duration": "45ms"
}
```

---

### `POST {{base_url}}/jobs/etl/pipeline`

Chain import → clean → normalize → deduplicate in a single job.

**Body:**
```json
{
  "priority": 1,
  "params": {
    "name": "movies-full-pipeline",
    "source_file_path": "data/csv/movies.csv",
    "steps": [
      {
        "action": "import",
        "config": {
          "delimiter": ",",
          "has_header": true,
          "dataset_id": "movies",
          "batch_size": 500
        }
      },
      {
        "action": "clean",
        "config": {
          "dataset_id": "movies",
          "rules": [
            { "column": "GENRE",  "operation": "trim_whitespace" },
            { "column": "RATING", "operation": "fill_null", "params": { "value": "N/A" } },
            { "column": "VOTES",  "operation": "fill_null", "params": { "value": "0" } }
          ],
          "null_handling": "skip",
          "create_copy": true,
          "output_dataset_id": "movies_clean"
        }
      },
      {
        "action": "normalize",
        "config": {
          "dataset_id": "movies_clean",
          "rules": [{ "column": "GENRE", "operation": "to_lowercase" }],
          "create_copy": true,
          "output_dataset_id": "movies_norm"
        }
      },
      {
        "action": "deduplicate",
        "config": {
          "dataset_id": "movies_norm",
          "match_columns": ["MOVIES"],
          "strategy": "exact",
          "keep_strategy": "first",
          "create_copy": true,
          "output_dataset_id": "movies_final"
        }
      }
    ],
    "keep_intermediates": true
  }
}
```

---

## 6. Image Processing Jobs

> **Note:** Image processing jobs are submitted and queued correctly but the actual image transformation is not yet implemented (executor returns "not yet implemented"). The job lifecycle (create → queue → run → fail gracefully) works end-to-end.

All image endpoints use the same `CreateJobRequest` wrapper as ETL.

### `POST {{base_url}}/jobs/image/resize`

**Body:**
```json
{
  "priority": 3,
  "tags": ["images"],
  "params": {
    "source_path": "C:/data/images/src",
    "dest_path": "C:/data/images/out",
    "width": 1920,
    "height": 1080,
    "maintain_aspect_ratio": true,
    "quality": 85,
    "resample_filter": "lanczos",
    "allowed_extensions": ["jpg", "png"],
    "overwrite": true
  }
}
```

`resample_filter` values: `lanczos` · `bilinear` · `nearest` · `catmull_rom`

### `POST {{base_url}}/jobs/image/convert`

**Body:**
```json
{
  "priority": 3,
  "params": {
    "source_path": "C:/data/images/src",
    "dest_path": "C:/data/images/webp",
    "target_format": "webp",
    "quality": 80,
    "lossless": false,
    "strip_metadata": true,
    "overwrite": true
  }
}
```

`target_format` values: `webp` · `png` · `jpg` · `avif` · `tiff`

### `POST {{base_url}}/jobs/image/watermark`

**Body:**
```json
{
  "priority": 3,
  "params": {
    "source_path": "C:/data/images/src",
    "dest_path": "C:/data/images/wm",
    "watermark_text": "© MyBrand",
    "position": "bottom-right",
    "opacity": 0.3,
    "scale": 0.2,
    "padding": 8,
    "allowed_extensions": ["jpg", "png"],
    "overwrite": true
  }
}
```

`position` values: `center` · `top-left` · `top-right` · `bottom-left` · `bottom-right` · `tile`

### `POST {{base_url}}/jobs/image/strip-metadata`

**Body:**
```json
{
  "priority": 5,
  "params": {
    "source_path": "C:/data/images/src",
    "dest_path": "C:/data/images/clean",
    "strip_exif": true,
    "strip_iptc": true,
    "strip_xmp": true,
    "strip_icc": false,
    "overwrite": true
  }
}
```

If all four `strip_*` flags are `false`, all metadata types are stripped by default.

### `POST {{base_url}}/jobs/image/pipeline`

**Body:**
```json
{
  "priority": 2,
  "params": {
    "source_path": "C:/data/images/src",
    "dest_path": "C:/data/images/out",
    "steps": [
      { "action": "resize",    "config": { "width": 1280, "height": 720, "quality": 80, "resample_filter": "lanczos" } },
      { "action": "convert",   "config": { "target_format": "webp", "quality": 80 } },
      { "action": "watermark", "config": { "watermark_text": "© Brand", "position": "bottom-right", "opacity": 0.3 } }
    ],
    "keep_intermediates": false,
    "overwrite": true
  }
}
```

`action` values: `resize` · `convert` · `watermark` · `strip_metadata`

---

## 7. Error Reference

| HTTP | Code | Meaning |
|---|---|---|
| `400` | `INVALID_JSON` | Malformed body or unknown field (decoder is strict) |
| `400` | `VALIDATION_ERROR` | Missing required field or invalid value |
| `400` | `SCALE_FAILED` | Tried to scale outside 5–10 range |
| `404` | `NOT_FOUND` | Endpoint does not exist |
| `404` | `JOB_NOT_FOUND` | Job ID not found |
| `404` | `DATASET_NOT_FOUND` | Dataset ID not found |
| `405` | `METHOD_NOT_ALLOWED` | Wrong HTTP method for this endpoint |
| `409` | `INVALID_STATE` | Cannot cancel completed/failed job |
| `429` | `RATE_LIMIT_EXCEEDED` | Rate limit hit — check `Retry-After` header |
| `500` | `CREATE_FAILED` | Internal store write error |

---

## 8. Full Endpoint Summary

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check with subsystem details |
| GET | `/ready` | Readiness probe (fails if RAM over cap) |
| GET | `/live` | Liveness probe |
| GET | `/stats` | Dispatcher + RAM + worker pool stats |
| GET | `/workers` | Worker pool stats |
| POST | `/workers/scale` | Scale pool to 5–10 workers |
| GET | `/datasets` | List all in-memory datasets |
| GET | `/datasets/{id}` | Preview dataset (first 100 records) |
| GET | `/datasets/{id}/export` | Download full dataset as CSV |
| GET | `/jobs` | List jobs with filter/pagination |
| GET | `/jobs/{id}` | Get full job details |
| DELETE | `/jobs/{id}` | Cancel a job |
| GET | `/jobs/{id}/progress` | Lightweight progress poll |
| POST | `/jobs/etl/import` | Import CSV into a dataset |
| POST | `/jobs/etl/clean` | Clean a dataset |
| POST | `/jobs/etl/normalize` | Normalize a dataset |
| POST | `/jobs/etl/deduplicate` | Deduplicate a dataset |
| POST | `/jobs/etl/pipeline` | Run full ETL pipeline |
| POST | `/jobs/image/resize` | Bulk image resize |
| POST | `/jobs/image/convert` | Bulk image format convert |
| POST | `/jobs/image/watermark` | Bulk watermark |
| POST | `/jobs/image/strip-metadata` | Strip EXIF/IPTC/XMP/ICC |
| POST | `/jobs/image/pipeline` | Chained image pipeline |

