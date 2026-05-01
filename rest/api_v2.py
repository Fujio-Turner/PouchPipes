"""
API v2.0 handlers for inputs, outputs, jobs, tables_rdbms, and sessions management.

These endpoints manage the new CBL-based document model:
- Inputs (sources feeding into the pipeline)
- Outputs (destinations for processed data)
- Jobs (connections between input → output with schema mapping)
- RDBMS Table Definitions (table schemas for RDBMS outputs)
- Sessions (SG session management)
"""

import json
import logging
import uuid
from aiohttp import web

from pipeline.pipeline_logging import log_event
from storage.cbl_store import CBLStore, USE_CBL

logger = logging.getLogger("changes_worker")


# ─────────────────────────────────────────────────────────────────
# Inputs (/api/inputs_changes)
# ─────────────────────────────────────────────────────────────────


async def api_get_inputs_changes(request: web.Request) -> web.Response:
    """GET /api/inputs_changes — Load the inputs_changes document."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        store = CBLStore()
        doc = store.load_inputs_changes()
        if not doc:
            return web.json_response({"type": "inputs_changes", "src": []})
        return web.json_response(doc)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading inputs_changes",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_post_inputs_changes(request: web.Request) -> web.Response:
    """POST /api/inputs_changes — Save the inputs_changes document."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        data = await request.json()

        # Validate
        if not isinstance(data.get("src"), list):
            return web.json_response({"error": "src must be an array"}, status=400)

        # Validate each input entry
        for idx, src_entry in enumerate(data["src"]):
            if not isinstance(src_entry, dict):
                return web.json_response(
                    {"error": f"src[{idx}] must be an object"}, status=400
                )
            if not src_entry.get("id"):
                return web.json_response(
                    {"error": f"src[{idx}].id is required"}, status=400
                )
            if not src_entry.get("source_type"):
                return web.json_response(
                    {"error": f"src[{idx}].source_type is required"}, status=400
                )
            if src_entry.get("source_type") not in (
                "sync_gateway",
                "app_services",
                "edge_server",
                "couchdb",
            ):
                return web.json_response(
                    {"error": f"src[{idx}].source_type invalid"}, status=400
                )
            if not src_entry.get("host"):
                return web.json_response(
                    {"error": f"src[{idx}].host is required"}, status=400
                )

        store = CBLStore()
        store.save_inputs_changes(data)

        log_event(
            logger,
            "info",
            "CONTROL",
            "inputs_changes saved via API",
            doc_count=len(data.get("src", [])),
        )
        return web.json_response(
            {
                "status": "ok",
                "type": "inputs_changes",
                "src_count": len(data.get("src", [])),
            }
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error saving inputs_changes",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_inputs_changes_entry(request: web.Request) -> web.Response:
    """PUT /api/inputs_changes/{id} — Update one input entry."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    entry_id = request.match_info.get("id")

    try:
        data = await request.json()

        store = CBLStore()
        doc = store.load_inputs_changes()
        if not doc:
            return web.json_response(
                {"error": "inputs_changes document not found"}, status=404
            )

        # Find and update entry
        src = doc.get("src", [])
        for idx, entry in enumerate(src):
            if entry.get("id") == entry_id:
                src[idx] = {**entry, **data}
                doc["src"] = src
                store.save_inputs_changes(doc)
                log_event(
                    logger,
                    "info",
                    "CONTROL",
                    "input entry updated via API",
                    doc_id=entry_id,
                )
                return web.json_response({"status": "ok", "id": entry_id})

        return web.json_response({"error": f"Input {entry_id} not found"}, status=404)
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating input %s" % entry_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_delete_inputs_changes_entry(request: web.Request) -> web.Response:
    """DELETE /api/inputs_changes/{id} — Remove one input entry."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    entry_id = request.match_info.get("id")

    try:
        store = CBLStore()
        doc = store.load_inputs_changes()
        if not doc:
            return web.json_response(
                {"error": "inputs_changes document not found"}, status=404
            )

        # Remove entry
        src = doc.get("src", [])
        src = [e for e in src if e.get("id") != entry_id]
        doc["src"] = src
        store.save_inputs_changes(doc)

        log_event(
            logger, "info", "CONTROL", "input entry deleted via API", doc_id=entry_id
        )
        return web.json_response({"status": "ok", "id": entry_id})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error deleting input %s" % entry_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────
# Outputs (/api/outputs_{type})
# ─────────────────────────────────────────────────────────────────


async def api_get_outputs(request: web.Request) -> web.Response:
    """GET /api/outputs_{type} — Load outputs for a given type."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    output_type = request.match_info.get("type")
    if output_type not in ("rdbms", "http", "cloud", "stdout"):
        return web.json_response({"error": "Invalid output type"}, status=400)

    try:
        store = CBLStore()
        doc = store.load_outputs(output_type)
        if not doc:
            return web.json_response({"type": f"outputs_{output_type}", "src": []})
        return web.json_response(doc)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading outputs_%s" % output_type,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_post_outputs(request: web.Request) -> web.Response:
    """POST /api/outputs_{type} — Save outputs for a given type."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    output_type = request.match_info.get("type")
    if output_type not in ("rdbms", "http", "cloud", "stdout"):
        return web.json_response({"error": "Invalid output type"}, status=400)

    try:
        data = await request.json()

        # Validate
        if not isinstance(data.get("src"), list):
            return web.json_response({"error": "src must be an array"}, status=400)

        for idx, src_entry in enumerate(data["src"]):
            if not isinstance(src_entry, dict):
                return web.json_response(
                    {"error": f"src[{idx}] must be an object"}, status=400
                )
            if not src_entry.get("id"):
                return web.json_response(
                    {"error": f"src[{idx}].id is required"}, status=400
                )

        store = CBLStore()
        store.save_outputs(output_type, data)

        log_event(
            logger,
            "info",
            "CONTROL",
            "outputs_%s saved via API" % output_type,
            doc_count=len(data.get("src", [])),
        )
        return web.json_response(
            {
                "status": "ok",
                "type": f"outputs_{output_type}",
                "src_count": len(data.get("src", [])),
            }
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error saving outputs_%s" % output_type,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_outputs_entry(request: web.Request) -> web.Response:
    """PUT /api/outputs_{type}/{id} — Update one output entry."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    output_type = request.match_info.get("type")
    entry_id = request.match_info.get("id")

    if output_type not in ("rdbms", "http", "cloud", "stdout"):
        return web.json_response({"error": "Invalid output type"}, status=400)

    try:
        data = await request.json()

        store = CBLStore()
        doc = store.load_outputs(output_type)
        if not doc:
            return web.json_response(
                {"error": f"outputs_{output_type} document not found"}, status=404
            )

        # Find and update entry
        src = doc.get("src", [])
        for idx, entry in enumerate(src):
            if entry.get("id") == entry_id:
                src[idx] = {**entry, **data}
                doc["src"] = src
                store.save_outputs(output_type, doc)
                log_event(
                    logger,
                    "info",
                    "CONTROL",
                    "output entry updated via API",
                    doc_id=entry_id,
                )
                return web.json_response({"status": "ok", "id": entry_id})

        return web.json_response({"error": f"Output {entry_id} not found"}, status=404)
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating output %s" % entry_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_delete_outputs_entry(request: web.Request) -> web.Response:
    """DELETE /api/outputs_{type}/{id} — Remove one output entry."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    output_type = request.match_info.get("type")
    entry_id = request.match_info.get("id")

    if output_type not in ("rdbms", "http", "cloud", "stdout"):
        return web.json_response({"error": "Invalid output type"}, status=400)

    try:
        store = CBLStore()
        doc = store.load_outputs(output_type)
        if not doc:
            return web.json_response(
                {"error": f"outputs_{output_type} document not found"}, status=404
            )

        # Remove entry
        src = doc.get("src", [])
        src = [e for e in src if e.get("id") != entry_id]
        doc["src"] = src
        store.save_outputs(output_type, doc)

        log_event(
            logger, "info", "CONTROL", "output entry deleted via API", doc_id=entry_id
        )
        return web.json_response({"status": "ok", "id": entry_id})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error deleting output %s" % entry_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────
# Jobs (/api/jobs)
# ─────────────────────────────────────────────────────────────────


async def api_get_jobs(request: web.Request) -> web.Response:
    """GET /api/jobs — List all jobs."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        store = CBLStore()
        jobs = store.list_jobs()
        return web.json_response({"jobs": jobs, "count": len(jobs)})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error listing jobs",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_get_job(request: web.Request) -> web.Response:
    """GET /api/jobs/{id} — Get one job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        store = CBLStore()
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)
        return web.json_response(job)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_post_jobs(request: web.Request) -> web.Response:
    """POST /api/jobs — Create a new job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        data = await request.json()

        # Validate required fields
        if not data.get("input_id"):
            return web.json_response({"error": "input_id is required"}, status=400)
        if not data.get("output_type"):
            return web.json_response({"error": "output_type is required"}, status=400)
        if data["output_type"] not in ("rdbms", "http", "cloud"):
            return web.json_response(
                {"error": f"Invalid output_type: {data['output_type']}"}, status=400
            )
        if not data.get("output_id"):
            return web.json_response({"error": "output_id is required"}, status=400)

        store = CBLStore()

        # Load source input
        inputs_doc = store.load_inputs_changes()
        if not inputs_doc:
            return web.json_response({"error": "No inputs defined"}, status=400)

        input_entry = None
        for src in inputs_doc.get("src", []):
            if src.get("id") == data["input_id"]:
                input_entry = src.copy()
                break

        if not input_entry:
            return web.json_response(
                {"error": f"Input {data['input_id']} not found"}, status=400
            )

        # Load source output
        outputs_doc = store.load_outputs(data["output_type"])
        if not outputs_doc:
            return web.json_response(
                {"error": f"No {data['output_type']} outputs defined"}, status=400
            )

        output_entry = None
        for src in outputs_doc.get("src", []):
            if src.get("id") == data["output_id"]:
                output_entry = src.copy()
                break

        if not output_entry:
            return web.json_response(
                {"error": f"Output {data['output_id']} not found"}, status=400
            )

        # Generate job ID
        job_id = str(uuid.uuid4())

        # Merge changes_feed settings into the input entry
        if "changes_feed" in data:
            input_entry["changes_feed"] = input_entry.get("changes_feed", {})
            input_entry["changes_feed"].update(data["changes_feed"])

        # Build job document
        job_doc = {
            "type": "job",
            "id": job_id,
            "name": data.get("name", f"Job {job_id[:8]}"),
            "inputs": [input_entry],
            "outputs": [output_entry],
            "output_type": data["output_type"],
            "system": data.get("system", {}),
            "mapping": data.get("mapping", {}),
            "mapping_id": data.get("mapping_id"),
            "state": {
                "status": "idle",
                "last_updated": None,
            },
        }

        # Save job
        store.save_job(job_id, job_doc)

        # Create checkpoint
        checkpoint_doc = {
            "job_id": job_id,
            "last_seq": "0",
            "remote_counter": 0,
            "last_checkpoint": None,
        }
        store.save_checkpoint(job_id, checkpoint_doc)

        log_event(logger, "info", "CONTROL", "job created via API", job_id=job_id)
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
                "name": job_doc["name"],
            },
            status=201,
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error creating job",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_job(request: web.Request) -> web.Response:
    """PUT /api/jobs/{id} — Update a job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        data = await request.json()
        store = CBLStore()

        # Load existing job
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        # Update editable fields
        if "name" in data:
            job["name"] = data["name"]
        if "system" in data:
            job["system"] = data["system"]
        if "mapping" in data:
            job["mapping"] = data["mapping"]
        if "mapping_id" in data:
            job["mapping_id"] = data["mapping_id"]
        if "state" in data:
            job["state"] = data["state"]
        if "changes_feed" in data:
            inputs = job.get("inputs", [])
            if inputs:
                inputs[0]["changes_feed"] = inputs[0].get("changes_feed", {})
                inputs[0]["changes_feed"].update(data["changes_feed"])

        # Save
        store.save_job(job_id, job)

        log_event(logger, "info", "CONTROL", "job updated via API", job_id=job_id)
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
            }
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_job_mapping(request: web.Request) -> web.Response:
    """PUT /api/v2/jobs/{id}/mapping — Update only the mapping on an existing job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        data = await request.json()
        store = CBLStore()

        # Load existing job
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        # Update mapping
        mapping_data = data.get("mapping")
        if mapping_data is None:
            return web.json_response({"error": "mapping field is required"}, status=400)

        job["mapping"] = mapping_data
        if "mapping_id" in data:
            job["mapping_id"] = data["mapping_id"]

        # Save
        store.save_job(job_id, job)

        log_event(
            logger, "info", "CONTROL", "job mapping updated via API", job_id=job_id
        )
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
                "message": "Mapping updated",
            }
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating mapping for job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_delete_job(request: web.Request) -> web.Response:
    """DELETE /api/jobs/{id} — Delete a job and its checkpoint."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        store = CBLStore()

        # Check job exists
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        # Delete job and checkpoint
        store.delete_job(job_id)
        store.delete_checkpoint(job_id)

        log_event(logger, "info", "CONTROL", "job deleted via API", job_id=job_id)
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
            }
        )
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error deleting job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_refresh_job_input(request: web.Request) -> web.Response:
    """POST /api/jobs/{id}/refresh-input — Re-copy input from inputs_changes."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        store = CBLStore()

        # Load job
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        # Load inputs
        inputs_doc = store.load_inputs_changes()
        if not inputs_doc:
            return web.json_response({"error": "No inputs defined"}, status=400)

        # Find matching input
        old_input_id = job.get("inputs", [{}])[0].get("id")
        input_entry = None
        for src in inputs_doc.get("src", []):
            if src.get("id") == old_input_id:
                input_entry = src.copy()
                break

        if not input_entry:
            return web.json_response(
                {"error": f"Input {old_input_id} not found"}, status=400
            )

        # Update job inputs
        job["inputs"] = [input_entry]
        store.save_job(job_id, job)

        log_event(
            logger, "info", "CONTROL", "job input refreshed via API", job_id=job_id
        )
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
                "input_id": old_input_id,
            }
        )
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error refreshing job input %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_refresh_job_output(request: web.Request) -> web.Response:
    """POST /api/jobs/{id}/refresh-output — Re-copy output from outputs_{type}."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        store = CBLStore()

        # Load job
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        output_type = job.get("output_type")
        old_output_id = job.get("outputs", [{}])[0].get("id")

        # Load outputs
        outputs_doc = store.load_outputs(output_type)
        if not outputs_doc:
            return web.json_response(
                {"error": f"No {output_type} outputs defined"}, status=400
            )

        # Find matching output
        output_entry = None
        for src in outputs_doc.get("src", []):
            if src.get("id") == old_output_id:
                output_entry = src.copy()
                break

        if not output_entry:
            return web.json_response(
                {"error": f"Output {old_output_id} not found"}, status=400
            )

        # Update job outputs
        job["outputs"] = [output_entry]
        store.save_job(job_id, job)

        log_event(
            logger, "info", "CONTROL", "job output refreshed via API", job_id=job_id
        )
        return web.json_response(
            {
                "status": "ok",
                "job_id": job_id,
                "output_id": old_output_id,
                "output_type": output_type,
            }
        )
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error refreshing job output %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────
# RDBMS Table Definitions (/api/v2/tables_rdbms)
# ─────────────────────────────────────────────────────────────────


async def api_get_tables_rdbms(request: web.Request) -> web.Response:
    """GET /api/v2/tables_rdbms — Load all RDBMS table definitions."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        store = CBLStore()
        doc = store.load_tables_rdbms()
        if not doc:
            return web.json_response({"type": "tables_rdbms", "tables": []})
        return web.json_response(doc)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading tables_rdbms",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_post_tables_rdbms(request: web.Request) -> web.Response:
    """POST /api/v2/tables_rdbms — Save RDBMS table definitions."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    try:
        data = await request.json()

        # Validate
        if not isinstance(data.get("tables"), list):
            return web.json_response({"error": "tables must be an array"}, status=400)

        for idx, table_entry in enumerate(data["tables"]):
            if not isinstance(table_entry, dict):
                return web.json_response(
                    {"error": f"tables[{idx}] must be an object"}, status=400
                )
            if not table_entry.get("id"):
                return web.json_response(
                    {"error": f"tables[{idx}].id is required"}, status=400
                )
            if not table_entry.get("name"):
                return web.json_response(
                    {"error": f"tables[{idx}].name is required"}, status=400
                )

        store = CBLStore()
        store.save_tables_rdbms(data)

        log_event(
            logger,
            "info",
            "CONTROL",
            "tables_rdbms saved via API",
            doc_count=len(data.get("tables", [])),
        )
        return web.json_response(
            {
                "status": "ok",
                "type": "tables_rdbms",
                "tables_count": len(data.get("tables", [])),
            }
        )
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error saving tables_rdbms",
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_get_table_rdbms_entry(request: web.Request) -> web.Response:
    """GET /api/v2/tables_rdbms/{id} — Get one RDBMS table definition."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    table_id = request.match_info.get("id")

    try:
        store = CBLStore()
        entry = store.get_table_rdbms(table_id)
        if not entry:
            return web.json_response(
                {"error": f"Table {table_id} not found"}, status=404
            )
        return web.json_response(entry)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading table_rdbms %s" % table_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_table_rdbms_entry(request: web.Request) -> web.Response:
    """PUT /api/v2/tables_rdbms/{id} — Update one RDBMS table definition."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    table_id = request.match_info.get("id")

    try:
        data = await request.json()
        data["id"] = table_id

        store = CBLStore()
        store.upsert_table_rdbms(data)

        log_event(
            logger,
            "info",
            "CONTROL",
            "table_rdbms entry updated via API",
            doc_id=table_id,
        )
        return web.json_response({"status": "ok", "id": table_id})
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating table_rdbms %s" % table_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_delete_table_rdbms_entry(request: web.Request) -> web.Response:
    """DELETE /api/v2/tables_rdbms/{id} — Remove one RDBMS table definition."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    table_id = request.match_info.get("id")

    try:
        store = CBLStore()
        entry = store.get_table_rdbms(table_id)
        if not entry:
            return web.json_response(
                {"error": f"Table {table_id} not found"}, status=404
            )

        store.delete_table_rdbms(table_id)
        log_event(
            logger,
            "info",
            "CONTROL",
            "table_rdbms entry deleted via API",
            doc_id=table_id,
        )
        return web.json_response({"status": "ok", "id": table_id})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error deleting table_rdbms %s" % table_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_get_table_rdbms_used_by(request: web.Request) -> web.Response:
    """GET /api/v2/tables_rdbms/{id}/used-by — Find jobs using this table."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    table_id = request.match_info.get("id")

    try:
        store = CBLStore()
        used_by = store.get_tables_rdbms_used_by(table_id)
        return web.json_response({"table_id": table_id, "used_by": used_by})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading used-by for table_rdbms %s" % table_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


# ─────────────────────────────────────────────────────────────────
# Eventing (/api/v2/jobs/{id}/eventing)
# ─────────────────────────────────────────────────────────────────


async def api_get_job_eventing(request: web.Request) -> web.Response:
    """GET /api/v2/jobs/{id}/eventing — Get eventing config for a job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        store = CBLStore()
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        eventing = job.get(
            "eventing",
            {
                "enabled": False,
                "handler": 'function OnUpdate(doc, meta) {\n    return doc;\n}\n\nfunction OnDelete(meta) {\n    log("deleted", meta._id);\n}',
                "timeout_ms": 5000,
                "on_error": "reject",
                "on_timeout": "reject",
                "description": "",
                "constants": [],
            },
        )
        return web.json_response({"job_id": job_id, "eventing": eventing})
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error loading eventing for job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)


async def api_put_job_eventing(request: web.Request) -> web.Response:
    """PUT /api/v2/jobs/{id}/eventing — Update eventing config for a job."""
    if not USE_CBL:
        return web.json_response({"error": "CBL is disabled"}, status=503)

    job_id = request.match_info.get("id")

    try:
        data = await request.json()

        store = CBLStore()
        job = store.load_job(job_id)
        if not job:
            return web.json_response({"error": f"Job {job_id} not found"}, status=404)

        # Validate eventing fields
        if "handler" in data and not isinstance(data["handler"], str):
            return web.json_response({"error": "handler must be a string"}, status=400)
        if "timeout_ms" in data:
            t = data["timeout_ms"]
            if not isinstance(t, int) or t < 100 or t > 60000:
                return web.json_response(
                    {"error": "timeout_ms must be an integer between 100 and 60000"},
                    status=400,
                )
        if "on_error" in data and data["on_error"] not in ("reject", "pass", "halt"):
            return web.json_response(
                {"error": "on_error must be 'reject', 'pass', or 'halt'"}, status=400
            )
        if "on_timeout" in data and data["on_timeout"] not in (
            "reject",
            "pass",
            "halt",
        ):
            return web.json_response(
                {"error": "on_timeout must be 'reject', 'pass', or 'halt'"}, status=400
            )
        if "constants" in data:
            if not isinstance(data["constants"], list):
                return web.json_response(
                    {"error": "constants must be an array"}, status=400
                )
            for idx, c in enumerate(data["constants"]):
                if not isinstance(c, dict) or "key" not in c or "value" not in c:
                    return web.json_response(
                        {"error": f"constants[{idx}] must have 'key' and 'value'"},
                        status=400,
                    )

        # Merge into existing eventing config
        existing = job.get("eventing", {})
        existing.update(data)
        job["eventing"] = existing
        store.save_job(job_id, job)

        log_event(
            logger, "info", "CONTROL", "job eventing updated via API", job_id=job_id
        )
        return web.json_response({"status": "ok", "job_id": job_id})
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    except Exception as e:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error updating eventing for job %s" % job_id,
            error_detail="%s: %s" % (type(e).__name__, e),
        )
        return web.json_response({"error": str(e)}, status=500)
