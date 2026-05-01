#!/usr/bin/env python3
"""
REST API endpoints for Phase 10: Multi-Job Control.

Endpoints:
  POST /api/jobs/{id}/start — start a job
  POST /api/jobs/{id}/stop — stop a job
  POST /api/jobs/{id}/restart — restart a job
  POST /api/_restart — restart all jobs
  POST /api/_offline — pause all jobs
  POST /api/_online — resume all jobs
  GET /api/jobs/{id}/state — get job state
"""

import asyncio
import json
import logging
from functools import partial
from typing import Optional, Any, Dict

import aiohttp.web

from pipeline.pipeline_logging import log_event
from pipeline.pipeline_manager import PipelineManager

logger = logging.getLogger("changes_worker")


def register_job_control_routes(
    app: aiohttp.web.Application,
    manager: Optional[PipelineManager],
) -> None:
    """Register Phase 10 job control endpoints."""
    if not manager:
        return

    app.router.add_post(
        "/api/jobs/{job_id}/start", partial(api_job_start, manager=manager)
    )
    app.router.add_post(
        "/api/jobs/{job_id}/stop", partial(api_job_stop, manager=manager)
    )
    app.router.add_post(
        "/api/jobs/{job_id}/restart", partial(api_job_restart, manager=manager)
    )
    app.router.add_post(
        "/api/jobs/{job_id}/kill", partial(api_job_kill, manager=manager)
    )
    app.router.add_get(
        "/api/jobs/{job_id}/state", partial(api_job_state, manager=manager)
    )
    app.router.add_post("/api/_restart", partial(api_restart_all, manager=manager))
    app.router.add_post("/api/_offline", partial(api_offline_all, manager=manager))
    app.router.add_post("/api/_online", partial(api_online_all, manager=manager))


async def api_job_start(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/jobs/{job_id}/start — Start a job."""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return aiohttp.web.json_response(
            {"error": "job_id required"},
            status=400,
        )

    log_event(logger, "info", "CONTROL", "starting job", job_id=job_id)
    loop = asyncio.get_event_loop()
    try:
        # Check if already running before attempting start
        state = await loop.run_in_executor(None, manager.get_job_state, job_id)
        if state and state.get("status") == "running":
            log_event(logger, "info", "CONTROL", "job already running", job_id=job_id)
            return aiohttp.web.json_response(
                {"status": "already_running", "job_id": job_id},
                status=409,
            )

        success = await loop.run_in_executor(None, manager.start_job, job_id)
        log_event(
            logger, "info", "CONTROL", "job start result: %s" % success, job_id=job_id
        )
        if success:
            return aiohttp.web.json_response(
                {"status": "started", "job_id": job_id},
                status=200,
            )
        else:
            return aiohttp.web.json_response(
                {"error": f"Failed to start job {job_id} — check logs for details"},
                status=500,
            )
    except Exception as exc:
        log_event(
            logger,
            "error",
            "CONTROL",
            "error starting job",
            job_id=job_id,
            error_detail="%s: %s" % (type(exc).__name__, exc),
        )
        return aiohttp.web.json_response(
            {"error": str(exc)},
            status=500,
        )


async def api_job_stop(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/jobs/{job_id}/stop — Stop a job."""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return aiohttp.web.json_response(
            {"error": "job_id required"},
            status=400,
        )

    log_event(logger, "info", "CONTROL", "stopping job", job_id=job_id)
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(None, manager.stop_job, job_id)
    if success:
        return aiohttp.web.json_response(
            {"status": "stopped", "job_id": job_id},
            status=200,
        )
    else:
        log_event(
            logger,
            "error",
            "CONTROL",
            "failed to stop job",
            job_id=job_id,
            error_detail="stop timed out",
        )
        return aiohttp.web.json_response(
            {"error": "Failed to stop job %s within timeout" % job_id},
            status=500,
        )


async def api_job_restart(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/jobs/{job_id}/restart — Restart a job."""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return aiohttp.web.json_response(
            {"error": "job_id required"},
            status=400,
        )

    log_event(logger, "info", "CONTROL", "restarting job", job_id=job_id)
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(None, manager.restart_job, job_id)
    if success:
        return aiohttp.web.json_response(
            {"status": "restarted", "job_id": job_id},
            status=200,
        )
    else:
        log_event(
            logger,
            "error",
            "CONTROL",
            "failed to restart job",
            job_id=job_id,
            error_detail="restart returned false",
        )
        return aiohttp.web.json_response(
            {"error": "Failed to restart job %s" % job_id},
            status=500,
        )


async def api_job_kill(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/jobs/{job_id}/kill — Kill a job (non-graceful)."""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return aiohttp.web.json_response(
            {"error": "job_id required"},
            status=400,
        )

    log_event(logger, "info", "CONTROL", "killing job", job_id=job_id)
    # For now, kill is the same as stop (hard stop)
    loop = asyncio.get_event_loop()
    success = await loop.run_in_executor(None, manager.stop_job, job_id)
    if success:
        return aiohttp.web.json_response(
            {"status": "killed", "job_id": job_id},
            status=200,
        )
    else:
        log_event(
            logger,
            "error",
            "CONTROL",
            "failed to kill job",
            job_id=job_id,
            error_detail="kill returned false",
        )
        return aiohttp.web.json_response(
            {"error": "Failed to kill job %s" % job_id},
            status=500,
        )


async def api_job_state(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """GET /api/jobs/{job_id}/state — Get job state."""
    job_id = request.match_info.get("job_id")
    if not job_id:
        return aiohttp.web.json_response(
            {"error": "job_id required"},
            status=400,
        )

    loop = asyncio.get_event_loop()
    state = await loop.run_in_executor(None, manager.get_job_state, job_id)
    if state:
        return aiohttp.web.json_response(state, status=200)
    else:
        return aiohttp.web.json_response(
            {"error": f"Job {job_id} not found"},
            status=404,
        )


async def api_restart_all(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/_restart — Restart all jobs."""
    log_event(logger, "info", "CONTROL", "restart all requested via /api/_restart")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, manager.restart_all)
    states = await loop.run_in_executor(None, manager.list_job_states)
    return aiohttp.web.json_response(
        {"status": "restarting_all", "jobs": states},
        status=200,
    )


async def api_offline_all(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/_offline — Pause all jobs (offline mode). Manager stays alive."""
    if manager.is_offline():
        return aiohttp.web.json_response(
            {"status": "already_offline"},
            status=200,
        )
    log_event(logger, "info", "CONTROL", "offline requested via /api/_offline")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, manager.go_offline)
    return aiohttp.web.json_response(
        {"status": "offline"},
        status=200,
    )


async def api_online_all(
    request: aiohttp.web.Request,
    manager: PipelineManager,
) -> aiohttp.web.Response:
    """POST /api/_online — Resume all enabled jobs (online mode)."""
    if not manager.is_offline():
        return aiohttp.web.json_response(
            {"status": "already_online"},
            status=200,
        )
    log_event(logger, "info", "CONTROL", "online requested via /api/_online")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, manager.go_online)
    states = await loop.run_in_executor(None, manager.list_job_states)
    return aiohttp.web.json_response(
        {"status": "online", "jobs": states},
        status=200,
    )
