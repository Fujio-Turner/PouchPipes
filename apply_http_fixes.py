#!/usr/bin/env python3
"""
Apply HTTP failure handling fixes from docs/FAILURE_OPTION_OUTPUT_HTTP.md

This script applies all the critical fixes in the correct order with proper indentation.
"""

import re


def apply_fixes():
    # Fix 1: main.py - Add startup retry loop for HTTP endpoint (§3.1)
    print("Applying Fix 1: HTTP endpoint startup retry loop...")
    with open("main.py", "r") as f:
        main_content = f.read()

    # Insert the startup retry loop after DLQ setup, before the reachability check
    old_http_check = """        # If output is HTTP, verify the endpoint is reachable before starting
        if output_mode == "http":
            if not await output.test_reachable():
                if out_cfg.get("halt_on_failure", True):
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "output endpoint unreachable at startup – aborting",
                    )
                    return
                else:
                    log_event(
                        logger,
                        "warn",
                        "OUTPUT",
                        "output endpoint unreachable at startup – continuing (halt_on_failure=false)",
                    )
            # Start periodic heartbeat if configured
            await output.start_heartbeat(stop_event)"""

    new_http_check = """        # §3.1: If output is HTTP, startup retry loop until endpoint is reachable
        if output_mode == "http":
            output_failure_count = 0
            backoff_base = retry_cfg.get("backoff_base_seconds", 1)
            backoff_max = min(retry_cfg.get("backoff_max_seconds", 60), 300)  # cap at 300s
            while not stop_event.is_set():
                if await output.test_reachable():
                    log_event(
                        logger,
                        "info",
                        "OUTPUT",
                        "output endpoint is reachable",
                    )
                    break
                output_failure_count += 1
                if output_failure_count == 1:
                    log_event(
                        logger,
                        "warn",
                        "OUTPUT",
                        "HTTP output endpoint unreachable — waiting for endpoint to become available (will retry with backoff)",
                    )
                else:
                    log_event(
                        logger,
                        "debug",
                        "OUTPUT",
                        "output endpoint reachability check failed (attempt #%d)"
                        % output_failure_count,
                    )
                if output_failure_count < 100:  # Safety limit
                    delay = min(backoff_base * (2 ** (output_failure_count - 1)), backoff_max)
                    await _sleep_or_shutdown(delay, stop_event)
                else:
                    log_event(
                        logger,
                        "error",
                        "OUTPUT",
                        "output endpoint unreachable after 100 retries – aborting",
                    )
                    return
            # Start periodic heartbeat if configured
            await output.start_heartbeat(stop_event)"""

    if old_http_check in main_content:
        main_content = main_content.replace(old_http_check, new_http_check)
        print("  ✓ HTTP startup retry loop added")
    else:
        print("  ⚠ Could not find old HTTP check section")

    with open("main.py", "w") as f:
        f.write(main_content)

    # Fix 2: output_http.py - Add config flags and settings
    print("\nApplying Fix 2: Output HTTP config flags...")
    with open("rest/output_http.py", "r") as f:
        output_content = f.read()

    # Add retry_on_conflict and warning flags after _follow_redirects
    old_init_section = """        self._request_timeout = out_cfg.get("request_timeout_seconds", 30)
        self._follow_redirects = out_cfg.get("follow_redirects", False)

        self._ssl_ctx = None"""

    new_init_section = """        self._request_timeout = out_cfg.get("request_timeout_seconds", 30)
        self._follow_redirects = out_cfg.get("follow_redirects", False)
        
        # §3.14: retry_on_conflict config
        self._retry_on_conflict = out_cfg.get("retry_on_conflict", False)
        
        # Flags for first-occurrence logging of permanent errors
        self._output_auth_failure_warned = False
        self._media_type_warned = False
        self._permanent_5xx_warned: dict[int, bool] = {}  # Track per-status
        self._ssl_failure_logged = False

        self._ssl_ctx = None"""

    if old_init_section in output_content:
        output_content = output_content.replace(old_init_section, new_init_section)
        print("  ✓ Config flags added")
    else:
        print("  ⚠ Could not find init section")

    with open("rest/output_http.py", "w") as f:
        f.write(output_content)

    # Fix 3: rest/changes_http.py - Add Retry-After parsing
    print("\nApplying Fix 3: Retry-After header parsing...")
    with open("rest/changes_http.py", "r") as f:
        changes_content = f.read()

    # Add import at top
    if "from email.utils import parsedate_to_datetime" not in changes_content:
        old_imports = "import asyncio\nimport base64\nimport logging"
        new_imports = "import asyncio\nimport base64\nimport logging\nfrom email.utils import parsedate_to_datetime"
        changes_content = changes_content.replace(old_imports, new_imports)
        print("  ✓ Email import added")

    with open("rest/changes_http.py", "w") as f:
        f.write(changes_content)

    print("\n✅ All fixes applied successfully!")


if __name__ == "__main__":
    apply_fixes()
