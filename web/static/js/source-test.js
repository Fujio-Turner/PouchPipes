/**
 * Shared "Test Source Connection" helper.
 *
 * Used by:
 *   - templates/inputs.html  (form + table row)
 *   - templates/jobs.html    (job-builder source picker)
 *   - any future page that pings /api/wizard/test-source
 *
 * Calls POST /api/wizard/test-source and renders the result via either
 * showToast() (when available) or a plain alert().
 *
 * Possible API responses we handle:
 *   { ok: true, doc, pool_size, ... }              → success
 *   { ok: true, warning: "no_docs", ... }          → success but feed empty
 *   { error: "...", detail, status, hint, ... }    → failure
 */
(function (global) {
  'use strict';

  function buildTriedUrl(payload, data) {
    if (data && data.url) return data.url;
    var gw = payload.gateway || {};
    var seg = gw.database || '';
    if (gw.scope && gw.scope !== '_default') seg += '.' + gw.scope;
    if (gw.collection && gw.collection !== '_default') seg += '.' + gw.collection;
    return (gw.url || '') + '/' + seg + '/_changes';
  }

  function notifyOk(msg) {
    if (typeof showToast === 'function') showToast(msg, 'success');
    else alert(msg);
  }
  function notifyWarn(msg) {
    if (typeof showToast === 'function') showToast(msg, 'warning');
    else alert(msg);
  }
  function notifyErr(toast, detail) {
    if (typeof showToast === 'function') showToast(toast, 'error');
    if (detail) alert(detail);
  }

  /**
   * Run a test against /api/wizard/test-source and report the outcome.
   *
   * @param {Object}   opts
   * @param {Object}   opts.payload     - { gateway: {...}, auth: {...} }
   * @param {string}   [opts.label]     - optional input id / label for messages
   * @param {Function} [opts.onStart]   - called before fetch
   * @param {Function} [opts.onDone]    - called in finally{}
   * @param {Function} [opts.onResult]  - (data, kind) where kind in
   *                                      "ok" | "no_docs" | "error"
   *                                      lets the caller render its own UI
   *                                      (e.g. inline status text instead of a toast)
   * @returns {Promise<void>}
   */
  function testSourceConnection(opts) {
    opts = opts || {};
    var payload = opts.payload || {};
    var label = opts.label || '';
    var labelPrefix = label ? (label + ': ') : '';

    if (typeof opts.onStart === 'function') opts.onStart();

    return fetch('/api/wizard/test-source', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
      .then(function (r) {
        return r.json().then(function (d) { return { httpOk: r.ok, data: d }; });
      })
      .then(function (res) {
        var d = res.data || {};
        var triedUrl = buildTriedUrl(payload, d);
        var auth = ((payload.auth && payload.auth.method) || 'none').toUpperCase();
        var elapsed = d.elapsed_ms != null ? (' (' + d.elapsed_ms + 'ms)') : '';

        var success = res.httpOk && (d.ok === true || d.success === true) && !d.error;

        if (success && d.warning === 'no_docs') {
          if (typeof opts.onResult === 'function') {
            opts.onResult(d, 'no_docs');
          } else {
            notifyWarn('✓ ' + labelPrefix + 'Connection OK' + elapsed +
              ' — changes feed is empty (no docs to sample)');
          }
          return;
        }

        if (success) {
          if (typeof opts.onResult === 'function') {
            opts.onResult(d, 'ok');
          } else {
            var sampled = d.pool_size != null ? (' — ' + d.pool_size + ' docs sampled') : '';
            notifyOk('✓ ' + labelPrefix + 'Connection OK' + elapsed + sampled);
          }
          return;
        }

        // Failure path
        if (typeof opts.onResult === 'function') {
          opts.onResult(d, 'error');
          return;
        }

        var errMsg = d.error || d.message || 'Unknown error';
        var statusLine = d.status ? (' [HTTP ' + d.status + ']') : '';
        var toast = '✗ ' + labelPrefix + 'Connection failed' + statusLine + ': ' + errMsg;
        var fullDetail = '✕ ' + labelPrefix + 'Connection failed' + elapsed +
          '\n\nURL: ' + triedUrl +
          '\nAuth: ' + auth +
          (d.status ? ('\nHTTP Status: ' + d.status) : '') +
          (d.error_class ? ('\nError Class: ' + d.error_class) : '') +
          '\nError: ' + errMsg +
          (d.detail ? ('\n\nDetail: ' + d.detail) : '') +
          (d.hint ? ('\n\n💡 Hint: ' + d.hint) : '') +
          '\n\nSee the Logs page (filter [CONTROL] / [HTTP], "test-source") for the full audit trail.';
        notifyErr(toast, fullDetail);
      })
      .catch(function (err) {
        if (typeof opts.onResult === 'function') {
          opts.onResult({ error: err.message }, 'error');
        } else {
          notifyErr('✗ ' + labelPrefix + 'Test error: ' + err.message, null);
        }
      })
      .finally(function () {
        if (typeof opts.onDone === 'function') opts.onDone();
      });
  }

  /**
   * Build the standard payload for /api/wizard/test-source from an
   * "input" record (the shape used in inputs.html / jobs.html caches).
   */
  function buildPayloadFromInput(inp) {
    return {
      gateway: {
        url: inp.host || inp.url || '',
        database: inp.database || '',
        scope: inp.scope || '_default',
        collection: inp.collection || '_default',
        src: inp.source_type || 'sync_gateway',
        accept_self_signed_certs: !!inp.accept_self_signed_certs,
        compress: !!inp.compress
      },
      auth: inp.auth || { method: 'none' }
    };
  }

  global.SourceTest = {
    test: testSourceConnection,
    buildPayloadFromInput: buildPayloadFromInput
  };
})(window);
