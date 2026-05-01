/**
 * Shared sidebar component — single source of truth for navigation.
 *
 * Each page only needs:
 *   <link href="/static/css/sidebar.css" rel="stylesheet" />
 *   <div id="sidebar-root"></div>
 *   <div class="sidebar-main"> … </div>
 *   <script src="/static/js/sidebar.js"></script>
 */
(function () {
  /* ── Nav definition ─────────────────────────────────────── */
  var NAV = [
    { type: 'section', label: 'Overview' },
    { href: '/',           label: 'Dashboard',       icon: '/static/icons/dashboard.svg' },

    { type: 'section', label: 'Pipeline' },
    { href: '/jobs',       label: 'Job Builder',      icon: '/static/icons/jobs.svg'     },
    { href: '/inputs',     label: 'Inputs',           icon: '/static/icons/inputs.svg'   },
    { href: '/outputs',    label: 'Outputs',          icon: '/static/icons/outputs.svg'  },
    { href: '/schema',     label: 'Schema Mapping',   icon: '/static/icons/schema.svg'   },

    { type: 'section', label: 'Guided' },
    { href: '/wizard',     label: 'Wizards',          icon: '/static/icons/wizard.svg'   },

    { type: 'divider' },

    { type: 'section', label: 'System' },
    { href: '/dlq',        label: 'Dead Letters',      icon: '/static/icons/dlq.svg'      },
    { href: '/logs',       label: 'Logs',              icon: '/static/icons/logs.svg'     },
    { href: '/settings',   label: 'Settings',         icon: '/static/icons/settings.svg' },

    { type: 'section', label: 'Reference' },
    { href: '/glossary', label: 'Glossary',         icon: '/static/icons/book.svg'     },
    { href: '/help',       label: 'Help',             icon: '/static/icons/help.svg'     }
  ];

  /* ── Dev-only nav items (visible only with ?dev=true) ──── */
  if (window.location.search.indexOf('dev=true') !== -1) {
    NAV.splice(NAV.length - 1, 0,
      { type: 'section', label: 'Experimental' },
      { href: '/eventing?dev=true',  label: 'Eventing',  icon: '/static/icons/eventing.svg' }
    );
  }

  /* ── Build HTML ─────────────────────────────────────────── */
  var path = window.location.pathname.replace(/\/+$/, '') || '/';

  function navLinks() {
    return NAV.map(function (item) {
      if (item.type === 'divider')  return '<div class="sidebar-divider"></div>';
      if (item.type === 'section') return '<div class="sidebar-section">' + item.label + '</div>';
      if (item.type === 'action') {
        return '<button type="button" id="' + item.id + '" class="sidebar-link" data-tooltip="' + item.label + '" style="width:100%;border:none;background:none;text-align:left;cursor:pointer">' +
          '<span class="sidebar-link-icon"><img src="' + item.icon + '" alt="" /></span>' +
          '<span class="sidebar-link-text">' + item.label + '</span></button>';
      }
      var active = (item.href === '/' ? path === '/' : path.indexOf(item.href) === 0) ? ' active' : '';
      return '<a href="' + item.href + '" class="sidebar-link' + active + '" data-tooltip="' + item.label + '">' +
        '<span class="sidebar-link-icon"><span class="sidebar-icon-mask" style="-webkit-mask-image:url(' + item.icon + ');mask-image:url(' + item.icon + ')"></span></span>' +
        '<span class="sidebar-link-text">' + item.label + '</span></a>';
    }).join('\n');
  }

  var html =
    '<button id="sidebarMobileToggle" class="sidebar-mobile-toggle">' +
      '<img src="/static/icons/menu.svg" alt="Menu" /></button>' +
    '<div id="sidebarOverlay" class="sidebar-overlay"></div>' +

    '<aside id="sidebar" class="sidebar">' +
      '<div class="sidebar-brand" id="sidebarBrand">' +
        '<img src="/static/favicon.svg" alt="Logo" />' +
        '<div class="tooltip tooltip-right" data-tip="Portable Over Unreliable Changes Handler Passing Instant Pipelines for Events and Streams">' +
          '<span class="sidebar-brand-text">PouchPipes</span>' +
        '</div>' +
      '</div>' +
      '<button id="sidebarToggle" class="sidebar-toggle">' +
        '<img src="/static/icons/menu.svg" alt="Toggle" /></button>' +

      '<nav class="sidebar-nav">' + navLinks() + '</nav>' +

      '<div class="sidebar-footer">' +
        '<div class="sidebar-controls" style="display:flex;align-items:center;gap:4px;margin-bottom:6px">' +
          '<button type="button" id="onlineBtn" class="sidebar-theme-btn" style="flex:1" data-tooltip="Go Offline">' +
            '<span class="sidebar-link-icon" id="onlineIcon" style="color:#36d399"><svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="12" r="6"/></svg></span>' +
            '<span class="sidebar-theme-label" id="onlineLabel">Online</span>' +
          '</button>' +
          '<span id="controlsHelpTrigger" class="sidebar-theme-label" style="font-size:10px;opacity:0.4;cursor:help;border:1px solid currentColor;border-radius:50%;width:14px;height:14px;display:inline-flex;align-items:center;justify-content:center">?</span>' +
        '</div>' +
        '<div class="sidebar-controls" style="display:flex;align-items:center;gap:4px;margin-bottom:6px">' +
          '<button type="button" id="restartBtn" class="sidebar-theme-btn" style="flex:1" data-tooltip="Restart Worker">' +
            '<span class="sidebar-link-icon"><img src="/static/icons/restart.svg" alt="" /></span>' +
            '<span class="sidebar-theme-label">Restart</span>' +
          '</button>' +
        '</div>' +
        '<div class="sidebar-controls" style="display:flex;align-items:center;gap:4px;margin-bottom:6px">' +
          '<button type="button" id="navShutdown" class="sidebar-theme-btn" style="flex:1" data-tooltip="Shutdown Worker">' +
            '<span class="sidebar-link-icon"><img src="/static/icons/shutdown.svg" alt="" /></span>' +
            '<span class="sidebar-theme-label">Shutdown</span>' +
          '</button>' +
        '</div>' +
        '<div class="sidebar-divider" style="margin:8px 0"></div>' +
        '<div class="sidebar-theme-toggle">' +
          '<button type="button" id="themeBtn" class="sidebar-theme-btn" data-tooltip="Toggle theme">' +
            '<span class="sidebar-link-icon" id="themeIcon"><img src="/static/icons/sun.svg" alt="" /></span>' +
            '<span class="sidebar-theme-label" id="themeLabel">Dark</span>' +
          '</button>' +
        '</div>' +
        '<div class="sidebar-version">v2.3.0</div>' +
      '</div>' +
    '</aside>';

  /* ── Inject ─────────────────────────────────────────────── */
  var root = document.getElementById('sidebar-root');
  if (root) root.innerHTML = html;
  else document.body.insertAdjacentHTML('afterbegin', html);

  /* ── Collapse / Expand ──────────────────────────────────── */
  var sidebar   = document.getElementById('sidebar');
  var toggleBtn = document.getElementById('sidebarToggle');
  var brand     = document.getElementById('sidebarBrand');
  var mobileBtn = document.getElementById('sidebarMobileToggle');
  var overlay   = document.getElementById('sidebarOverlay');

  var sidebarMain = document.querySelector('.sidebar-main');

  if (localStorage.getItem('cw_sidebar_collapsed') === 'true') {
    sidebar.classList.add('collapsed');
    if (sidebarMain) sidebarMain.classList.add('collapsed');
  }

  function toggleSidebar() {
    sidebar.classList.toggle('collapsed');
    if (sidebarMain) sidebarMain.classList.toggle('collapsed');
    localStorage.setItem('cw_sidebar_collapsed', sidebar.classList.contains('collapsed'));
  }

  // Hamburger collapses (visible only when expanded)
  toggleBtn.addEventListener('click', toggleSidebar);

  // Clicking the brand/logo area expands when collapsed
  brand.addEventListener('click', function () {
    if (sidebar.classList.contains('collapsed')) toggleSidebar();
  });

  mobileBtn.addEventListener('click', function () {
    sidebar.classList.toggle('mobile-open');
    overlay.classList.toggle('active');
  });
  overlay.addEventListener('click', function () {
    sidebar.classList.remove('mobile-open');
    overlay.classList.remove('active');
  });

  /* ── Theme toggle ───────────────────────────────────────── */
  var themeBtn   = document.getElementById('themeBtn');
  var themeIcon  = document.getElementById('themeIcon');
  var themeLabel = document.getElementById('themeLabel');

  function applyThemeUI(theme) {
    var isLight = theme === 'light';
    // In dark mode show sun (click to go light), in light mode show moon (click to go dark)
    themeIcon.querySelector('img').src = isLight ? '/static/icons/moon.svg' : '/static/icons/sun.svg';
    themeLabel.textContent = isLight ? 'Light' : 'Dark';
  }

  applyThemeUI(localStorage.getItem('cw_theme') || 'dark');

  themeBtn.addEventListener('click', function () {
    var current = localStorage.getItem('cw_theme') || 'dark';
    var next = current === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('cw_theme', next);
    applyThemeUI(next);
  });

  /* ── Restart Worker button ──────────────────────────────── */
  var restartBtn = document.getElementById('restartBtn');

  // Ensure a toast container exists on the page
  if (!document.getElementById('toastContainer')) {
    var tc = document.createElement('div');
    tc.id = 'toastContainer';
    tc.className = 'toast toast-end toast-bottom z-50';
    document.body.appendChild(tc);
  }

  function sidebarToast(msg, type) {
    var container = document.getElementById('toastContainer');
    var cls = type === 'success' ? 'alert-success' : type === 'error' ? 'alert-error' : 'alert-info';
    var el = document.createElement('div');
    el.className = 'alert ' + cls;
    el.innerHTML = '<span>' + msg + '</span>';
    container.appendChild(el);
    setTimeout(function () { el.remove(); }, 3000);
  }

  restartBtn.addEventListener('click', function () {
    if (!confirm('Restart PouchPipes? The feed will reconnect with the current config.')) return;
    sidebarToast('Restarting worker...', 'info');
    fetch('/api/restart', { method: 'POST' })
      .then(function (res) { return res.json(); })
      .then(function (data) {
        if (data.ok) { sidebarToast('Worker restart signal sent', 'success'); updateOnlineUI(true); }
        else sidebarToast('Restart failed: ' + (data.error || 'unknown'), 'error');
      })
      .catch(function (e) {
        sidebarToast('Restart failed: ' + e.message, 'error');
      });
  });

  /* ── Online / Offline toggle ────────────────────────────── */
  var onlineBtn   = document.getElementById('onlineBtn');
  var onlineIcon  = document.getElementById('onlineIcon');
  var onlineLabel = document.getElementById('onlineLabel');
  var isOnline    = true;

  function updateOnlineUI(online) {
    isOnline = online;
    onlineIcon.style.color = online ? '#36d399' : '#f87272';
    onlineLabel.textContent = online ? 'Online' : 'Offline';
    onlineBtn.setAttribute('data-tooltip', online ? 'Go Offline' : 'Go Online');
  }

  onlineBtn.addEventListener('click', function () {
    var action = isOnline ? 'offline' : 'online';
    var msg = isOnline
      ? 'Take worker offline? The feed will pause but the worker stays alive.'
      : 'Bring worker online? The feed will resume with the current config.';
    if (!confirm(msg)) return;
    sidebarToast(isOnline ? 'Taking worker offline...' : 'Bringing worker online...', 'info');
    fetch('/api/' + action, { method: 'POST' })
      .then(function (res) { return res.json(); })
      .then(function (data) {
        if (data.ok || data.message) {
          updateOnlineUI(!isOnline);
          sidebarToast('Worker is now ' + (isOnline ? 'online' : 'offline'), 'success');
        } else {
          sidebarToast('Failed: ' + (data.error || 'unknown'), 'error');
        }
      })
      .catch(function (e) {
        sidebarToast('Failed: ' + e.message, 'error');
      });
  });

  // Poll worker status to sync the UI
  function pollWorkerStatus() {
    fetch('/api/worker-status')
      .then(function (res) { return res.json(); })
      .then(function (data) {
        if (typeof data.online === 'boolean') updateOnlineUI(data.online);
      })
      .catch(function () {});
  }
  pollWorkerStatus();
  setInterval(pollWorkerStatus, 10000);

  /* ── Shutdown button ────────────────────────────────────── */
  var shutdownBtn = document.getElementById('navShutdown');
  if (shutdownBtn) {
    shutdownBtn.addEventListener('click', function () {
      if (!confirm('Shutdown PouchPipes? This will gracefully stop the feed and exit the process.')) return;
      sidebarToast('Shutting down worker...', 'info');
      fetch('/api/shutdown', { method: 'POST' })
        .then(function (res) { return res.json(); })
        .then(function (data) {
          if (data.ok) sidebarToast('Shutdown signal sent', 'success');
          else sidebarToast('Shutdown failed: ' + (data.error || 'unknown'), 'error');
        })
        .catch(function (e) {
          sidebarToast('Shutdown failed: ' + e.message, 'error');
        });
    });
  }

  /* ── Controls help tooltip (appended to body to escape overflow) ── */
  var helpTrigger = document.getElementById('controlsHelpTrigger');
  if (helpTrigger) {
    var tipText = 'Online: Feed is running. Offline: Feed paused, worker stays alive \u2014 update config/mappings safely, then go Online. Restart: Reload config & reconnect. Shutdown: Graceful full stop.';
    var tipEl = null;

    helpTrigger.addEventListener('mouseenter', function () {
      tipEl = document.createElement('div');
      tipEl.textContent = tipText;
      tipEl.style.cssText = 'position:fixed;z-index:9999;max-width:280px;padding:8px 12px;border-radius:6px;font-size:12px;line-height:1.4;pointer-events:none;background:oklch(var(--color-neutral));color:oklch(var(--color-neutral-content));box-shadow:0 2px 8px rgba(0,0,0,0.3)';
      document.body.appendChild(tipEl);
      var rect = helpTrigger.getBoundingClientRect();
      tipEl.style.left = rect.left + 'px';
      tipEl.style.bottom = (window.innerHeight - rect.top + 6) + 'px';
    });

    helpTrigger.addEventListener('mouseleave', function () {
      if (tipEl) { tipEl.remove(); tipEl = null; }
    });
  }
})();
