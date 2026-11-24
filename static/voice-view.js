(function (root, factory) {
  var deps = typeof module === 'object' && module && module.exports ? require('./captions.js') : root.ZoeCaptions;
  var api = factory(deps, root);
  if (typeof module === 'object' && module && module.exports) module.exports = api;
  else root.ZoeViewer = api;
}(typeof window !== 'undefined' ? window : this, function (Captions, root) {
  'use strict';

  // Read-only upstream context viewer, opened as /viewer?session=<gateway_session_id>&gen=<n>.
  // Never calls the dial, microphone, device or conversation-store code in voice.html.
  // Events are kept per seq, so duplicates and out-of-order arrivals are harmless.

  var PAGE_SIZE = 500;
  var MAX_PAGES = 200;
  var MAX_EVENT_ROWS = 1000;
  var RETRY_MAX_MS = 30000;
  var REFRESH_MS = 30000;
  var THEME_KEY = 'chatgpt_web_voice_theme_v1';
  var DIRECTION_LABELS = {
    upstream_to_client: 'upstream → client',
    client_to_upstream: 'client → upstream',
    gateway_to_upstream: 'gateway → upstream',
    gateway_creation: 'creation'
  };

  var GROUPS = [
    { id: 'conversation', label: 'Conversation' },
    { id: 'delegation', label: 'Delegation' },
    { id: 'instructions', label: 'Instructions & opening' },
    { id: 'context', label: 'Context writes' },
    { id: 'client', label: 'Client messages' },
    { id: 'session', label: 'Session & usage' },
    { id: 'other', label: 'Other' }
  ];
  var GROUP_IDS = GROUPS.map(function (g) { return g.id; });
  var PRESETS = [
    { id: 'conversation', label: 'Conversation only', groups: ['conversation'] },
    { id: 'delegation', label: 'Delegation only', groups: ['delegation'] },
    { id: 'everything', label: 'Everything', groups: GROUP_IDS }
  ];
  var CONVERSATION_TYPES = {
    'turn.created': true,
    'turn.delta': true,
    'turn.text.delta': true,
    'turn.done': true,
    'chat_message_delta': true,
    'output_text.delta': true,
    'output_audio_transcript.delta': true
  };
  var DELEGATION_MARKER = /"delegation_item_id"\s*:/;

  function toSeq(value) {
    var n = Number(value);
    return isFinite(n) && n > 0 && Math.floor(n) === n ? n : 0;
  }

  function toInt(value) {
    var n = Number(value);
    return isFinite(n) && Math.floor(n) === n ? n : 0;
  }

  // Event seqs may start at 0, so unlike generations only negatives are invalid.
  function toEventSeq(value) {
    var n = Number(value);
    return isFinite(n) && n >= 0 && Math.floor(n) === n ? n : -1;
  }

  function parseJSON(text) {
    if (typeof text !== 'string') return text && typeof text === 'object' ? text : null;
    try { return JSON.parse(text); } catch (e) { return null; }
  }

  function prettyPayload(text) {
    var value = parseJSON(text);
    return value !== null ? JSON.stringify(value, null, 2) : String(text == null ? '' : text);
  }

  function parseGeneration(value) {
    if (value === null || value === undefined || String(value).trim() === '') return { value: 0 };
    var n = toSeq(String(value).trim());
    return n ? { value: n } : { error: 'Invalid generation parameter. Use /viewer?session=<session>&gen=<positive integer>.' };
  }

  function chooseGeneration(gens, requested) {
    var list = (Array.isArray(gens) ? gens : []).filter(function (g) { return g && toSeq(g.generation) > 0; });
    if (!list.length) return { error: 'No generations are recorded for this session.' };
    if (requested) {
      var hit = list.filter(function (g) { return toSeq(g.generation) === requested; })[0];
      return hit ? { gen: requested, item: hit } : { error: 'Generation ' + requested + ' was not found for this session.' };
    }
    var active = list.filter(function (g) { return g.active === true; });
    var pool = active.length ? active : list;
    var pick = pool.reduce(function (best, g) { return toSeq(g.generation) > toSeq(best.generation) ? g : best; });
    return { gen: toSeq(pick.generation), item: pick };
  }

  function findGeneration(gens, gen) {
    var hit = (Array.isArray(gens) ? gens : []).filter(function (g) { return g && toSeq(g.generation) === Number(gen); })[0];
    return hit || null;
  }

  function enc(value) { return encodeURIComponent(String(value)); }

  function generationsUrl(sessionId) {
    return '/api/call-sessions/' + enc(sessionId) + '/generations';
  }

  function eventsUrl(sessionId, gen, after, limit) {
    return '/api/call-sessions/' + enc(sessionId) + '/generations/' + enc(gen) +
      '/events?after=' + toInt(after) + '&limit=' + toInt(limit || PAGE_SIZE);
  }

  function streamUrl(sessionId, gen, after) {
    return '/api/call-sessions/' + enc(sessionId) + '/generations/' + enc(gen) +
      '/events/stream?after=' + toInt(after);
  }

  function groupLabel(id) {
    for (var i = 0; i < GROUPS.length; i++) if (GROUPS[i].id === id) return GROUPS[i].label;
    return id;
  }

  // Inner type of a frame. Only data_message carries the real type in its payload.
  function innerType(type, payload) {
    if (type !== 'data_message') return type;
    var obj = Captions.parseEventPayload(payload);
    return obj && typeof obj.type === 'string' && obj.type && obj.type !== 'data_message' ? obj.type : type;
  }

  function groupOf(direction, type, payload) {
    if (direction === 'gateway_creation') return 'instructions';
    if (type.indexOf('delegation') >= 0) return 'delegation';
    var contextWrite = type === 'session.context.append' || type === 'session.context.appended';
    if (contextWrite && DELEGATION_MARKER.test(String(payload || ''))) return 'delegation';
    if (contextWrite) return 'context';
    if (direction === 'client_to_upstream') return 'client';
    if (direction === 'gateway_to_upstream') return 'context';
    if (CONVERSATION_TYPES[type] || /^(input|output)_transcript\./.test(type)) return 'conversation';
    if (type.indexOf('session.') === 0) return 'session';
    return 'other';
  }

  // Exactly one group per event. Direction-based groups are checked first so that
  // gateway writes about delegations land in Delegation, not Context writes.
  function classifyEvent(raw) {
    var direction = String((raw && raw.direction) || '');
    var payload = raw && raw.payload !== undefined && raw.payload !== null ? raw.payload : '';
    if (typeof payload !== 'string') payload = JSON.stringify(payload);
    var wire = String((raw && raw.type) || '');
    var type = innerType(wire, payload) || wire;
    return { type: type, group: groupOf(direction, type, payload) };
  }

  function eventMatches(ev, filter) {
    var f = filter || {};
    if (f.groups && f.groups.indexOf(ev.group) < 0) return false;
    if (f.type && ev.type !== f.type) return false;
    return searchMatches(ev, f.q);
  }

  // Case-insensitive substring match over type, direction, source and payload text.
  // Payload text is only compared, never parsed for execution.
  function searchMatches(ev, q) {
    var needle = String(q || '').trim().toLowerCase();
    if (!needle) return true;
    var hay = (ev.type + ' ' + ev.direction + ' ' + ev.source + ' ' + ev.payload).toLowerCase();
    return hay.indexOf(needle) >= 0;
  }

  // Counts per group over events matching the search text (group and type filters ignored).
  function groupCounts(list, q) {
    var counts = {};
    GROUP_IDS.forEach(function (id) { counts[id] = 0; });
    (Array.isArray(list) ? list : []).forEach(function (ev) {
      if (ev && counts[ev.group] !== undefined && searchMatches(ev, q)) counts[ev.group]++;
    });
    return counts;
  }

  function presetGroups(id) {
    for (var i = 0; i < PRESETS.length; i++) {
      if (PRESETS[i].id === id) return PRESETS[i].groups.slice();
    }
    return null;
  }

  function presetOf(groups) {
    var list = Array.isArray(groups) ? groups : [];
    for (var i = 0; i < PRESETS.length; i++) {
      var want = PRESETS[i].groups;
      if (want.length === list.length && want.every(function (id) { return list.indexOf(id) >= 0; })) return PRESETS[i].id;
    }
    return '';
  }

  function filterParams(filter) {
    var f = filter || {};
    var out = {};
    var groups = Array.isArray(f.groups) ? f.groups : GROUP_IDS;
    if (groups.length < GROUP_IDS.length) {
      out.groups = GROUP_IDS.filter(function (id) { return groups.indexOf(id) >= 0; }).join(',');
    }
    if (f.type) out.type = String(f.type);
    if (f.q) out.q = String(f.q);
    return out;
  }

  // Reads filter state from a URLSearchParams or a plain object. An absent groups
  // parameter means all groups; an empty one means none.
  function filterFromParams(params) {
    var get = function (key) {
      var v = params && typeof params.get === 'function' ? params.get(key) : (params || {})[key];
      return v === null || v === undefined ? null : String(v);
    };
    var g = get('groups');
    var groups = g === null ? GROUP_IDS.slice() : GROUP_IDS.filter(function (id) {
      return g.split(',').map(function (s) { return s.trim(); }).indexOf(id) >= 0;
    });
    return { groups: groups, type: get('type') || '', q: get('q') || '' };
  }

  // Delegation panel rows. delegation.created carries handoff/target/text in item;
  // gateway writes (delegation.context.append or session.context.append) expose
  // delegation_item_id, found at top level or under item.
  function delegationRows(list) {
    return (Array.isArray(list) ? list : []).filter(function (ev) {
      return ev && ev.group === 'delegation';
    }).map(function (ev) {
      var obj = Captions.parseEventPayload(ev.payload) || {};
      var item = obj.item && typeof obj.item === 'object' ? obj.item : {};
      var created = ev.type === 'delegation.created';
      var row = {
        seq: ev.seq,
        at: ev.at,
        type: ev.type,
        kind: created ? 'created' : 'write',
        delivery: ev.delivery,
        truncated: ev.truncated,
        handoff: '',
        target: '',
        delegationItemId: '',
        text: ''
      };
      if (created) {
        row.handoff = String(item.handoff_id || '');
        row.target = String(item.target || '');
        row.delegationItemId = String(item.id || '');
        row.text = Captions.delegationText(item.content);
      } else {
        row.handoff = String(obj.handoff_id || item.handoff_id || '');
        row.target = String(obj.target || item.target || '');
        row.delegationItemId = String(obj.delegation_item_id || item.delegation_item_id || '');
        row.text = Captions.delegationText(obj.content || item.content);
      }
      return row;
    });
  }

  function queryString(obj) {
    var keys = Object.keys(obj);
    return keys.length ? '&' + keys.map(function (k) { return enc(k) + '=' + enc(obj[k]); }).join('&') : '';
  }

  function viewerUrl(sessionId, gen, filter) {
    return '/viewer?session=' + enc(sessionId) + (gen ? '&gen=' + enc(gen) : '') +
      (filter ? queryString(filterParams(filter)) : '');
  }

  // Event store: seq-keyed, sorted, deduplicated. Gaps are reported both by the
  // gateway (flags, SSE gap events) and detected locally between loaded seqs.
  function createStore() {
    var byseq = {};
    var seqs = [];
    var reported = [];
    var serverGap = false;
    var serverDropped = 0;
    var api;

    function normalize(ev) {
      var payload = ev.payload;
      if (payload === null || payload === undefined) payload = '';
      else if (typeof payload !== 'string') payload = JSON.stringify(payload);
      var direction = String(ev.direction || '');
      var cls = classifyEvent({ direction: direction, type: ev.type, payload: payload });
      return {
        seq: toEventSeq(ev.seq),
        at: String(ev.at || ''),
        direction: direction,
        type: cls.type,
        group: cls.group,
        source: String(ev.source || ''),
        payload: payload,
        size: toInt(ev.size),
        truncated: ev.truncated === true,
        delivery: String(ev.delivery || 'unknown'),
        raw: ev
      };
    }

    function insert(seq) {
      if (!seqs.length || seq > seqs[seqs.length - 1]) { seqs.push(seq); return; }
      var lo = 0;
      var hi = seqs.length;
      while (lo < hi) {
        var mid = (lo + hi) >> 1;
        if (seqs[mid] < seq) lo = mid + 1; else hi = mid;
      }
      seqs.splice(lo, 0, seq);
    }

    api = {
      add: function (events) {
        var added = 0;
        (Array.isArray(events) ? events : []).forEach(function (ev) {
          if (!ev || typeof ev !== 'object') return;
          var item = normalize(ev);
          if (item.seq < 0 || byseq[item.seq]) return;
          byseq[item.seq] = item;
          insert(item.seq);
          added++;
        });
        return added;
      },
      list: function () { return seqs.map(function (s) { return byseq[s]; }); },
      size: function () { return seqs.length; },
      maxSeq: function () { return seqs.length ? seqs[seqs.length - 1] : 0; },
      at: function (seq) { return byseq[seq] ? byseq[seq].at : ''; },
      setServerFlags: function (gap, dropped) {
        serverGap = serverGap || gap === true || toInt(gap) > 0;
        serverDropped = Math.max(serverDropped, toInt(dropped));
      },
      reportGap: function (from, to, source) {
        reported.push({ from: toEventSeq(from), to: toEventSeq(to), source: source });
      },
      gaps: function () {
        var missing = [];
        for (var i = 1; i < seqs.length; i++) {
          if (seqs[i] - seqs[i - 1] > 1) missing.push({ from: seqs[i - 1] + 1, to: seqs[i] - 1 });
        }
        return { reported: reported.slice(), missing: missing };
      },
      dropped: function () { return serverDropped; },
      incomplete: function () {
        var g = api.gaps();
        return serverGap || serverDropped > 0 || g.reported.length > 0 || g.missing.length > 0;
      },
      truncatedUpstream: function () {
        return api.list().filter(function (ev) {
          return ev.direction === 'upstream_to_client' && ev.truncated;
        }).length;
      },
      captions: function () {
        var asm = Captions.createCaptionAssembler();
        api.list().forEach(function (ev) {
          if (ev.direction !== 'upstream_to_client' || ev.truncated) return;
          var obj = Captions.parseEventPayload(ev.payload);
          if (obj) asm.consume(obj, ev.seq);
        });
        return asm.snapshot();
      },
      filtered: function (filter) {
        return api.list().filter(function (ev) { return eventMatches(ev, filter); });
      },
      counts: function (q) { return groupCounts(api.list(), q); },
      delegations: function () { return delegationRows(api.list()); },
      types: function () {
        var seen = {};
        api.list().forEach(function (ev) { if (ev.type) seen[ev.type] = true; });
        return Object.keys(seen).sort();
      },
      creation: function () {
        var list = api.list();
        for (var i = 0; i < list.length; i++) {
          if (list[i].direction === 'gateway_creation') {
            return { seq: list[i].seq, at: list[i].at, text: list[i].payload, parsed: parseJSON(list[i].payload), truncated: list[i].truncated };
          }
        }
        return null;
      }
    };
    return api;
  }

  function requested() {
    return typeof window !== 'undefined' && window.location.pathname === '/viewer';
  }

  function start() {
    var doc = root.document;
    var $ = function (id) { return doc.getElementById(id); };
    var el = {
      conn: $('vwConn'), orb: $('vwOrb'), session: $('vwSession'), zoe: $('vwZoe'), gen: $('vwGen'), status: $('vwStatus'),
      genSelect: $('vwGenSelect'), tabCaptions: $('vwTabCaptions'), tabEvents: $('vwTabEvents'), notice: $('vwNotice'),
      creation: $('vwCreation'), creationBody: $('vwCreationBody'), captionsPane: $('vwCaptionsPane'), captions: $('vwCaptions'),
      eventsPane: $('vwEventsPane'), events: $('vwEvents'), direction: $('vwDirection'), type: $('vwType'),
      copy: $('vwCopy'), follow: $('vwFollow'), theme: $('vwTheme'), toast: $('vwToast')
    };
    if (!el.captions) return;

    function make(tag, cls, text) {
      var node = doc.createElement(tag);
      if (cls) node.className = cls;
      if (text !== undefined && text !== null) node.textContent = String(text);
      return node;
    }

    function formatTime(value) {
      var d = new Date(value);
      if (!value || isNaN(d.getTime())) return value ? String(value) : '';
      return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }

    function formatBytes(n) {
      return n < 1024 ? n + ' B' : (n / 1024).toFixed(1) + ' KB';
    }

    function safeClass(value) {
      return String(value || 'unknown').replace(/[^a-z_-]/gi, '-').toLowerCase();
    }

    var params = new URLSearchParams(root.location.search);
    var sessionId = String(params.get('session') || '').trim();
    var requestedGen = parseGeneration(params.get('gen'));
    var view = {
      gens: [], gen: 0, zoeId: '', store: null, es: null, retryTimer: 0, retryDelay: 1000,
      token: 0, active: false, startedAt: '', endedAt: '', conn: '', error: '', capped: false,
      tab: 'captions', following: true, renderQueued: false, filters: { direction: '', type: '' },
      typesKey: null, open: {}
    };

    function setConn(label, kind) {
      view.conn = label;
      el.conn.textContent = label;
      el.orb.className = 'orb ' + (kind || 'idle');
    }

    function showError(message) {
      view.error = String(message || 'Unknown error');
      setConn('Error', 'error');
      renderNotice();
    }

    async function getJSON(url) {
      var resp = await fetch(url, { credentials: 'same-origin', headers: { accept: 'application/json' } });
      if (resp.status === 401) {
        root.location.replace('/login');
        throw new Error('Session expired');
      }
      var data = null;
      try { data = await resp.json(); } catch (e) {}
      if (!resp.ok) {
        var message = data && (typeof data.error === 'string' ? data.error : (data.detail && data.detail.error));
        throw new Error(message || ('HTTP ' + resp.status));
      }
      return data || {};
    }

    function stopStream() {
      if (view.es) {
        try { view.es.close(); } catch (e) {}
        view.es = null;
      }
    }

    function scheduleRender() {
      if (view.renderQueued) return;
      view.renderQueued = true;
      root.requestAnimationFrame(function () {
        view.renderQueued = false;
        renderAll();
      });
    }

    function renderAll() {
      renderHeader();
      renderNotice();
      renderCreation();
      renderCaptions();
      renderEvents();
    }

    function renderHeader() {
      el.session.textContent = sessionId || '—';
      el.zoe.textContent = view.zoeId || '—';
      el.gen.textContent = view.gen ? String(view.gen) : '—';
      el.status.textContent = view.active ? 'Live' : (view.endedAt ? 'Ended ' + formatTime(view.endedAt) : 'Ended');
    }

    function renderNotice() {
      var lines = [];
      if (view.error) lines.push(view.error);
      if (view.store && view.store.incomplete()) {
        var gaps = view.store.gaps();
        lines.push('Incomplete log: this view is NOT a complete record.');
        if (view.store.dropped() > 0) lines.push(view.store.dropped() + ' event(s) were dropped by the gateway.');
        gaps.reported.forEach(function (g) { lines.push('Gap reported for seq ' + g.from + '–' + g.to + '.'); });
        if (gaps.missing.length) {
          var sample = gaps.missing.slice(0, 3).map(function (g) { return g.from + '–' + g.to; }).join(', ');
          lines.push(gaps.missing.length + ' missing seq range(s) between loaded events: ' + sample + (gaps.missing.length > 3 ? ', …' : '') + '.');
        }
      }
      if (view.store && view.store.truncatedUpstream() > 0) {
        lines.push(view.store.truncatedUpstream() + ' upstream event(s) were truncated; their captions may be missing.');
      }
      if (view.capped) lines.push('Loading stopped early; events beyond the loaded range are not shown.');
      el.notice.textContent = lines.join('\n');
      el.notice.hidden = !lines.length;
      el.notice.setAttribute('data-kind', view.error ? 'error' : 'warn');
    }

    function renderCreation() {
      var c = view.store ? view.store.creation() : null;
      el.creation.hidden = !c;
      if (!c) return;
      var lines = [];
      var parsed = c.parsed;
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        if (typeof parsed.instructions === 'string') {
          lines.push('instructions:');
          lines.push(parsed.instructions);
        } else if (parsed.instructions !== undefined) {
          lines.push('instructions: ' + JSON.stringify(parsed.instructions, null, 2));
        }
        if (parsed.initial_items !== undefined) lines.push('initial_items: ' + JSON.stringify(parsed.initial_items, null, 2));
        if (!lines.length) lines.push(prettyPayload(c.text));
      } else {
        lines.push(prettyPayload(c.text));
      }
      if (c.truncated) lines.push('[payload truncated by gateway]');
      el.creationBody.textContent = lines.join('\n\n');
    }

    function boundaryRow(text, cls) {
      return make('div', cls || 'viewer-boundary', text);
    }

    function gapRow(text) {
      return make('div', 'viewer-gap', text);
    }

    function emptyRow(text) {
      return make('p', 'caption-empty', text);
    }

    function applyScroll(pane, top, active) {
      if (!active) { pane.scrollTop = top; return; }
      pane.scrollTop = view.following ? pane.scrollHeight : top;
    }

    function captionRow(item, at) {
      var role = item.role === 'assistant' ? 'assistant' : 'user';
      var row = make('div', 'caption-log-line ' + role + (item.final ? '' : ' pending'));
      var avatar = make('div', 'message-avatar', role === 'assistant' ? 'V' : 'U');
      var body = make('div', 'message-body');
      var meta = make('div', 'message-meta', (role === 'assistant' ? 'Assistant' : 'User') + (at ? ' · ' + formatTime(at) : ''));
      var bubble = make('div', 'message-bubble');
      bubble.appendChild(make('span', 'caption-log-content', item.text));
      body.appendChild(meta);
      body.appendChild(bubble);
      row.appendChild(avatar);
      row.appendChild(body);
      return row;
    }

    function renderCaptions() {
      var top = el.captionsPane.scrollTop;
      el.captions.textContent = '';
      if (!view.store) return;
      el.captions.appendChild(boundaryRow('Generation ' + view.gen + (view.startedAt ? ' started ' + formatTime(view.startedAt) : ' started')));
      var items = view.store.captions();
      if (!items.length) el.captions.appendChild(emptyRow('No captions in this generation yet.'));
      items.forEach(function (item) {
        el.captions.appendChild(captionRow(item, view.store.at(item.seq)));
      });
      if (!view.active && view.store.size()) {
        el.captions.appendChild(boundaryRow('Generation ' + view.gen + ' ended' + (view.endedAt ? ' ' + formatTime(view.endedAt) : '')));
      }
      applyScroll(el.captionsPane, top, view.tab === 'captions');
    }

    function passes(ev, filter) {
      return (!filter.direction || ev.direction === filter.direction) && (!filter.type || ev.type === filter.type);
    }

    function syncTypeOptions() {
      var types = view.store.types();
      var key = types.join('\n');
      if (key === view.typesKey) return;
      view.typesKey = key;
      var current = el.type.value;
      el.type.textContent = '';
      var all = make('option', '', 'All types');
      all.value = '';
      el.type.appendChild(all);
      types.forEach(function (type) {
        var opt = make('option', '', type);
        opt.value = type;
        el.type.appendChild(opt);
      });
      el.type.value = types.indexOf(current) >= 0 ? current : '';
      view.filters.type = el.type.value;
    }

    function eventRow(ev) {
      var row = make('div', 'viewer-event');
      var head = make('div', 'viewer-event-head');
      head.appendChild(make('span', 'viewer-badge dir-' + safeClass(ev.direction), DIRECTION_LABELS[ev.direction] || ev.direction || 'unknown'));
      head.appendChild(make('span', 'viewer-type mono', ev.type || '(no type)'));
      head.appendChild(make('span', 'viewer-meta', formatTime(ev.at)));
      head.appendChild(make('span', 'viewer-meta', '#' + ev.seq));
      head.appendChild(make('span', 'viewer-meta', formatBytes(ev.size)));
      if (ev.truncated) head.appendChild(make('span', 'viewer-badge warn', 'truncated'));
      head.appendChild(make('span', 'viewer-badge', 'delivery: ' + ev.delivery));
      if (ev.source) head.appendChild(make('span', 'viewer-meta', ev.source));
      row.appendChild(head);

      var details = make('details', 'viewer-payload');
      details.appendChild(make('summary', '', 'Payload'));
      var filled = false;
      var fill = function () {
        if (filled) return;
        filled = true;
        var pre = make('pre', 'viewer-pre', prettyPayload(ev.payload));
        details.appendChild(pre);
        if (ev.truncated) details.appendChild(make('div', 'viewer-meta', '[payload truncated by gateway]'));
      };
      if (view.open[ev.seq]) { details.open = true; fill(); }
      details.addEventListener('toggle', function () {
        if (details.open) { view.open[ev.seq] = true; fill(); } else { delete view.open[ev.seq]; }
      });
      row.appendChild(details);
      return row;
    }

    function renderEvents() {
      var top = el.eventsPane.scrollTop;
      el.events.textContent = '';
      if (!view.store) return;
      syncTypeOptions();
      el.events.appendChild(boundaryRow('Generation ' + view.gen + (view.startedAt ? ' started ' + formatTime(view.startedAt) : ' started')));
      var gaps = view.store.gaps();
      gaps.reported.forEach(function (g) { el.events.appendChild(gapRow('Gap reported by gateway: seq ' + g.from + '–' + g.to)); });
      var all = view.store.list();
      var rows = [];
      var prev = -1;
      all.forEach(function (ev) {
        if (prev >= 0 && ev.seq - prev > 1) rows.push({ gap: 'Missing seq ' + (prev + 1) + '–' + (ev.seq - 1) + ' (not loaded)' });
        prev = ev.seq;
        if (passes(ev, view.filters)) rows.push({ ev: ev });
      });
      var visible = rows.slice(-MAX_EVENT_ROWS);
      if (visible.length < rows.length) {
        el.events.appendChild(emptyRow('Showing the latest ' + MAX_EVENT_ROWS + ' rows of ' + rows.length + '. Use filters to narrow the list.'));
      }
      visible.forEach(function (row) {
        el.events.appendChild(row.ev ? eventRow(row.ev) : gapRow(row.gap));
      });
      if (!all.length) el.events.appendChild(emptyRow('No events loaded for this generation yet.'));
      else if (!rows.some(function (row) { return row.ev; })) el.events.appendChild(emptyRow('No events match the current filters.'));
      if (!view.active && all.length) {
        el.events.appendChild(boundaryRow('Generation ' + view.gen + ' ended' + (view.endedAt ? ' ' + formatTime(view.endedAt) : '')));
      }
      applyScroll(el.eventsPane, top, view.tab === 'events');
    }

    function renderGenSelect() {
      el.genSelect.textContent = '';
      view.gens.forEach(function (g) {
        var opt = make('option', '', 'Gen ' + g.generation + (g.active ? ' · active' : ' · ended') + ' · ' + toInt(g.event_count) + ' events');
        opt.value = String(g.generation);
        if (Number(g.generation) === view.gen) opt.selected = true;
        el.genSelect.appendChild(opt);
      });
      el.genSelect.disabled = !view.gens.length;
    }

    async function loadGenerations() {
      var data = await getJSON(generationsUrl(sessionId));
      view.gens = Array.isArray(data.generations) ? data.generations : [];
      renderGenSelect();
    }

    async function loadSnapshot(token) {
      var after = 0;
      view.capped = false;
      for (var page = 0; ; page++) {
        if (page >= MAX_PAGES) { view.capped = true; break; }
        var data = await getJSON(eventsUrl(sessionId, view.gen, after, PAGE_SIZE));
        if (token !== view.token) return false;
        var events = Array.isArray(data.events) ? data.events : [];
        view.store.setServerFlags(data.gap, data.dropped);
        if (data.zoe_session_id) view.zoeId = String(data.zoe_session_id);
        if (typeof data.active === 'boolean') view.active = data.active;
        view.store.add(events);
        var next = view.store.maxSeq();
        var newest = toSeq(data.newest_seq);
        if (events.length < PAGE_SIZE || next <= after || (newest && next >= newest)) break;
        after = next;
        scheduleRender();
      }
      return true;
    }

    function startStream(token) {
      if (token !== view.token || !view.active || !root.EventSource) return;
      stopStream();
      var es = new root.EventSource(streamUrl(sessionId, view.gen, view.store.maxSeq()), { withCredentials: true });
      view.es = es;
      es.onopen = function () {
        if (es !== view.es) return;
        view.retryDelay = 1000;
        setConn('Live', 'listening');
      };
      es.addEventListener('event', function (ev) {
        if (es !== view.es) return;
        var item = parseJSON(ev.data);
        if (!item || typeof item !== 'object') return;
        view.store.add([item]);
        scheduleRender();
      });
      es.addEventListener('gap', function (ev) {
        if (es !== view.es) return;
        var g = parseJSON(ev.data) || {};
        view.store.reportGap(g.from, g.to, 'stream');
        scheduleRender();
      });
      es.addEventListener('end', function () {
        if (es !== view.es) return;
        view.active = false;
        if (!view.endedAt) view.endedAt = new Date().toISOString();
        stopStream();
        setConn('Ended', 'idle');
        scheduleRender();
      });
      es.onerror = function () {
        if (es !== view.es) return;
        stopStream();
        if (token !== view.token || !view.active) return;
        setConn('Reconnecting', 'idle');
        var delay = view.retryDelay;
        view.retryDelay = Math.min(view.retryDelay * 2, RETRY_MAX_MS);
        view.retryTimer = root.setTimeout(function () { startStream(token); }, delay);
      };
    }

    function openGeneration(gen, item) {
      stopStream();
      root.clearTimeout(view.retryTimer);
      view.token++;
      var token = view.token;
      view.store = createStore();
      view.gen = Number(gen);
      view.zoeId = item && item.zoe_session_id ? String(item.zoe_session_id) : '';
      view.active = !!(item && item.active === true);
      view.startedAt = item && item.started_at ? String(item.started_at) : '';
      view.endedAt = item && item.ended_at ? String(item.ended_at) : '';
      view.error = '';
      view.retryDelay = 1000;
      view.open = {};
      view.typesKey = null;
      view.filters.type = '';
      el.type.value = '';
      root.history.replaceState(null, '', viewerUrl(sessionId, view.gen));
      setConn('Loading', 'idle');
      renderGenSelect();
      scheduleRender();
      loadSnapshot(token).then(function (ok) {
        if (!ok || token !== view.token) return;
        renderHeader();
        if (view.active) {
          setConn('Connecting', 'idle');
          startStream(token);
        } else {
          setConn('Ended', 'idle');
        }
        scheduleRender();
      }).catch(function (e) {
        if (token === view.token) showError(e && e.message ? e.message : e);
      });
    }

    async function refreshGenerations() {
      try {
        await loadGenerations();
      } catch (e) {}
    }

    function setTab(tab) {
      view.tab = tab;
      el.tabCaptions.setAttribute('aria-selected', String(tab === 'captions'));
      el.tabEvents.setAttribute('aria-selected', String(tab === 'events'));
      el.captionsPane.hidden = tab !== 'captions';
      el.eventsPane.hidden = tab !== 'events';
      view.following = true;
      el.follow.hidden = true;
      renderAll();
    }

    function activePane() {
      return view.tab === 'events' ? el.eventsPane : el.captionsPane;
    }

    function onPaneScroll(pane) {
      if (pane !== activePane()) return;
      view.following = pane.scrollHeight - pane.scrollTop - pane.clientHeight < 40;
      el.follow.hidden = view.following;
    }

    function paintTheme() {
      var dark = doc.documentElement.getAttribute('data-theme') === 'dark';
      el.theme.textContent = dark ? 'Light' : 'Dark';
    }

    function setTheme(next) {
      doc.documentElement.setAttribute('data-theme', next);
      try { root.localStorage.setItem(THEME_KEY, next); } catch (e) {}
      paintTheme();
    }

    function toast(message) {
      el.toast.textContent = message;
      el.toast.classList.add('show');
      root.clearTimeout(view.toastTimer);
      view.toastTimer = root.setTimeout(function () { el.toast.classList.remove('show'); }, 2000);
    }

    async function copyText(text) {
      if (root.navigator.clipboard && root.navigator.clipboard.writeText) {
        await root.navigator.clipboard.writeText(text);
        return;
      }
      var area = make('textarea', '');
      area.value = text;
      doc.body.appendChild(area);
      area.select();
      var ok = doc.execCommand('copy');
      doc.body.removeChild(area);
      if (!ok) throw new Error('copy unsupported');
    }

    function bind() {
      el.genSelect.addEventListener('change', function () {
        var gen = Number(el.genSelect.value);
        openGeneration(gen, findGeneration(view.gens, gen));
      });
      el.tabCaptions.addEventListener('click', function () { setTab('captions'); });
      el.tabEvents.addEventListener('click', function () { setTab('events'); });
      el.direction.addEventListener('change', function () { view.filters.direction = el.direction.value; scheduleRender(); });
      el.type.addEventListener('change', function () { view.filters.type = el.type.value; scheduleRender(); });
      el.copy.addEventListener('click', async function () {
        var list = view.store ? view.store.filtered(view.filters) : [];
        var text = JSON.stringify(list.map(function (ev) { return ev.raw; }), null, 2);
        try {
          await copyText(text);
          toast('Copied ' + list.length + ' event(s) as JSON');
        } catch (e) {
          toast('Copy failed');
        }
      });
      el.follow.addEventListener('click', function () {
        view.following = true;
        el.follow.hidden = true;
        scheduleRender();
      });
      el.captionsPane.addEventListener('scroll', function () { onPaneScroll(el.captionsPane); }, { passive: true });
      el.eventsPane.addEventListener('scroll', function () { onPaneScroll(el.eventsPane); }, { passive: true });
      el.theme.addEventListener('click', function () {
        setTheme(doc.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
      });
      paintTheme();
      root.addEventListener('pagehide', function () {
        stopStream();
        root.clearTimeout(view.retryTimer);
      });
    }

    async function init() {
      doc.title = 'Upstream viewer · ' + (sessionId || 'no session');
      bind();
      setConn('Loading', 'idle');
      renderHeader();
      if (!sessionId) return showError('Missing session id. Open /viewer?session=<gateway_session_id>.');
      if (requestedGen.error) return showError(requestedGen.error);
      try {
        await loadGenerations();
        var chosen = chooseGeneration(view.gens, requestedGen.value);
        if (chosen.error) return showError(chosen.error);
        openGeneration(chosen.gen, chosen.item);
      } catch (e) {
        showError(e && e.message ? e.message : e);
      }
      root.setInterval(refreshGenerations, REFRESH_MS);
    }

    el.follow.hidden = true;
    init();
  }

  return {
    start: start,
    requested: requested,
    createStore: createStore,
    chooseGeneration: chooseGeneration,
    parseGeneration: parseGeneration,
    generationsUrl: generationsUrl,
    eventsUrl: eventsUrl,
    streamUrl: streamUrl,
    viewerUrl: viewerUrl,
    prettyPayload: prettyPayload,
    PAGE_SIZE: PAGE_SIZE,
    GROUPS: GROUPS.map(function (g) { return { id: g.id, label: g.label }; }),
    PRESETS: PRESETS.map(function (p) { return { id: p.id, label: p.label, groups: p.groups.slice() }; }),
    classifyEvent: classifyEvent,
    groupLabel: groupLabel,
    eventMatches: eventMatches,
    searchMatches: searchMatches,
    groupCounts: groupCounts,
    presetGroups: presetGroups,
    presetOf: presetOf,
    filterParams: filterParams,
    filterFromParams: filterFromParams,
    delegationRows: delegationRows
  };
}));
