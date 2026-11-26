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
    { id: 'creation', label: 'Creation' },
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

  // Display only: expands string `data` holding JSON (up to 3 levels) so no escaped quotes remain.
  function unwrapPayloadData(value, depth) {
    if (!value || typeof value !== 'object' || value.type !== 'data_message' || typeof value.data !== 'string' || depth >= 3) return value;
    var inner = parseJSON(value.data);
    if (inner === null || typeof inner !== 'object') return value;
    var out = {};
    Object.keys(value).forEach(function (key) { out[key] = value[key]; });
    out.data = unwrapPayloadData(inner, depth + 1);
    return out;
  }

  function displayPayload(text) {
    var value = parseJSON(text);
    var unwrapped = unwrapPayloadData(value, 0);
    return unwrapped === value ? prettyPayload(text) : readableJSON(unwrapped);
  }

  // Display only: indented JSON where string values show real line breaks instead of \n.
  function readableJSON(value) {
    var text = JSON.stringify(value, null, 2);
    if (typeof text !== 'string') return '';
    return text.replace(/"(?:[^"\\\n]|\\.)*"/g, function (token, offset) {
      var str = parseJSON(token);
      if (typeof str !== 'string' || str.indexOf('\n') < 0) return token;
      var lineStart = text.lastIndexOf('\n', offset) + 1;
      var indent = /^ */.exec(text.slice(lineStart, offset))[0] + '  ';
      return '"' + str.split('\n').map(function (part) { return JSON.stringify(part).slice(1, -1); }).join('\n' + indent) + '"';
    });
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
    var active = list.filter(summaryIsActive);
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
    if (direction === 'gateway_creation') return 'creation';
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
      hasGap: function () {
        var g = api.gaps();
        return serverGap || g.reported.length > 0 || g.missing.length > 0;
      },
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
      get: function (seq) { return byseq[seq] || null; },
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
          if (list[i].direction === 'gateway_creation') return creationFromEvent(list[i]);
        }
        return null;
      }
    };
    return api;
  }

  var DIRECTION_ARROWS = {
    upstream_to_client: '←',
    client_to_upstream: '→',
    gateway_to_upstream: '⇒',
    gateway_creation: '•'
  };

  function lookup(map, key, fallback) {
    return Object.prototype.hasOwnProperty.call(map, key) ? map[key] : fallback;
  }

  function pad2(n) { return n < 10 ? '0' + n : String(n); }

  // Local HH:MM:SS, or '' when the timestamp is missing or invalid.
  function formatClock(value) {
    if (!value) return '';
    var d = new Date(value);
    if (isNaN(d.getTime())) return '';
    return pad2(d.getHours()) + ':' + pad2(d.getMinutes()) + ':' + pad2(d.getSeconds());
  }

  function formatBytes(n) {
    var b = toInt(n);
    if (b < 1024) return b + ' B';
    if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
    return (b / 1048576).toFixed(1) + ' MB';
  }

  // Keeps the start and end of a long id so it stays recognisable on one line.
  function middleEllipsis(text, max) {
    var s = String(text || '');
    var limit = toInt(max) > 8 ? toInt(max) : 24;
    if (s.length <= limit) return s;
    var head = Math.ceil((limit - 1) / 2);
    var tail = limit - 1 - head;
    return s.slice(0, head) + '…' + s.slice(s.length - tail);
  }

  // One compact timeline row. Only display fields; the payload is never parsed here.
  // Creation rows show "creation" plus a Creation tag; rawType keeps the wire type visible.
  function compactEventRow(ev) {
    var e = ev || {};
    var direction = String(e.direction || '');
    var rawType = String(e.type || '(no type)');
    var creation = direction === 'gateway_creation';
    return {
      seq: toEventSeq(e.seq),
      time: formatClock(e.at),
      arrow: lookup(DIRECTION_ARROWS, direction, '?'),
      direction: direction,
      dirLabel: lookup(DIRECTION_LABELS, direction, direction || 'unknown'),
      type: creation ? 'creation' : rawType,
      rawType: rawType,
      tag: creation ? 'Creation' : '',
      size: formatBytes(e.size),
      delivery: String(e.delivery || 'unknown'),
      group: String(e.group || 'other'),
      delegation: e.group === 'delegation',
      truncated: e.truncated === true
    };
  }

  // One transcript line per turn. Pending turns are still streaming.
  function transcriptLine(item, at) {
    var role = item && item.role === 'assistant' ? 'assistant' : 'user';
    return {
      time: formatClock(at),
      role: role,
      roleLabel: role === 'assistant' ? 'Assistant' : 'User',
      text: String((item && item.text) || ''),
      pending: !(item && item.final)
    };
  }

  // The single definition of delegation counts. Every delegation count in the UI comes from here:
  // "created" is the number of delegation.created events; "writes" are the other group members
  // (delegation.context.append, and session.context.append* that carry delegation_item_id).
  function delegationCounts(list, q) {
    var out = { total: 0, created: 0, writes: 0 };
    (Array.isArray(list) ? list : []).forEach(function (ev) {
      if (!ev || ev.group !== 'delegation' || !searchMatches(ev, q)) return;
      out.total++;
      if (ev.type === 'delegation.created') out.created++; else out.writes++;
    });
    return out;
  }

  function delegationTooltip(c) {
    return c.total + ' event' + (c.total === 1 ? '' : 's') + ': ' + c.created + ' created, ' +
      c.writes + ' context write' + (c.writes === 1 ? '' : 's');
  }

  function filterIsActive(filter) {
    var f = filter || {};
    var groups = Array.isArray(f.groups) ? f.groups : GROUP_IDS;
    var allGroups = GROUP_IDS.every(function (id) { return groups.indexOf(id) >= 0; });
    return !allGroups || !!f.type || !!String(f.q || '').trim();
  }

  function clearedFilter() {
    return { groups: GROUP_IDS.slice(), type: '', q: '' };
  }

  function filterSummary(shown, total, filter) {
    if (!filterIsActive(filter)) return '';
    return 'Filtered: ' + shown + ' of ' + total + ' event' + (total === 1 ? '' : 's');
  }

  function creationFromEvent(ev) {
    return { seq: ev.seq, at: ev.at, text: ev.payload, parsed: parseJSON(ev.payload), truncated: ev.truncated };
  }

  // Creation facts. Structured payloads get counts; anything else is shown as raw text.
  function creationDetails(c) {
    if (!c) return null;
    var obj = c.parsed && typeof c.parsed === 'object' && !Array.isArray(c.parsed) ? c.parsed : null;
    var out = {
      parsed: !!obj,
      hasInstructions: false,
      instructionChars: 0,
      hasItems: false,
      items: 0,
      itemChars: 0,
      rawChars: String(c.text || '').length,
      truncated: c.truncated === true,
      body: ''
    };
    var lines = [];
    if (obj && obj.instructions !== undefined) {
      var text = typeof obj.instructions === 'string' ? obj.instructions : readableJSON(obj.instructions);
      out.hasInstructions = true;
      out.instructionChars = text.length;
      lines.push('instructions:\n' + text);
    }
    if (obj && obj.initial_items !== undefined) {
      var itemsText = readableJSON(obj.initial_items);
      out.hasItems = true;
      out.items = Array.isArray(obj.initial_items) ? obj.initial_items.length : 0;
      out.itemChars = itemsText.length;
      lines.push('initial_items:\n' + itemsText);
    }
    if (!lines.length) lines.push(c.parsed === null || c.parsed === undefined ? String(c.text || '') : readableJSON(c.parsed));
    out.body = lines.join('\n\n');
    if (out.truncated) out.body += '\n\n[payload truncated by gateway]';
    return out;
  }

  function creationSummary(d) {
    if (!d) return '';
    if (!d.parsed) return d.rawChars + ' chars (unstructured)';
    var parts = [];
    if (d.hasInstructions) parts.push('instructions ' + d.instructionChars + ' chars');
    if (d.hasItems) parts.push(d.items + ' initial item' + (d.items === 1 ? '' : 's') + ' · ' + d.itemChars + ' chars');
    return parts.join(' · ') || 'no instructions or items';
  }

  // Transcript anchors in seq order: the creation event, delegation.created events, context writes
  // and client messages. Other delegation writes stay in the Delegation group and get no anchor.
  function anchorRows(list) {
    var events = Array.isArray(list) ? list : [];
    var out = [];
    var creation = events.filter(function (ev) { return ev && ev.direction === 'gateway_creation'; })[0];
    if (creation) {
      out.push({ seq: creation.seq, at: creation.at, kind: 'creation', label: 'Creation · ' + creationSummary(creationDetails(creationFromEvent(creation))) });
    }
    delegationRows(events).forEach(function (r) {
      if (r.kind !== 'created') return;
      out.push({
        seq: r.seq,
        at: r.at,
        kind: 'delegation',
        label: 'Delegation' + (r.target ? ' · target ' + r.target : '') + (r.handoff ? ' · handoff ' + r.handoff : '')
      });
    });
    events.forEach(function (ev) {
      if (!ev) return;
      if (ev.group === 'context') out.push({ seq: ev.seq, at: ev.at, kind: 'context', label: 'Context write · ' + ev.type });
      if (ev.group === 'client') out.push({ seq: ev.seq, at: ev.at, kind: 'client', label: 'Client message · ' + ev.type });
    });
    return out.sort(function (a, b) { return a.seq - b.seq; });
  }

  // Creation anchors come first; every other anchor sits before the first turn whose seq is not lower.
  function transcriptEntries(items, anchors) {
    var sorted = (Array.isArray(anchors) ? anchors : []).slice().sort(function (a, b) { return a.seq - b.seq; });
    var out = sorted.filter(function (a) { return a.kind === 'creation'; }).map(function (a) { return { anchor: a }; });
    var rest = sorted.filter(function (a) { return a.kind !== 'creation'; });
    var i = 0;
    (Array.isArray(items) ? items : []).forEach(function (item) {
      while (i < rest.length && rest[i].seq <= item.seq) out.push({ anchor: rest[i++] });
      out.push({ turn: item });
    });
    while (i < rest.length) out.push({ anchor: rest[i++] });
    return out;
  }

  // Warnings for an incomplete, truncated or capped log. Empty when the log is complete.
  function storeWarnings(store, capped) {
    var lines = [];
    if (!store) return lines;
    if (store.incomplete()) {
      var gaps = store.gaps();
      lines.push('Incomplete log: this view is NOT a complete record.');
      if (store.dropped() > 0) lines.push(store.dropped() + ' event(s) were dropped by the gateway.');
      gaps.reported.forEach(function (g) { lines.push('Gap reported for seq ' + g.from + '–' + g.to + '.'); });
      if (gaps.missing.length) {
        var sample = gaps.missing.slice(0, 3).map(function (g) { return g.from + '–' + g.to; }).join(', ');
        lines.push(gaps.missing.length + ' missing seq range(s) between loaded events: ' + sample + (gaps.missing.length > 3 ? ', …' : '') + '.');
      }
    }
    var truncated = store.truncatedUpstream();
    if (truncated > 0) lines.push(truncated + ' upstream event(s) were truncated; their captions may be missing.');
    if (capped) lines.push('Loading stopped early; events beyond the loaded range are not shown.');
    return lines;
  }

  // Audio facts from a generation summary. Returns null when the generation has no audio fields at all.
  function audioFacts(item) {
    if (!item || typeof item !== 'object') return null;
    var keys = ['audio_events', 'audio_bytes', 'audio', 'audio_samples'];
    if (!keys.some(function (k) { return k in item; })) return null;
    var facts = Array.isArray(item.audio) ? item.audio : [];
    var samples = Array.isArray(item.audio_samples) ? item.audio_samples : [];
    return {
      events: toInt(item.audio_events),
      bytes: toInt(item.audio_bytes),
      facts: facts.filter(function (f) { return f && typeof f === 'object'; }).map(function (f) {
        return {
          direction: String(f.direction || ''),
          source: String(f.source || ''),
          type: String(f.type || ''),
          events: toInt(f.events),
          bytes: toInt(f.bytes),
          maxBytes: toInt(f.max_bytes)
        };
      }),
      samples: samples.filter(function (s) { return s && typeof s === 'object'; }).map(function (s) {
        return {
          direction: String(s.direction || ''),
          type: String(s.type || ''),
          byteLength: toInt(s.byte_length),
          paths: Array.isArray(s.paths) ? s.paths.map(function (p) { return String(p); }) : []
        };
      })
    };
  }

  function audioFactLine(f) {
    var head = [f.direction, f.source, f.type].filter(Boolean).join(' · ');
    return head + ': ' + f.events + ' event' + (f.events === 1 ? '' : 's') + ', ' + formatBytes(f.bytes) +
      (f.maxBytes ? ' (max ' + formatBytes(f.maxBytes) + ')' : '');
  }

  function audioSampleLine(s) {
    return [s.direction, s.type].filter(Boolean).join(' · ') + ' · ' + formatBytes(s.byteLength);
  }

  // A generation counts as active only while its summary says so and it has no ended_at.
  function summaryIsActive(g) {
    return !!g && g.active === true && !g.ended_at;
  }

  // Picker state. The open generation follows the same live flag as the status pill.
  function generationStateLabel(g, isOpen, liveNow) {
    var active = isOpen ? liveNow === true : summaryIsActive(g);
    return active ? 'active' : 'ended';
  }

  // Picker option text. For the open generation both the count and the state come from the live
  // store and flag, so the picker and the status bar cannot disagree.
  function generationOptionLabel(g, live) {
    var state = live && live.open ? (live.active ? 'active' : 'ended') : (summaryIsActive(g) ? 'active' : 'ended');
    var count = live && live.open ? live.count : toInt(g.event_count);
    return 'Gen ' + g.generation + ' · ' + state + ' · ' + count + ' event' + (count === 1 ? '' : 's');
  }

  // Full banner only for a real gap or an error. Dropped events alone get one compact line.
  function noticeFor(store, capped, error) {
    if (error) return { kind: 'error', full: true, lines: [error].concat(storeWarnings(store, capped)) };
    if (store && store.hasGap()) return { kind: 'warn', full: true, lines: storeWarnings(store, capped) };
    var parts = [];
    var dropped = store ? store.dropped() : 0;
    var truncated = store ? store.truncatedUpstream() : 0;
    if (dropped > 0) parts.push('Incomplete log: ' + dropped + ' event(s) dropped by the gateway');
    if (truncated > 0) parts.push(truncated + ' upstream event(s) truncated');
    if (capped) parts.push('loading stopped early');
    if (!parts.length) return { kind: 'none', full: false, lines: [] };
    return { kind: 'warn', full: false, lines: [parts.join(' · ') + '.'] };
  }

  function requested() {
    return typeof window !== 'undefined' && window.location.pathname === '/viewer';
  }

  function start() {
    var doc = root.document;
    var $ = function (id) { return doc.getElementById(id); };
    var el = {
      theme: $('vwTheme'), delegationBadge: $('vwDelegationBadge'),
      statusPill: $('vwStatusPill'), session: $('vwSession'), zoe: $('vwZoe'), genSelect: $('vwGenSelect'),
      eventCount: $('vwEventCount'), lastEvent: $('vwLastEvent'), gapPill: $('vwGapPill'),
      audioWrap: $('vwAudioWrap'), audioBtn: $('vwAudioBtn'), audioSummary: $('vwAudioSummary'), audioPop: $('vwAudioPop'),
      notice: $('vwNotice'), creation: $('vwCreation'), creationSummary: $('vwCreationSummary'),
      creationBody: $('vwCreationBody'), creationCopy: $('vwCreationCopy'),
      groups: $('vwGroups'), filterLine: $('vwFilterLine'), filterText: $('vwFilterText'), filterClear: $('vwFilterClear'),
      search: $('vwSearch'), menuBtn: $('vwMenuBtn'), menu: $('vwMenu'), type: $('vwType'),
      copy: $('vwCopy'), download: $('vwDownload'),
      tabTranscript: $('vwTabTranscript'), tabEvents: $('vwTabEvents'), tabFrames: $('vwTabFrames'),
      colTranscript: $('vwColTranscript'), transcriptScroll: $('vwTranscriptScroll'), transcript: $('vwTranscript'),
      transcriptCount: $('vwTranscriptCount'), followTranscript: $('vwFollowTranscript'),
      panelEvents: $('vwPanelEvents'), eventsScroll: $('vwEventsScroll'), events: $('vwEvents'),
      timelineCount: $('vwTimelineCount'), followEvents: $('vwFollowEvents'),
      panelFrames: $('vwPanelFrames'), captureState: $('vwCaptureState'),
      framesAudio: $('vwFramesAudio'), framesAudioBody: $('vwFramesAudioBody'),
      toast: $('vwToast')
    };
    if (!el.transcript) return;

    function make(tag, cls, text) {
      var node = doc.createElement(tag);
      if (cls) node.className = cls;
      if (text !== undefined && text !== null) node.textContent = String(text);
      return node;
    }

    function plural(n, word) {
      return n + ' ' + word + (n === 1 ? '' : 's');
    }

    function safeClass(value) {
      return String(value || 'unknown').replace(/[^a-z_-]/gi, '-').toLowerCase();
    }

    var params = new URLSearchParams(root.location.search);
    var sessionId = String(params.get('session') || '').trim();
    var requestedGen = parseGeneration(params.get('gen'));
    var mq = root.matchMedia ? root.matchMedia('(min-width: 1000px)') : null;
    var view = {
      gens: [], gen: 0, zoeId: '', store: null, es: null, retryTimer: 0, retryDelay: 1000, toastTimer: 0,
      token: 0, active: false, startedAt: '', endedAt: '', error: '', capped: false,
      tab: 'transcript', following: { transcript: true, events: true }, renderQueued: false,
      filters: filterFromParams(params), typesKey: null, open: {}, menuOpen: false, audioOpen: false,
      frames: { ring: 'all', token: 0, loading: false, error: '', data: null, open: {} }
    };

    function isWide() {
      return mq ? mq.matches : true;
    }

    // Connection state lives in view.conn; the status pill is its only visible rendering.
    function setConn(label) {
      view.conn = label;
      renderStatus();
    }

    function showError(message) {
      view.error = String(message || 'Unknown error');
      renderAll();
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
      renderLayout();
      renderGenSelect();
      renderStatus();
      renderNotice();
      renderCreation();
      renderGroupChips();
      renderFilterLine();
      renderDelegationBadge();
      renderAudio();
      renderTranscript();
      renderEvents();
      syncUrl();
    }

    function syncUrl() {
      if (!view.gen) return;
      var url = viewerUrl(sessionId, view.gen, view.filters);
      if (url !== root.location.pathname + root.location.search) root.history.replaceState(null, '', url);
    }

    // Tabs pick the single column on narrow screens; on wide screens the transcript is always shown
    // and the right column shows the timeline or the frames.
    function renderLayout() {
      var wide = isWide();
      var t = view.tab;
      var rightVisible = wide || t !== 'transcript';
      var frames = t === 'frames';
      el.colTranscript.hidden = !(wide || t === 'transcript');
      el.panelEvents.hidden = !(rightVisible && !frames);
      el.panelFrames.hidden = !(rightVisible && frames);
      el.tabTranscript.setAttribute('aria-selected', String(t === 'transcript'));
      el.tabEvents.setAttribute('aria-selected', String(t === 'events' || (wide && t === 'transcript')));
      el.tabFrames.setAttribute('aria-selected', String(frames));
    }

    // One state label drives the status pill and the picker: Live while the open generation streams,
    // Ended once it is closed, and the connection state (Connecting, Reconnecting) while it is open.
    function stateLabel() {
      if (view.error) return { text: 'Error', kind: 'error' };
      if (!view.gen) return { text: 'Loading', kind: 'neutral' };
      if (view.active) return { text: view.conn && view.conn !== 'Live' ? view.conn : 'Live', kind: 'live' };
      return { text: 'Ended', kind: 'neutral' };
    }

    function renderStatus() {
      var list = view.store ? view.store.list() : [];
      var last = list.length ? list[list.length - 1] : null;
      var state = stateLabel();
      el.statusPill.textContent = state.text;
      el.statusPill.className = 'vw-pill ' + state.kind;
      el.session.textContent = middleEllipsis(sessionId, 30) || '—';
      el.session.title = sessionId;
      el.session.disabled = !sessionId;
      el.zoe.textContent = middleEllipsis(view.zoeId, 30) || '—';
      el.zoe.title = view.zoeId;
      el.zoe.disabled = !view.zoeId;
      var count = view.store ? view.store.size() : 0;
      el.eventCount.textContent = plural(count, 'event');
      var dropped = view.store ? view.store.dropped() : 0;
      el.gapPill.textContent = 'Incomplete' + (dropped > 0 ? ' · ' + dropped + ' dropped' : '');
      el.gapPill.hidden = !(dropped > 0 || (view.store && view.store.hasGap()));
      var lastAt = last ? formatClock(last.at) : '';
      el.lastEvent.textContent = 'Last event ' + (lastAt || '—');
    }

    function renderNotice() {
      var n = noticeFor(view.store, view.capped, view.error);
      el.notice.textContent = '';
      el.notice.hidden = n.kind === 'none';
      el.notice.className = 'vw-notice' + (n.full ? '' : ' compact');
      el.notice.setAttribute('data-kind', n.kind === 'error' ? 'error' : 'warn');
      if (!n.lines.length) return;
      if (n.full) {
        el.notice.appendChild(make('div', 'vw-notice-title', n.lines[0]));
        n.lines.slice(1).forEach(function (line) { el.notice.appendChild(make('div', 'vw-notice-line', line)); });
      } else {
        el.notice.appendChild(make('div', 'vw-notice-line', n.lines[0]));
      }
    }

    function renderCreation() {
      var d = creationDetails(view.store ? view.store.creation() : null);
      el.creation.hidden = !d;
      if (!d) return;
      el.creationSummary.textContent = creationSummary(d);
      el.creationBody.textContent = d.body;
    }

    function boundaryRow(text) {
      return make('div', 'vw-boundary', text);
    }

    function emptyRow(text) {
      return make('p', 'vw-empty', text);
    }

    function applyScroll(pane, top, key) {
      if (view.following[key]) {
        pane.scrollTop = pane.scrollHeight;
      } else {
        pane.scrollTop = top;
      }
      var btn = key === 'transcript' ? el.followTranscript : el.followEvents;
      if (btn && key !== 'frames') btn.hidden = !!view.following[key];
    }

    function turnRow(line) {
      var row = make('div', 'vw-turn ' + line.role + (line.pending ? ' pending' : ''));
      row.appendChild(make('span', 'vw-time', line.time));
      row.appendChild(make('span', 'vw-role', line.roleLabel));
      row.appendChild(make('span', 'vw-text', line.text));
      return row;
    }

    function anchorRow(a) {
      var row = make('div', 'vw-marker vw-anchor kind-' + a.kind);
      row.setAttribute('role', 'button');
      row.tabIndex = 0;
      row.title = 'Show #' + a.seq + ' in the timeline';
      row.appendChild(make('span', 'vw-time', formatClock(a.at)));
      row.appendChild(make('span', 'vw-marker-text', a.label));
      row.appendChild(make('span', 'vw-seq', '#' + a.seq));
      row.addEventListener('click', function () { jumpToSeq(a.seq); });
      row.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); jumpToSeq(a.seq); }
      });
      return row;
    }

    function renderTranscript() {
      var pane = el.transcriptScroll;
      var top = pane.scrollTop;
      el.transcript.textContent = '';
      if (!view.store) { applyScroll(pane, top, 'transcript'); return; }
      el.transcript.appendChild(boundaryRow('Generation ' + view.gen + (view.startedAt ? ' started ' + formatClock(view.startedAt) : ' started')));
      var items = view.store.captions();
      var anchors = anchorRows(view.store.list());
      transcriptEntries(items, anchors).forEach(function (entry) {
        if (entry.anchor) el.transcript.appendChild(anchorRow(entry.anchor));
        else el.transcript.appendChild(turnRow(transcriptLine(entry.turn, view.store.at(entry.turn.seq))));
      });
      if (!items.length && !anchors.length) el.transcript.appendChild(emptyRow('No captions in this generation yet.'));
      if (!view.active && view.store.size()) {
        el.transcript.appendChild(boundaryRow('Generation ' + view.gen + ' ended' + (view.endedAt ? ' ' + formatClock(view.endedAt) : '')));
      }
      el.transcriptCount.textContent = plural(items.length, 'turn');
      applyScroll(pane, top, 'transcript');
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

    // Chip counts respect the search text, not the group toggles, so a user can see what a toggle would add.
    function renderGroupChips() {
      var list = view.store ? view.store.list() : [];
      var counts = view.store ? view.store.counts(view.filters.q) : {};
      el.groups.textContent = '';
      GROUPS.forEach(function (g) {
        var on = view.filters.groups.indexOf(g.id) >= 0;
        var btn = make('button', 'vw-chip');
        btn.type = 'button';
        btn.setAttribute('aria-pressed', String(on));
        btn.setAttribute('data-group', g.id);
        btn.appendChild(make('span', 'vw-chip-dot'));
        btn.appendChild(make('span', '', g.label));
        btn.appendChild(make('span', 'vw-count', String(counts[g.id] || 0)));
        if (g.id === 'delegation') btn.title = delegationTooltip(delegationCounts(list, view.filters.q));
        el.groups.appendChild(btn);
      });
    }

    function renderFilterLine() {
      var active = filterIsActive(view.filters);
      el.filterLine.hidden = !active;
      if (!active) return;
      var shown = view.store ? view.store.filtered(view.filters).length : 0;
      var total = view.store ? view.store.size() : 0;
      el.filterText.textContent = filterSummary(shown, total, view.filters);
    }

    // The badge and the Delegation group both come from delegationCounts: created events only for the badge.
    function renderDelegationBadge() {
      var c = delegationCounts(view.store ? view.store.list() : [], '');
      el.delegationBadge.textContent = plural(c.created, 'delegation');
      el.delegationBadge.hidden = c.created === 0;
    }

    function renderAudioInto(box, facts) {
      box.textContent = '';
      box.appendChild(make('div', 'vw-audio-h', 'Totals'));
      box.appendChild(make('div', '', plural(facts.events, 'audio event') + ' · ' + formatBytes(facts.bytes)));
      box.appendChild(make('div', 'vw-audio-h', 'By direction, source and type'));
      if (facts.facts.length) {
        var ul = make('ul', 'vw-list');
        facts.facts.forEach(function (f) { ul.appendChild(make('li', '', audioFactLine(f))); });
        box.appendChild(ul);
      } else {
        box.appendChild(make('div', 'vw-empty', 'No per-type audio facts recorded.'));
      }
      if (facts.samples.length) {
        box.appendChild(make('div', 'vw-audio-h', 'Shape samples (structure only, no payload content)'));
        var sl = make('ul', 'vw-list');
        facts.samples.forEach(function (s) {
          var li = make('li', '', audioSampleLine(s));
          if (s.paths.length) {
            var pl = make('ul', 'vw-list');
            s.paths.forEach(function (p) { pl.appendChild(make('li', '', p)); });
            li.appendChild(pl);
          }
          sl.appendChild(li);
        });
        box.appendChild(sl);
      }
    }

    function renderAudio() {
      var facts = audioFacts(findGeneration(view.gens, view.gen));
      el.audioWrap.hidden = !facts;
      var generation = findGeneration(view.gens, view.gen);
      el.captureState.textContent = 'File capture: ' + (generation && generation.audio_capture ? 'on' : 'off');
      el.framesAudio.hidden = !facts;
      if (!facts) return;
      el.audioSummary.textContent = plural(facts.events, 'event') + ' · ' + formatBytes(facts.bytes);
      el.audioPop.hidden = !view.audioOpen;
      el.audioBtn.setAttribute('aria-expanded', String(view.audioOpen));
      renderAudioInto(el.audioPop, facts);
      renderAudioInto(el.framesAudioBody, facts);
    }

    function eventRow(ev) {
      var r = compactEventRow(ev);
      var wrap = make('div', 'vw-event' + (r.delegation ? ' is-delegation' : ''));
      wrap.setAttribute('data-seq', String(r.seq));
      var head = make('div', 'vw-row');
      head.setAttribute('role', 'button');
      head.tabIndex = 0;
      head.setAttribute('aria-expanded', 'false');
      head.title = r.group + ' · ' + r.dirLabel + (r.tag ? ' · type ' + r.rawType : '');
      head.appendChild(make('span', 'vw-time', r.time));
      var arrow = make('span', 'vw-arrow dir-' + safeClass(r.direction), r.arrow);
      arrow.title = r.dirLabel;
      arrow.setAttribute('aria-label', r.dirLabel);
      head.appendChild(arrow);
      var typeCell = make('span', 'vw-type', r.type);
      if (r.tag) typeCell.appendChild(make('span', 'vw-tag', r.tag));
      head.appendChild(typeCell);
      head.appendChild(make('span', 'vw-size', r.size));
      head.appendChild(make('span', 'vw-delivery', r.delivery));
      head.appendChild(make('span', 'vw-seq', '#' + r.seq));
      wrap.appendChild(head);

      var body = make('div', 'vw-payload');
      body.hidden = true;
      var filled = false;
      var fill = function () {
        filled = true;
        body.appendChild(make('div', 'vw-payload-meta', [groupLabel(ev.group), r.tag ? 'type ' + r.rawType : '', ev.at, ev.source, 'delivery ' + ev.delivery].filter(Boolean).join(' · ')));
        if (r.direction === 'gateway_creation') {
          body.appendChild(make('pre', 'vw-pre vw-event-pre', creationDetails(creationFromEvent(ev)).body));
          return;
        }
        body.appendChild(make('pre', 'vw-pre vw-event-pre', displayPayload(ev.payload)));
        if (ev.truncated) body.appendChild(make('div', 'vw-payload-meta warn', '[payload truncated by gateway]'));
      };
      var setOpen = function (open) {
        if (open && !filled) fill();
        body.hidden = !open;
        head.setAttribute('aria-expanded', String(open));
        if (open) view.open[ev.seq] = true; else delete view.open[ev.seq];
      };
      if (view.open[ev.seq]) setOpen(true);
      head.addEventListener('click', function () { setOpen(body.hidden); });
      head.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setOpen(body.hidden); }
      });
      wrap.appendChild(body);
      return wrap;
    }

    function renderEvents() {
      var pane = el.eventsScroll;
      var top = pane.scrollTop;
      el.events.textContent = '';
      if (!view.store) { applyScroll(pane, top, 'events'); return; }
      syncTypeOptions();
      el.events.appendChild(boundaryRow('Generation ' + view.gen + (view.startedAt ? ' started ' + formatClock(view.startedAt) : ' started')));
      view.store.gaps().reported.forEach(function (g) {
        el.events.appendChild(make('div', 'vw-gap-row', 'Gap reported by gateway: seq ' + g.from + '–' + g.to));
      });
      var all = view.store.list();
      var rows = [];
      var prev = -1;
      all.forEach(function (ev) {
        if (prev >= 0 && ev.seq - prev > 1) rows.push({ gap: 'Missing seq ' + (prev + 1) + '–' + (ev.seq - 1) + ' (not loaded)' });
        prev = ev.seq;
        if (eventMatches(ev, view.filters)) rows.push({ ev: ev });
      });
      var shownRows = rows.filter(function (row) { return row.ev; });
      var visible = rows.slice(-MAX_EVENT_ROWS);
      if (visible.length < rows.length) {
        el.events.appendChild(emptyRow('Showing the latest ' + MAX_EVENT_ROWS + ' rows of ' + rows.length + '. Use filters to narrow the list.'));
      }
      visible.forEach(function (row) {
        el.events.appendChild(row.ev ? eventRow(row.ev) : make('div', 'vw-gap-row', row.gap));
      });
      if (!all.length) el.events.appendChild(emptyRow('No events loaded for this generation yet.'));
      else if (!shownRows.length) el.events.appendChild(emptyRow('No events match the current filters.'));
      if (!view.active && all.length) {
        el.events.appendChild(boundaryRow('Generation ' + view.gen + ' ended' + (view.endedAt ? ' ' + formatClock(view.endedAt) : '')));
      }
      el.timelineCount.textContent = plural(shownRows.length, 'row');
      applyScroll(pane, top, 'events');
    }

    function renderGenSelect() {
      el.genSelect.textContent = '';
      view.gens.forEach(function (g) {
        var isOpen = Number(g.generation) === view.gen;
        var live = isOpen ? { open: true, active: view.active, count: view.store ? view.store.size() : 0 } : null;
        var opt = make('option', '', generationOptionLabel(g, live));
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
        setConn('Live');
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
        setConn('Ended');
        scheduleRender();
      });
      es.onerror = function () {
        if (es !== view.es) return;
        stopStream();
        if (token !== view.token || !view.active) return;
        setConn('Reconnecting');
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
      view.frames.token++;
      view.frames.loading = false;
      view.frames.error = '';
      view.frames.data = null;
      view.frames.open = {};
      root.history.replaceState(null, '', viewerUrl(sessionId, view.gen, view.filters));
      setConn('Loading');
      renderGenSelect();
      scheduleRender();
      loadSnapshot(token).then(function (ok) {
        if (!ok || token !== view.token) return;
        if (view.active) {
          setConn('Connecting');
          startStream(token);
        } else {
          setConn('Ended');
        }
        scheduleRender();
      }).catch(function (e) {
        if (token === view.token) showError(e && e.message ? e.message : e);
      });
    }

    async function refreshGenerations() {
      try {
        await loadGenerations();
        renderAudio();
        renderStatus();
      } catch (e) {}
    }

    function setTab(tab) {
      view.tab = tab;
      view.following = { transcript: true, events: true };
      renderAll();
    }

    function onPaneScroll(key, pane) {
      if (!pane.clientHeight) return;
      view.following[key] = pane.scrollHeight - pane.scrollTop - pane.clientHeight < 40;
      var btn = key === 'transcript' ? el.followTranscript : el.followEvents;
      btn.hidden = view.following[key];
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

    async function copyWithToast(text, label) {
      try {
        await copyText(text);
        toast(label);
      } catch (e) {
        toast('Copy failed');
      }
    }

    function visibleJSON() {
      var list = view.store ? view.store.filtered(view.filters) : [];
      return { count: list.length, text: JSON.stringify(list.map(function (ev) { return ev.raw; }), null, 2) };
    }

    function setMenu(open) {
      view.menuOpen = open;
      el.menu.hidden = !open;
      el.menuBtn.setAttribute('aria-expanded', String(open));
    }

    function setAudio(open) {
      view.audioOpen = open;
      el.audioPop.hidden = !open;
      el.audioBtn.setAttribute('aria-expanded', String(open));
    }

    function clearFilters() {
      view.filters = clearedFilter();
      el.search.value = '';
      el.type.value = '';
      renderAll();
    }

    // Opens the timeline on an event: widens only the filters that hide it, expands it and flashes it.
    function jumpToSeq(seq) {
      var ev = view.store ? view.store.get(seq) : null;
      if (!ev) return;
      var f = view.filters;
      if (!eventMatches(ev, f)) {
        if (f.groups.indexOf(ev.group) < 0) f.groups = GROUP_IDS.filter(function (id) { return id === ev.group || f.groups.indexOf(id) >= 0; });
        if (f.type && f.type !== ev.type) { f.type = ''; el.type.value = ''; }
        if (f.q && !searchMatches(ev, f.q)) { f.q = ''; el.search.value = ''; }
      }
      view.open[seq] = true;
      view.tab = 'events';
      view.following.events = false;
      renderAll();
      var row = el.events.querySelector('[data-seq="' + seq + '"]');
      if (!row) { toast('Event #' + seq + ' is not in the visible rows'); return; }
      var pane = el.eventsScroll;
      pane.scrollTop = pane.scrollTop + row.getBoundingClientRect().top - pane.getBoundingClientRect().top - 40;
      row.classList.add('flash');
      root.setTimeout(function () { row.classList.remove('flash'); }, 1600);
    }

    function bind() {
      el.genSelect.addEventListener('change', function () {
        var gen = Number(el.genSelect.value);
        openGeneration(gen, findGeneration(view.gens, gen));
      });
      el.tabTranscript.addEventListener('click', function () { setTab('transcript'); });
      el.tabEvents.addEventListener('click', function () { setTab('events'); });
      el.tabFrames.addEventListener('click', function () { setTab('frames'); });
      el.delegationBadge.addEventListener('click', function () {
        view.filters.groups = ['delegation'];
        view.filters.type = '';
        el.type.value = '';
        setTab('events');
      });
      el.type.addEventListener('change', function () { view.filters.type = el.type.value; scheduleRender(); });
      el.search.value = view.filters.q;
      var searchTimer = 0;
      el.search.addEventListener('input', function () {
        view.filters.q = el.search.value.trim();
        root.clearTimeout(searchTimer);
        searchTimer = root.setTimeout(scheduleRender, 120);
      });
      el.groups.addEventListener('click', function (e) {
        var btn = e.target && e.target.closest ? e.target.closest('button[data-group]') : null;
        if (!btn) return;
        var id = btn.getAttribute('data-group');
        var on = view.filters.groups.indexOf(id) >= 0;
        view.filters.groups = GROUP_IDS.filter(function (g) {
          return g === id ? !on : view.filters.groups.indexOf(g) >= 0;
        });
        scheduleRender();
      });
      el.filterClear.addEventListener('click', clearFilters);
      el.menuBtn.addEventListener('click', function () { setMenu(!view.menuOpen); });
      Array.prototype.forEach.call(el.menu.querySelectorAll('button[data-preset]'), function (b) {
        b.addEventListener('click', function () {
          view.filters.groups = presetGroups(b.getAttribute('data-preset')) || GROUP_IDS.slice();
          setMenu(false);
          scheduleRender();
        });
      });
      el.copy.addEventListener('click', function () {
        var out = visibleJSON();
        setMenu(false);
        copyWithToast(out.text, 'Copied ' + plural(out.count, 'event') + ' as JSON');
      });
      el.download.addEventListener('click', function () {
        var out = visibleJSON();
        setMenu(false);
        var blob = new root.Blob([out.text + '\n'], { type: 'application/json' });
        var url = root.URL.createObjectURL(blob);
        var a = make('a', '');
        a.href = url;
        a.download = 'viewer-' + (sessionId || 'session').replace(/[^a-z0-9_-]/gi, '_') + '-gen' + view.gen + '.json';
        doc.body.appendChild(a);
        a.click();
        doc.body.removeChild(a);
        root.setTimeout(function () { root.URL.revokeObjectURL(url); }, 1000);
        toast('Downloading ' + plural(out.count, 'event'));
      });
      el.audioBtn.addEventListener('click', function () { setAudio(!view.audioOpen); });
      el.creationCopy.addEventListener('click', function () {
        copyWithToast(el.creationBody.textContent, 'Copied creation text');
      });
      el.session.addEventListener('click', function () { if (sessionId) copyWithToast(sessionId, 'Copied session id'); });
      el.zoe.addEventListener('click', function () { if (view.zoeId) copyWithToast(view.zoeId, 'Copied Zoe session id'); });
      el.followTranscript.addEventListener('click', function () {
        view.following.transcript = true;
        scheduleRender();
      });
      el.followEvents.addEventListener('click', function () {
        view.following.events = true;
        scheduleRender();
      });
      el.transcriptScroll.addEventListener('scroll', function () { onPaneScroll('transcript', el.transcriptScroll); }, { passive: true });
      el.eventsScroll.addEventListener('scroll', function () { onPaneScroll('events', el.eventsScroll); }, { passive: true });
      el.theme.addEventListener('click', function () {
        setTheme(doc.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark');
      });
      doc.addEventListener('click', function (e) {
        if (view.menuOpen && !el.menuBtn.parentNode.contains(e.target)) setMenu(false);
        if (view.audioOpen && !el.audioWrap.contains(e.target)) setAudio(false);
      });
      doc.addEventListener('keydown', function (e) {
        if (e.key !== 'Escape') return;
        if (view.menuOpen) { setMenu(false); el.menuBtn.focus(); }
        if (view.audioOpen) { setAudio(false); el.audioBtn.focus(); }
      });
      if (mq && mq.addEventListener) mq.addEventListener('change', renderAll);
      paintTheme();
      root.addEventListener('pagehide', function () {
        stopStream();
        root.clearTimeout(view.retryTimer);
      });
    }

    async function init() {
      doc.title = 'Upstream viewer · ' + (sessionId || 'no session');
      bind();
      setConn('Loading');
      renderAll();
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
    displayPayload: displayPayload,
    readableJSON: readableJSON,
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
    delegationRows: delegationRows,
    summaryIsActive: summaryIsActive,
    generationStateLabel: generationStateLabel,
    generationOptionLabel: generationOptionLabel,
    noticeFor: noticeFor,
    formatClock: formatClock,
    formatBytes: formatBytes,
    middleEllipsis: middleEllipsis,
    compactEventRow: compactEventRow,
    transcriptLine: transcriptLine,
    delegationCounts: delegationCounts,
    delegationTooltip: delegationTooltip,
    filterIsActive: filterIsActive,
    clearedFilter: clearedFilter,
    filterSummary: filterSummary,
    creationDetails: creationDetails,
    creationSummary: creationSummary,
    anchorRows: anchorRows,
    transcriptEntries: transcriptEntries,
    storeWarnings: storeWarnings,
    audioFacts: audioFacts,
    audioFactLine: audioFactLine,
    audioSampleLine: audioSampleLine
  };
}));
