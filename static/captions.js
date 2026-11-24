(function (root, factory) {
  var api = factory();
  if (typeof module === 'object' && module && module.exports) module.exports = api;
  else root.ZoeCaptions = api;
}(typeof window !== 'undefined' ? window : this, function () {
  'use strict';

  // Caption parsing shared by the live call page (voice.html) and the read-only
  // upstream viewer (voice-view.js). createCaptionAssembler mirrors the gateway
  // rules in internal/voice/transcript_forward.go; keep both in sync.

  function extractTextParts(parts) {
    if (!Array.isArray(parts)) return '';
    return parts.map(function (part) {
      if (typeof part === 'string') return part;
      if (!part || typeof part !== 'object') return '';
      if (typeof part.text === 'string') return part.text;
      if (typeof part.content === 'string') return part.content;
      if (part.content && typeof part.content.text === 'string') return part.content.text;
      if (typeof part.transcript === 'string') return part.transcript;
      return '';
    }).filter(Boolean).join('\n').trim();
  }

  function applyCaptionDelta(state, delta) {
    var previous = state && typeof state === 'object' ? state : {};
    var next = {
      role: previous.role === 'assistant' || previous.role === 'user' ? previous.role : '',
      messageId: String(previous.messageId || ''),
      text: String(previous.text || '')
    };
    if (!delta || typeof delta !== 'object') return next;

    var message = delta.v && delta.v.message;
    if (message && typeof message === 'object') {
      var parts = message.content && Array.isArray(message.content.parts) ? message.content.parts : [];
      var role = (message.author && message.author.role) || '';
      if (role !== 'assistant' && role !== 'user') {
        for (var partIndex = 0; partIndex < parts.length; partIndex++) {
          var part = parts[partIndex];
          if (!part || typeof part !== 'object' || part.content_type !== 'audio_transcription') continue;
          if (part.direction === 'in') role = 'user';
          if (part.direction === 'out') role = 'assistant';
          break;
        }
      }
      if (role !== 'assistant' && role !== 'user') return next;
      var text = parts.map(function (part) {
        if (typeof part === 'string') return part;
        if (!part || typeof part !== 'object') return '';
        if (typeof part.text === 'string') return part.text;
        if (typeof part.content === 'string') return part.content;
        if (typeof part.transcript === 'string') return part.transcript;
        return '';
      }).filter(Boolean).join('\n').trim();
      return {
        role: role,
        messageId: message.id ? String(message.id) : next.messageId,
        text: text
      };
    }

    if (!Array.isArray(delta.v) || !next.role) return next;
    var appended = '';
    for (var operationIndex = 0; operationIndex < delta.v.length; operationIndex++) {
      var operation = delta.v[operationIndex];
      if (!operation || operation.o !== 'append') continue;
      if (!/^\/message\/content\/parts\/\d+\/text$/.test(String(operation.p || ''))) continue;
      appended += String(operation.v || '');
    }
    if (appended) next.text += appended;
    return next;
  }

  function extractTranscriptText(obj, payload) {
    var source = payload || obj || {};
    var candidates = [
      obj && obj.transcript,
      obj && obj.text,
      obj && obj.delta,
      source && source.transcript,
      source && source.text,
      source && source.delta,
      source && source.item && source.item.transcript,
      source && source.item && source.item.content && extractTextParts(source.item.content.parts),
      source && source.message && source.message.content && extractTextParts(source.message.content.parts)
    ];
    for (var i = 0; i < candidates.length; i++) {
      if (typeof candidates[i] === 'string' && candidates[i].trim()) return candidates[i].trim();
    }
    return '';
  }

  // AVAS text: a string, a list of typed parts, or an object with a text-bearing key.
  function turnText(value) {
    if (typeof value === 'string') return value;
    if (Array.isArray(value)) return value.map(turnText).join('');
    if (value && typeof value === 'object') {
      var keys = ['transcript', 'text', 'content', 'parts'];
      for (var i = 0; i < keys.length; i++) {
        var s = turnText(value[keys[i]]);
        if (s) return s;
      }
    }
    return '';
  }

  function delegationText(content) {
    if (!Array.isArray(content)) return '';
    return content.map(function (part) {
      return part && part.type === 'input_text' && typeof part.text === 'string' ? part.text : '';
    }).join('');
  }

  function unwrapDataMessage(obj) {
    if (!obj || typeof obj !== 'object' || obj.type !== 'data_message') return obj;
    if (typeof obj.data === 'string') {
      try { return JSON.parse(obj.data); } catch (e) { return obj; }
    }
    return obj.data && typeof obj.data === 'object' ? obj.data : obj;
  }

  // Event payloads are JSON text on the wire; returns null when not an object.
  function parseEventPayload(payload) {
    var value = payload;
    if (typeof payload === 'string') {
      try { value = JSON.parse(payload); } catch (e) { return null; }
    }
    if (!value || typeof value !== 'object') return null;
    return unwrapDataMessage(value);
  }

  // Assembles captions from upstream events fed in seq order. Unknown events
  // return false; callers keep them in the raw view.
  function createCaptionAssembler() {
    var captions = [];
    var byKey = {};
    var seen = {};
    var delegated = {};
    var cur = null;
    var lastUser = null;
    var legacy = { role: '', messageId: '', text: '' };

    function emit(role, id, text, seq, final) {
      if ((role !== 'user' && role !== 'assistant') || !String(text || '').trim()) return false;
      if (id) {
        if (seen[role + ':' + id]) return false;
        seen[role + ':' + id] = true;
      }
      captions.push({ key: role + ':' + (id || '#' + seq), role: role, id: id, text: text, final: !!final, seq: seq });
      return true;
    }

    function upsert(key, role, id, text, seq, final) {
      var entry = byKey[key];
      if (!entry) {
        entry = { key: key, role: role, id: id, text: '', final: false, seq: seq };
        byKey[key] = entry;
        captions.push(entry);
      }
      entry.text = text;
      entry.final = !!final;
      entry.seq = seq;
      return true;
    }

    function flush() {
      if (cur && cur.role) emit(cur.role, cur.id, cur.text, cur.seq, false);
      cur = null;
    }

    function onTurnCreated(turn, seq) {
      var role = String(turn.role || '');
      var id = String(turn.id || '');
      if (role === 'user' && id && ((cur && cur.role === 'user' && cur.id === id) || seen['user:' + id])) return false;
      flush();
      if (role === 'user') lastUser = null;
      cur = { role: role, id: id, text: turnText(turn.transcript) || turnText(turn.content) || turnText(turn.text), seq: seq };
      return true;
    }

    function onTurnDone(turn, seq) {
      var role = String(turn.role || (cur && cur.role) || '');
      var id = String(turn.id || (cur && cur.id) || '');
      if (id && seen[role + ':' + id]) return false;
      var text = turnText(turn.transcript) || turnText(turn.content) || turnText(turn.text) || (cur ? cur.text : '');
      if (cur && cur.role && (cur.role !== role || (id && cur.id && cur.id !== id))) flush();
      cur = null;
      if (role === 'user') {
        if (lastUser && lastUser.text === text && (!id || !lastUser.id || id === lastUser.id)) return false;
        lastUser = { text: text, id: id };
      }
      return emit(role, id, text, seq, true);
    }

    function onDelegation(item, seq) {
      if (item.target !== 'client') return false;
      var id = String(item.id || '');
      var text = delegationText(item.content);
      if (!id || !text.trim() || delegated[id]) return false;
      delegated[id] = true;
      if (cur && cur.role === 'assistant') flush();
      var turnID = (cur && cur.id) || id;
      cur = null;
      return emit('user', turnID, text, seq, true);
    }

    function onLegacy(delta, seq) {
      var next = applyCaptionDelta(legacy, delta);
      var changed = !!(next.role && next.text && (next.role !== legacy.role || next.messageId !== legacy.messageId || next.text !== legacy.text));
      legacy = next;
      if (!changed) return false;
      return upsert('legacy:' + next.role + ':' + (next.messageId || 'anon'), next.role, next.messageId, next.text, seq, false);
    }

    function onTranscript(event, payload, type, seq) {
      var isUser = type.indexOf('input_audio') >= 0 && type.indexOf('transcription') >= 0;
      var fragment = extractTranscriptText(event, payload);
      var itemId = String(payload.item_id || event.item_id || '');
      var key = (isUser ? 'in:' : 'out:') + (itemId || 'current');
      var isDelta = /\.delta$/.test(type);
      var previous = byKey[key];
      var text = isDelta ? ((previous ? previous.text : '') + fragment) : fragment;
      if (!String(text || '').trim()) return false;
      return upsert(key, isUser ? 'user' : 'assistant', itemId, text, seq, !isDelta);
    }

    function consume(raw, seq) {
      if (!raw || typeof raw !== 'object') return false;
      var type = String(raw.type || '');
      var turn = raw.turn && typeof raw.turn === 'object' ? raw.turn : {};
      var item = raw.item && typeof raw.item === 'object' ? raw.item : {};
      var payload = raw.payload && typeof raw.payload === 'object' ? raw.payload : {};
      switch (type) {
        case 'turn.created':
          return onTurnCreated(turn, seq);
        case 'turn.done':
          return onTurnDone(turn, seq);
        case 'delegation.created':
          return onDelegation(item, seq);
        case 'turn.text.delta':
        case 'output_text.delta':
        case 'output_audio_transcript.delta':
          if (!cur || !cur.role) return false;
          cur.text = cur.text + turnText(raw.delta);
          cur.seq = seq;
          return true;
        case 'chat_message_delta':
          return onLegacy(payload.delta || raw.delta || {}, seq);
        default:
          if (type.indexOf('input_audio') >= 0 && type.indexOf('transcription') >= 0) return onTranscript(raw, payload, type, seq);
          if (type.indexOf('audio_transcript') >= 0) return onTranscript(raw, payload, type, seq);
          return false;
      }
    }

    function snapshot() {
      var out = captions.slice();
      if (cur && cur.role && cur.text) {
        out.push({ key: 'current', role: cur.role, id: cur.id, text: cur.text, final: false, seq: cur.seq });
      }
      return out;
    }

    return { consume: consume, snapshot: snapshot };
  }

  return {
    extractTextParts: extractTextParts,
    applyCaptionDelta: applyCaptionDelta,
    extractTranscriptText: extractTranscriptText,
    turnText: turnText,
    delegationText: delegationText,
    unwrapDataMessage: unwrapDataMessage,
    parseEventPayload: parseEventPayload,
    createCaptionAssembler: createCaptionAssembler
  };
}));
