'use strict';

// Run: node --test tests/js/
// Synthetic fixtures only: the conversation, handoff ids and payloads are invented.
const test = require('node:test');
const assert = require('node:assert/strict');
const V = require('../../static/voice-view.js');

function ev(seq, extra) {
  return Object.assign({
    seq,
    at: '2026-11-18T10:00:00Z',
    direction: 'upstream_to_client',
    type: 'turn.done',
    source: 'avas',
    payload: '{}',
    size: 2,
    truncated: false,
    delivery: 'observed'
  }, extra || {});
}

function fixtureStore() {
  const s = V.createStore();
  s.add([
    ev(1, { type: 'turn.created', payload: JSON.stringify({ type: 'turn.created', turn: { role: 'user', id: 'u1', transcript: 'Synthetic question' } }) }),
    ev(2, { type: 'delegation.created', payload: JSON.stringify({ type: 'delegation.created', item: { id: 'di_1', handoff_id: 'h_1', target: 'client', content: [{ type: 'input_text', text: 'Synthetic note' }] } }) }),
    ev(3, { direction: 'gateway_to_upstream', type: 'session.context.append', payload: JSON.stringify({ delegation_item_id: 'di_1' }) }),
    ev(4, { direction: 'gateway_to_upstream', type: 'delegation.context.append', payload: JSON.stringify({ delegation_item_id: 'di_1' }) }),
    ev(5, { type: 'session.usage.updated', payload: '{}' })
  ]);
  return s;
}

test('delegation count is the number of delegation.created events; writes are the rest of the group', () => {
  const c = V.delegationCounts(fixtureStore().list(), '');
  assert.deepEqual(c, { total: 3, created: 1, writes: 2 });
  assert.equal(V.delegationTooltip(c), '3 events: 1 created, 2 context writes');
});

test('delegation count respects the search text', () => {
  const list = fixtureStore().list();
  assert.deepEqual(V.delegationCounts(list, 'synthetic note'), { total: 1, created: 1, writes: 0 });
  assert.deepEqual(V.delegationCounts(list, 'nothing-matches'), { total: 0, created: 0, writes: 0 });
  assert.deepEqual(V.delegationCounts(list, 'di_1'), { total: 3, created: 1, writes: 2 });
});

test('delegation tooltip uses singular forms for one event', () => {
  assert.equal(V.delegationTooltip({ total: 1, created: 1, writes: 0 }), '1 event: 1 created, 0 context writes');
  assert.equal(V.delegationTooltip({ total: 0, created: 0, writes: 0 }), '0 events: 0 created, 0 context writes');
});

test('the delegation group count equals the events in the group', () => {
  const s = fixtureStore();
  assert.equal(s.counts('').delegation, V.delegationCounts(s.list(), '').total);
});

test('filter state is active only when a group is off, a type is chosen or search text is set', () => {
  const all = { groups: V.GROUPS.map(g => g.id), type: '', q: '' };
  assert.equal(V.filterIsActive(all), false);
  assert.equal(V.filterIsActive(Object.assign({}, all, { groups: ['conversation'] })), true);
  assert.equal(V.filterIsActive(Object.assign({}, all, { type: 'turn.done' })), true);
  assert.equal(V.filterIsActive(Object.assign({}, all, { q: '   ' })), false);
  assert.equal(V.filterIsActive(Object.assign({}, all, { q: 'turn' })), true);
  assert.equal(V.filterIsActive({}), false);
});

test('clearing the filter restores every group and removes type and search', () => {
  assert.deepEqual(V.clearedFilter(), { groups: V.GROUPS.map(g => g.id), type: '', q: '' });
});

test('filter summary reports shown of total only while a filter is active', () => {
  assert.equal(V.filterSummary(5, 9, { groups: V.GROUPS.map(g => g.id), type: '', q: '' }), '');
  assert.equal(V.filterSummary(2, 9, { groups: ['delegation'], type: '', q: '' }), 'Filtered: 2 of 9 events');
  assert.equal(V.filterSummary(1, 1, { groups: [], type: '', q: '' }), 'Filtered: 1 of 1 event');
});

test('compact event row formats time, arrow, type, size, delivery and seq', () => {
  const row = V.compactEventRow(ev(7, { type: 'turn.done', group: 'conversation', size: 1536, delivery: 'sent', at: '2026-11-18T10:01:02Z' }));
  assert.equal(row.seq, 7);
  assert.equal(row.arrow, '←');
  assert.equal(row.dirLabel, 'upstream → client');
  assert.equal(row.type, 'turn.done');
  assert.equal(row.size, '1.5 KB');
  assert.equal(row.delivery, 'sent');
  assert.equal(row.delegation, false);
  assert.match(row.time, /^\d{2}:\d{2}:\d{2}$/);
});

test('compact event row marks delegation rows and tolerates missing fields', () => {
  const row = V.compactEventRow({ group: 'delegation', type: 'delegation.created' });
  assert.equal(row.delegation, true);
  assert.equal(row.arrow, '?');
  assert.equal(row.time, '');
  assert.equal(row.size, '0 B');
  assert.equal(row.delivery, 'unknown');
});

test('creation row shows the creation label and an Instructions tag, keeping the raw type', () => {
  const row = V.compactEventRow(ev(1, { direction: 'gateway_creation', type: 'text', group: 'instructions' }));
  assert.equal(row.type, 'creation');
  assert.equal(row.rawType, 'text');
  assert.equal(row.tag, 'Instructions');
  assert.equal(row.dirLabel, 'creation');
});

test('non-creation rows keep their type and have no tag', () => {
  const row = V.compactEventRow(ev(2, { direction: 'upstream_to_client', type: 'text', group: 'conversation' }));
  assert.equal(row.type, 'text');
  assert.equal(row.rawType, 'text');
  assert.equal(row.tag, '');
  const turn = V.compactEventRow(ev(3, { type: 'turn.done', group: 'conversation' }));
  assert.equal(turn.type, 'turn.done');
  assert.equal(turn.tag, '');
});

test('formatClock returns HH:MM:SS for valid times and empty text otherwise', () => {
  assert.match(V.formatClock('2026-11-18T10:00:00Z'), /^\d{2}:\d{2}:\d{2}$/);
  assert.equal(V.formatClock('not a time'), '');
  assert.equal(V.formatClock(''), '');
});

test('formatBytes uses B, KB and MB', () => {
  assert.equal(V.formatBytes(0), '0 B');
  assert.equal(V.formatBytes(1023), '1023 B');
  assert.equal(V.formatBytes(2048), '2.0 KB');
  assert.equal(V.formatBytes(3 * 1048576), '3.0 MB');
  assert.equal(V.formatBytes('x'), '0 B');
});

test('middle ellipsis keeps both ends of a long id', () => {
  assert.equal(V.middleEllipsis('vs_short', 24), 'vs_short');
  const long = 'vs_0123456789abcdef0123456789abcdef';
  const out = V.middleEllipsis(long, 16);
  assert.equal(out.length, 16);
  assert.ok(out.startsWith('vs_0123'));
  assert.ok(out.endsWith('cdef'));
  assert.ok(out.indexOf('…') > 0);
});

test('transcript line keeps role, time and text; pending turns are marked', () => {
  const line = V.transcriptLine({ role: 'assistant', text: 'It is sunny.', final: false }, '2026-11-18T10:00:05Z');
  assert.equal(line.roleLabel, 'Assistant');
  assert.equal(line.text, 'It is sunny.');
  assert.equal(line.pending, true);
  const user = V.transcriptLine({ role: 'user', text: 'Weather?', final: true }, '');
  assert.equal(user.role, 'user');
  assert.equal(user.roleLabel, 'User');
  assert.equal(user.time, '');
  assert.equal(user.pending, false);
  assert.equal(V.transcriptLine(null, '').role, 'user');
});

test('creation details count instructions and initial items without reading them as markup', () => {
  const store = V.createStore();
  store.add([ev(1, {
    direction: 'gateway_creation',
    type: 'session.create',
    payload: JSON.stringify({ instructions: 'Be brief.', initial_items: [{ type: 'message', text: '<b>x</b>' }, { type: 'message' }, { type: 'message' }] })
  })]);
  const d = V.creationDetails(store.creation());
  assert.equal(d.hasInstructions, true);
  assert.equal(d.instructionChars, 'Be brief.'.length);
  assert.equal(d.items, 3);
  assert.ok(d.body.indexOf('instructions:\nBe brief.') === 0);
  assert.equal(V.creationSummary(d), 'instructions 9 chars · 3 initial items · ' + d.itemChars + ' chars');
});

test('creation summary handles one item, no items and unstructured payloads', () => {
  const one = V.creationDetails({ parsed: { instructions: 'x', initial_items: [{}] }, text: '' });
  assert.match(V.creationSummary(one), /1 initial item ·/);
  const none = V.creationDetails({ parsed: {}, text: '{}' });
  assert.equal(V.creationSummary(none), 'no instructions or items');
  const raw = V.creationDetails({ parsed: null, text: 'plain text here', truncated: true });
  assert.equal(V.creationSummary(raw), '15 chars (unstructured)');
  assert.match(raw.body, /\[payload truncated by gateway\]$/);
  assert.equal(V.creationDetails(null), null);
  assert.equal(V.creationSummary(null), '');
});

test('store warnings are empty for a complete log and name every gap source', () => {
  const clean = V.createStore();
  clean.add([ev(1), ev(2)]);
  assert.deepEqual(V.storeWarnings(clean, false), []);

  const broken = V.createStore();
  broken.add([ev(1), ev(2), ev(9), ev(10, { truncated: true, direction: 'upstream_to_client' })]);
  broken.setServerFlags(true, 3);
  broken.reportGap(20, 22, 'stream');
  const lines = V.storeWarnings(broken, true);
  assert.equal(lines[0], 'Incomplete log: this view is NOT a complete record.');
  assert.ok(lines.indexOf('3 event(s) were dropped by the gateway.') >= 0);
  assert.ok(lines.indexOf('Gap reported for seq 20–22.') >= 0);
  assert.ok(lines.some(l => /missing seq range/.test(l)));
  assert.ok(lines.some(l => /truncated/.test(l)));
  assert.ok(lines[lines.length - 1].indexOf('Loading stopped early') === 0);
});

test('frames URL encodes the ring and falls back to all for unknown rings', () => {
  assert.equal(V.framesUrl('vs_1', 2, 'audio'), '/api/call-sessions/vs_1/generations/2/frames?ring=audio');
  assert.equal(V.framesUrl('vs_1', 2, 'bogus'), '/api/call-sessions/vs_1/generations/2/frames?ring=all');
  assert.equal(V.framesUrl('vs_1', 2, undefined), '/api/call-sessions/vs_1/generations/2/frames?ring=all');
});

test('ring stats are sorted and tolerate absent fields', () => {
  assert.deepEqual(V.ringStats({ rings: { oversize: { size_frames: 1, size_bytes: 10, dropped: 0 }, audio: { size_frames: 4, size_bytes: 2048, dropped: 2 } } }), [
    { name: 'audio', frames: 4, bytes: 2048, dropped: 2 },
    { name: 'oversize', frames: 1, bytes: 10, dropped: 0 }
  ]);
  assert.deepEqual(V.ringStats(null), []);
  assert.deepEqual(V.ringStats({ rings: { conversation: null } }), [{ name: 'conversation', frames: 0, bytes: 0, dropped: 0 }]);
});

test('frame rows flag base64 and oversize frames and are never decoded', () => {
  const row = V.frameRow({ seq: 3, at: '2026-11-18T10:00:00Z', direction: 'client_to_upstream', ring: 'audio', length: 4096, encoding: 'base64', oversize: true, delivery: 'sent', truncated: false });
  assert.equal(row.base64, true);
  assert.equal(row.oversize, true);
  assert.equal(row.arrow, '→');
  assert.equal(V.framePayloadText(row, 'AAAA'), 'AAAA');
  assert.match(V.frameNote(row), /base64, shown as received, not decoded/);
  const text = V.frameRow({ seq: 4, encoding: 'utf8', length: 7 });
  assert.equal(V.framePayloadText(text, '{"a":1}'), '{\n  "a": 1\n}');
  assert.equal(V.frameRow(null).seq, 0);
});

test('audio facts are absent for old generations and readable for new ones', () => {
  assert.equal(V.audioFacts({ generation: 1 }), null);
  assert.equal(V.audioFacts(null), null);
  const facts = V.audioFacts({
    audio_events: 12,
    audio_bytes: 4096,
    audio: [{ direction: 'client_to_upstream', source: 'mic', type: 'binary', events: 10, bytes: 3000, max_bytes: 512 }, null],
    audio_samples: [{ direction: 'upstream_to_client', type: 'output_audio.delta', byte_length: 1024, paths: ['item.content[0]'] }, 'x']
  });
  assert.equal(facts.events, 12);
  assert.equal(facts.facts.length, 1);
  assert.equal(V.audioFactLine(facts.facts[0]), 'client_to_upstream · mic · binary: 10 events, 2.9 KB (max 512 B)');
  assert.equal(facts.samples.length, 1);
  assert.deepEqual(facts.samples[0].paths, ['item.content[0]']);
  assert.equal(V.audioSampleLine(facts.samples[0]), 'upstream_to_client · output_audio.delta · 1.0 KB');
});

test('a generation with ended_at is never active, even when the summary says active', () => {
  assert.equal(V.summaryIsActive({ active: true, ended_at: '' }), true);
  assert.equal(V.summaryIsActive({ active: true, ended_at: '2026-11-18T10:00:30Z' }), false);
  assert.equal(V.summaryIsActive({ active: false, ended_at: '' }), false);
  assert.equal(V.summaryIsActive(null), false);
});

test('picker state follows the live flag for the open generation and the summary for the others', () => {
  const open = { generation: 2, active: true, ended_at: '' };
  assert.equal(V.generationStateLabel(open, true, true), 'active');
  assert.equal(V.generationStateLabel(open, true, false), 'ended');
  assert.equal(V.generationStateLabel({ generation: 1, active: true, ended_at: '' }, false, false), 'active');
  assert.equal(V.generationStateLabel({ generation: 1, active: false, ended_at: '2026-11-18T10:00:10Z' }, false, true), 'ended');
});

test('frames are grouped by ring in a fixed order with unknown rings last', () => {
  const groups = V.frameGroups([
    { seq: 1, ring: 'audio' },
    { seq: 2, ring: 'conversation' },
    { seq: 3, ring: 'zeta' },
    { seq: 4, ring: 'audio' },
    { seq: 5, ring: 'oversize' }
  ]);
  assert.deepEqual(groups.map(g => g.ring), ['conversation', 'audio', 'oversize', 'zeta']);
  assert.deepEqual(groups[1].frames.map(f => f.seq), [1, 4]);
  assert.deepEqual(V.frameGroups(undefined), []);
});

test('frame delivery and encoding are separate: base64 is an encoding tag, not a delivery value', () => {
  const row = V.frameRow({ seq: 1, ring: 'audio', delivery: 'sent', encoding: 'base64', length: 4096 });
  assert.deepEqual(V.frameDisplay(row), { delivery: 'sent', encoding: 'base64', base64: true });
  const text = V.frameRow({ seq: 2, ring: 'conversation', delivery: 'observed', encoding: 'utf8' });
  assert.deepEqual(V.frameDisplay(text), { delivery: 'observed', encoding: 'utf8', base64: false });
  assert.equal(V.frameDisplay(V.frameRow({ seq: 3 })).delivery, 'unknown');
});

test('dropped events without a gap get one compact line; a real gap gets the full banner', () => {
  const dropOnly = V.createStore();
  dropOnly.add([ev(1), ev(2), ev(3)]);
  dropOnly.setServerFlags(false, 2);
  const compact = V.noticeFor(dropOnly, false, '');
  assert.equal(compact.full, false);
  assert.equal(compact.lines.length, 1);
  assert.equal(compact.lines[0], 'Incomplete log: 2 event(s) dropped by the gateway.');
  assert.equal(dropOnly.hasGap(), false);

  const gap = V.createStore();
  gap.add([ev(1), ev(2), ev(5)]);
  gap.setServerFlags(false, 2);
  const full = V.noticeFor(gap, false, '');
  assert.equal(full.full, true);
  assert.equal(full.kind, 'warn');
  assert.equal(gap.hasGap(), true);
  assert.equal(full.lines[0], 'Incomplete log: this view is NOT a complete record.');
});

test('a clean log shows no notice and an error is always the full banner', () => {
  const clean = V.createStore();
  clean.add([ev(1), ev(2)]);
  assert.equal(V.noticeFor(clean, false, '').kind, 'none');
  const err = V.noticeFor(clean, false, 'Session not found.');
  assert.equal(err.kind, 'error');
  assert.equal(err.full, true);
  assert.equal(err.lines[0], 'Session not found.');
});

test('the real backend numbers events from seq 1; seq 0 is accepted but not required', () => {
  const s = V.createStore();
  s.add([ev(1), ev(2), ev(3)]);
  assert.equal(s.gaps().missing.length, 0);
  assert.equal(s.incomplete(), false);
  assert.equal(s.maxSeq(), 3);
});

test('picker label uses the live count and state for the open generation', () => {
  const g = { generation: 2, active: false, ended_at: '2026-11-18T10:00:30Z', event_count: 25 };
  assert.equal(V.generationOptionLabel(g, { open: true, active: true, count: 27 }), 'Gen 2 · active · 27 events');
  assert.equal(V.generationOptionLabel(g, { open: true, active: false, count: 27 }), 'Gen 2 · ended · 27 events');
  assert.equal(V.generationOptionLabel(g, { open: true, active: true, count: 1 }), 'Gen 2 · active · 1 event');
});

test('picker label for a closed generation uses the summary count and ended_at', () => {
  const ended = { generation: 1, active: true, ended_at: '2026-11-18T10:00:10Z', event_count: 6 };
  assert.equal(V.generationOptionLabel(ended, null), 'Gen 1 · ended · 6 events');
  const running = { generation: 3, active: true, ended_at: '', event_count: 0 };
  assert.equal(V.generationOptionLabel(running, null), 'Gen 3 · active · 0 events');
  assert.equal(V.generationOptionLabel({ generation: 4 }, null), 'Gen 4 · ended · 0 events');
});

test('audio facts tolerate partial shapes', () => {
  const facts = V.audioFacts({ audio_events: 'x', audio: 'not a list' });
  assert.equal(facts.events, 0);
  assert.deepEqual(facts.facts, []);
  assert.deepEqual(facts.samples, []);
  assert.equal(V.audioFactLine({ direction: '', source: '', type: '', events: 1, bytes: 0, maxBytes: 0 }), ': 1 event, 0 B');
});
