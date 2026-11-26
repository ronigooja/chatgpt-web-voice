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
    ev(3, { direction: 'gateway_to_upstream', type: 'session.context.append', source_kind: 'task_result', payload: JSON.stringify({ delegation_item_id: 'di_1' }) }),
    ev(4, { direction: 'gateway_to_upstream', type: 'delegation.context.append', source_kind: 'user_text', payload: JSON.stringify({ delegation_item_id: 'di_1' }) }),
    ev(5, { type: 'session.usage.updated', payload: '{}' })
  ]);
  return s;
}

test('delegation count is the number of delegation.created events; context writes are not delegations', () => {
  const c = V.delegationCounts(fixtureStore().list(), '');
  assert.deepEqual(c, { created: 1 });
  assert.equal(V.delegationTooltip(c), '1 delegation created');
});

test('delegation count respects the search text', () => {
  const list = fixtureStore().list();
  assert.deepEqual(V.delegationCounts(list, 'synthetic note'), { created: 1 });
  assert.deepEqual(V.delegationCounts(list, 'nothing-matches'), { created: 0 });
  assert.deepEqual(V.delegationCounts(list, 'di_1'), { created: 1 });
});

test('delegation tooltip uses plural forms unless there is exactly one', () => {
  assert.equal(V.delegationTooltip({ created: 1 }), '1 delegation created');
  assert.equal(V.delegationTooltip({ created: 0 }), '0 delegations created');
  assert.equal(V.delegationTooltip({ created: 2 }), '2 delegations created');
});

test('the delegation group count equals the delegation.created count', () => {
  const s = fixtureStore();
  assert.equal(s.counts('').delegation, V.delegationCounts(s.list(), '').created);
  assert.equal(s.counts('').delegation, 1);
  assert.equal(s.counts('').client, 1);
  assert.equal(s.counts('').context, 1);
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

test('compact event row carries source_kind, empty when absent', () => {
  const sourced = V.compactEventRow(ev(8, { direction: 'gateway_to_upstream', type: 'session.context.append', group: 'context', source_kind: 'ui_feedback' }));
  assert.equal(sourced.sourceKind, 'ui_feedback');
  assert.equal(V.compactEventRow(ev(9, { type: 'turn.done', group: 'conversation' })).sourceKind, '');
});

test('compact event row marks delegation rows and tolerates missing fields', () => {
  const row = V.compactEventRow({ group: 'delegation', type: 'delegation.created' });
  assert.equal(row.delegation, true);
  assert.equal(row.arrow, '?');
  assert.equal(row.time, '');
  assert.equal(row.size, '0 B');
  assert.equal(row.delivery, 'unknown');
});

test('creation row shows the creation label and a Creation tag, keeping the raw type', () => {
  const row = V.compactEventRow(ev(1, { direction: 'gateway_creation', type: 'text', group: 'creation' }));
  assert.equal(row.type, 'creation');
  assert.equal(row.rawType, 'text');
  assert.equal(row.tag, 'Creation');
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
  assert.ok(d.body.indexOf('initial_items:\n') > 0);
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

function anchorFixture() {
  const s = V.createStore();
  s.add([
    ev(1, { direction: 'gateway_creation', type: 'session.create', payload: JSON.stringify({ instructions: 'Line one.\nLine two.', initial_items: [{ type: 'message', content: [{ type: 'input_text', text: 'Hi\nthere' }] }] }) }),
    ev(2, { type: 'turn.created', payload: JSON.stringify({ type: 'turn.created', turn: { role: 'user', id: 'u1', transcript: 'hello' } }) }),
    ev(3, { direction: 'client_to_upstream', type: 'text', payload: '{"text":"hi"}' }),
    ev(4, { type: 'delegation.created', payload: JSON.stringify({ type: 'delegation.created', item: { id: 'di_1', handoff_id: 'h_1', target: 'client', content: [] } }) }),
    ev(5, { direction: 'gateway_to_upstream', type: 'session.context.append', source_kind: 'task_result', payload: JSON.stringify({ delegation_item_id: 'di_1' }) }),
    ev(6, { direction: 'gateway_to_upstream', type: 'session.context.append', payload: JSON.stringify({ content: [] }) }),
    ev(7, { type: 'turn.done', payload: JSON.stringify({ type: 'turn.done', turn: { role: 'assistant', id: 'a1', transcript: 'ok' } }) }),
    ev(8, { direction: 'gateway_to_upstream', type: 'delegation.context.append', source_kind: 'user_text', payload: JSON.stringify({ channel: 'speakable', content: [] }) }),
    ev(9, { type: 'session.context.appended', payload: '{}' }),
    ev(10, { type: 'session.usage.updated', payload: '{}' })
  ]);
  return s;
}

test('anchor rows cover creation, delegation.created, source-labelled context writes and client messages in seq order', () => {
  const rows = V.anchorRows(anchorFixture().list());
  assert.deepEqual(rows.map(r => r.seq), [1, 3, 4, 5, 6, 8]);
  assert.deepEqual(rows.map(r => r.kind), ['creation', 'client', 'delegation', 'context', 'context', 'client']);
  assert.equal(rows[1].label, 'Client message · text');
  assert.equal(rows[2].label, 'Delegation · target client · handoff h_1');
  assert.equal(rows[3].label, 'Context write · task_result');
  assert.equal(rows[4].label, 'Context write · session.context.append');
  assert.equal(rows[5].label, 'Client message · user_text');
});

test('anchor tags show speakable and delegation links for display only', () => {
  const rows = V.anchorRows(anchorFixture().list());
  assert.deepEqual(rows[0].tags, []);
  assert.deepEqual(rows[3].tags, ['delegation']);
  assert.deepEqual(rows[4].tags, []);
  assert.deepEqual(rows[5].tags, ['speakable']);
  assert.deepEqual(V.writeTags('not json'), []);
  assert.deepEqual(V.writeTags('null'), []);
  assert.deepEqual(V.writeTags(JSON.stringify({ channel: 'speakable', delegation_item_id: 'd' })), ['speakable', 'delegation']);
});

test('acknowledgements, usage, session and conversation events get no anchor', () => {
  const seqs = V.anchorRows(anchorFixture().list()).map(r => r.seq);
  [2, 7, 9, 10].forEach(seq => assert.equal(seqs.indexOf(seq), -1, 'seq ' + seq));
});

test('a context write from the gateway gets exactly one anchor, labelled by source', () => {
  const rows = V.anchorRows(anchorFixture().list());
  assert.equal(rows.filter(r => r.seq === 5).length, 1);
  assert.equal(rows.filter(r => r.seq === 5)[0].kind, 'context');
});

test('the creation anchor is labelled with the creation summary', () => {
  const store = anchorFixture();
  const rows = V.anchorRows(store.list());
  assert.equal(rows[0].label, 'Creation · ' + V.creationSummary(V.creationDetails(store.creation())));
  assert.match(rows[0].label, /^Creation · instructions 19 chars · 1 initial item · \d+ chars$/);
  assert.equal(rows[0].at, '2026-11-18T10:00:00Z');
  assert.equal(V.anchorRows([]).length, 0);
});

test('the Delegation group holds only delegation.created and gets exactly one anchor', () => {
  const list = anchorFixture().list();
  assert.equal(list.filter(e => e.group === 'delegation').length, 1);
  const rows = V.anchorRows(list);
  assert.equal(rows.filter(r => r.kind === 'delegation').length, 1);
  assert.equal(rows.filter(r => r.seq === 4).length, 1);
});

test('anchors are placed between turns by seq and the creation anchor stays first', () => {
  const items = [{ seq: 2, role: 'user', text: 'hello', final: true }, { seq: 7, role: 'assistant', text: 'ok', final: true }];
  const entries = V.transcriptEntries(items, V.anchorRows(anchorFixture().list()));
  assert.deepEqual(entries.map(e => e.anchor ? e.anchor.kind : 'turn'),
    ['creation', 'turn', 'client', 'delegation', 'context', 'context', 'turn', 'client']);
  assert.equal(entries[1].turn.seq, 2);
  assert.deepEqual(V.transcriptEntries([], []), []);
});

test('creation details show instructions and initial item strings with real line breaks', () => {
  const store = V.createStore();
  store.add([ev(1, {
    direction: 'gateway_creation',
    type: 'session.create',
    payload: JSON.stringify({ instructions: 'Be brief.\nNo filler.', initial_items: [{ type: 'message', content: [{ type: 'input_text', text: 'Line A\nLine B' }] }] })
  })]);
  const d = V.creationDetails(store.creation());
  assert.ok(d.body.indexOf('instructions:\nBe brief.\nNo filler.') === 0);
  assert.ok(d.body.indexOf('"text": "Line A\n') > 0);
  assert.ok(d.body.indexOf('Line B"') > 0);
  assert.equal(d.body.indexOf('\\n'), -1);
});

test('readableJSON breaks multi-line strings and keeps other JSON unchanged', () => {
  assert.equal(V.readableJSON({ a: 'x\ny' }), '{\n  "a": "x\n    y"\n}');
  assert.equal(V.readableJSON(['a\nb']), '[\n  "a\n    b"\n]');
  assert.equal(V.readableJSON({ n: 1, s: 'plain', q: 'say "hi"' }), JSON.stringify({ n: 1, s: 'plain', q: 'say "hi"' }, null, 2));
  assert.equal(V.readableJSON(undefined), '');
});

test('the truncation note stays on the creation body', () => {
  const d = V.creationDetails({ parsed: { instructions: 'a\nb' }, text: '', truncated: true });
  assert.match(d.body, /\[payload truncated by gateway\]$/);
});
