'use strict';

// Run: node --test tests/js/
const test = require('node:test');
const assert = require('node:assert/strict');
const V = require('../../static/voice-view.js');

function ev(seq, extra) {
  return Object.assign({
    seq,
    at: '2026-11-18T10:00:0' + (seq % 10) + 'Z',
    direction: 'upstream_to_client',
    type: 'turn.done',
    source: 'avas',
    payload: '{}',
    size: 2,
    truncated: false,
    delivery: 'observed'
  }, extra || {});
}

test('store orders events by seq and dedupes repeated seqs', () => {
  const s = V.createStore();
  assert.equal(s.add([ev(3), ev(1), ev(2)]), 3);
  assert.equal(s.add([ev(2), ev(3, { type: 'changed' })]), 0);
  assert.deepEqual(s.list().map(e => e.seq), [1, 2, 3]);
  assert.equal(s.list()[2].type, 'turn.done');
  assert.equal(s.maxSeq(), 3);
});

test('seq 0 is a valid event seq; negative and non-numeric seqs are rejected', () => {
  const s = V.createStore();
  s.add([ev(0), ev(-1), ev('x'), ev(1.5), ev(1)]);
  assert.deepEqual(s.list().map(e => e.seq), [0, 1]);
});

test('malformed entries do not throw', () => {
  const s = V.createStore();
  assert.equal(s.add([null, 'x', 7, ev(1)]), 1);
  assert.equal(s.size(), 1);
});

test('local gaps between loaded seqs are detected and make the log incomplete', () => {
  const s = V.createStore();
  s.add([ev(1), ev(2), ev(5), ev(6), ev(9)]);
  assert.deepEqual(s.gaps().missing, [{ from: 3, to: 4 }, { from: 7, to: 8 }]);
  assert.equal(s.incomplete(), true);
});

test('gateway gap flags and SSE gap events mark the log incomplete', () => {
  const flagged = V.createStore();
  flagged.add([ev(1), ev(2)]);
  flagged.setServerFlags(true, 0);
  assert.equal(flagged.incomplete(), true);

  const dropped = V.createStore();
  dropped.add([ev(1)]);
  dropped.setServerFlags(false, 4);
  assert.equal(dropped.dropped(), 4);
  assert.equal(dropped.incomplete(), true);

  const streamed = V.createStore();
  streamed.add([ev(1), ev(2)]);
  streamed.reportGap(3, 7, 'stream');
  assert.equal(streamed.incomplete(), true);
  assert.deepEqual(streamed.gaps().reported, [{ from: 3, to: 7, source: 'stream' }]);
});

test('a complete log with no flags is not marked incomplete', () => {
  const s = V.createStore();
  s.add([ev(1), ev(2), ev(3)]);
  s.setServerFlags(false, 0);
  assert.equal(s.incomplete(), false);
});

test('truncated upstream events are counted and skipped for captions', () => {
  const s = V.createStore();
  s.add([
    ev(1, { type: 'turn.created', payload: JSON.stringify({ type: 'turn.created', turn: { role: 'assistant', id: 'a' } }) }),
    ev(2, { type: 'turn.done', truncated: true, payload: '{"type":"turn.done","turn":{"role":"assistant","id":"a","transcript":"cut' }),
  ]);
  assert.equal(s.truncatedUpstream(), 1);
  assert.equal(s.captions().length, 0);
});

test('captions come from upstream events in seq order, wrapped or native', () => {
  const s = V.createStore();
  s.add([
    ev(2, { type: 'turn.done', payload: JSON.stringify({ type: 'data_message', data: JSON.stringify({ type: 'turn.done', turn: { role: 'assistant', id: 'a1', transcript: 'Hi!' } }) }) }),
    ev(1, { type: 'turn.created', payload: JSON.stringify({ type: 'turn.created', turn: { role: 'user', id: 'u1', transcript: 'hello' } }) }),
    ev(3, { direction: 'client_to_upstream', payload: JSON.stringify({ type: 'turn.done', turn: { role: 'user', id: 'x', transcript: 'ignored' } }) }),
  ]);
  const caps = s.captions();
  assert.deepEqual(caps.map(c => [c.role, c.text]), [['user', 'hello'], ['assistant', 'Hi!']]);
});

test('unknown events remain in the raw list and are not dropped', () => {
  const s = V.createStore();
  s.add([ev(1, { type: 'mystery.event', payload: '{"x":1}' }), ev(2, { type: 'turn.done', payload: '{}' })]);
  assert.equal(s.list().length, 2);
  assert.deepEqual(s.types(), ['mystery.event', 'turn.done']);
  assert.equal(s.filtered({ type: 'mystery.event' }).length, 1);
  assert.equal(s.filtered({ direction: 'gateway_creation' }).length, 0);
});

test('creation event is exposed with its parsed payload', () => {
  const s = V.createStore();
  s.add([
    ev(1, { direction: 'gateway_creation', type: 'session.create', payload: JSON.stringify({ instructions: 'Be brief.', initial_items: [{ type: 'message' }] }) }),
  ]);
  const c = s.creation();
  assert.equal(c.seq, 1);
  assert.equal(c.parsed.instructions, 'Be brief.');
  assert.equal(V.createStore().creation(), null);
});

test('payload strings are kept as text and objects are serialized', () => {
  const s = V.createStore();
  s.add([ev(1, { payload: '{"a":1}' }), ev(2, { payload: { b: 2 } })]);
  assert.equal(s.list()[0].payload, '{"a":1}');
  assert.equal(s.list()[1].payload, '{"b":2}');
  assert.equal(V.prettyPayload('{"a":1}'), '{\n  "a": 1\n}');
  assert.equal(V.prettyPayload('{"a":'), '{"a":');
});

test('filtered view copies raw events, not normalized ones', () => {
  const s = V.createStore();
  const raw = ev(1, { delivery: 'sent', extra_field: 'kept' });
  s.add([raw]);
  assert.equal(s.filtered({}).length, 1);
  assert.equal(s.list()[0].raw.extra_field, 'kept');
});

test('chooseGeneration: explicit gen, active default, newest fallback, not found, empty', () => {
  const gens = [
    { generation: 1, zoe_session_id: 'z1', active: false },
    { generation: 2, zoe_session_id: 'z2', active: true },
    { generation: 3, zoe_session_id: 'z3', active: false },
  ];
  assert.equal(V.chooseGeneration(gens, 0).gen, 2);
  assert.equal(V.chooseGeneration(gens, 1).gen, 1);
  assert.match(V.chooseGeneration(gens, 9).error, /not found/);
  const ended = gens.map(g => Object.assign({}, g, { active: false }));
  assert.equal(V.chooseGeneration(ended, 0).gen, 3);
  assert.match(V.chooseGeneration([], 0).error, /No generations/);
  assert.match(V.chooseGeneration(undefined, 0).error, /No generations/);
});

test('parseGeneration accepts absent values and rejects invalid ones', () => {
  assert.deepEqual(V.parseGeneration(null), { value: 0 });
  assert.deepEqual(V.parseGeneration(''), { value: 0 });
  assert.deepEqual(V.parseGeneration('4'), { value: 4 });
  assert.ok(V.parseGeneration('0').error);
  assert.ok(V.parseGeneration('-2').error);
  assert.ok(V.parseGeneration('abc').error);
  assert.ok(V.parseGeneration('1.5').error);
});

test('URLs encode ids and use the documented paths', () => {
  assert.equal(V.generationsUrl('vs_a/b'), '/api/call-sessions/vs_a%2Fb/generations');
  assert.equal(V.eventsUrl('vs_1', 2, 10, 500), '/api/call-sessions/vs_1/generations/2/events?after=10&limit=500');
  assert.equal(V.streamUrl('vs_1', 2, 7), '/api/call-sessions/vs_1/generations/2/events/stream?after=7');
  assert.equal(V.viewerUrl('vs_1', 2), '/voice?view=vs_1&gen=2');
  assert.equal(V.viewerUrl('vs_1', 0), '/voice?view=vs_1');
});
