'use strict';

// Run: node --test tests/js/
// All payloads are synthetic and only mirror the key structure of live events.
const test = require('node:test');
const assert = require('node:assert/strict');
const V = require('../../static/voice-view.js');

function raw(type, extra, payload) {
  return Object.assign({
    seq: 1,
    at: '2026-11-18T10:00:00Z',
    direction: 'upstream_to_client',
    type,
    source: 'avas',
    payload: payload === undefined ? '{}' : payload,
    size: 2,
    truncated: false,
    delivery: 'observed'
  }, extra || {});
}

function wrap(inner) {
  return JSON.stringify({ type: 'data_message', data: JSON.stringify(inner) });
}

function groupOfRaw(extra, payload) {
  return V.classifyEvent(raw(extra.type, extra, payload)).group;
}

test('every live inner type maps to the documented group', () => {
  const cases = [
    ['output_transcript.added', 'upstream_to_client', 'conversation'],
    ['input_transcript.added', 'upstream_to_client', 'conversation'],
    ['turn.created', 'upstream_to_client', 'conversation'],
    ['turn.delta', 'upstream_to_client', 'conversation'],
    ['turn.done', 'upstream_to_client', 'conversation'],
    ['chat_message_delta', 'upstream_to_client', 'conversation'],
    ['output_text.delta', 'upstream_to_client', 'conversation'],
    ['output_audio_transcript.delta', 'upstream_to_client', 'conversation'],
    ['delegation.created', 'upstream_to_client', 'delegation'],
    ['delegation.context.append', 'gateway_to_upstream', 'context'],
    ['delegation.context.appended', 'upstream_to_client', 'context'],
    ['session.context.append', 'gateway_to_upstream', 'context'],
    ['session.context.appended', 'upstream_to_client', 'context'],
    ['session.started', 'upstream_to_client', 'session'],
    ['session.usage.updated', 'upstream_to_client', 'usage'],
    ['session.something_else', 'upstream_to_client', 'session'],
    ['session.create', 'gateway_creation', 'creation'],
    ['text', 'client_to_upstream', 'client'],
    ['binary', 'client_to_upstream', 'client'],
    ['mystery.event', 'upstream_to_client', 'other'],
    ['', 'upstream_to_client', 'other']
  ];
  cases.forEach(([type, direction, group]) => {
    const got = groupOfRaw({ type, direction });
    assert.equal(got, group, type + ' ' + direction);
  });
});

test('context writes are grouped by source_kind: user messages are Client, everything else is Context', () => {
  const write = { type: 'session.context.append', direction: 'gateway_to_upstream' };
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'user_text' }, write)), 'client');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'ui_input_received' }, write)), 'client');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'ui_feedback' }, write)), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'task_result' }, write)), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'fast_reply' }, write)), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'ui_input_request' }, write)), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'plan_note' }, write)), 'context');
  assert.equal(groupOfRaw(write), 'context');
});

test('delegation_item_id and payload text never decide the group of a context write', () => {
  const payload = JSON.stringify({ delegation_item_id: 'di_1', content: [{ type: 'input_text', text: 'Hi' }] });
  const write = { type: 'delegation.context.append', direction: 'gateway_to_upstream' };
  assert.equal(groupOfRaw(write, payload), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'user_text' }, write), payload), 'client');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'task_result' }, write), payload), 'context');
  assert.equal(groupOfRaw({ type: 'session.context.append', direction: 'gateway_to_upstream' }, '{"text":"user_text"}'), 'context');
});

test('delegation.created is the only delegation group member', () => {
  assert.equal(groupOfRaw({ type: 'delegation.created', direction: 'upstream_to_client' }), 'delegation');
  assert.equal(groupOfRaw({ type: 'delegation.created', direction: 'gateway_to_upstream' }), 'delegation');
  assert.equal(groupOfRaw({ type: 'delegation.context.appended', direction: 'upstream_to_client' }), 'context');
});

test('appended acknowledgements go to Context writes even when they carry a source_kind', () => {
  assert.equal(groupOfRaw({ type: 'session.context.appended', direction: 'upstream_to_client' }), 'context');
  assert.equal(groupOfRaw(Object.assign({ source_kind: 'user_text' }, { type: 'session.context.appended', direction: 'upstream_to_client' })), 'context');
});

test('client messages are the client_to_upstream direction unless they are context writes', () => {
  assert.equal(groupOfRaw({ type: 'text', direction: 'client_to_upstream' }), 'client');
  assert.equal(groupOfRaw({ type: 'session.context.appended', direction: 'client_to_upstream' }), 'context');
});

test('a gateway frame that matches no rule goes to Other, with no context fallback', () => {
  assert.equal(groupOfRaw({ type: 'mystery.frame', direction: 'gateway_to_upstream' }), 'other');
  assert.equal(groupOfRaw({ type: 'response.create', direction: 'gateway_to_upstream' }), 'other');
});

test('the store keeps source_kind on events and uses an empty string when it is absent', () => {
  const s = V.createStore();
  s.add([
    raw('session.context.append', { seq: 1, direction: 'gateway_to_upstream', source_kind: 'ui_feedback' }, '{}'),
    raw('session.context.append', { seq: 2, direction: 'gateway_to_upstream' }, '{}')
  ]);
  assert.equal(s.get(1).source_kind, 'ui_feedback');
  assert.equal(s.get(2).source_kind, '');
  assert.equal(s.get(1).group, 'context');
});

test('a wrapped data_message resolves to its inner type and group', () => {
  const c = V.classifyEvent(raw('data_message', {}, wrap({ type: 'delegation.created', item: { id: 'di_1' } })));
  assert.deepEqual(c, { type: 'delegation.created', group: 'delegation' });
  const t = V.classifyEvent(raw('data_message', {}, wrap({ type: 'turn.done', turn: { role: 'assistant' } })));
  assert.deepEqual(t, { type: 'turn.done', group: 'conversation' });
});

test('a data_message with no readable inner type stays as data_message in Other', () => {
  assert.deepEqual(V.classifyEvent(raw('data_message', {}, 'not json')), { type: 'data_message', group: 'other' });
  assert.deepEqual(V.classifyEvent(raw('data_message', {}, JSON.stringify({ data: 'x' }))), { type: 'data_message', group: 'other' });
});

test('an already-inner type is kept as-is even when the payload carries another type', () => {
  const c = V.classifyEvent(raw('turn.done', {}, wrap({ type: 'turn.created' })));
  assert.deepEqual(c, { type: 'turn.done', group: 'conversation' });
});

test('classification tolerates missing or non-string payloads', () => {
  assert.equal(V.classifyEvent(raw('turn.done', {}, null)).group, 'conversation');
  assert.equal(V.classifyEvent(raw('session.context.append', { direction: 'gateway_to_upstream' }, { delegation_item_id: 'x' })).group, 'context');
  assert.equal(V.classifyEvent(null).group, 'other');
});

test('counts sum to the number of events and respect the search text', () => {
  const s = V.createStore();
  s.add([
    raw('turn.done', { seq: 1 }, JSON.stringify({ turn: { transcript: 'Hello Tokyo' } })),
    raw('turn.done', { seq: 2 }, JSON.stringify({ turn: { transcript: 'Bye' } })),
    raw('delegation.created', { seq: 3 }, JSON.stringify({ item: { target: 'client', content: [{ type: 'input_text', text: 'Book a table' }] } })),
    raw('session.usage.updated', { seq: 4 }),
    raw('session.create', { seq: 5, direction: 'gateway_creation' }, '{"instructions":"Be brief."}'),
    raw('session.context.append', { seq: 6, direction: 'gateway_to_upstream' }, '{}'),
    raw('text', { seq: 7, direction: 'client_to_upstream' }, '{"text":"hi"}'),
    raw('mystery.event', { seq: 8 })
  ]);
  const all = s.counts('');
  assert.deepEqual(all, { conversation: 2, creation: 1, delegation: 1, client: 1, context: 1, usage: 1, session: 0, other: 1 });
  const total = Object.keys(all).reduce((n, k) => n + all[k], 0);
  assert.equal(total, s.size());

  assert.equal(s.counts('tokyo').conversation, 1);
  assert.equal(s.counts('tokyo').delegation, 0);
  assert.equal(s.counts('TABLE').delegation, 1);
});

test('counts place user and context writes in their own groups by source_kind', () => {
  const s = V.createStore();
  s.add([
    raw('session.context.append', { seq: 1, direction: 'gateway_to_upstream', source_kind: 'user_text' }, '{}'),
    raw('session.context.append', { seq: 2, direction: 'gateway_to_upstream', source_kind: 'ui_input_received' }, '{}'),
    raw('session.context.append', { seq: 3, direction: 'gateway_to_upstream', source_kind: 'ui_feedback' }, '{}'),
    raw('session.context.appended', { seq: 4, direction: 'upstream_to_client' }, '{}'),
    raw('delegation.context.append', { seq: 5, direction: 'gateway_to_upstream', source_kind: 'task_result' }, JSON.stringify({ delegation_item_id: 'di_1' })),
    raw('turn.done', { seq: 6 }, '{}')
  ]);
  assert.deepEqual(s.counts(''), { conversation: 1, creation: 0, delegation: 0, client: 2, context: 3, usage: 0, session: 0, other: 0 });
});

test('presets select exactly the documented groups', () => {
  assert.deepEqual(V.presetGroups('conversation'), ['conversation']);
  assert.deepEqual(V.presetGroups('client'), ['client']);
  assert.deepEqual(V.presetGroups('delegation'), ['delegation']);
  assert.deepEqual(V.presetGroups('everything'), ['conversation', 'creation', 'delegation', 'client', 'context', 'usage', 'session', 'other']);
  assert.equal(V.presetGroups('context'), null);
  assert.equal(V.presetGroups('nope'), null);
  assert.equal(V.presetOf(['conversation']), 'conversation');
  assert.equal(V.presetOf(['client']), 'client');
  assert.equal(V.presetOf(['other', 'session', 'usage', 'context', 'client', 'delegation', 'creation', 'conversation']), 'everything');
  assert.equal(V.presetOf(['conversation', 'delegation']), '');
  assert.equal(V.presetOf(undefined), '');
});

test('client and delegation groups round-trip through the groups parameter in GROUPS order', () => {
  const f = { groups: ['client', 'delegation'], type: '', q: '' };
  assert.equal(V.viewerUrl('vs_1', 2, f), '/viewer?session=vs_1&gen=2&groups=delegation%2Cclient');
  assert.deepEqual(V.filterFromParams(new URLSearchParams('groups=delegation,client')).groups, ['delegation', 'client']);
  assert.deepEqual(V.filterFromParams(new URLSearchParams('groups=session,usage,context')).groups, ['context', 'usage', 'session']);
});

test('search matches source_kind as well as type and payload', () => {
  const s = V.createStore();
  s.add([raw('session.context.append', { seq: 1, direction: 'gateway_to_upstream', source_kind: 'plan_note' }, '{}')]);
  assert.equal(V.searchMatches(s.get(1), 'PLAN_NOTE'), true);
  assert.equal(s.filtered({ q: 'plan_note' }).length, 1);
  assert.equal(s.counts('plan_note').context, 1);
  assert.equal(s.counts('plan_note').client, 0);
});

test('search is case-insensitive over type and payload text and never parses for execution', () => {
  const evt = { type: 'turn.done', direction: 'upstream_to_client', source: 'avas', payload: '{"x":"<img src=x onerror=alert(1)>"}', group: 'conversation' };
  assert.equal(V.searchMatches(evt, 'TURN.DONE'), true);
  assert.equal(V.searchMatches(evt, 'ONERROR'), true);
  assert.equal(V.searchMatches(evt, '  '), true);
  assert.equal(V.searchMatches(evt, 'zebra'), false);
  assert.equal(V.searchMatches(evt, undefined), true);
});

test('group toggles filter events and the search text narrows them further', () => {
  const s = V.createStore();
  s.add([
    raw('turn.done', { seq: 1 }, JSON.stringify({ turn: { transcript: 'apples' } })),
    raw('turn.done', { seq: 2 }, JSON.stringify({ turn: { transcript: 'pears' } })),
    raw('delegation.created', { seq: 3 }),
    raw('session.context.append', { seq: 4, direction: 'gateway_to_upstream', source_kind: 'user_text' }, '{}')
  ]);
  assert.equal(s.filtered({ groups: ['conversation'] }).length, 2);
  assert.equal(s.filtered({ groups: ['conversation'], q: 'pears' }).length, 1);
  assert.equal(s.filtered({ groups: ['delegation'] }).length, 1);
  assert.equal(s.filtered({ groups: [] }).length, 0);
  assert.equal(s.filtered({ groups: ['client'] }).length, 1);
  assert.equal(s.filtered({}).length, 4);
});

test('type filter is applied on the inner type', () => {
  const s = V.createStore();
  s.add([raw('data_message', { seq: 1 }, wrap({ type: 'turn.done' })), raw('turn.delta', { seq: 2 })]);
  assert.equal(s.filtered({ type: 'turn.done' }).length, 1);
  assert.deepEqual(s.types(), ['turn.delta', 'turn.done']);
});

test('filter state round-trips through URL parameters', () => {
  const f = { groups: ['conversation', 'delegation'], type: 'turn.done', q: 'a b' };
  const qs = new URLSearchParams(V.filterParams(f));
  assert.equal(qs.get('groups'), 'conversation,delegation');
  assert.equal(qs.get('q'), 'a b');
  assert.deepEqual(V.filterFromParams(qs), { groups: ['conversation', 'delegation'], type: 'turn.done', q: 'a b' });
  assert.equal(V.viewerUrl('vs_1', 2, f), '/viewer?session=vs_1&gen=2&groups=conversation%2Cdelegation&type=turn.done&q=a%20b');
});

test('the creation group is named creation in ids, labels, presets and URLs', () => {
  assert.deepEqual(V.GROUPS.find(g => g.id === 'creation'), { id: 'creation', label: 'Creation' });
  assert.equal(V.GROUPS.some(g => g.id === 'instructions'), false);
  assert.equal(V.presetGroups('everything').indexOf('creation') >= 0, true);
  const f = { groups: ['creation', 'conversation'], type: '', q: '' };
  assert.equal(V.viewerUrl('vs_1', 1, f), '/viewer?session=vs_1&gen=1&groups=conversation%2Ccreation');
  assert.deepEqual(V.filterFromParams(new URLSearchParams('groups=conversation,creation')).groups, ['conversation', 'creation']);
  assert.deepEqual(V.filterFromParams(new URLSearchParams('groups=instructions')).groups, []);
});

test('default filter state is all groups; an empty groups parameter means none', () => {
  assert.deepEqual(V.filterFromParams(new URLSearchParams('')).groups, V.GROUPS.map(g => g.id));
  assert.deepEqual(V.filterFromParams(new URLSearchParams('groups=')).groups, []);
  assert.deepEqual(V.filterParams({ groups: V.GROUPS.map(g => g.id) }), {});
  assert.equal(V.viewerUrl('vs_1', 0, {}), '/viewer?session=vs_1');
});

test('delegation rows come from the live delegation.created shape', () => {
  const s = V.createStore();
  s.add([
    raw('delegation.created', { seq: 2, at: '2026-11-18T10:00:02Z' }, JSON.stringify({
      type: 'delegation.created',
      offset_ms: 1200,
      item: { id: 'di_1', type: 'function_call', target: 'client', handoff_id: 'h_1', user_bidi_turn_id: 'ut_1', content: [{ type: 'input_text', text: 'Part one. ' }, { type: 'input_text', text: 'Part two.' }] }
    })),
    raw('session.context.append', { seq: 3, direction: 'gateway_to_upstream', delivery: 'sent', source_kind: 'task_result' }, JSON.stringify({ delegation_item_id: 'di_1', handoff_id: 'h_1' })),
    raw('delegation.context.append', { seq: 4, direction: 'gateway_to_upstream', delivery: 'failed', source_kind: 'user_text' }, JSON.stringify({ delegation_item_id: 'di_1', content: [{ type: 'input_text', text: 'Again.' }] })),
    raw('turn.done', { seq: 5 }, JSON.stringify({ turn: { role: 'assistant', transcript: 'ok' } }))
  ]);
  const rows = s.delegations();
  assert.deepEqual(rows.map(r => r.seq), [2]);
  assert.deepEqual(rows[0], {
    seq: 2,
    at: '2026-11-18T10:00:02Z',
    type: 'delegation.created',
    delivery: 'observed',
    truncated: false,
    handoff: 'h_1',
    target: 'client',
    delegationItemId: 'di_1',
    text: 'Part one. Part two.'
  });
  assert.equal(s.get(3).group, 'context');
  assert.equal(s.get(4).group, 'client');
});

test('delegation rows tolerate older payloads without item or content', () => {
  const rows = V.delegationRows([
    { seq: 1, type: 'delegation.created', group: 'delegation', payload: '{}', delivery: 'observed', truncated: false, at: '' },
    { seq: 2, type: 'delegation.context.append', group: 'context', payload: 'not json', delivery: 'unknown', truncated: true, at: '' },
    { seq: 3, type: 'turn.done', group: 'conversation', payload: '{}' }
  ]);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].text, '');
  assert.equal(rows[0].handoff, '');
});
