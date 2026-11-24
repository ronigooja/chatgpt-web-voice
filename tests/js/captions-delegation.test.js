'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { createCaptionAssembler } = require('../../static/captions.js');

const created = { type: 'turn.created', turn: { id: 'u1', role: 'user' } };
const done = { type: 'turn.done', turn: { id: 'u1', role: 'user', transcript: 'Complete transcript' } };
const delegation = (id) => ({ type: 'delegation.created', item: {
  id, target: 'client', content: [{ type: 'input_text', text: 'Summary' }]
} });

function feed(events) {
  const assembler = createCaptionAssembler();
  const results = events.map((event, index) => assembler.consume(event, index + 1));
  return { lines: assembler.snapshot(), results };
}

test('completed transcript replaces delegation summary without moving its marker', () => {
  const { lines } = feed([created, delegation('d1'), done]);
  assert.equal(lines.length, 1);
  assert.deepEqual({ id: lines[0].id, text: lines[0].text, final: lines[0].final, seq: lines[0].seq },
    { id: 'u1', text: 'Complete transcript', final: true, seq: 2 });
});

test('delegation after completed turn does not duplicate or replace it', () => {
  const { lines, results } = feed([created, done, delegation('d1')]);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].text, 'Complete transcript');
  assert.equal(lines[0].final, true);
  assert.equal(results[2], false);
});

test('delegation without completion stays non-final', () => {
  const { lines } = feed([created, delegation('d1')]);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].text, 'Summary');
  assert.equal(lines[0].final, false);
});

test('two delegations in one turn retain one caption and finalize it', () => {
  const { lines } = feed([created, delegation('d1'), delegation('d2'), done]);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].text, 'Complete transcript');
  assert.equal(lines[0].seq, 2);
  assert.equal(lines[0].final, true);
});

test('matching and empty completion finalize an existing line without duplication', () => {
  for (const transcript of ['Summary', '']) {
    const { lines } = feed([created, delegation('d1'), { type: 'turn.done', turn: { id: 'u1', role: 'user', transcript } }]);
    assert.equal(lines.length, 1);
    assert.equal(lines[0].text, 'Summary');
    assert.equal(lines[0].final, true);
  }
});

test('turn deltas appear pending and are replaced by completion', () => {
  const assembler = createCaptionAssembler();
  [created, { type: 'turn.delta', turn_id: 'u1', delta: 'Partial' },
    { type: 'turn.delta', turn_id: 'u1', delta: ' text' }].forEach((event, index) => assembler.consume(event, index + 1));
  assert.deepEqual(assembler.snapshot().map(({ text, final }) => ({ text, final })), [{ text: 'Partial text', final: false }]);
  assert.equal(assembler.consume({ type: 'turn.delta', turn_id: 'other', delta: 'wrong' }, 4), false);
  assembler.consume(done, 5);
  assert.deepEqual(assembler.snapshot().map(({ text, final }) => ({ text, final })), [{ text: 'Complete transcript', final: true }]);
  assert.equal(assembler.consume({ type: 'turn.delta', turn_id: 'u1', delta: 'late' }, 6), false);
});

test('flushed non-final turn is finalized in place', () => {
  const { lines } = feed([created, { type: 'turn.delta', turn_id: 'u1', delta: 'Partial' },
    { type: 'turn.created', turn: { id: 'a1', role: 'assistant' } }, done]);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].text, 'Complete transcript');
  assert.equal(lines[0].final, true);
});
