'use strict';

// Run: node --test tests/js/
const test = require('node:test');
const assert = require('node:assert/strict');
const C = require('../../static/captions.js');

function feed(events) {
  const asm = C.createCaptionAssembler();
  const results = events.map((raw, i) => asm.consume(raw, i + 1));
  return { asm, results, snapshot: asm.snapshot() };
}

test('native user and assistant turns assemble with final markers', () => {
  const { snapshot } = feed([
    { type: 'turn.created', turn: { role: 'user', id: 'u1', transcript: 'hel' } },
    { type: 'turn.done', turn: { role: 'user', id: 'u1', transcript: 'hello' } },
    { type: 'turn.created', turn: { role: 'assistant', id: 'a1' } },
    { type: 'turn.text.delta', delta: 'Hi' },
    { type: 'output_text.delta', delta: ' there' },
  ]);
  assert.equal(snapshot.length, 2);
  assert.deepEqual(
    { role: snapshot[0].role, text: snapshot[0].text, final: snapshot[0].final },
    { role: 'user', text: 'hello', final: true }
  );
  assert.deepEqual(
    { key: snapshot[1].key, text: snapshot[1].text, final: snapshot[1].final },
    { key: 'current', text: 'Hi there', final: false }
  );
});

test('turn.done for an already emitted id is ignored (dedupe)', () => {
  const { results, snapshot } = feed([
    { type: 'turn.created', turn: { role: 'assistant', id: 'a1', text: 'one' } },
    { type: 'turn.done', turn: { role: 'assistant', id: 'a1' } },
    { type: 'turn.done', turn: { role: 'assistant', id: 'a1', transcript: 'two' } },
  ]);
  assert.deepEqual(results, [true, true, false]);
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].text, 'one');
});

test('repeated user turn.done with the same id and text is not duplicated', () => {
  const { results, snapshot } = feed([
    { type: 'turn.done', turn: { role: 'user', id: 'u1', transcript: 'same' } },
    { type: 'turn.done', turn: { role: 'user', id: 'u1', transcript: 'same' } },
  ]);
  assert.deepEqual(results, [true, false]);
  assert.equal(snapshot.length, 1);
});

test('user turn.done with identical text but a different id is kept (gateway rule)', () => {
  const { snapshot } = feed([
    { type: 'turn.done', turn: { role: 'user', id: 'u1', transcript: 'same' } },
    { type: 'turn.done', turn: { role: 'user', id: 'u2', transcript: 'same' } },
  ]);
  assert.equal(snapshot.length, 2);
});

test('turn.created with an already seen user id is skipped', () => {
  const { results } = feed([
    { type: 'turn.done', turn: { role: 'user', id: 'u1', transcript: 'x' } },
    { type: 'turn.created', turn: { role: 'user', id: 'u1', transcript: 'x' } },
  ]);
  assert.deepEqual(results, [true, false]);
});

test('wrapped data_message with a string payload is unwrapped', () => {
  const inner = { type: 'turn.done', turn: { role: 'assistant', id: 'a2', transcript: 'ok' } };
  const wire = { type: 'data_message', data: JSON.stringify(inner) };
  const parsed = C.parseEventPayload(JSON.stringify(wire));
  assert.deepEqual(parsed, inner);
  const asm = C.createCaptionAssembler();
  assert.equal(asm.consume(parsed, 1), true);
  assert.equal(asm.snapshot()[0].text, 'ok');
});

test('wrapped data_message with an object payload is unwrapped', () => {
  const inner = { type: 'turn.created', turn: { role: 'assistant', id: 'a3' } };
  assert.deepEqual(C.parseEventPayload({ type: 'data_message', data: inner }), inner);
});

test('truncated or non-object payloads yield null without throwing', () => {
  assert.equal(C.parseEventPayload('{"type":"turn.done","turn":{"role":"assi'), null);
  assert.equal(C.parseEventPayload('"just a string"'), null);
  assert.equal(C.parseEventPayload(''), null);
  assert.equal(C.parseEventPayload(null), null);
});

test('legacy chat_message_delta builds a message and appends ops', () => {
  const { results, snapshot } = feed([
    { type: 'chat_message_delta', payload: { delta: { v: { message: { id: 'm1', author: { role: 'assistant' }, content: { parts: ['Hello'] } } } } } },
    { type: 'chat_message_delta', payload: { delta: { v: [{ o: 'append', p: '/message/content/parts/0/text', v: ' world' }] } } },
  ]);
  assert.deepEqual(results, [true, true]);
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].role, 'assistant');
  assert.equal(snapshot[0].text, 'Hello world');
  assert.equal(snapshot[0].final, false);
});

test('legacy audio_transcription direction maps to user and assistant', () => {
  const state = C.applyCaptionDelta(null, {
    v: { message: { id: 'x', author: {}, content: { parts: [{ content_type: 'audio_transcription', direction: 'in', text: 'hi' }] } } }
  });
  assert.equal(state.role, 'user');
  assert.equal(state.text, 'hi');
  const out = C.applyCaptionDelta(null, {
    v: { message: { id: 'y', author: {}, content: { parts: [{ content_type: 'audio_transcription', direction: 'out', text: 'yo' }] } } }
  });
  assert.equal(out.role, 'assistant');
});

test('output_audio_transcript.delta without an open turn is not applied', () => {
  const { results, snapshot } = feed([{ type: 'output_audio_transcript.delta', delta: 'orphan' }]);
  assert.deepEqual(results, [false]);
  assert.equal(snapshot.length, 0);
});

test('output_audio_transcript.delta appends inside an open assistant turn', () => {
  const { snapshot } = feed([
    { type: 'turn.created', turn: { role: 'assistant', id: 'a9' } },
    { type: 'output_audio_transcript.delta', delta: 'spoken' },
  ]);
  assert.equal(snapshot[0].text, 'spoken');
});

test('client delegation becomes a user caption and dedupes by id', () => {
  const item = { id: 'd1', target: 'client', content: [{ type: 'input_text', text: 'check calendar' }] };
  const { results, snapshot } = feed([
    { type: 'delegation.created', item },
    { type: 'delegation.created', item },
    { type: 'delegation.created', item: { id: 'd2', target: 'upstream', content: [{ type: 'input_text', text: 'no' }] } },
  ]);
  assert.deepEqual(results, [true, false, false]);
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].role, 'user');
  assert.equal(snapshot[0].text, 'check calendar');
});

test('unknown and non-caption events return false so callers keep them in the raw view', () => {
  const { results, snapshot } = feed([
    { type: 'response.created' },
    { type: 'state_update', payload: { new_state: 'listening' } },
    { type: 'conversation_update', conversation_id: 'c' },
    null,
    'text',
  ]);
  assert.deepEqual(results, [false, false, false, false, false]);
  assert.equal(snapshot.length, 0);
});

test('turnText reads transcript, content parts and text keys', () => {
  assert.equal(C.turnText('plain'), 'plain');
  assert.equal(C.turnText([{ type: 'input_text', text: 'a' }, { text: 'b' }]), 'ab');
  assert.equal(C.turnText({ transcript: [{ text: 'from-transcript' }] }), 'from-transcript');
  assert.equal(C.turnText({ unrelated: 'nope' }), '');
  assert.equal(C.turnText(undefined), '');
});

test('input transcription events feed user captions with item id keys', () => {
  const { snapshot } = feed([
    { type: 'conversation.item.input_audio_transcription.delta', item_id: 'i1', delta: 'par' },
    { type: 'conversation.item.input_audio_transcription.delta', item_id: 'i1', delta: 'tial' },
    { type: 'conversation.item.input_audio_transcription.completed', item_id: 'i1', transcript: 'partial done' },
  ]);
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].role, 'user');
  assert.equal(snapshot[0].text, 'partial done');
  assert.equal(snapshot[0].final, true);
});

test('extractTranscriptText keeps the first non-empty candidate', () => {
  assert.equal(C.extractTranscriptText({ delta: '  hi  ' }), 'hi');
  assert.equal(C.extractTranscriptText({}, { item: { content: { parts: [{ text: 'deep' }] } } }), 'deep');
  assert.equal(C.extractTranscriptText({}, {}), '');
});

test('extractTextParts trims and joins typed parts', () => {
  assert.equal(C.extractTextParts(['a', { text: 'b' }, { content: { text: 'c' } }, null]), 'a\nb\nc');
  assert.equal(C.extractTextParts('nope'), '');
});

test('applyCaptionDelta ignores messages without a caption role', () => {
  const prev = { role: 'user', messageId: 'k', text: 'keep' };
  assert.deepEqual(C.applyCaptionDelta(prev, { v: { message: { id: 'n', author: { role: 'tool' }, content: { parts: ['x'] } } } }), prev);
});
