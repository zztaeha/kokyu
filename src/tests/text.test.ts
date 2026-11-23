import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as ff from 'fflate'
import * as k from '../engine/index.js'
import { cat, randomBytes, throwsCode } from './helpers.js'

const s = 'héllo wörld 🎉 日本語 ' + 'x'.repeat(1000)

test('utf-8 matches fflate both ways', () => {
  assert.deepEqual(k.encodeText(s), ff.strToU8(s))
  assert.equal(k.decodeText(ff.strToU8(s)), s)
})

test('latin1 keeps one byte per character', () => {
  // Bytes past 0x7F, where latin1 and UTF-8 differ
  const raw = randomBytes(5000), binary = String.fromCharCode(...raw)
  assert.deepEqual(k.encodeText(binary, { latin1: true }), raw)
  assert.equal(k.decodeText(raw, { latin1: true }), binary)
  assert.ok(throwsCode(() => k.encodeText('€', { latin1: true }), 'invalid-option'))
})

test('stream decoding matches one-shot decoding at every split, including BOMs and invalid bytes', () => {
  const bytes = cat([
    new Uint8Array([0xEF, 0xBB, 0xBF]), k.encodeText('aé€🎉\uFEFFz'),
    new Uint8Array([0xE2, 0x41, 0xF0, 0x9F, 0x80, 0xC0, 0x80, 0xED, 0xA0, 0x80, 0xF4, 0x90, 0x80, 0x80, 0xE2, 0x82]),
  ])
  const want = new TextDecoder().decode(bytes)
  for (let i = 0; i <= bytes.length; i++) {
    for (let j = i; j <= bytes.length; j++) {
      const dec = k.decodeTextStream()
      const got = dec.push(bytes.subarray(0, i)) + dec.push(bytes.subarray(i, j)) + dec.finish(bytes.subarray(j))
      assert.equal(got, want, `split at ${i} and ${j}`)
    }
  }
})
