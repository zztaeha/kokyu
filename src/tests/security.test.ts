// Hostile input and misuse: work that must stay linear, archives built to amplify or to slip past a filter, streams
// after a failure, async calls racing the caller, and option values that must be refused rather than truncated.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { deflateRawSync, inflateRawSync, constants, crc32 as zlibCrc32 } from 'node:zlib'
import { runInNewContext } from 'node:vm'
import { execFileSync } from 'node:child_process'
import { Worker } from 'node:worker_threads'
import * as k from '../engine/index.js'
import { inflater } from '../engine/inflate.js'
import { deflater } from '../engine/deflate.js'
import { UnzipReader, type AskChunk } from '../engine/zip.js'
import {
  bitsOf, cat, codeOf, codeOfAsync, joinChunks, pieces, randomBytes, sample, throwsCode, totalAllocation, u16, u32, type Field,
} from './helpers.js'

const ascii = (s: string): number[] => [...s].map(c => c.charCodeAt(0))
const mtime = new Date(2020, 0, 1)

interface Entry {
  name: string
  body: Uint8Array
  method: number
  usize: number
  crc?: number
  offset?: number
  // Central record overrides
  at?: number
  csizeC?: number
  usizeC?: number
  extraC?: number[]
}

// A zip written byte by byte, so it can hold what no writer would produce
function build(entries: Entry[], comment: number[] = []): Uint8Array {
  const parts: Uint8Array[] = [], central: Uint8Array[] = []
  let off = 0
  for (const e of entries) {
    const nm = ascii(e.name)
    e.offset = off
    const head = Uint8Array.from([
      ...u32(0x04034b50), ...u16(20), ...u16(0), ...u16(e.method), ...u16(0), ...u16(0x21), ...u32(e.crc ?? k.crc32(e.body)),
      ...u32(e.body.length), ...u32(e.usize), ...u16(nm.length), ...u16(0), ...nm,
    ])
    parts.push(head, e.body)
    off += head.length + e.body.length
  }
  for (const e of entries) {
    const nm = ascii(e.name), ex = e.extraC ?? []
    central.push(Uint8Array.from([
      ...u32(0x02014b50), ...u16(20), ...u16(20), ...u16(0), ...u16(e.method), ...u16(0), ...u16(0x21),
      ...u32(e.crc ?? k.crc32(e.body)), ...u32(e.csizeC ?? e.body.length), ...u32(e.usizeC ?? e.usize),
      ...u16(nm.length), ...u16(ex.length), ...u16(0), ...u16(0), ...u16(0), ...u32(0), ...u32(e.at ?? e.offset!), ...nm, ...ex,
    ]))
  }
  const cd = cat(central)
  return cat([...parts, cd, Uint8Array.from([
    ...u32(0x06054b50), ...u16(0), ...u16(0), ...u16(entries.length), ...u16(entries.length), ...u32(cd.length), ...u32(off),
    ...u16(comment.length), ...comment,
  ])])
}

const stored = (name: string, body: Uint8Array): Entry => ({ name, body, method: 0, usize: body.length })

// A call that must fail through its promise, never by throwing
function called(call: () => Promise<unknown>, what: string): Promise<unknown> {
  let promise: Promise<unknown> | undefined
  assert.doesNotThrow(() => { promise = call() }, what)
  return promise!
}

test('unzipStream finds a stored entry\'s end in linear time, however many false descriptors it holds', () => {
  const n = 1 << 20, body = new Uint8Array(n)
  // Each false descriptor's size matches its offset, so only the CRC tells it apart
  for (let o = 0; o + 16 <= n; o += 16) body.set([...u32(0x08074b50), ...u32(0x12345678), ...u32(o), ...u32(o)], o)
  const head = Uint8Array.from([...u32(0x04034b50), ...u16(20), ...u16(8), ...u16(0), ...u16(0), ...u16(0), ...u32(0), ...u32(0), ...u32(0), ...u16(1), ...u16(0), 97])
  const t = performance.now()
  k.unzipStream().push(cat([head, body]))
  // Linear takes milliseconds; summing from the entry's start at each candidate would take about 16 s here
  assert.ok(performance.now() - t < 1500, `took ${performance.now() - t} ms`)
})

test('gunzipStream never holds an unfinished header name, and reads headers split anywhere', () => {
  const s = k.gunzipStream(), piece = new Uint8Array(1 << 16).fill(97)
  s.push(Uint8Array.of(0x1f, 0x8b, 8, 8 | 16, 0, 0, 0, 0, 0, 255))
  const bytes = totalAllocation(() => { for (let i = 0; i < 64; i++) s.push(piece) })
  assert.ok(bytes < 1 << 16, `allocated ${bytes} bytes for 4 MB of name`)
  assert.ok(throwsCode(() => s.finish(), 'truncated'))

  // Every field, then the same again as a second member, pushed in pieces of every size up to the header's
  const body = k.gzip(sample.subarray(0, 3000))
  const extra = [...u16(300), ...new Uint8Array(300).fill(7)]
  const head = Uint8Array.from([0x1f, 0x8b, 8, 4 | 8 | 16 | 2, 0, 0, 0, 0, 0, 255, ...extra, ...ascii('name.txt'), 0, ...ascii('a comment'), 0, 0xAB, 0xCD])
  const member = cat([head, body.subarray(10)]), both = cat([member, member]), want = cat([sample.subarray(0, 3000), sample.subarray(0, 3000)])
  for (let n = 1; n <= head.length + 2; n++) {
    assert.deepEqual(cat(pieces(k.gunzipStream(), both, n)), want, `pieces of ${n}`)
  }
  // Cut inside the second member's header: truncated, not a quiet end
  for (const cut of [member.length + 5, member.length + 12, member.length + 320]) {
    assert.ok(throwsCode(() => k.gunzipStream().finish(both.subarray(0, cut)), 'truncated'), `cut at ${cut}`)
  }
})

test('gunzip reads many tiny members in linear time', () => {
  const member = k.gzip(new Uint8Array(0)), n = 20000
  const all = new Uint8Array(member.length * n)
  for (let i = 0; i < n; i++) all.set(member, i * member.length)
  let out: Uint8Array = new Uint8Array(1)
  // Guessing each member's size from all the input still left would allocate about 16 GB here
  const bytes = totalAllocation(() => { out = k.gunzip(all) })
  assert.equal(out.length, 0)
  assert.ok(bytes < 100 * all.length, `allocated ${bytes} bytes`)
})

test('unzip refuses entries that share bytes (an overlapping zip bomb)', () => {
  const data = new Uint8Array(1 << 20), body = k.deflate(data)
  const entries: Entry[] = [{ name: 'a', body, method: 8, usize: data.length, crc: k.crc32(data) }]
  for (let i = 1; i < 20; i++) entries.push({ name: `e${i}`, body: new Uint8Array(0), method: 8, usize: data.length, crc: k.crc32(data), csizeC: body.length, at: 0 })
  const bomb = build(entries)
  assert.ok(throwsCode(() => k.unzip(bomb), 'invalid-data'))
  // One of them alone is fine, and so are neighbors that only touch
  assert.equal(k.unzip(bomb, { filter: e => e.name === 'e3' })['e3']!.length, data.length)
  assert.deepEqual(Object.keys(k.unzip(build([stored('x', Uint8Array.of(1)), stored('y', Uint8Array.of(2))]))), ['x', 'y'])
})

test('unzipStreamAsync never decodes a skipped entry whose sizes are in its header, and asks about every entry, even two of one name', async () => {
  // The skipped entry is broken deflate data: decoding it at all would fail the stream
  const pad = new Uint8Array(20000).fill(5)
  const z = build([
    { name: 'skip', body: Uint8Array.of(0xFF, 0xFF, 0xFF, 0xFF), method: 8, usize: 1 << 30 },
    stored('keep', pad),
    stored('dup', Uint8Array.of(1, 2, 3)),
    stored('dup', new Uint8Array(30000).fill(9)),
  ])
  const filter = (e: k.ZipEntryInfo): boolean => e.name !== 'skip' && (e.originalSize ?? Infinity) < 25000
  const sync = k.unzipStream({ filter }).finish(z)
  // Each entry as name and size: how its data splits into chunks follows the pushes
  const summary = (chunks: k.UnzipChunk[]): string[] => {
    const out: string[] = []
    let size = 0
    for (const c of chunks) {
      size += c.data.length
      if (c.final) { out.push(`${c.entry.name}:${size}`); size = 0 }
    }
    return out
  }
  // In a worker (large) and on this thread (small, whole in one call)
  const worker = await k.unzipStreamAsync({ filter }).finish(z)
  assert.deepEqual(summary(worker), summary(sync))
  const small = build([stored('dup', Uint8Array.of(1)), stored('dup', new Uint8Array(500))])
  const tiny = (e: k.ZipEntryInfo): boolean => (e.originalSize ?? Infinity) < 100
  assert.deepEqual(summary(await k.unzipStreamAsync({ filter: tiny }).finish(small)), summary(k.unzipStream({ filter: tiny }).finish(small)))
  // Pushed in pieces, with calls overlapping: the same entries in the same order
  assert.deepEqual(summary((await Promise.all(pieces(k.unzipStreamAsync({ filter }), z, 7000))).flat()), summary(sync))
  // Any truthy answer keeps an entry, as in unzipStream(), even one that can't be sent to a worker
  const truthy = (e: k.ZipEntryInfo): boolean => (filter(e) && (() => 1)) as unknown as boolean
  assert.deepEqual(summary(await k.unzipStreamAsync({ filter: truthy }).finish(z)), summary(sync))
  // A filter that throws fails the stream for good, as in unzipStream: going on would lose what that call decoded
  // before the entry the filter was asked about
  for (const open of [k.unzipStream, k.unzipStreamAsync]) {
    let thrown = false
    const once = (e: k.ZipEntryInfo): boolean => { if (e.name === 'dup' && !thrown) { thrown = true; throw new Error('filter failed') } return filter(e) }
    const r = open({ filter: once })
    await assert.rejects(async () => r.push(z), /filter failed/, open.name)
    await assert.rejects(async () => r.finish(), /filter failed/, open.name)
  }
})

test('a filter can\'t push to the stream that asked it, which would parse over the call under way', () => {
  const big = new Uint8Array(5000).map((_, i) => i * 13)
  const z = k.zip({ a: big, b: big, c: k.encodeText('see '.repeat(900)) }, { level: 0 })
  // Pushing the rest of the archive from the filter would give wrong entries without an error at some cuts
  for (let cut = 1; cut < z.length; cut += 97) {
    const s = k.unzipStream({ filter: e => { if (e.name === 'b') assert.ok(throwsCode(() => s.push(z.subarray(cut)), 'invalid-state')); return true } })
    const out = [...s.push(z.subarray(0, cut)), ...s.finish(z.subarray(cut))]
    assert.deepEqual(joinChunks(out)['b'], big, `cut at ${cut}`)
  }
})

test('entries kokyu shows the caller are the caller\'s: editing one changes nothing, inline or in a worker', async () => {
  const name = 'Dir/File.txt'
  // Normalizing a name, or noting a size, on the entry object itself
  const edit = (e: k.ZipEntryInfo): boolean => { e.name = e.name.toLowerCase(); e.originalSize = 1; return true }
  // The archive's size decides where the async calls run
  for (const data of [Uint8Array.of(1, 2, 3), randomBytes(40000)]) {
    const z = k.zip({ [name]: data }), what = `${z.length} B archive`
    assert.deepEqual(k.unzip(z, { filter: edit }), { [name]: data }, what)
    assert.deepEqual(await k.unzipAsync(z, { filter: edit }), { [name]: data }, what)
    for (const chunks of [k.unzipStream({ filter: edit }).finish(z), await k.unzipStreamAsync({ filter: edit }).finish(z)]) {
      assert.deepEqual([...new Set(chunks.map(c => c.entry.name))], [name], what)
      assert.deepEqual(cat(chunks.map(c => c.data)), data, what)
    }
  }
  // Nor does editing an entry a stream returned, as code showing its size in KB might
  const z = k.zip({ a: randomBytes(60000) }, { level: 0 })
  for (const open of [k.unzipStream, k.unzipStreamAsync]) {
    const s = open()
    let total = 0
    for (let i = 0; i < z.length; i += 8192) {
      for (const c of await s.push(z.subarray(i, i + 8192))) {
        total += c.data.length
        c.entry.originalSize = 59
      }
    }
    for (const c of await s.finish()) total += c.data.length
    assert.equal(total, 60000, open.name)
  }
  // Nor does deleting a name a chunk carries: a size check that fails later names the entry as read
  const short = k.zip({ 'real.txt': randomBytes(5000) }, { level: 0 })
  new DataView(short.buffer).setUint32(22, 5001, true)
  const r = k.unzipStream()
  for (const c of r.push(short.subarray(0, 1000))) delete (c.entry as { name?: string }).name
  assert.throws(() => r.finish(short.subarray(1000)), { code: 'invalid-data', message: /"real\.txt" has the wrong size/ })
})

test('kokyu writes no end record or ZIP64 locator where readers search for one, and reads past a locator it didn\'t write', async () => {
  const d = Uint8Array.of(1), chars = (...c: number[]): string => String.fromCharCode(...c)
  // An end record signature in the archive comment is found first, searching back from the end
  const comment = 'note: ' + chars(0x50, 0x4B, 5, 6) + chars(0).repeat(18)
  const writes = [() => k.zip({ a: d }, { comment }), () => k.zipStream({ comment }), () => k.zipAsync({ a: d }, { comment }), () => k.zipStreamAsync({ comment }).finish()]
  for (const write of writes) assert.equal(await codeOfAsync(write), 'invalid-option')
  assert.deepEqual(Object.keys(k.unzip(k.zip({ a: d }, { comment: 'PK and more' }))), ['a'])
  // Readers that know ZIP64 take a locator in the 20 bytes before the end record, here the last entry's comment
  const locator = chars(0x50, 0x4B, 6, 7) + chars(0).repeat(12) + chars(1, 0, 0, 0)
  assert.ok(throwsCode(() => k.zip({ a: d, b: [d, { comment: locator }] }), 'invalid-option'))
  const z = k.zipStream()
  z.file('a', { comment: locator }).finish(d)
  assert.ok(throwsCode(() => z.finish(), 'invalid-option'))
  // One in an archive another writer made counts only when an end record field is too small for its value
  const archive = k.zip({ a: d, b: [d, { comment: 'x'.repeat(20) }] }, { mtime })
  archive.set(k.encodeText(locator), archive.length - 42)
  assert.deepEqual(Object.keys(k.unzip(archive)), ['a', 'b'])
})

test('a filter asked about every entry of a long chunk costs linear time, not quadratic', () => {
  const files: Record<string, Uint8Array> = {}
  for (let i = 0; i < 32_000; i++) files[i] = new Uint8Array(0)
  const z = k.zip(files, { level: 0, mtime })
  const asked = (): number => {
    const t = performance.now(), r = new UnzipReader({}, true)
    for (let out: AskChunk[] = r.finish(z); out.at(-1)?.ask;) out = r.decide(false)
    return performance.now() - t
  }
  const plain = (): number => { const t = performance.now(); k.unzipStream({ filter: () => false }).finish(z); return performance.now() - t }
  // Moving the rest of the chunk at each answer took 1.3 s here, about 100 times the plain read
  const [ours, base] = [Math.min(asked(), asked()), Math.min(plain(), plain())]
  assert.ok(ours < 10 * base + 50, `${ours.toFixed(0)} ms asking, ${base.toFixed(0)} ms not`)
})

test('an upload holding a forged data descriptor can\'t end its entry early in a zip stream, even at level 0', async () => {
  // A descriptor matching the upload's own first bytes, then a forged local header and a central directory signature
  const prefix = k.encodeText('harmless picture bytes'), evil = k.encodeText('#!/bin/sh'), name = k.encodeText('../../evil.sh')
  const upload = Uint8Array.from([
    ...prefix, ...u32(0x08074B50), ...u32(k.crc32(prefix)), ...u32(prefix.length), ...u32(prefix.length),
    ...u32(0x04034B50), ...u16(20), ...u16(0), ...u16(0), ...u32(0), ...u32(k.crc32(evil)), ...u32(evil.length), ...u32(evil.length),
    ...u16(name.length), ...u16(0), ...name, ...evil, ...u32(0x02014B50),
  ])
  const report = k.encodeText('quarterly numbers')
  for (const make of [k.zipStream, k.zipStreamAsync]) {
    const s = make({ mtime, level: 0 })
    const archive = cat(await Promise.all([s.file('upload.png').finish(upload), s.file('report.txt').finish(report), s.finish()]))
    const want = { 'upload.png': upload, 'report.txt': report }
    assert.deepEqual(joinChunks(k.unzipStream().finish(archive)), want, make.name)
    assert.deepEqual(joinChunks(await k.unzipStreamAsync().finish(archive)), want, make.name)
  }
  // Only an entry that ends empty in its first call is stored, as nothing comes between its header and descriptor
  const s = k.zipStream({ mtime }), parts = [s.file('dir/').finish()], w = s.file('empty')
  parts.push(w.push(new Uint8Array(0)), w.finish(), s.file('dir2/').finish(report), s.finish())
  assert.deepEqual(k.zipEntries(cat(parts)).map(e => [e.name, e.compression]), [['dir/', 0], ['empty', 8], ['dir2/', 8]])
})

test('a stream called from inside its own call, by a getter on a chunk or option, refuses it and stays correct', () => {
  const text = sample.subarray(0, 5000)
  // A chunk whose length getter calls the stream on its first read, while the chunk is being checked
  const trapped = (data: Uint8Array, act: () => unknown): [Uint8Array, () => string] => {
    const chunk = new Uint8Array(data), length = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(Uint8Array.prototype), 'length')!
    let reads = 0, got = 'not called'
    Object.defineProperty(chunk, 'length', { get() { if (++reads === 1) got = codeOf(act); return length.get!.call(this) } })
    return [chunk, () => got]
  }
  for (const [name, make, unpack] of [['deflateStream', k.deflateStream, k.inflate], ['gzipStream', k.gzipStream, k.gunzip]] as const) {
    const s = make(), [chunk, inner] = trapped(text, () => s.finish())
    const out = cat([s.push(chunk), s.finish()])
    assert.equal(inner(), 'invalid-state', name)
    assert.deepEqual(unpack(out), text, name)
  }
  {
    const s = k.inflateStream(), [chunk, inner] = trapped(k.deflate(text), () => s.finish())
    assert.deepEqual(cat([s.push(chunk), s.finish()]), text)
    assert.equal(inner(), 'invalid-state')
  }
  {
    const s = k.unzipStream(), [chunk, inner] = trapped(k.zip({ a: text }, { mtime }), () => s.finish())
    assert.deepEqual(joinChunks(s.finish(chunk)), { a: text })
    assert.equal(inner(), 'invalid-state')
  }
  {
    const z = k.zipStream({ mtime }), w = z.file('a'), [chunk, inner] = trapped(text, () => w.finish())
    const parts = [w.push(chunk), w.finish()]
    assert.equal(inner(), 'invalid-state')
    // Nor can an option's getter finish the archive while an entry is being opened
    let finished = 'not called'
    parts.push(z.file('b', { get comment(): string { finished = codeOf(() => z.finish()); return 'note' } }).finish(text))
    assert.equal(finished, 'invalid-state')
    assert.deepEqual(k.unzip(cat([...parts, z.finish()])), { a: text, b: text })
  }
  // Text that isn't a string runs the caller's toString as it's read
  for (const s of [k.encodeTextStream(), k.encodeTextStream({ latin1: true })]) {
    let inner = 'not called'
    const out = s.push({ toString: () => { inner = codeOf(() => s.finish('b')); return 'a' } } as never)
    assert.equal(inner, 'invalid-state')
    assert.equal(k.decodeText(cat([out, s.finish('c')])), 'ac')
  }
})

test('a deflater used after it finished never writes into the tables later one-shot calls borrow', () => {
  // Misuse no public stream allows, but its finish borrowed the shared tables, so they must be let go of
  for (let trial = 0; trial < 40; trial++) {
    const d = deflater(6)
    d.push(sample.subarray(0, 100 + trial * 37), true)
    d.push(sample.subarray(trial * 997, trial * 997 + 70_000 + trial * 101), false)
    const next = sample.subarray(trial * 13, trial * 13 + 3000 + trial * 29)
    assert.deepEqual(inflateRawSync(k.deflate(next)), Buffer.from(next), `trial ${trial}`)
  }
})

test('a chunk refused for its type leaves any stream as it was, so a good one can follow', async () => {
  const text = sample.subarray(0, 3000)
  const sync: [string, () => { push(c: never): unknown; finish(c?: never): unknown }, unknown, (out: unknown[]) => unknown, unknown][] = [
    ['deflateStream', k.deflateStream, text, o => k.inflate(cat(o as Uint8Array[])), text],
    ['zlibStream', k.zlibStream, text, o => k.unzlib(cat(o as Uint8Array[])), text],
    ['inflateStream', k.inflateStream, k.deflate(text), o => cat(o as Uint8Array[]), text],
    ['gunzipStream', k.gunzipStream, k.gzip(text), o => cat(o as Uint8Array[]), text],
    ['decompressStream', k.decompressStream, k.zlib(text), o => cat(o as Uint8Array[]), text],
    ['unzipStream', k.unzipStream, k.zip({ a: text }, { mtime }), o => joinChunks((o as k.UnzipChunk[][]).flat()), { a: text }],
    ['decodeTextStream', k.decodeTextStream, text, o => o.join(''), k.decodeText(text)],
    ['latin1 decodeTextStream', () => k.decodeTextStream({ latin1: true }), text, o => o.join(''), k.decodeText(text, { latin1: true })],
  ]
  for (const [name, make, input, read, want] of sync) {
    const s = make()
    assert.equal(codeOf(() => s.push('not bytes' as never)), 'invalid-option', `${name} push`)
    assert.equal(codeOf(() => s.finish('not bytes' as never)), 'invalid-option', `${name} finish`)
    assert.deepEqual(read([s.push(input as never), s.finish()]), want, name)
  }
  for (const s of [k.encodeTextStream(), k.encodeTextStream({ latin1: true })]) {
    assert.throws(() => s.finish(Symbol('not text') as never), TypeError)
    assert.equal(k.decodeText(cat([s.push('ab'), s.finish('c')])), 'abc')
  }
  const z = k.zipStream({ mtime }), w = z.file('a')
  assert.equal(codeOf(() => w.finish('not bytes' as never)), 'invalid-option')
  assert.deepEqual(k.unzip(cat([w.finish(text), z.finish()])), { a: text })
  const a = k.gunzipStreamAsync()
  assert.equal(await codeOfAsync(() => a.finish('not bytes' as never)), 'invalid-option')
  assert.deepEqual(await a.finish(k.gzip(text)), text)
})

test('no field of the end record kokyu writes, or its comment, spells an end record signature', () => {
  const d = Uint8Array.of(1), chars = (...c: number[]): string => String.fromCharCode(...c)
  // A reader searching back from the end would take it for the record. A length field spelling PK would need a
  // comment of 0x4B50 bytes, past the limit.
  const comment = 'note ' + chars(0x50, 0x4B, 5, 6) + ' end'
  assert.ok(throwsCode(() => k.zip({ a: d }, { comment }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zipStream({ comment }), 'invalid-option'))
  assert.equal(k.zipEntries(k.zip({ a: d }, { comment: comment.replace(chars(6), chars(7)) })).length, 1)
  // 0x4B50 entries, their central directory sized so its low half is 0x0605: count and size spell it at offset 10
  const files: Record<string, Uint8Array> = {}, n = 0x4B50
  const names = Array.from({ length: n }, (_, i) => `n${i}`)
  const cdSize = (z: Uint8Array): number => new DataView(z.buffer, z.byteOffset).getUint32(z.length - 10, true)
  const size = cdSize(k.zip(Object.fromEntries(names.map(m => [m, d])), { mtime }))
  const delta = ((0x0605 - size) % 0x10000 + 0x10000) % 0x10000
  names.forEach((m, i) => { files[m + 'x'.repeat(Math.floor(delta / n) + +(i < delta % n))] = d })
  const plain = k.zip(files, { mtime })
  assert.equal(cdSize(plain) & 0xFFFF, 0x0605)
  assert.equal(k.zipEntries(plain).length, n)
  assert.ok(throwsCode(() => k.zip(files, { mtime, comment: 'note' }), 'invalid-option'))
})

type Resizable = ArrayBuffer & { resize(n: number): void }

test('zip and zipAsync refuse a stored entry whose bytes an option getter detaches or shrinks before they\'re written', async () => {
  for (const size of [5000, 40000]) {
    for (const hit of [(b: Resizable) => structuredClone(b, { transfer: [b] }), (b: Resizable) => b.resize(10)]) {
      for (const where of ['entry', 'archive']) {
        for (const call of [k.zip, k.zipAsync]) {
          const buf = Reflect.construct(ArrayBuffer, [size, { maxByteLength: 2 * size }]) as Resizable, data = new Uint8Array(buf).fill(1)
          const getter = { get comment() { hit(buf); return 'n' } }
          const input: k.ZipInput = { 'a.bin': [data, { level: 0 }], 'b.txt': [new Uint8Array(3), where === 'entry' ? getter : {}] }
          const opts = Object.defineProperties({ mtime }, where === 'archive' ? Object.getOwnPropertyDescriptors(getter) : {})
          assert.equal(await codeOfAsync(async () => call(input, opts)), 'invalid-option')
        }
      }
    }
  }
})

test('async calls refuse an input that a transfer or signal getter detaches or shrinks after the call has checked it', async () => {
  // Fixed-length views of resizable buffers, so shrinking leaves them out of bounds
  const view = (src: Uint8Array): Uint8Array => {
    const v = new Uint8Array(Reflect.construct(ArrayBuffer, [src.length, { maxByteLength: src.length }]) as Resizable, 0, src.length)
    v.set(src)
    return v
  }
  const data = new Uint8Array(40000).fill(1), z = k.zip({ a: [data, { level: 0 }] })
  for (const hit of [(b: Resizable) => structuredClone(b, { transfer: [b] }), (b: Resizable) => b.resize(10)]) {
    for (const key of ['transfer', 'signal']) {
      const late = (b: Uint8Array): object => ({ get [key]() { hit(b.buffer as Resizable); return key === 'transfer' || undefined } })
      const d = view(data), z2 = view(z), first = view(data)
      assert.equal(await codeOfAsync(() => k.deflateAsync(d, late(d))), 'invalid-option', `deflateAsync, ${key}`)
      assert.equal(await codeOfAsync(() => k.unzipAsync(z2, late(z2))), 'invalid-option', `unzipAsync, ${key}`)
      assert.equal(await codeOfAsync(() => k.zipAsync({ a: first, b: view(data) }, late(first))), 'invalid-option', `zipAsync, ${key}`)
    }
  }
})

test('unzipStream fails with a KokyuError when its filter detaches or resizes the chunk being read', () => {
  const z = k.zip({ a: new Uint8Array(300).fill(7), b: new Uint8Array(300).fill(9) }, { level: 0 })
  for (const change of [(b: Resizable) => structuredClone(b, { transfer: [b] }), (b: Resizable) => b.resize(10), (b: Resizable) => b.resize(z.length + 50)]) {
    // A length-tracking view, which a resize leaves readable but at another length
    const buf = Reflect.construct(ArrayBuffer, [z.length, { maxByteLength: z.length + 100 }]) as Resizable, v = new Uint8Array(buf)
    v.set(z)
    assert.equal(codeOf(() => k.unzipStream({ filter: () => { change(buf); return true } }).finish(v)), 'invalid-option')
  }
})

test('kokyu writes no name or field that other readers read as another name', () => {
  const d = Uint8Array.of(1), chars = (...c: number[]): string => String.fromCharCode(...c)
  // Python and Info-ZIP end a name at a NUL: 'invoice.exe' + NUL + '.pdf' extracts as invoice.exe
  const nul = 'invoice.exe' + chars(0) + '.pdf'
  assert.ok(throwsCode(() => k.zip({ [nul]: d }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zipStream().file(nul), 'invalid-option'))
  // Info-ZIP takes an empty name for the entry before it, so this would extract over invoice.pdf
  assert.ok(throwsCode(() => k.zip({ 'invoice.pdf': d, '': d }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zipStream().file(''), 'invalid-option'))
  // Python takes the name from a Unicode Path field whose CRC matches the header name
  assert.ok(throwsCode(() => k.zip({ a: [d, { extra: { 0x7075: Uint8Array.of(1, 0, 0, 0, 0) } }] }), 'invalid-option'))
  // UTF-8 writes every lone surrogate as U+FFFD, so these would be one name three times
  const same = ['r' + chars(0xD800), 'r' + chars(0xDC00), 'r' + chars(0xFFFD)]
  assert.ok(throwsCode(() => k.zip({ [same[0]!]: d, [same[1]!]: d }), 'invalid-option'))
  const z = k.zipStream()
  z.file(same[0]!).finish(d)
  assert.ok(throwsCode(() => z.file(same[2]!), 'invalid-option'))
  // bsdtar extracts a file and a folder of one name onto one path, the second silently replacing the first
  for (const files of [{ a: d, 'a/': {} }, { 'a/': {}, a: d }, { d: { b: d }, 'd/b/': {} }] as k.ZipInput[]) {
    assert.ok(throwsCode(() => k.zip(files), 'invalid-option'), JSON.stringify(Object.keys(files)))
  }
  for (const [first, second] of [['a', 'a/'], ['a/', 'a']]) {
    const s = k.zipStream()
    s.file(first!).finish(first!.endsWith('/') ? undefined : d)
    assert.ok(throwsCode(() => s.file(second!), 'invalid-option'), `${first} then ${second}`)
  }
  // Each of these extracts over docs/invoice.pdf: Info-ZIP drops controls and a file name's trailing ; and digits, and
  // reads \ as / for MS-DOS hosts, bsdtar reads \ as / and drops a drive, and all three drop leading /, empty, . and ..
  const over = ['docs\\invoice.pdf', 'docs/invoice' + chars(9) + '.pdf', 'docs/invoice' + chars(0x7F) + '.pdf', '/docs/invoice.pdf',
    './docs/invoice.pdf', 'docs//invoice.pdf', 'docs/./invoice.pdf', 'x/../docs/invoice.pdf', 'C:docs/invoice.pdf',
    'docs/invoice.pdf;1', 'docs/invoice.pdf;']
  for (const name of over) {
    assert.ok(throwsCode(() => k.zip({ docs: { 'invoice.pdf': d }, [name]: d }), 'invalid-option'), JSON.stringify(name))
    assert.ok(throwsCode(() => k.zipStream().file(name), 'invalid-option'), JSON.stringify(name))
  }
  assert.ok(throwsCode(() => k.zip({ docs: { '': { 'invoice.pdf': d } } }), 'invalid-option'))
  // Names that only look like those are kept
  const kept = ['.profile', 'a..b', '...', 'notes:v2.txt', 'docs/.hidden/', 'docs/', 'v;1/', 'a;b', 'x;1y']
  assert.deepEqual(k.zipEntries(k.zip(Object.fromEntries(kept.map(n => [n, n.endsWith('/') ? {} : d])))).map(e => e.name), kept)
})

test('input under 10 bytes that already isn\'t gzip fails as such, however it arrives', () => {
  const cases = [[[0x50, 0x4B], 'invalid-header'], [[0x50, 0x4B, 3, 4], 'invalid-header'], [[0x1F, 0x8B, 9], 'unsupported'], [[0x1F, 0x8B, 8, 0xE0], 'invalid-header'], [[0x1F, 0x8B, 8], 'truncated']] as const
  for (const [bytes, code] of cases) {
    const d = Uint8Array.from(bytes)
    assert.equal(codeOf(() => k.gunzip(d)), code, `${bytes} one-shot`)
    assert.equal(codeOf(() => k.gunzipStream().push(d)), code === 'truncated' ? 'ok' : code, `${bytes} pushed`)
  }
})

// 2 GiB of memory, so only with BIG=1
test('crc32, which gzip and zip run on all their data, handles 2 GiB and more', { skip: !process.env['BIG'] }, () => {
  const d = new Uint8Array(2 ** 31 + 16)
  for (let i = 0; i < d.length; i += 4093) d[i] = i & 255
  assert.equal(k.crc32(d), zlibCrc32(d))
  assert.equal(k.crc32(d.subarray(3)), zlibCrc32(d.subarray(3)))
})

type Growable = SharedArrayBuffer & { readonly growable: boolean; readonly maxByteLength: number; grow(n: number): void }
const growable = (n: number, max: number): Growable => Reflect.construct(SharedArrayBuffer, [n, { maxByteLength: max }]) as Growable

const grown = (b: Growable): Growable => {
  if (b instanceof SharedArrayBuffer && b.growable && b.byteLength < b.maxByteLength) b.grow(b.byteLength + 4096)
  return b
}
// Grows its buffer whenever kokyu takes a view of it or reads its buffer, standing in for another thread's grow()
class Growing extends Uint8Array {
  constructor(...a: unknown[]) {
    if (a.length > 1) grown(a[0] as Growable)
    super(...(a as [ArrayBuffer]))
  }
}
const bufferOf = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(Uint8Array.prototype), 'buffer')!.get!
Object.defineProperty(Growing.prototype, 'buffer', { get(this: Uint8Array) { return grown(bufferOf.call(this) as Growable) } })

test('a view of a growable SharedArrayBuffer that grows during a call is read at a length it had', () => {
  const data = new Uint8Array(100000).map((_, i) => (i * 2654435761) >>> 24)
  const growing = (): Uint8Array => {
    const v = new Growing(growable(data.length, 1 << 20))
    v.set(data)
    return v
  }
  assert.equal(k.crc32(growing()), zlibCrc32(data))
  // gzip's checksum and its deflate each take the length once, so the bytes may run past it
  assert.deepEqual(k.gunzip(k.gzip(growing())).subarray(0, data.length), data)
  assert.deepEqual(k.gunzip(k.gzipStream().finish(growing())).subarray(0, data.length), data)
  // A dictionary too: each call works from the bytes it had, so decoding with those gives the data back
  const text = k.encodeText('the quick brown fox jumps over the lazy dog '.repeat(2000)), held = text.subarray(0, 28672)
  const dictionary = (): Uint8Array => {
    const v = new Growing(growable(held.length, 1 << 20))
    v.set(held)
    return v
  }
  for (const packed of [k.deflate(text, { dictionary: dictionary() }), k.deflateStream({ dictionary: dictionary() }).finish(text)]) {
    assert.deepEqual(k.inflate(packed, { dictionary: held }), text)
  }
  assert.deepEqual(k.unzlib(k.zlib(text, { dictionary: dictionary() }), { dictionary: held }), text)
  assert.deepEqual(k.unzlib(k.zlibStream({ dictionary: dictionary() }).finish(text), { dictionary: held }), text)
  const packed = k.deflate(text, { dictionary: held })
  assert.deepEqual(k.inflate(packed, { dictionary: dictionary() }), text)
  assert.deepEqual(k.inflateStream({ dictionary: dictionary() }).finish(packed), text)
})

test('an inflate stream never rereads a chunk once the caller reuses its buffer', () => {
  // One resizable read buffer, refilled for each packet: a view of it that a stream kept would show the next packet
  const data = k.encodeText('0123456789'.repeat(1000))
  for (const [packed, stream] of [[k.deflate(data, { level: 0 }), k.inflateStream], [k.deflate(data), k.inflateStream],
    [k.zlib(data, { level: 0 }), k.unzlibStream], [k.gzip(data, { level: 0 }), k.gunzipStream]] as const) {
    for (const cut of [40, 100, 1000]) {
      const ab = Reflect.construct(ArrayBuffer, [0, { maxByteLength: 1 << 16 }]) as Resizable, s = stream()
      const packet = (from: number, to: number): Uint8Array => { ab.resize(to - from); const v = new Uint8Array(ab); v.set(packed.subarray(from, to)); return v }
      const out = [s.push(packet(0, cut))]
      for (let at = cut; at < packed.length; at += 2 * cut) out.push(s.push(packet(at, Math.min(at + 2 * cut, packed.length))))
      assert.deepEqual(cat([...out, s.finish()]), data, `${stream.name}, cut at ${cut}`)
    }
  }
})

test('a view another thread grows during a call never breaks it with an error of the wrong kind', async () => {
  // Grows 4 KB a microsecond from the moment it's told to until the call is over. 3.5 MB fills a 4 MB buffer most of
  // the way, so a length read twice soon runs past it.
  const worker = new Worker(`const { parentPort } = require('node:worker_threads')
  parentPort.on('message', ({ sab, go }) => {
    Atomics.store(go, 0, 1); Atomics.notify(go, 0)
    while (Atomics.load(go, 0) === 1 && sab.byteLength + 4096 <= sab.maxByteLength) {
      sab.grow(sab.byteLength + 4096)
      for (const t = performance.now() + 0.001; performance.now() < t;);
    }
    parentPort.postMessage(0)
  })`, { eval: true })
  const n = 7 << 19, src = new Uint8Array(n).map((_, i) => (i * 2654435761) >>> 24)
  const calls: Record<string, (v: Uint8Array) => unknown> = {
    deflateStream: v => { const s = k.deflateStream({ level: 1 }); s.push(v); return s.finish() },
    inflateStream: v => { const s = k.inflateStream(); s.push(Uint8Array.of(1)); return s.push(v) },
    gunzipStream: v => { const s = k.gunzipStream(); s.push(Uint8Array.of(0x1F)); return s.push(v) },
    zip: v => k.zip({ a: v }, { level: 0 }),
  }
  try {
    for (const [name, call] of Object.entries(calls)) {
      for (let i = 0; i < 4; i++) {
        const sab = growable(n, n + (8 << 20)), v = new Uint8Array(sab), go = new Int32Array(new SharedArrayBuffer(4))
        v.set(src)
        const stopped = new Promise(r => worker.once('message', r))
        worker.postMessage({ sab, go })
        Atomics.wait(go, 0, 0)
        try { call(v) } catch (e) { assert.ok(e instanceof k.KokyuError, `${name}: ${String(e)}`) }
        Atomics.store(go, 0, 2)
        await stopped
      }
    }
  } finally {
    await worker.terminate()
  }
})

// Asks for up to 4 GiB of address space, so only with BIG=1
test('in JavaScriptCore no buffer is asked to pass the 4 GiB a typed array may hold', { skip: !process.env['BIG'] }, t => {
  try { execFileSync('bun', ['--version']) } catch { return t.skip('bun is not installed') }
  // Doubling from 3 bytes jumps from 3 GiB past 4 GiB, and a size guess can be any size at all
  const script = `const { grow } = await import('${new URL('../engine/shared.js', import.meta.url).href}')
const { inflateRawEnd } = await import('${new URL('../engine/inflate.js', import.meta.url).href}')
console.log(grow(new Uint8Array(3), 0, 2 ** 32).length, inflateRawEnd(Uint8Array.of(3, 0), { sizeHint: 2 ** 33 }).bytes.length)`
  assert.equal(String(execFileSync('bun', ['-e', script], { timeout: 120_000 })).trim(), `${2 ** 32} 0`)
})

test('a filter is called on its own with its own Date, in every reader alike', async () => {
  const data = sample.subarray(0, 3000), z = k.zip({ 'a.txt': data, 'b.txt': data }, { mtime })
  // Keeps an entry only when called without a `this`, and edits the Date it was handed
  function filter(this: unknown, e: { mtime: Date }): boolean { e.mtime.setFullYear(1999); return this === undefined }
  assert.deepEqual(Object.keys(k.unzip(z, { filter })), ['a.txt', 'b.txt'])
  assert.deepEqual(Object.keys(await k.unzipAsync(z, { filter })), ['a.txt', 'b.txt'])
  for (const chunks of [k.unzipStream({ filter }).finish(z), await k.unzipStreamAsync({ filter }).finish(z)]) {
    assert.deepEqual(chunks.filter(c => c.final).map(c => [c.entry.name, c.entry.mtime.getTime()]), [['a.txt', mtime.getTime()], ['b.txt', mtime.getTime()]])
  }
})

test('a name that starts with U+FEFF keeps it, so two names never fold into one', () => {
  const bom = String.fromCharCode(0xFEFF), x = Uint8Array.of(1), y = Uint8Array.of(2, 2)
  const z = k.zip({ [bom + 'a.txt']: [x, { comment: bom + 'note' }], 'a.txt': y }, { mtime })
  assert.deepEqual(k.unzip(z), { [bom + 'a.txt']: x, 'a.txt': y })
  assert.deepEqual(k.zipEntries(z).map(e => [e.name, e.comment]), [[bom + 'a.txt', bom + 'note'], ['a.txt', '']])
  assert.deepEqual(k.unzipStream().finish(z).filter(c => c.final).map(c => c.entry.name), [bom + 'a.txt', 'a.txt'])
  // Without the UTF-8 flag, a name that is valid UTF-8 still reads as UTF-8, its U+FEFF included
  const flagless = z.slice(), v = new DataView(flagless.buffer), cd = v.getUint32(flagless.length - 6, true)
  for (const at of [6, cd + 8]) v.setUint16(at, v.getUint16(at, true) & ~0x800, true)
  assert.deepEqual(Object.keys(k.unzip(flagless)), [bom + 'a.txt', 'a.txt'])
})

test('unzipAsync keeps exactly the entries its filter keeps, by position, when names repeat', async () => {
  const z = build([stored('x', Uint8Array.of(1, 2, 3, 4, 5)), stored('x', new Uint8Array(1 << 20).fill(66))])
  const filter = (e: k.ZipEntryInfo): boolean => (e.originalSize ?? Infinity) < 100
  assert.equal(k.unzip(z, { filter })['x']!.length, 5)
  assert.equal((await k.unzipAsync(z, { filter }))['x']!.length, 5)
  assert.equal((await k.unzipAsync(z))['x']!.length, 1 << 20)
})

const canonical = (lengths: number[]): number[] => {
  const count = new Array<number>(16).fill(0), next = new Array<number>(16).fill(0)
  for (const l of lengths) if (l) count[l]!++
  for (let len = 1, code = 0; len < 16; len++) next[len] = code = (code + count[len - 1]!) << 1
  return lengths.map(l => l ? next[l]!++ : 0)
}

// A dynamic block's fields: the code-length code's lengths by symbol, then the code lengths as [symbol, extra] steps
function dynamicFields(final: number, hlit: number, hdist: number, clen: Record<number, number>, steps: [number, number?][], data: Field[]): Field[] {
  const order = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15]
  const lengths = Array.from({ length: 19 }, (_, s) => clen[s] ?? 0), codes = canonical(lengths)
  let hclen = 19
  while (hclen > 4 && !lengths[order[hclen - 1]!]) hclen--
  return [
    [final, 1], [2, 2], [hlit - 257, 5], [hdist - 1, 5], [hclen - 4, 4],
    ...order.slice(0, hclen).map((s): Field => [lengths[s]!, 3]),
    ...steps.flatMap(([s, e = 0]): Field[] => [[codes[s]!, lengths[s]!, true], ...(s >= 16 ? [[e, [2, 3, 7][s - 16]!] as Field] : [])]),
    ...data,
  ]
}
const dynamicBlock = (...args: Parameters<typeof dynamicFields> extends [number, ...infer R] ? R : never): Uint8Array => bitsOf(dynamicFields(1, ...args))

test('deflate data that breaks a rule is refused as zlib refuses it, never decoded', () => {
  // 'a' and the end-of-block code at one bit each: 97 zeros, 1, 158 zeros, 1, then one zero distance length
  const lengths: [number, number?][] = [[18, 86], [1], [18, 127], [18, 9], [1], [0]]
  const a: Field = [0, 1, true], eob: Field = [1, 1, true], clen = { 0: 2, 1: 2, 18: 1 }
  const valid = dynamicBlock(257, 1, clen, lengths, [a, eob])
  assert.deepEqual(k.inflate(valid), Uint8Array.of(97))
  assert.deepEqual(new Uint8Array(inflateRawSync(valid)), Uint8Array.of(97))
  // 30 more zeros fill either 287 literal/length lengths or 31 distance lengths, one more than each alphabet has
  const longer: [number, number?][] = [...lengths.slice(0, -1), [18, 20]]
  const fixed = (...fields: Field[]): Uint8Array => bitsOf([[1, 1], [1, 2], ...fields])
  const cases: [string, Uint8Array, RegExp?][] = [
    ['a stored block whose length check fails', bitsOf([[1, 1], [0, 2], [0, 5], [4, 16], [4, 16], [97, 8], [98, 8], [99, 8], [100, 8]])],
    ['the reserved block type', bitsOf([[1, 1], [3, 2], [0, 13]])],
    ['287 literal/length codes', dynamicBlock(287, 1, clen, longer, [a, eob])],
    ['31 distance codes', dynamicBlock(257, 31, clen, longer, [a, eob])],
    ['a repeat with no length before it', dynamicBlock(257, 1, { ...clen, 16: 2, 18: 2 }, [[16, 0], [18, 83], ...lengths.slice(1)], [a, eob])],
    ['a repeat past the last length', dynamicBlock(257, 1, { ...clen, 17: 2, 18: 2 }, [...lengths.slice(0, -1), [17, 0]], [a, eob])],
    ['no end-of-block code', dynamicBlock(257, 1, clen, [[18, 86], [1], [1], [18, 127], [18, 9], [0]], [a, a, a])],
    ['an over-subscribed literal/length code', dynamicBlock(257, 1, clen, [[18, 86], [1], [1], [18, 127], [18, 8], [1], [0]], [a, eob])],
    ['an over-subscribed code-length code', dynamicBlock(257, 1, { 0: 1, 1: 1, 18: 1 }, lengths, [a, eob])],
    ['a code-length code with no codes', dynamicBlock(257, 1, {}, lengths, [a, eob]), /invalid Huffman code/],
    // The fixed code has patterns for literal/length 286 and 287 and for distances 30 and 31, which mean nothing
    ['literal/length symbol 286', fixed([0x91, 8, true], [0xC6, 8, true], [0, 7, true]), /invalid length or distance symbol/],
    ['distance symbol 30', fixed([0x91, 8, true], [1, 7, true], [30, 5, true], [0, 7, true]), /invalid length or distance symbol/],
  ]
  for (const [what, data, message] of cases) {
    assert.throws(() => inflateRawSync(data), what)
    for (const decode of [() => k.inflate(data), () => k.inflateStream().finish(data)]) {
      assert.ok(throwsCode(decode, 'invalid-data'), what)
      if (message) assert.throws(decode, message, what)
    }
  }
})

test('unzipStream never returns more of an entry than its header claims', () => {
  const body = k.deflate(new Uint8Array(1 << 20))
  const head = Uint8Array.from([...u32(0x04034b50), ...u16(20), ...u16(0), ...u16(8), ...u16(0), ...u16(0), ...u32(0), ...u32(body.length), ...u32(10), ...u16(1), ...u16(0), 97])
  const deflated = cat([head, body]), stored = build([{ name: 's', body: new Uint8Array(100), method: 0, usize: 10 }])
  const small = { filter: (e: k.ZipEntryInfo) => (e.originalSize ?? Infinity) < 1000 }
  const cases = [['deflated', deflated, 512, {}], ['deflated, filtered', deflated, 512, small], ['stored', stored, 8, {}]] as const
  for (const [what, archive, size, opts] of cases) {
    const s = k.unzipStream(opts)
    let got = 0
    const code = codeOf(() => { for (let i = 0; i < archive.length; i += size) for (const c of s.push(archive.subarray(i, i + size))) got += c.data.length })
    assert.equal(code, 'invalid-data', what)
    assert.ok(got <= 10, `${what}: returned ${got} bytes`)
  }
})

test('unzipStream fails an entry that ends short of the size its header claims, rather than returning less', () => {
  const data = sample.subarray(0, 5000)
  for (const level of [0, 6] as const) {
    // One byte more than the entry holds, in the local header, which is all a stream reads
    const z = k.zip({ a: data, b: Uint8Array.of(1) }, { level })
    new DataView(z.buffer).setUint32(22, data.length + 1, true)
    assert.equal(codeOf(() => pieces(k.unzipStream(), z, 1000)), 'invalid-data', `level ${level}, in pieces`)
    assert.equal(codeOf(() => k.unzipStream().finish(z)), 'invalid-data', `level ${level}, in one call`)
    // The same archive, honest, reads back
    new DataView(z.buffer).setUint32(22, data.length, true)
    assert.deepEqual(joinChunks(k.unzipStream().finish(z))['a'], data, `level ${level}, honest`)
  }
})

// A zipStream() archive: every entry's sizes follow its data, in a data descriptor
function streamed(files: [string, Uint8Array][]): { zip: Uint8Array; ends: number[] } {
  const s = k.zipStream({ mtime }), parts: Uint8Array[] = [], ends: number[] = []
  for (const [name, data] of files) {
    const w = s.file(name)
    parts.push(w.push(data), w.finish())
    ends.push(cat(parts).length)
  }
  parts.push(s.finish())
  return { zip: cat(parts), ends }
}

test('a stream leaves out the sizes it doesn\'t know, so a size filter isn\'t told zero', async () => {
  const { zip } = streamed([['big.bin', new Uint8Array(1 << 20)], ['noise.bin', randomBytes(20000)]])
  const seen: k.ZipEntryInfo[] = []
  const small = (e: k.ZipEntryInfo): boolean => { seen.push(e); return (e.originalSize ?? Infinity) < 1000 }
  assert.deepEqual(k.unzipStream({ filter: small }).finish(zip), [])
  // In a worker the filter answers each header from here, with the same view of it
  assert.deepEqual(await k.unzipStreamAsync({ filter: small }).finish(zip), [])
  assert.equal(seen.length, 4)
  assert.ok(seen.every(e => !('size' in e) && !('originalSize' in e)), JSON.stringify(seen))
  // The central directory has them
  const sizes: Record<string, number | undefined> = {}
  k.unzip(zip, { filter: e => { sizes[e.name] = e.originalSize; return false } })
  assert.deepEqual(sizes, { 'big.bin': 1 << 20, 'noise.bin': 20000 })
})

test('skipping an entry whose sizes follow its data costs its input, not what it decodes to', () => {
  const big = new Uint8Array(16 << 20), next = k.encodeText('next entry')
  // Zeros decode through matches, noise through stored blocks, and 16 values through literals and short matches
  const noise = randomBytes(1 << 20), sixteen = noise.map(b => b & 15)
  const { zip, ends } = streamed([['big.bin', big], ['noise.bin', noise], ['sixteen.bin', sixteen], ['next.txt', next]])
  const skip = (e: k.ZipEntryInfo): boolean => e.name === 'next.txt'
  let chunks: k.UnzipChunk[] = []
  // Decoding the skipped entry to find its end would allocate at least 8 MB here, for big.bin's 17 KB
  const bytes = totalAllocation(() => { chunks = pieces(k.unzipStream({ filter: skip }), zip, 4096).flat() })
  assert.ok(bytes < 1 << 20, `allocated ${bytes} bytes for a ${zip.length} B archive`)
  assert.deepEqual(chunks.map(c => [c.entry.name, c.data.length, c.final]), [['next.txt', next.length, false], ['next.txt', 0, true]])
  // The skipped entry's descriptor is still held to what its data decoded to
  const bad = zip.slice()
  bad[ends[0]! - 4]! ^= 1
  assert.ok(throwsCode(() => k.unzipStream({ filter: skip }).finish(bad), 'invalid-data'))
  chunks = k.unzipStream().finish(zip)
  assert.equal(chunks.filter(c => c.entry.name === 'big.bin').reduce((n, c) => n + c.data.length, 0), big.length)
  // Literals alone never pass a match to outrun the buffer, so they show any write that slipped through.
  // zlib's Huffman-only strategy writes nothing else.
  const literals = deflateRawSync(new Uint8Array(1 << 20), { strategy: constants.Z_HUFFMAN_ONLY })
  const inf = inflater({ discard: true })
  const used = totalAllocation(() => {
    for (let i = 0; i < literals.length; i += 4096) inf.push(literals.subarray(i, i + 4096), i + 4096 >= literals.length)
  })
  assert.ok(used < 2 * literals.length, `allocated ${used} bytes for ${literals.length} B of literals`)
  assert.equal(inf.skipped, 1 << 20)
})

test('ZIP64 sizes must lie inside their extra field', () => {
  const body = Uint8Array.of(104, 105)
  // Both sizes say "see ZIP64", but the field holds one value; the next bytes belong to the end record
  const short = build([{ ...stored('h', body), csizeC: 0xFFFFFFFF, usizeC: 0xFFFFFFFF, extraC: [1, 0, 8, 0, ...u32(2), ...u32(0)] }], ascii('comment!'))
  assert.ok(throwsCode(() => k.unzip(short), 'invalid-data'))
  // A field running past the extra area
  const long = build([{ ...stored('h', body), usizeC: 0xFFFFFFFF, extraC: [1, 0, 16, 0, ...u32(2), ...u32(0)] }])
  assert.ok(throwsCode(() => k.unzip(long), 'invalid-data'))
  // A whole field still works, and so does one that only holds what overflowed. Deflated, so the two sizes differ
  // and reading them in the wrong order fails.
  const packed = k.deflate(body), deflated = { name: 'h', body: packed, method: 8, usize: body.length, crc: k.crc32(body) }
  const whole = build([{ ...deflated, csizeC: 0xFFFFFFFF, usizeC: 0xFFFFFFFF, extraC: [1, 0, 16, 0, ...u32(body.length), ...u32(0), ...u32(packed.length), ...u32(0)] }])
  const partial = build([{ ...deflated, csizeC: 0xFFFFFFFF, extraC: [1, 0, 8, 0, ...u32(packed.length), ...u32(0)] }])
  for (const ok of [whole, partial]) assert.deepEqual(k.unzip(ok)['h'], body)
  const head = Uint8Array.from([...u32(0x04034b50), ...u16(20), ...u16(0), ...u16(0), ...u16(0), ...u16(0), ...u32(0), ...u32(0xFFFFFFFF), ...u32(2), ...u16(1), ...u16(4), 104, 1, 0, 0, 0])
  assert.ok(throwsCode(() => k.unzipStream().push(head), 'invalid-data'))
})

test('a decoder stream that threw rethrows the same error, and never decodes on', () => {
  const text = k.encodeText('the quick brown fox jumps over the lazy dog, again and again, '.repeat(2000))
  const kinds: [string, () => k.Stream, (d: Uint8Array) => Uint8Array, number][] = [
    ['inflate', () => k.inflateStream(), d => k.deflate(d), 0],
    ['unzlib', () => k.unzlibStream(), d => k.zlib(d), 2],
    ['gunzip', () => k.gunzipStream(), d => k.gzip(d), 10],
    ['decompress', () => k.decompressStream(), d => k.gzip(d), 10],
  ]
  for (const [name, make, encode, skip] of kinds) {
    let failures = 0
    const good = encode(text)
    for (let seed = 1; seed <= 200; seed++) {
      const bad = good.slice()
      bad[skip + (seed * 7919) % (bad.length - skip - 8)]! ^= 1 << (seed % 8)
      const s = make()
      let first: unknown = null
      for (let i = 0; i < bad.length; i += 13) {
        let out: Uint8Array | undefined
        try {
          out = s.push(bad.subarray(i, i + 13))
        } catch (e) {
          if (first === null) { first = e; failures++ } else assert.equal(e, first, `${name} seed ${seed}: a different error`)
        }
        assert.ok(first === null || out === undefined, `${name} seed ${seed}: went on after failing`)
      }
      if (first !== null) assert.throws(() => s.finish(), (e: unknown) => e === first)
    }
    assert.ok(failures > 10, `${name}: only ${failures} corruptions failed`)
  }
  // unzipStream: an encrypted entry fails the stream for good, rather than dropping the entries after it
  const z = k.zip({ a: k.encodeText('AAAA'), b: k.encodeText('BBBB') }, { level: 0, mtime })
  z[6]! |= 1
  const u = k.unzipStream()
  assert.ok(throwsCode(() => u.push(z.subarray(0, 40)), 'unsupported'))
  assert.ok(throwsCode(() => u.push(z.subarray(40)), 'unsupported'))
})

test('zipStreamAsync refuses a second entry or an early finish the way zipStream does, and recovers the same', async () => {
  const z = k.zipStreamAsync({ mtime }), a = z.file('a'), parts: Uint8Array[] = []
  parts.push(await a.push(new Uint8Array(20000).fill(65)))
  const b = z.file('b')
  await assert.rejects(b.finish(new Uint8Array(20000).fill(66)), /finish the open zip entry/)
  await assert.rejects(z.finish(), /finish the open zip entry before the archive/)
  parts.push(await a.finish(Uint8Array.of(65)))
  parts.push(await z.finish())
  const files = k.unzip(cat(parts))
  assert.deepEqual(Object.keys(files), ['a'])
  assert.deepEqual(files['a'], new Uint8Array(20001).fill(65))
  // An entry the worker refuses (a bad date) doesn't block the next, even left unfinished
  const y = k.zipStreamAsync({ mtime }), bad = y.file('x', { mtime: new Date(1900, 0, 1) })
  await assert.rejects(bad.push(new Uint8Array(20000)), /1980/)
  const ok = y.file('x')
  const tail = [await ok.finish(new Uint8Array(20000)), await y.finish()]
  assert.equal(k.unzip(cat(tail))['x']!.length, 20000)
  // A refused chunk leaves the entry open, so finishing it again works, as in zipStream: the archive is never cut short
  for (const size of [10, 20000]) {
    const w = k.zipStreamAsync({ mtime }), e = w.file('a'), out = [await e.push(new Uint8Array(size).fill(65))]
    await assert.rejects(e.finish('not bytes' as never), { code: 'invalid-option' })
    out.push(await e.finish(Uint8Array.of(66)), await w.finish())
    assert.equal(k.unzip(cat(out))['a']!.length, size + 1, `${size} B`)
  }
})

test('async calls take their options when called: later changes to a buffer or Date stay out', async () => {
  const ext = Uint8Array.of(1, 2, 3, 4)
  const p = k.zipAsync({ f: [new Uint8Array(40000), { extra: { 0x6666: ext } }] }, { mtime })
  const d = new Date(2020, 0, 1)
  const g = k.gzipAsync(new Uint8Array(40000), { mtime: d })
  const s = k.zipStreamAsync({ mtime }), ext2 = Uint8Array.of(1, 2, 3, 4), w = s.file('f', { extra: { 0x6666: ext2 } })
  const dict = new Uint8Array(100).fill(3), ds = k.deflateStreamAsync({ dictionary: dict })
  ext[0] = 99; ext2[0] = 99; d.setFullYear(2001); dict.fill(4)
  const z = await p, dv = new DataView(z.buffer)
  const cd = dv.getUint32(z.length - 6, true)
  assert.equal(z[cd + 46 + 1 + 4], 1)
  assert.equal(new Date(new DataView((await g).buffer).getUint32(4, true) * 1000).getFullYear(), 2020)
  const parts = [await w.push(new Uint8Array(40000)), await w.finish(), await s.finish()]
  assert.equal(cat(parts)[30 + 1 + 4], 1)
  const packed = cat([await ds.push(new Uint8Array(40000).fill(3)), await ds.finish()])
  assert.deepEqual(packed, k.deflate(new Uint8Array(40000).fill(3), { dictionary: new Uint8Array(100).fill(3) }))
})

test('streams take their options when made: a buffer, Date or options object changed later changes nothing', async () => {
  const dict = k.encodeText('a shared dictionary with words the data reuses '.repeat(20))
  const data = k.encodeText('words the data reuses, a shared dictionary '.repeat(500))
  const raw = k.deflate(data, { dictionary: dict }), z = k.zlib(data, { dictionary: dict })
  // Raw deflate has no dictionary check, so a dictionary read at the first push would decode the wrong bytes silently
  for (const [make, input] of [[k.decompressStream, raw], [k.decompressStream, z], [k.unzlibStream, z], [k.inflateStream, raw]] as const) {
    const reused = dict.slice(), s = make({ dictionary: reused })
    reused.fill(32)
    assert.deepEqual(s.finish(input), data, `${make.name}, buffer reused`)
    const opts: k.InflateOptions = { dictionary: dict }, t = make(opts)
    delete opts.dictionary
    assert.deepEqual(t.finish(input), data, `${make.name}, options changed`)
    assert.ok(throwsCode(() => make({ dictionary: new ArrayBuffer(8) as unknown as Uint8Array }), 'invalid-option'), make.name)
  }
  // zipStream's archive-wide level and date, as zipStreamAsync takes them
  const when = new Date(2020, 0, 1), zopts: k.ZipOptions = { mtime: when, level: 9 }
  const s = k.zipStream(zopts), sa = k.zipStreamAsync(zopts), entry = new Uint8Array(5000)
  when.setFullYear(2030)
  zopts.level = 0
  const sync = cat([s.file('a').finish(entry), s.finish()])
  assert.deepEqual(sync, cat([await sa.file('a').finish(entry), await sa.finish()]))
  const head = new DataView(sync.buffer)
  assert.deepEqual([head.getUint16(8, true), 1980 + (head.getUint16(12, true) >> 9)], [8, 2020])
  // A latin1 text stream stays latin1, whatever its options object says later
  const latin1 = { latin1: true }, enc = k.encodeTextStream(latin1), dec = k.decodeTextStream(latin1)
  latin1.latin1 = false
  assert.deepEqual([...enc.push('é'), ...enc.finish('é')], [0xE9, 0xE9])
  assert.equal(dec.push(Uint8Array.of(0xE9)) + dec.finish(Uint8Array.of(0xE9)), 'éé')
  // An async stream made without transfer never starts moving the caller's chunks
  const late: k.AsyncOptions = {}, ds = k.deflateStreamAsync(late), chunk = new Uint8Array(40000).fill(1)
  late.transfer = true
  await ds.push(chunk)
  assert.equal(chunk.length, 40000)
  await ds.finish()
})

test('transfer takes the buffer as the call is made, so writing to it afterwards changes nothing', async () => {
  const fresh = (n: number): Uint8Array => new Uint8Array(n).map((_, i) => i % 251)
  const want = fresh(40000), a = fresh(40000), b = fresh(40000), c = fresh(40000)
  const one = k.deflateAsync(a, { transfer: true })
  // One buffer under two names and in a view: the view is copied before the buffer moves, and it moves once
  const zipped = k.zipAsync({ x: b, y: b, z: [b.subarray(7), { extra: { 0x6666: b.subarray(0, 4) } }] }, { transfer: true, mtime })
  const s = k.deflateStreamAsync({ transfer: true }), pushed = s.push(c)
  // Taken now, not when posted: otherwise a reader refilling one for its next chunk would change this one's result
  assert.deepEqual([a.length, b.length, c.length], [0, 0, 0])
  assert.deepEqual(k.inflate(await one), want)
  assert.deepEqual(await zipped, k.zip({ x: want, y: want, z: [want.subarray(7), { extra: { 0x6666: want.subarray(0, 4) } }] }, { mtime }))
  assert.deepEqual(k.inflate(cat([await pushed, await s.finish()])), want)
  // A chunk small enough to run on this thread is copied, not moved, as the option promises
  const small = fresh(1000), g = k.gzipStreamAsync({ transfer: true }).finish(small)
  assert.equal(small.length, 1000)
  small.fill(0)
  assert.deepEqual(k.gunzip(await g), want.subarray(0, 1000))
  // WebAssembly memory can't move, so it's copied rather than failing the call
  const view = new Uint8Array(new WebAssembly.Memory({ initial: 1 }).buffer)
  view.set(want)
  assert.deepEqual(k.inflate(await k.deflateAsync(view, { transfer: true })), view)
  // A push after finish() is refused before its chunk is taken, whether finish ran on this thread or in a worker
  for (const last of [fresh(100), fresh(40000)]) {
    const t = k.deflateStreamAsync({ transfer: true }), done = t.finish(last), early = fresh(20000)
    await assert.rejects(t.push(early), { code: 'invalid-state' })
    await done
    const late = fresh(20000)
    await assert.rejects(t.push(late), { code: 'invalid-state' })
    assert.deepEqual([early.length, late.length], [20000, 20000], `finishing on ${last.length} B`)
  }
})

test('an async stream\'s finish with a refused chunk leaves it open, so a good finish still ends it', async () => {
  for (const size of [100, 20000]) {
    const s = k.deflateStreamAsync(), d = randomBytes(size), head = await s.push(d)
    await assert.rejects(s.finish('not bytes' as never), { code: 'invalid-option' })
    assert.deepEqual(k.inflate(cat([head, await s.finish()])), d, `${size} B`)
  }
})

test('a signal aborted while an async call is being made rejects it, as one aborted before', async () => {
  const big = randomBytes(40000), c = new AbortController(), d = new AbortController()
  // An entry option read in the call, and an unzip filter, are caller code that can abort
  const opts = { get level() { c.abort(); return 6 } } as k.ZipEntryOptions
  await assert.rejects(k.zipAsync({ a: [big, opts] }, { signal: c.signal }), e => e === c.signal.reason)
  await assert.rejects(k.unzipAsync(k.zip({ a: big }), { signal: d.signal, filter: () => { d.abort(); return true } }), e => e === d.signal.reason)
})

test('an async stream whose options can\'t be sent to a worker says so on every call', async () => {
  const s = k.zlibStreamAsync({ level: () => 0 } as unknown as k.DeflateOptions)
  await assert.rejects(s.push(new Uint8Array(20000)), /can't be sent to a worker/)
  await assert.rejects(s.finish(), /can't be sent to a worker/)
})

test('async one-shots reject on a bad argument rather than throwing', async () => {
  const calls: [string, () => Promise<unknown>][] = [
    ['deflateAsync(null)', () => k.deflateAsync(null as unknown as Uint8Array)],
    ['gunzipAsync(data, null)', () => k.gunzipAsync(new Uint8Array(1), null as unknown as undefined)],
    ['zipAsync with a number', () => k.zipAsync({ a: 1 as unknown as Uint8Array })],
    ['unzipAsync(null)', () => k.unzipAsync(null as unknown as Uint8Array)],
  ]
  for (const [what, call] of calls) await assert.rejects(called(call, what), what)
})

test('options that aren\'t objects are ignored by the async calls too, as by the sync ones', async () => {
  const big = randomBytes(40000)
  for (const odd of [5, 'x', true] as never[]) {
    assert.deepEqual(await k.deflateAsync(big, odd), k.deflate(big, odd), String(odd))
    assert.deepEqual(await k.unzipAsync(await k.zipAsync({ a: big }, odd), odd), { a: big }, String(odd))
    const s = k.gzipStreamAsync(odd)
    assert.deepEqual(k.gunzip(cat([await s.push(big), await s.finish()])), big, String(odd))
    const z = k.zipStreamAsync(odd), w = z.file('a', odd)
    assert.deepEqual(k.unzip(cat([await w.finish(big), await z.finish()])), { a: big }, String(odd))
  }
})

test('zip input must be bytes, [bytes, options] or a folder, each path once', async () => {
  const instance = new (class Folder {})()
  for (const bad of [{ 'a.txt': 'hello' }, { n: 5 }, { d: new Date() }, { m: new Map() }, { c: instance }, { a: [1, {}] }, { a: null }, [Uint8Array.of(1)], instance, 5, null]) {
    assert.ok(throwsCode(() => k.zip(bad as unknown as k.ZipInput), 'invalid-option'), JSON.stringify(bad))
    assert.equal(await codeOfAsync(() => k.zipAsync(bad as unknown as k.ZipInput)), 'invalid-option')
  }
  assert.equal(k.unzip(k.zip({ a: [Uint8Array.of(1)] as unknown as [Uint8Array, k.ZipEntryOptions] }))['a']![0], 1)
  for (const dup of [{ 'a/b': Uint8Array.of(1), a: { b: Uint8Array.of(2) } }, { 'a/': {}, a: {} }]) {
    assert.ok(throwsCode(() => k.zip(dup), 'invalid-option'))
    assert.equal(await codeOfAsync(() => k.zipAsync(dup)), 'invalid-option')
  }
  const s = k.zipStream()
  s.file('a').finish()
  assert.ok(throwsCode(() => s.file('a'), 'invalid-option'))
  // A folder of nested entries in a worker: the same archive as on this thread
  const tree = { top: { mid: { leaf: new Uint8Array(30000).fill(1) }, empty: {} }, '0': Uint8Array.of(2) }
  assert.deepEqual(await k.zipAsync(tree, { mtime }), k.zip(tree, { mtime }))
})

test('zip levels, os and attrs out of range are refused, never truncated', () => {
  const data = new Uint8Array(100)
  const refused: [string, () => unknown][] = [
    ['level -1', () => k.zip({ a: data }, { level: -1 as k.Level })],
    ['level 99 on an empty file', () => k.zip({ a: new Uint8Array(0) }, { level: 99 as k.Level })],
    ['level 1.5 in a folder', () => k.zip({ a: {} }, { level: 1.5 as k.Level })],
    ['zipStream level -1', () => k.zipStream().file('a', { level: -1 as k.Level })],
    ['os 300', () => k.zip({ a: [data, { os: 300 }] })],
    ['attrs 2^32 + 5', () => k.zip({ a: [data, { attrs: 2 ** 32 + 5 }] })],
    ['attrs 1.5', () => k.zip({ a: [data, { attrs: 1.5 }] })],
    ['attrs -2^31 - 1', () => k.zip({ a: [data, { attrs: -(2 ** 31) - 1 }] })],
  ]
  for (const [what, fn] of refused) assert.ok(throwsCode(fn, 'invalid-option'), what)
  assert.doesNotThrow(() => k.zip({ a: [data, { os: 255 }] }), 'os 255')
  for (const attrs of [2 ** 32 - 1, -(2 ** 31)]) assert.doesNotThrow(() => k.zip({ a: [data, { attrs }] }), `attrs ${attrs}`)
  // A Unix mode shifted up is negative in JavaScript, and keeps its bits
  const z = k.zip({ a: [data, { os: 3, attrs: 0o100644 << 16 }] })
  const cd = new DataView(z.buffer).getUint32(z.length - 6, true)
  assert.equal(new DataView(z.buffer).getUint32(cd + 38, true), (0o100644 << 16) >>> 0)
  assert.equal(z[cd + 5], 3)
})

test('text and zip options of the wrong type are read the way the rest of the API reads them, or refused', () => {
  const g = k.gzip(Uint8Array.of(1), { filename: 123 as unknown as string })
  assert.deepEqual([...g.subarray(10, 14)], [49, 50, 51, 0])
  assert.deepEqual(k.encodeText(123 as unknown as string, { latin1: true }), k.encodeText(123 as unknown as string))
  const symbol = Symbol('s') as unknown as string
  assert.throws(() => k.encodeText(symbol), TypeError)
  assert.throws(() => k.encodeText(symbol, { latin1: true }), TypeError)
  // A refusal's message names the value, which must not itself throw
  assert.ok(throwsCode(() => k.zip({ a: new Uint8Array(1) }, { level: symbol as unknown as k.Level }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zip({ a: [new Uint8Array(1), { os: symbol as unknown as number }] }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zip({ a: [new Uint8Array(1), { attrs: symbol as unknown as number }] }), 'invalid-option'))
  // A caller's ZIP64 field would tell readers to expect 8-byte descriptor sizes after kokyu's 4-byte ones
  assert.ok(throwsCode(() => k.zipStream().file('a', { extra: { 1: new Uint8Array(16) } }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zip({ a: [Uint8Array.of(1), { extra: { 1: new Uint8Array(16) } }] }), 'invalid-option'))
  for (const extra of [5, true, 'ab']) {
    assert.ok(throwsCode(() => k.zip({ a: [Uint8Array.of(1), { extra: extra as unknown as Record<number, Uint8Array> }] }), 'invalid-option'), String(extra))
  }
  // undefined is TextEncoder's default, '', for every encoding and stream; other values go through ToString
  assert.equal(k.encodeText(undefined as unknown as string, { latin1: true }).length, 0)
  for (const latin1 of [false, true]) {
    const s = k.encodeTextStream({ latin1 }), odd = [123, undefined, null] as unknown as string[]
    assert.equal(k.decodeText(cat([...odd.map(v => s.push(v)), s.finish()])), '123null')
    assert.throws(() => k.encodeTextStream({ latin1 }).push(symbol), TypeError)
  }
})

test('bytes that aren\'t a readable Uint8Array are refused everywhere, never read as nothing', async () => {
  const data = k.encodeText('hello hello hello hello'), gz = k.gzip(data), d = k.deflate(data)
  const ab = data.buffer.slice(0)
  // Views with nothing left to read: a buffer transferred away, and one resized out from under its view
  const gone = Uint8Array.of(1, 2, 3)
  structuredClone(gone.buffer, { transfer: [gone.buffer] })
  const resizable = Reflect.construct(ArrayBuffer, [8, { maxByteLength: 16 }]) as ArrayBuffer & { resize(n: number): void }
  const shrunk = new Uint8Array(resizable, 4)
  resizable.resize(2)
  for (const bad of [ab, new Int8Array(4), new Uint16Array(4), new DataView(ab), 'text', gone, shrunk] as unknown as Uint8Array[]) {
    const calls: [string, () => unknown][] = [
      ['deflate', () => k.deflate(bad)], ['inflate', () => k.inflate(bad)], ['zlib', () => k.zlib(bad)], ['unzlib', () => k.unzlib(bad)],
      ['gzip', () => k.gzip(bad)], ['gunzip', () => k.gunzip(bad)], ['decompress', () => k.decompress(bad)], ['unzip', () => k.unzip(bad)],
      ['crc32', () => k.crc32(bad)], ['adler32', () => k.adler32(bad)],
      ['decodeText', () => k.decodeText(bad)], ['decodeText latin1', () => k.decodeText(bad, { latin1: true })],
      ['decodeTextStream', () => k.decodeTextStream().push(bad)], ['decodeTextStream latin1', () => k.decodeTextStream({ latin1: true }).push(bad)],
      ['zipStream entry', () => k.zipStream().file('a').push(bad)], ['unzipStream', () => k.unzipStream().push(bad)],
      // Each of these would go on as if the chunk had been empty
      ['gunzipStream, after a member', () => { const s = k.gunzipStream(); s.push(gz); return s.push(bad) }],
      ['decompressStream, after one byte', () => { const s = k.decompressStream(); s.push(gz.subarray(0, 1)); return s.push(bad) }],
      ['deflate dictionary', () => k.deflate(data, { dictionary: bad })], ['inflate dictionary', () => k.inflate(d, { dictionary: bad })],
      ['inflateInto out', () => k.inflateInto(d, bad)], ['gunzipInto out', () => k.gunzipInto(gz, bad)],
      // Even where the data needs no dictionary, as the streams refuse one when they're made
      ['unzlib dictionary', () => k.unzlib(k.zlib(data), { dictionary: bad })], ['decompress dictionary', () => k.decompress(gz, { dictionary: bad })],
      ['zip input', () => k.zip({ a: bad })], ['zip extra', () => k.zip({ a: [data, { extra: { 0x5455: bad } }] })],
    ]
    for (const [name, open] of [['deflate', k.deflateStream], ['inflate', k.inflateStream], ['zlib', k.zlibStream], ['unzlib', k.unzlibStream],
      ['gzip', k.gzipStream], ['gunzip', k.gunzipStream], ['decompress', k.decompressStream]] as const) {
      calls.push([`${name}Stream push`, () => open().push(bad)], [`${name}Stream finish`, () => open().finish(bad)])
    }
    for (const [what, call] of calls) assert.ok(throwsCode(call, 'invalid-option'), `${what}(${bad.constructor.name})`)
    const async: [string, () => Promise<unknown>][] = [
      ['deflateAsync', () => k.deflateAsync(bad)], ['unzipAsync', () => k.unzipAsync(bad)], ['zipAsync', () => k.zipAsync({ a: bad })],
      ['zipAsync with options', () => k.zipAsync({ a: [bad, {}] })],
      ['deflateStreamAsync', () => k.deflateStreamAsync().push(bad)], ['unzipStreamAsync', () => k.unzipStreamAsync().finish(bad)],
      ['zipStreamAsync', () => k.zipStreamAsync().file('a').push(bad)],
      ['zlibAsync dictionary', () => k.zlibAsync(data, { dictionary: bad })],
      ['zlibAsync dictionary, in a worker', () => k.zlibAsync(new Uint8Array(40000), { dictionary: bad })],
    ]
    for (const [what, call] of async) {
      const promise = called(call, what)
      assert.equal(await codeOfAsync(() => promise), 'invalid-option', `${what}(${bad.constructor.name})`)
    }
  }
  // The refusal names the entry, by its full path
  for (const input of [{ 'dir/x': gone }, { 'dir/x': [gone, {}] }] as unknown as k.ZipInput[]) {
    assert.throws(() => k.zip(input), /zip input "dir\/x" can't be read/)
  }
  // No bytes at all, too (an option left null or undefined is only absent)
  const oneShots = [k.deflate, k.inflate, k.zlib, k.unzlib, k.gzip, k.gunzip, k.decompress, k.unzip, k.crc32, k.adler32, k.decodeText]
  for (const call of oneShots as ((d: unknown) => unknown)[]) {
    for (const v of [null, undefined]) assert.ok(throwsCode(() => call(v), 'invalid-option'), `${call.name}(${String(v)})`)
  }
})

test('bytes, dates and objects from another realm read like this realm\'s', async () => {
  const make = runInNewContext('n => new Uint8Array(n)') as (n: number) => Uint8Array
  const foreign = (b: Uint8Array): Uint8Array => { const f = make(b.length); f.set(b); return f }
  const data = k.encodeText('hi hi hi hi'), f = foreign(data)
  assert.ok(!(f instanceof Uint8Array))
  const z = k.zip({ a: f, b: [f, { extra: { 0x5455: foreign(Uint8Array.of(1)) } }] }, { mtime })
  assert.deepEqual(k.unzip(foreign(z)), { a: data, b: data })
  assert.equal(k.crc32(f), k.crc32(data))
  assert.deepEqual(k.inflate(k.deflate(f)), data)
  // An async call takes its own copy at call time, wherever the buffer came from
  const big = sample.subarray(0, 40000), dict = sample.subarray(0, 1000), theirs = foreign(dict)
  const result = k.zlibAsync(foreign(big), { dictionary: theirs })
  theirs.fill(0)
  assert.deepEqual(await result, k.zlib(big, { dictionary: dict }))
  // transfer moves a buffer from another realm as it moves this realm's
  const moved = foreign(big)
  assert.deepEqual(k.inflate(await k.deflateAsync(moved, { transfer: true })), big)
  assert.equal(moved.length, 0)
  const date = runInNewContext('new Date(2020, 0, 1)') as Date, dated = k.gzipAsync(big, { mtime: date })
  date.setFullYear(2030)
  assert.deepEqual(await dated, k.gzip(big, { mtime }))
  // A Date where text goes reads as its text, as the sync calls read it, not as its time
  const odd = { filename: mtime as unknown as string, comment: mtime as unknown as string }
  assert.deepEqual(await k.gzipAsync(big, odd), k.gzip(big, odd))
  const entries = { a: [big, odd] as const }
  assert.deepEqual(await k.zipAsync(entries, { ...odd, mtime }), k.zip(entries, { ...odd, mtime }))
  // Plain objects too, as the input and as folders inside it
  const input = runInNewContext('b => ({ a: b, dir: { b: b }, empty: {} })')(f) as k.ZipInput
  const files = { a: data, 'dir/b': data, 'empty/': new Uint8Array(0) }
  assert.deepEqual(k.unzip(k.zip(input)), files)
  assert.deepEqual(k.unzip(k.zip({ x: input['dir'] as k.ZipInput })), { 'x/b': data })
  assert.deepEqual(k.unzip(await k.zipAsync(input)), files)
})

test('option objects read in a worker as the sync call reads them, though a structured clone drops their prototype', async () => {
  class Name { toString(): string { return 'report.txt' } }
  class Stamp { valueOf(): number { return Date.UTC(2020, 5, 1, 12) } }
  const big = randomBytes(40000), mtime = new Stamp() as unknown as Date, name = new Name() as unknown as string
  assert.deepEqual(await k.gzipAsync(big, { filename: name, mtime }), k.gzip(big, { filename: name, mtime }))
  const opts = { comment: name, mtime }, input = { a: [big, opts] as const }
  assert.deepEqual(await k.zipAsync(input, opts), k.zip(input, opts))
  const z = k.zipStreamAsync(opts), w = z.file('a', opts), s = k.zipStream(opts), sw = s.file('a', opts)
  assert.deepEqual(cat([await w.push(big), await w.finish(), await z.finish()]), cat([sw.push(big), sw.finish(), s.finish()]))
})

test('zip entry names must be strings, so 123 and "123" can\'t both get in', async () => {
  for (const name of [123, undefined, null, {}] as unknown as string[]) {
    assert.ok(throwsCode(() => k.zipStream().file(name), 'invalid-option'), String(name))
  }
  const s = k.zipStream()
  s.file('123').finish()
  assert.ok(throwsCode(() => s.file(123 as unknown as string), 'invalid-option'))
  assert.equal(await codeOfAsync(() => k.zipStreamAsync().file(123 as unknown as string).finish(Uint8Array.of(1))), 'invalid-option')
})

test('inflate and decompress ignore options that aren\'t theirs', () => {
  const d = k.deflate(new Uint8Array(1000).fill(7))
  const internal = { limit: 1, sizeHint: 2 ** 40, expected: 3, shared: false } as k.InflateOptions
  assert.equal(k.inflate(d, internal).length, 1000)
  assert.equal(k.decompress(d, internal).length, 1000)
})

test('unzipStream joins a header waiting in tiny pushes in linear time', () => {
  const head = new Uint8Array(30 + 65535 + 65535)
  head.set([...u32(0x04034b50), ...u16(20), ...u16(0), ...u16(0), ...u16(0), ...u16(0), ...u32(0), ...u32(0), ...u32(0), ...u16(65535), ...u16(65535)])
  const s = k.unzipStream()
  // Copying the held bytes again on every push would allocate about 1 GB here
  const bytes = totalAllocation(() => { for (let i = 0; i < head.length; i += 16) s.push(head.subarray(i, i + 16)) })
  assert.ok(bytes < 8 * head.length, `allocated ${bytes} bytes`)
})

test('an async stream that\'s collected unfinished closes its worker session', async () => {
  const registered: [object, () => void][] = []
  const real = globalThis.FinalizationRegistry
  globalThis.FinalizationRegistry = class<T> {
    constructor(private readonly cleanup: (held: T) => void) {}
    register(target: object, held: T): void { registered.push([target, () => this.cleanup(held)]) }
  } as unknown as FinalizationRegistryConstructor
  let pool: typeof import('../engine/pool.js')
  try {
    pool = await import(new URL('../engine/pool.js?collected', import.meta.url).href) as typeof import('../engine/pool.js')
  } finally {
    globalThis.FinalizationRegistry = real
  }
  const s = pool.deflateStreamAsync()
  await s.push(new Uint8Array(20000))
  const entry = registered.find(([t]) => t === s)
  assert.ok(entry, 'the stream was registered')
  entry[1]()
  await assert.rejects(s.push(new Uint8Array(10)), /already finished/)
})

test('a worker that can\'t start rejects its calls with a KokyuError coded worker, in every runtime', async () => {
  // A fresh pool, with no workers started yet
  const pool = await import(new URL('../engine/pool.js?blocked', import.meta.url).href) as typeof import('../engine/pool.js')
  const scope = globalThis as { Worker?: unknown }
  // As in a page whose policy forbids workers, or Node run with --permission but not --allow-worker
  scope.Worker = class { constructor() { throw new Error('workers are blocked') } }
  try {
    for (const call of [() => pool.deflateAsync(new Uint8Array(20000)), () => pool.gzipStreamAsync().finish(new Uint8Array(20000))]) {
      const e = await call().then(() => null, (e: unknown) => e)
      assert.ok(e instanceof k.KokyuError && e.code === 'worker' && e.message.includes('workers are blocked'), String(e))
    }
  } finally {
    delete scope.Worker
  }
})

test('empty blocks with 15-bit codes cost about what zlib takes for them, not 40 times more', () => {
  // 28 bytes each: literal/length and distance codes 15 bits deep, and only end-of-block. Lengths 2 to 15 and 15
  // again, with end-of-block at 1 bit, fill the literal code exactly; the distance code is 1 to 15 and 15.
  const clen: Record<number, number> = { 18: 4 }
  for (let l = 1; l <= 15; l++) clen[l] = 4
  const steps: [number, number?][] = [...Array.from({ length: 14 }, (_, s): [number] => [s + 2]), [15], [18, 127], [18, 92], [1]]
  for (let l = 1; l <= 15; l++) steps.push([l])
  steps.push([15])
  const blocks = 5000
  const data = bitsOf(Array.from({ length: blocks }, (_, i) => dynamicFields(+(i === blocks - 1), 257, 16, clen, steps, [[0, 1, true]])).flat())
  assert.equal(inflateRawSync(data).length, 0)
  const best = (fn: () => unknown): number => Math.min(...Array.from({ length: 3 }, () => { const t = performance.now(); fn(); return performance.now() - t }))
  const ours = best(() => k.inflate(data)), zlib = best(() => inflateRawSync(data))
  // Writing every slot of every code took 40–50x zlib's time; built width by width, about 4x
  assert.ok(ours < 15 * zlib + 20, `${ours.toFixed(1)} ms against zlib's ${zlib.toFixed(1)} ms`)
})

test('maxSize stops a decode into out the moment output passes it, writing nothing past it', () => {
  const zeros = new Uint8Array(1 << 20)
  const cases = [[k.inflateInto, k.deflate(zeros)], [k.unzlibInto, k.zlib(zeros)], [k.gunzipInto, k.gzip(zeros)], [k.decompressInto, k.gzip(zeros)]] as const
  for (const [into, packed] of cases) {
    const out = new Uint8Array(zeros.length).fill(0xAA)
    assert.ok(throwsCode(() => into(packed, out, { maxSize: 1024 }), 'too-large'), into.name)
    assert.ok(out.subarray(1024).every(b => b === 0xAA), `${into.name} wrote past maxSize`)
  }
  // Output that passes maxSize before the data is cut short fails the same way however it was pushed
  const text = k.encodeText('the quick brown fox jumps over the lazy dog. '.repeat(40)).subarray(0, 1800), packed = k.deflate(text)
  for (let cut = 1; cut <= 6; cut++) {
    const b = packed.subarray(0, packed.length - cut), s = k.inflateStream({ maxSize: 1100 })
    const pushed = codeOf(() => { for (let i = 0; i < b.length; i++) s.push(b.subarray(i, i + 1)); s.finish() })
    assert.equal(codeOf(() => k.inflate(b, { maxSize: 1100 })), pushed, `cut ${cut} bytes short`)
  }
})

test('a one-shot decode started inside another, from a getter on its input, leaves the outer one\'s tables alone', () => {
  // Upper case codes to the same lengths as lower case, so tables shared by mistake decode without an error
  const lower = k.encodeText(Array.from({ length: 4000 }, (_, i) => 'etaoinshrdlucmfwy'[(i * i * 7 + i) % 17]).join(''))
  const outer = k.deflate(lower), inner = k.deflate(k.encodeText(k.decodeText(lower).toUpperCase()))
  for (let at = 1, reads = 0; reads >= at - 1; at++) {
    reads = 0
    const input = new Uint8Array(outer)
    Object.defineProperty(input, 'length', { get: () => { if (++reads === at) k.inflate(inner); return outer.length } })
    assert.deepEqual(k.inflate(input), lower, `inner decode at length read ${at}`)
  }
})

test('an option read through a getter is read once, so a header always matches its data', () => {
  const a = sample.subarray(0, 2000), b = sample.subarray(2000, 4000), data = sample.subarray(4000, 9000)
  let reads = 0
  const z = k.zlib(data, { get dictionary() { return reads++ ? b : a } })
  assert.deepEqual(k.unzlib(z, { dictionary: a }), data)
  let names = 0
  const gz = k.gzip(data, { get filename() { return names++ ? '' : 'name.txt' } })
  assert.deepEqual([gz[3], k.decodeText(gz.subarray(10, 18))], [8, 'name.txt'])
})

test('decoder options set on Object.prototype, as a pollution bug elsewhere would, change nothing', () => {
  const text = sample.subarray(0, 3000), packed = k.deflate(text), gz = k.gzip(text), archive = k.zip({ a: text }, { mtime })
  const spy = new Uint8Array(1 << 16), proto = Object.prototype as Record<string, unknown>
  const polluted: Record<string, unknown> = { out: spy, discard: true, shared: true, sizeHint: 1e9 }
  for (const [key, value] of Object.entries(polluted)) Object.defineProperty(proto, key, { value, configurable: true, writable: true })
  try {
    const x = k.inflateStream(), y = k.inflateStream(), half = packed.length >> 1
    const interleaved = [x.push(packed.subarray(0, half)), y.push(packed.subarray(0, half)), x.finish(packed.subarray(half)), y.finish(packed.subarray(half))]
    const results: [string, Uint8Array][] = [
      ['inflate', k.inflate(packed)], ['gunzip', k.gunzip(gz)], ['gunzipStream', k.gunzipStream().finish(gz)],
      ['unzip', k.unzip(archive)['a']!], ['unzipStream', cat(k.unzipStream().finish(archive).map(c => c.data))],
      ['interleaved inflateStreams', cat([interleaved[0]!, interleaved[2]!])], ['the other one', cat([interleaved[1]!, interleaved[3]!])],
    ]
    for (const [what, got] of results) {
      assert.deepEqual(got, text, what)
      assert.notEqual(got.buffer, spy.buffer, what)
    }
  } finally {
    for (const key of Object.keys(polluted)) delete proto[key]
  }
  assert.ok(spy.every(v => v === 0), 'nothing was written into the polluted buffer')
})

test('error messages quote entry names, so a name can\'t forge a log line', () => {
  // Line breaks (LF, NEL, U+2028, U+2029), escape codes (ESC [ and its one-character C1 form), bidi controls and DEL
  const name = 'evil\n[admin] a\u0085[admin] b\u2028[admin] c\u2029[admin] d\u001b[2K\u009b2K\u202Edrowssap\u007F\u200F\u200E\u061C'
  // kokyu won't write control characters in a name, so they replace a placeholder of the same length in both headers
  const z = k.zip({ [name.replace(/[\0-\x1F\x7F]/g, 'x')]: new Uint8Array(1) }, { mtime })
  const placeholder = k.encodeText(name.replace(/[\0-\x1F\x7F]/g, 'x')), real = k.encodeText(name)
  for (let at = 0; (at = Buffer.from(z.buffer).indexOf(placeholder, at)) >= 0; at += real.length) z.set(real, at)
  const cd = new DataView(z.buffer).getUint32(z.length - 6, true)
  z[6]! |= 1
  z[cd + 8]! |= 1
  for (const fn of [() => k.unzip(z), () => k.unzipStream().push(z)]) {
    try {
      fn()
      assert.fail('should have thrown')
    } catch (e) {
      const m = (e as Error).message
      assert.ok(!/[\u0000-\u001F\u007F-\u009F\u061C\u200E\u200F\u2028\u2029\u202A-\u202E\u2066-\u2069]/.test(m), JSON.stringify(m))
      // The quoted name reads back exactly
      assert.equal(JSON.parse(/"(?:[^"\\]|\\.)*"/.exec(m)![0]), name)
    }
  }
  // Nor can a caller's option value, for a server that passes one through from a request: it shows only by its type
  const value = '7\n[admin] b\u001b[2K' as never, d = Uint8Array.of(1)
  const refused = [
    () => k.deflate(d, { level: value }), () => k.inflate(k.deflate(d), { maxSize: value }), () => k.zip({ a: [d, { mode: value }] }),
    () => k.zip({ a: [d, { os: value }] }), () => k.zip({ a: [d, { attrs: value }] }),
  ]
  for (const fn of refused) {
    const m = (() => { try { fn() } catch (e) { return (e as Error).message } return 'no error' })()
    assert.ok(!/[\u0000-\u001F]/.test(m) && m.endsWith('got string'), JSON.stringify(m))
  }
})
