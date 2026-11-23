import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as k from '../engine/index.js'
import {
  cat, chunked, failureCost, fullText, has, largestAllocation, pieces, python, randomBytes, same, sample, throwsCode,
} from './helpers.js'

test('the package exports exactly its public API, nothing internal', () => {
  assert.deepEqual(Object.keys(k).sort(), [
    'KokyuError', 'adler32', 'crc32', 'decodeText', 'decodeTextStream', 'decompress', 'decompressAsync', 'decompressInto',
    'decompressStream', 'decompressStreamAsync', 'deflate', 'deflateAsync', 'deflateStream', 'deflateStreamAsync', 'encodeText',
    'encodeTextStream', 'gunzip', 'gunzipAsync', 'gunzipInto', 'gunzipStream', 'gunzipStreamAsync', 'gzip', 'gzipAsync',
    'gzipStream', 'gzipStreamAsync', 'inflate', 'inflateAsync', 'inflateInto', 'inflateStream', 'inflateStreamAsync',
    'toTransformStream', 'unzip', 'unzipAsync', 'unzipStream', 'unzipStreamAsync', 'unzlib', 'unzlibAsync', 'unzlibInto',
    'unzlibStream', 'unzlibStreamAsync', 'zip', 'zipAsync', 'zipEntries', 'zipStream', 'zipStreamAsync', 'zlib', 'zlibAsync',
    'zlibStream', 'zlibStreamAsync',
  ])
})

const decoders = [
  ['inflate', k.deflate, k.inflate, k.inflateStream],
  ['unzlib', k.zlib, k.unzlib, k.unzlibStream],
  ['gunzip', k.gzip, k.gunzip, k.gunzipStream],
  ['decompress', k.gzip, k.decompress, k.decompressStream],
] as const

test('maxSize lets exactly that many bytes out, in one call and across a stream\'s pushes', () => {
  const d = sample
  for (const [name, pack, one, stream] of decoders) {
    const packed = pack(d)
    same(one(packed, { maxSize: d.length }), d, `${name} at the exact size`)
    assert.ok(throwsCode(() => one(packed, { maxSize: d.length - 1 }), 'too-large'), `${name} one byte under`)
    // Tiny pushes reuse the stream's buffer, so the cap has to count what earlier pushes returned
    same(chunked(stream({ maxSize: d.length }), packed, 200), d, `${name} stream at the exact size`)
    assert.ok(throwsCode(() => chunked(stream({ maxSize: d.length - 1 }), packed, 200), 'too-large'), `${name} stream one byte under`)
  }
})

test('with out and maxSize together, the smaller one caps the output', () => {
  const d = sample.subarray(0, 40_000), out = new Uint8Array(200_000)
  const got = k.inflateInto(k.deflate(d), out, { maxSize: 50_000 })
  same(got, d, 'fits under both')
  assert.equal(got.buffer, out.buffer, 'decoded in place')
  assert.ok(throwsCode(() => k.inflateInto(k.deflate(sample), out, { maxSize: 45_000 }), 'too-large'), 'maxSize is the smaller cap')
  assert.ok(throwsCode(() => k.gunzipInto(k.gzip(sample), new Uint8Array(45_000), { maxSize: 1e6 }), 'too-large'), 'out is the smaller cap')
  // A dictionary decodes outside `out` and takes its cap from out's length, so maxSize has to shorten out itself
  const dictionary = sample.subarray(0, 2000)
  for (const [pack, unpack] of [[k.deflate, k.inflateInto], [k.zlib, k.unzlibInto]] as const) {
    const packed = pack(sample, { dictionary })
    assert.ok(throwsCode(() => unpack(packed, out, { dictionary, maxSize: 45_000 }), 'too-large'), 'with a dictionary')
    same(unpack(packed, out, { dictionary, maxSize: sample.length }), sample, 'with a dictionary, at the exact size')
  }
})

test('a stream near its cap can\'t grow its buffer toward the whole cap', () => {
  const ds = k.deflateStream(), first = fullText.subarray(0, 600_000)
  const head = cat([ds.push(first), ds.flush()]), bomb = ds.finish(new Uint8Array(5_000_000))
  const s = k.inflateStream({ maxSize: 650_000 })
  same(chunked({ push: c => s.push(c), finish: () => new Uint8Array(0) }, head, 2000), first, 'the first 600 KB')
  const { code, largest } = failureCost(() => s.push(bomb))
  assert.equal(code, 'too-large')
  assert.ok(largest < 300_000, `allocated ${largest} bytes with 50 KB of the cap left`)
})

test('maxSize covers every member of a multi-member gzip together', () => {
  const half = sample.subarray(0, 25_000), two = cat([k.gzip(half), k.gzip(half)])
  same(k.gunzip(two, { maxSize: 50_000 }), cat([half, half]), 'one-shot at the exact size')
  assert.ok(throwsCode(() => k.gunzip(two, { maxSize: 49_999 }), 'too-large'), 'one-shot one byte under')
  same(chunked(k.gunzipStream({ maxSize: 50_000 }), two, 300), cat([half, half]), 'stream at the exact size')
  assert.ok(throwsCode(() => chunked(k.gunzipStream({ maxSize: 49_999 }), two, 300), 'too-large'), 'stream one byte under')
})

test('a decompression bomb with maxSize fails without allocating anything near its full size', () => {
  const bomb = k.deflate(new Uint8Array(10_000_000)), archive = k.zip({ 'big.bin': new Uint8Array(10_000_000) })
  assert.ok(bomb.length < 15_000 && archive.length < 15_000)
  const cases: [string, () => unknown][] = [
    ['inflate', () => k.inflate(bomb, { maxSize: 1_000_000 })],
    ['inflateStream', () => k.inflateStream({ maxSize: 1_000_000 }).push(bomb)],
    ['unzip', () => k.unzip(archive, { maxSize: 1_000_000 })],
    ['unzipStream', () => k.unzipStream({ maxSize: 1_000_000 }).push(archive)],
  ]
  for (const [name, run] of cases) {
    const { code, largest } = failureCost(run)
    assert.equal(code, 'too-large', name)
    assert.ok(largest < 3_000_000, `${name} allocated ${largest} bytes`)
  }
})

test('unzip and unzipStream cap the total of the entries they keep', () => {
  const files = { 'a.txt': sample, 'b.txt': sample, 'c.txt': sample }, archive = k.zip(files)
  const notC = (e: k.ZipEntryInfo): boolean => e.name !== 'c.txt'
  assert.ok(throwsCode(() => k.unzip(archive, { maxSize: 2 * sample.length }), 'too-large'))
  assert.deepEqual(Object.keys(k.unzip(archive, { maxSize: 2 * sample.length, filter: notC })), ['a.txt', 'b.txt'])
  const stream = (opts: k.UnzipStreamOptions): number => {
    const s = k.unzipStream(opts)
    return [...s.push(archive), ...s.finish()].reduce((n, c) => n + c.data.length, 0)
  }
  assert.ok(throwsCode(() => stream({ maxSize: 2 * sample.length }), 'too-large'))
  assert.equal(stream({ maxSize: 2 * sample.length, filter: notC }), 2 * sample.length)
})

test('maxSize reaches the async API, inline and in a worker', async () => {
  const small = sample.subarray(0, 4000), big = randomBytes(100_000)
  for (const d of [small, big]) {
    const at = `${d.length} B`
    same(await k.inflateAsync(k.deflate(d), { maxSize: d.length }), d, `inflateAsync, ${at}`)
    await assert.rejects(k.inflateAsync(k.deflate(d), { maxSize: d.length - 1 }), { code: 'too-large' }, `inflateAsync, ${at}`)
    await assert.rejects(k.gunzipAsync(k.gzip(d), { maxSize: d.length - 1 }), { code: 'too-large' }, `gunzipAsync, ${at}`)
    await assert.rejects(k.inflateStreamAsync({ maxSize: d.length - 1 }).finish(k.deflate(d)), { code: 'too-large' }, `inflateStreamAsync, ${at}`)
    const archive = k.zip({ 'a.bin': d, 'b.bin': d })
    await assert.rejects(k.unzipAsync(archive, { maxSize: 2 * d.length - 1 }), { code: 'too-large' }, `unzipAsync, ${at}`)
    await assert.rejects(k.unzipStreamAsync({ maxSize: 2 * d.length - 1 }).finish(archive), { code: 'too-large' }, `unzipStreamAsync, ${at}`)
    const kept = await k.unzipAsync(archive, { maxSize: d.length, filter: e => e.name === 'a.bin' })
    same(kept['a.bin']!, d, `unzipAsync with a filter, ${at}`)
  }
})

test('maxSize must be a whole number of bytes; Infinity means no cap', () => {
  for (const bad of [-1, 1.5, NaN, '10', {}]) {
    const opts = { maxSize: bad } as never
    for (const run of [() => k.inflate(k.deflate(sample), opts), () => k.gunzipStream(opts), () => k.unzip(k.zip({ a: sample }), opts)]) {
      assert.ok(throwsCode(run, 'invalid-option'), `maxSize ${String(bad)}`)
    }
  }
  same(k.inflate(k.deflate(sample), { maxSize: Infinity }), sample, 'Infinity')
  same(k.inflate(k.deflate(new Uint8Array(0)), { maxSize: 0 }), new Uint8Array(0), '0 allows empty output')
})

test('zipEntries lists names and sizes without decompressing', () => {
  const archive = k.zip({ 'big.bin': new Uint8Array(5_000_000), docs: { 'a.txt': sample }, empty: {} })
  let entries: k.ZipEntryInfo[] = []
  const largest = largestAllocation(() => { entries = k.zipEntries(archive) })
  assert.deepEqual(entries.map(e => [e.name, e.originalSize, e.compression]), [
    ['big.bin', 5_000_000, 8], ['docs/a.txt', sample.length, 8], ['empty/', 0, 0],
  ])
  assert.ok(entries.every(e => typeof e.size === 'number'))
  assert.ok(largest < 100_000, `allocated ${largest} bytes`)
})

test('null options mean no options, everywhere', async () => {
  const d = sample, none = null as never, z = k.zip({ a: d })
  const sync: [string, () => Uint8Array | string | Record<string, Uint8Array>][] = [
    ['deflate', () => k.inflate(k.deflate(d, none), none)],
    ['zlib', () => k.unzlib(k.zlib(d, none), none)],
    ['gzip', () => k.gunzip(k.gzip(d, none), none)],
    ['decompress', () => k.decompress(k.gzip(d), none)],
    ['streams', () => k.inflateStream(none).finish(k.deflateStream(none).finish(d))],
    ['zlib streams', () => k.unzlibStream(none).finish(k.zlibStream(none).finish(d))],
    ['gzip streams', () => k.gunzipStream(none).finish(k.gzipStream(none).finish(d))],
    ['decompressStream', () => k.decompressStream(none).finish(k.gzip(d))],
    ['text', () => k.decodeText(k.encodeText('hi', none), none) + k.decodeTextStream(none).finish(k.encodeTextStream(none).finish('hi'))],
    ['zip', () => k.unzip(k.zip({ a: d }, none), none)],
    ['zipStream', () => { const s = k.zipStream(none); return cat([s.file('a', none).finish(d), s.finish()]) }],
    ['unzipStream', () => cat(k.unzipStream(none).finish(z).map(c => c.data))],
  ]
  for (const [name, run] of sync) assert.doesNotThrow(run, name)
  same(await k.inflateAsync(await k.deflateAsync(d, none), none), d, 'one-shot async')
  same(await k.gunzipStreamAsync(none).finish(await k.gzipStreamAsync(none).finish(d)), d, 'async streams')
  same((await k.unzipAsync(await k.zipAsync({ a: d }, none), none))['a']!, d, 'async zip')
  const zs = k.zipStreamAsync(none)
  const archive = cat([await zs.file('a', none).finish(d), await zs.finish()])
  same(cat((await k.unzipStreamAsync(none).finish(archive)).map(c => c.data)), d, 'async zip streams')
})

test('a zip stream writes entries through plain Stream objects', () => {
  const zs: k.ZipStream = k.zipStream()
  const entry: k.Stream = zs.file('a.txt')
  const archive = cat([entry.push(sample.subarray(0, 100)), entry.finish(sample.subarray(100, 200)), zs.finish()])
  same(k.unzip(archive)['a.txt']!, sample.subarray(0, 200), 'round trip')
  const reader: k.UnzipStream = k.unzipStream()
  same(cat(reader.finish(archive).map(c => c.data)), sample.subarray(0, 200), 'read back as a stream')
})

test('zip metadata reads back as written: mtime everywhere, comment and Unix mode from the directory', async () => {
  const mtime = new Date(2024, 4, 6, 7, 8, 10), at = mtime.getTime()
  const archive = k.zip({ 'run.sh': [sample.subarray(0, 100), { mode: 0o755, comment: 'entry note' }], 'a.txt': sample }, { mtime })
  const listed = k.zipEntries(archive).map(e => [e.name, e.mtime.getTime(), e.comment, e.mode])
  // An entry without a mode gets the Unix default
  assert.deepEqual(listed, [['run.sh', at, 'entry note', 0o755], ['a.txt', at, '', 0o644]])
  // unzip's filter gets the same full entry, both sizes typed as present
  const seen: k.ZipEntry[] = []
  k.unzip(archive, { filter: e => { seen.push(e); return e.originalSize < 1e6 } })
  assert.deepEqual(seen.map(e => [e.comment, e.mode]), [['entry note', 0o755], ['', 0o644]])
  // A stream knows only what each local header says: the mtime, but no comment or mode
  const chunks = k.unzipStream().finish(archive)
  assert.ok(chunks.length && chunks.every(c => c.entry.mtime.getTime() === at && !('comment' in c.entry) && !('mode' in c.entry)))
  // Every writer takes mode: zipStream (a directory too), and the async paths in a worker
  const zs = k.zipStream({ mtime })
  const streamed = cat([zs.file('bin/', { mode: 0o700 }).finish(), zs.file('bin/tool', { mode: 0o755 }).finish(sample), zs.finish()])
  assert.deepEqual(k.zipEntries(streamed).map(e => [e.name, e.mode, e.mtime.getTime()]), [['bin/', 0o700, at], ['bin/tool', 0o755, at]])
  const big = randomBytes(40_000)
  assert.equal(k.zipEntries(await k.zipAsync({ 'x.bin': [big, { mode: 0o640 }] }))[0]!.mode, 0o640)
  const za = k.zipStreamAsync()
  const asyncStreamed = cat([await za.file('y.bin', { mode: 0o600 }).finish(big), await za.finish()])
  assert.equal(k.zipEntries(asyncStreamed)[0]!.mode, 0o600)
  // Each entry gets its own stamp, as a Date of its own
  const older = new Date(2020, 0, 2, 3, 4, 6)
  const mixed = k.zipEntries(k.zip({ a: [sample, { mtime: older }], b: [sample, { mtime }], c: [sample, { mtime }] }))
  assert.deepEqual(mixed.map(e => e.mtime.getTime()), [older.getTime(), at, at])
  assert.notEqual(mixed[1]!.mtime, mixed[2]!.mtime)
  // Attrs only hold a mode when the entry says it was made on Unix
  assert.equal(k.zipEntries(k.zip({ 'dos.bin': [sample, { os: 0, attrs: 0o100644 << 16 }] }))[0]!.mode, undefined)
})

test('other zip readers see the mode and mtime kokyu writes', t => {
  if (!has('python3', '-V')) { t.skip('no python3 on this machine'); return }
  const archive = k.zip({ 'run.sh': [k.encodeText('#!/bin/sh\n'), { mode: 0o755 }] }, { mtime: new Date(2024, 4, 6, 7, 8, 10) })
  const script = 'import io, sys, zipfile, json\ni = zipfile.ZipFile(io.BytesIO(sys.stdin.buffer.read())).infolist()[0]\n'
    + 'print(json.dumps([i.create_system, i.external_attr >> 16, list(i.date_time)]))'
  const read = JSON.parse(String(python(script, archive))) as unknown
  assert.deepEqual(read, [3, 0o100755, [2024, 5, 6, 7, 8, 10]])
})

test('mode is permission bits alone, never mixed with attrs or os', () => {
  for (const opts of [{ mode: 0o755, attrs: 0 }, { mode: 0o755, os: 3 }, { mode: 0o10000 }, { mode: -1 }, { mode: 1.5 }]) {
    assert.ok(throwsCode(() => k.zip({ a: [sample, opts] }), 'invalid-option'), JSON.stringify(opts))
    assert.ok(throwsCode(() => k.zipStream().file('a', opts), 'invalid-option'), `zipStream ${JSON.stringify(opts)}`)
  }
  assert.equal(k.zipEntries(k.zip({ a: [sample, { mode: 0o7777 }] }))[0]!.mode, 0o7777, 'every permission bit')
  assert.equal(k.zipEntries(k.zip({ a: [sample, { mode: 0 }] }))[0]!.mode, 0, 'no permission bits')
})

test('every chunk of one zip entry carries the same entry object, sync and in a worker', async () => {
  const archive = k.zip({ 'a.bin': randomBytes(30_000, 1), 'b.bin': randomBytes(30_000, 2), 'c.bin': randomBytes(30_000, 3) })
  const check = (chunks: k.UnzipChunk[], how: string): void => {
    const first = new Map<string, k.ZipEntryInfo>()
    for (const c of chunks) {
      const e = first.get(c.entry.name)
      if (e) assert.equal(c.entry, e, `${how}: ${c.entry.name}`)
      else first.set(c.entry.name, c.entry)
    }
    assert.equal(new Set(first.values()).size, 3, how)
    assert.ok(chunks.filter(c => c.entry.name === 'a.bin').length > 1, `${how}: several chunks per entry`)
  }
  check(pieces(k.unzipStream(), archive, 100).flat(), 'sync')
  // Pieces of 20 KB take the stream to a worker, and each entry spans more than one of its replies
  const a = k.unzipStreamAsync(), got: k.UnzipChunk[] = []
  for (let o = 0; o < archive.length; o += 20_000) got.push(...await a.push(archive.subarray(o, o + 20_000)))
  check([...got, ...await a.finish()], 'worker')
})

test('every output is in an ArrayBuffer of its own, even from shared memory, so web APIs take it', async () => {
  const shared = (b: Uint8Array): Uint8Array<SharedArrayBuffer> => { const s = new Uint8Array(new SharedArrayBuffer(b.length)); s.set(b); return s }
  const small = sample.subarray(0, 5000), big = randomBytes(40_000, 4)
  const text = shared(small), raw = shared(big), archive = shared(k.zip({ t: small, r: [big, { level: 0 }] }))
  const packed = { deflate: shared(k.deflate(small)), zlib: shared(k.zlib(small)), gzip: shared(k.gzip(small)), bigGzip: shared(k.gzip(big)) }
  const streamed = <T>(s: { push(c: Uint8Array): T; finish(c?: Uint8Array): T }, input: Uint8Array): T[] => [s.push(input.subarray(0, 700)), s.finish(input.subarray(700))]
  const zs = k.zipStream(), file = zs.file('a')
  const outputs: [string, Uint8Array | Uint8Array[]][] = [
    ['deflate', k.deflate(text)], ['deflate level 0', k.deflate(raw, { level: 0 })], ['zlib', k.zlib(text)], ['gzip', k.gzip(text)],
    ['inflate', k.inflate(packed.deflate)], ['unzlib', k.unzlib(packed.zlib)], ['gunzip', k.gunzip(packed.gzip)], ['decompress', k.decompress(packed.gzip)],
    ['zip', k.zip({ a: text, b: [raw, { level: 0 }] })], ['unzip', Object.values(k.unzip(archive))],
    ['zipStream', [file.push(text), file.finish(raw), zs.finish()]],
    ['unzipStream', streamed(k.unzipStream(), archive).flat().map(c => c.data)],
    ['deflateStream', streamed(k.deflateStream(), text)], ['zlibStream', streamed(k.zlibStream(), text)], ['gzipStream', streamed(k.gzipStream(), text)],
    ['inflateStream', streamed(k.inflateStream(), packed.deflate)], ['unzlibStream', streamed(k.unzlibStream(), packed.zlib)],
    ['gunzipStream', streamed(k.gunzipStream(), packed.gzip)], ['decompressStream', streamed(k.decompressStream(), packed.zlib)],
    ['encodeText', k.encodeText('kokyu')],
    // Under 16 KB runs here, the rest in a worker, with and without transfer (shared memory can't move, so it's copied)
    ['deflateAsync inline', await k.deflateAsync(text)], ['gunzipAsync worker', await k.gunzipAsync(packed.bigGzip)],
    ['inflateAsync worker, transfer', await k.inflateAsync(shared(k.deflate(big)), { transfer: true })],
    ['zipAsync', await k.zipAsync({ r: raw })], ['unzipAsync', Object.values(await k.unzipAsync(archive))],
    ['unzipStreamAsync', (await (async () => { const s = k.unzipStreamAsync(); return [...await s.push(archive), ...await s.finish()] })()).map(c => c.data)],
  ]
  for (const [what, out] of outputs) for (const o of [out].flat()) assert.ok(o.buffer instanceof ArrayBuffer, what)
  // With out, the result is a view of out, shared or not, and typed so
  const into = new Uint8Array(new SharedArrayBuffer(small.length))
  const view: Uint8Array<SharedArrayBuffer> = k.inflateInto(packed.deflate, into)
  assert.ok(view.buffer === into.buffer, 'out'); same(view, small, 'out')
  // The buffer type comes from out, never from what the result is assigned to
  // @ts-expect-error inflateInto() into a plain buffer never returns shared memory
  const claimed: Uint8Array<SharedArrayBuffer> = k.inflateInto(packed.deflate, new Uint8Array(small.length))
  assert.ok(claimed.buffer instanceof ArrayBuffer)
  // @ts-expect-error out is an argument of the …Into functions, not an option
  k.inflate(packed.deflate, { out: into })
  // These compile only because outputs are typed Uint8Array<ArrayBuffer>
  assert.equal(new Blob([k.gzip(text), k.zip({ a: text })]).size > 0, true)
  assert.equal((await new Response(k.inflate(packed.deflate)).arrayBuffer()).byteLength, small.length)
  assert.equal((await crypto.subtle.digest('SHA-256', k.unzip(archive)['t']!)).byteLength, 32)
})

test('the …Into decoders need a readable out, and never fall back to allocating', () => {
  const body = sample.subarray(0, 3000), detached = new Uint8Array(8)
  structuredClone(detached.buffer, { transfer: [detached.buffer] })
  const intos = [[k.inflateInto, k.deflate(body)], [k.unzlibInto, k.zlib(body)], [k.gunzipInto, k.gzip(body)], [k.decompressInto, k.gzip(body)]] as const
  for (const [into, packed] of intos) {
    const bads = { undefined, null: null, zero: 0, ArrayBuffer: new ArrayBuffer(body.length), array: [1, 2], detached }
    for (const [what, bad] of Object.entries(bads)) assert.ok(throwsCode(() => into(packed, bad as never), 'invalid-option'), `${into.name}, ${what}`)
    const buffer = new Uint8Array(body.length + 10), view = into(packed, buffer)
    assert.ok(view.buffer === buffer.buffer && view.byteOffset === 0, into.name)
    same(view, body, into.name)
  }
})

test('an …Into decode whose out shares memory with its input still decodes right', () => {
  const d = sample.subarray(0, 4000)
  const packs = [[k.deflate, k.inflateInto], [k.zlib, k.unzlibInto], [(x: Uint8Array) => cat([k.gzip(x.subarray(0, 100)), k.gzip(x.subarray(100))]), k.gunzipInto],
    [k.gzip, k.decompressInto]] as const
  for (const [pack, unpack] of packs) {
    const packed = pack(d)
    // out starting anywhere from d.length before the input to its end, including over it
    for (let gap = -d.length; gap <= packed.length; gap += 7) {
      const space = new Uint8Array(packed.length + 2 * d.length + 64), at = d.length + 32
      space.set(packed, at)
      same(unpack(space.subarray(at, at + packed.length), space.subarray(at + gap, at + gap + d.length)), d, `${unpack.name}, out at ${gap}`)
    }
  }
})

test('an …Into decode reads input right when out is the same shared memory through another SharedArrayBuffer', () => {
  const d = sample.subarray(0, 4000)
  for (const [pack, unpack] of [[k.deflate, k.inflateInto], [k.zlib, k.unzlibInto]] as const) {
    const packed = pack(d)
    for (let at = 0; at <= d.length; at += 7) {
      // structuredClone gives a second SharedArrayBuffer object over the same memory, as postMessage and a wasm grow do
      const sab = new SharedArrayBuffer(d.length + packed.length), twin = structuredClone(sab)
      const input = new Uint8Array(twin, at, packed.length)
      input.set(packed)
      same(unpack(input, new Uint8Array(sab, 0, d.length)), d, `${unpack.name}, input at ${at}`)
    }
  }
})
