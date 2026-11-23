// Every kokyu function against its fflate counterpart: async calls and streams, every level, options and
// metadata, input shapes, boundary sizes and truncation. node:zlib referees wherever the two disagree.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as zl from 'node:zlib'
import * as f from 'fflate'
import * as k from '../engine/index.js'
import {
  attempt, binary, cat, centralRecords, chunked, chunkedAsync, ffChunks, ffStream, ffZip, fullText, has, joinChunks, pieces, python,
  randomBytes, rng, same, sameFiles, spans, throwsCode, unzipInPieces,
} from './helpers.js'

// fflate's async one-shots as promises
const ffAsync = <T>(start: (cb: (err: f.FlateError | null, out: T) => void) => unknown): Promise<T> =>
  new Promise((resolve, reject) => { start((err, out) => err ? reject(err) : resolve(out)) })

// fflate's worker streams, fed copies (they move each pushed buffer) in random chunks of up to `max` bytes
const ffAsyncStream = (make: (cb: f.AsyncFlateStreamHandler) => { push(c: Uint8Array, final?: boolean): void }, d: Uint8Array, max: number): Promise<Uint8Array> =>
  ffChunks(cb => {
    const s = make(cb)
    if (!d.length) return s.push(new Uint8Array(0), true)
    for (const [a, b] of spans(d.length, max, rng(5))) s.push(d.slice(a, b), b >= d.length)
  }).then(cat)

// Both sides of the 16 KB line where async calls stop running inline, and a size that takes a while in a worker
const ASYNC_SIZES = [0, 1000, 16_383, 16_384, 16_385, 1 << 20]

test('async one-shots match fflate both ways, inline and in workers', async () => {
  for (const n of ASYNC_SIZES) {
    const d = fullText.subarray(0, n), at = `${n} B`
    const files = { 'a.txt': d, 'dir/b.bin': binary.subarray(0, n >> 2), 'empty/': new Uint8Array(0) }
    same(f.inflateSync(await k.deflateAsync(d, { level: 9 })), d, `deflateAsync → fflate, ${at}`)
    same(f.unzlibSync(await k.zlibAsync(d)), d, `zlibAsync → fflate, ${at}`)
    same(f.gunzipSync(await k.gzipAsync(d, { level: 1 })), d, `gzipAsync → fflate, ${at}`)
    sameFiles(f.unzipSync(await k.zipAsync(files)), files, `zipAsync → fflate, ${at}`)
    // fflate's sync output and its own worker output, decoded by kokyu's async calls
    for (const how of ['sync', 'async'] as const) {
      const via = `fflate ${how} → kokyu, ${at}`
      const raw = how === 'sync' ? f.deflateSync(d) : await ffAsync<Uint8Array>(cb => f.deflate(d, {}, cb))
      const z = how === 'sync' ? f.zlibSync(d) : await ffAsync<Uint8Array>(cb => f.zlib(d, {}, cb))
      const gz = how === 'sync' ? f.gzipSync(d) : await ffAsync<Uint8Array>(cb => f.gzip(d, {}, cb))
      const zip = how === 'sync' ? f.zipSync(files) : await ffAsync<Uint8Array>(cb => f.zip(files, {}, cb))
      same(await k.inflateAsync(raw), d, `inflateAsync, ${via}`)
      same(await k.unzlibAsync(z), d, `unzlibAsync, ${via}`)
      same(await k.gunzipAsync(gz), d, `gunzipAsync, ${via}`)
      for (const packed of [raw, z, gz]) same(await k.decompressAsync(packed), d, `decompressAsync, ${via}`)
      sameFiles(await k.unzipAsync(zip), files, `unzipAsync, ${via}`)
      const want = { 'a.txt': d }
      sameFiles(await k.unzipAsync(zip, { filter: e => e.name === 'a.txt' }), want, `unzipAsync with a filter, ${via}`)
    }
  }
})

test('async streams match fflate both ways, every kind, inline and in workers', async () => {
  const dictionary = fullText.subarray(500_000, 532_768)
  for (const n of [0, 5000, 300_000]) {
    const d = fullText.subarray(0, n), at = `${n} B`
    for (const max of [1000, 70_000]) {
      const by = `${at}, chunks ≤${max}`
      same(f.inflateSync(cat(await chunkedAsync(k.deflateStreamAsync({ level: 6 }), d, max))), d, `deflateStreamAsync → fflate, ${by}`)
      same(f.unzlibSync(cat(await chunkedAsync(k.zlibStreamAsync({ level: 9 }), d, max))), d, `zlibStreamAsync → fflate, ${by}`)
      same(f.gunzipSync(cat(await chunkedAsync(k.gzipStreamAsync({ level: 1 }), d, max))), d, `gzipStreamAsync → fflate, ${by}`)
      same(f.inflateSync(cat(await chunkedAsync(k.deflateStreamAsync({ dictionary }), d, max)), { dictionary }), d, `deflateStreamAsync with a dictionary → fflate, ${by}`)
      // fflate's own worker streams, decoded by kokyu's
      const raw = await ffAsyncStream(cb => new f.AsyncDeflate({ level: 6 }, cb), d, max)
      const z = await ffAsyncStream(cb => new f.AsyncZlib({ level: 6 }, cb), d, max)
      const gz = await ffAsyncStream(cb => new f.AsyncGzip({ level: 6 }, cb), d, max)
      same(cat(await chunkedAsync(k.inflateStreamAsync(), raw, max)), d, `fflate AsyncDeflate → inflateStreamAsync, ${by}`)
      same(cat(await chunkedAsync(k.unzlibStreamAsync(), z, max)), d, `fflate AsyncZlib → unzlibStreamAsync, ${by}`)
      same(cat(await chunkedAsync(k.gunzipStreamAsync(), gz, max)), d, `fflate AsyncGzip → gunzipStreamAsync, ${by}`)
      for (const packed of [raw, z, gz]) same(cat(await chunkedAsync(k.decompressStreamAsync(), packed, max)), d, `decompressStreamAsync, ${by}`)
      const withDict = f.deflateSync(d, { dictionary })
      same(cat(await chunkedAsync(k.inflateStreamAsync({ dictionary }), withDict, max)), d, `fflate with a dictionary → inflateStreamAsync, ${by}`)
      same(cat(await chunkedAsync(k.unzlibStreamAsync({ dictionary }), f.zlibSync(d, { dictionary }), max)), d, `fflate zlib with a dictionary → unzlibStreamAsync, ${by}`)
    }
    // Zip both ways: kokyu's async writer read by fflate, fflate's worker writer read by kokyu's async reader
    const files = { 'a.txt': d, 'b.bin': binary.subarray(0, n >> 1), 'c/stored.txt': d.subarray(0, 999) }
    const z = k.zipStreamAsync({ comment: 'kokyu' }), parts: Promise<Uint8Array>[] = []
    for (const [name, data] of Object.entries(files)) {
      parts.push(...pieces(z.file(name, { level: name.includes('stored') ? 0 : 6 }), data, 40_000))
    }
    parts.push(z.finish())
    sameFiles(f.unzipSync(cat(await Promise.all(parts))), files, `zipStreamAsync → fflate, ${at}`)
    const theirs = cat(await ffChunks(cb => {
      const zip = new f.Zip(cb)
      for (const [name, data] of Object.entries(files)) {
        const file = name.includes('stored') ? new f.ZipPassThrough(name) : new f.AsyncZipDeflate(name, { level: 6 })
        zip.add(file)
        file.push(data.slice(), true)
      }
      zip.end()
    }))
    sameFiles(joinChunks((await chunkedAsync(k.unzipStreamAsync(), theirs, 30_000)).flat()), files, `fflate Zip + AsyncZipDeflate → unzipStreamAsync, ${at}`)
  }
})

test('sync streams match fflate\'s streams at every level, with and without dictionaries', () => {
  const d = cat([fullText.subarray(0, 120_000), binary.subarray(0, 40_000)])
  const dictionary = fullText.subarray(700_000, 732_768)
  for (let level = 0; level <= 9; level++) {
    const lv = level as k.Level, at = `level ${level}`
    same(ffStream(new f.Inflate(), chunked(k.deflateStream({ level: lv }), d, 9000), 7000), d, `deflateStream → fflate Inflate, ${at}`)
    same(ffStream(new f.Unzlib(), chunked(k.zlibStream({ level: lv }), d, 9000), 7000), d, `zlibStream → fflate Unzlib, ${at}`)
    same(ffStream(new f.Gunzip(), chunked(k.gzipStream({ level: lv }), d, 9000), 7000), d, `gzipStream → fflate Gunzip, ${at}`)
    same(chunked(k.inflateStream(), ffStream(new f.Deflate({ level: lv }), d, 9000), 7000), d, `fflate Deflate → inflateStream, ${at}`)
    same(chunked(k.unzlibStream(), ffStream(new f.Zlib({ level: lv }), d, 9000), 7000), d, `fflate Zlib → unzlibStream, ${at}`)
    same(chunked(k.gunzipStream(), ffStream(new f.Gzip({ level: lv }), d, 9000), 7000), d, `fflate Gzip → gunzipStream, ${at}`)
    same(chunked(k.decompressStream(), ffStream(new f.Gzip({ level: lv }), d, 9000), 3), d, `fflate Gzip → decompressStream, ${at}`)
    if (level === 0) continue
    same(ffStream(new f.Inflate({ dictionary }), chunked(k.deflateStream({ level: lv, dictionary }), d, 9000), 7000), d, `deflateStream with a dictionary → fflate, ${at}`)
    same(ffStream(new f.Unzlib({ dictionary }), chunked(k.zlibStream({ level: lv, dictionary }), d, 9000), 7000), d, `zlibStream with a dictionary → fflate, ${at}`)
    same(chunked(k.inflateStream({ dictionary }), ffStream(new f.Deflate({ level: lv, dictionary }), d, 9000), 7000), d, `fflate Deflate with a dictionary → inflateStream, ${at}`)
    same(chunked(k.unzlibStream({ dictionary }), ffStream(new f.Zlib({ level: lv, dictionary }), d, 9000), 7000), d, `fflate Zlib with a dictionary → unzlibStream, ${at}`)
  }
})

test('flushed output decodes in fflate\'s streaming decoders up to the flush', () => {
  const d = fullText.subarray(0, 200_000)
  const cases: [string, k.CompressStream, { ondata: f.FlateStreamHandler; push(c: Uint8Array, final?: boolean): void }][] = [
    ['deflateStream', k.deflateStream(), new f.Inflate()],
    ['zlibStream', k.zlibStream(), new f.Unzlib()],
    ['gzipStream', k.gzipStream({ level: 9 }), new f.Gunzip()],
  ]
  for (const [name, s, dec] of cases) {
    const got: Uint8Array[] = []
    dec.ondata = c => { got.push(c.slice()) }
    for (const end of [1, 5000, 70_000, 200_000]) {
      dec.push(cat([s.push(d.subarray(got.reduce((n, c) => n + c.length, 0), end)), s.flush()]))
      same(cat(got), d.subarray(0, end), `${name} flushed at ${end}`)
    }
    dec.push(s.finish(), true)
    same(cat(got), d, `${name} after finish`)
  }
})

test('multi-member gzip decodes the same as node:zlib and fflate\'s Gunzip stream', () => {
  const a = fullText.subarray(0, 30_000), b = binary.subarray(0, 20_000), c = new Uint8Array(0)
  const multi = cat([f.gzipSync(a), k.gzip(b, { filename: 'b.bin' }), zl.gzipSync(c), f.gzipSync(a, { level: 0 })])
  const want = zl.gunzipSync(multi)
  same(want, cat([a, b, c, a]), 'node:zlib reads every member')
  same(k.gunzip(multi), want, 'gunzip')
  same(k.decompress(multi), want, 'decompress')
  same(chunked(k.gunzipStream(), multi, 777), want, 'gunzipStream')
  same(ffStream(new f.Gunzip(), multi, 777), want, 'fflate Gunzip')
})

// gzip header: the flag byte, MTIME and FNAME, as fflate writes them
const gzipFields = (g: Uint8Array): { flags: number; mtime: number; name: string } => {
  const flags = g[3]!, mtime = new DataView(g.buffer, g.byteOffset).getUint32(4, true)
  const name = flags & 8 ? String.fromCharCode(...g.subarray(10, g.indexOf(0, 10))) : ''
  return { flags, mtime, name }
}

test('gzip header fields match fflate\'s for every mtime form, and kokyu takes strings too', () => {
  const d = sample()
  const when = new Date('2024-05-01T12:34:56.789Z')
  for (const mtime of [when, when.getTime(), when.toISOString(), 'May 1, 2024 12:34:56 GMT']) {
    for (const filename of [undefined, 'notes.txt', 'café.txt']) {
      const opts = filename === undefined ? { mtime } : { mtime, filename }
      const ours = k.gzip(d, opts), theirs = f.gzipSync(d, opts)
      assert.deepEqual(gzipFields(ours), gzipFields(theirs), `${JSON.stringify(opts)}`)
      same(f.gunzipSync(ours), d, 'kokyu gzip decodes in fflate')
    }
  }
  assert.ok(throwsCode(() => k.gzip(d, { mtime: 'not a date' }), 'invalid-option'))
  assert.ok(throwsCode(() => k.zip({ a: d }, { mtime: 'not a date' }), 'invalid-option'))
})

function sample(): Uint8Array { return fullText.subarray(3000, 13_000) }

interface CentralFields {
  madeBy: number; needed: number; flags: number; method: number; time: number; date: number; crc: number
  usize: number; name: string; extra: number[]; comment: string; attrs: number; localExtra: number[]
}

// The central directory, record by record, with each entry's local extra field
function centralDirectory(z: Uint8Array): { comment: string; entries: CentralFields[] } {
  const v = new DataView(z.buffer, z.byteOffset, z.byteLength), text = new TextDecoder(), { end: e, at } = centralRecords(z)
  const entries = at.map((p): CentralFields => {
    const nl = v.getUint16(p + 28, true), el = v.getUint16(p + 30, true), cl = v.getUint16(p + 32, true)
    const local = v.getUint32(p + 42, true), lnl = v.getUint16(local + 26, true), lel = v.getUint16(local + 28, true)
    return {
      madeBy: v.getUint16(p + 4, true), needed: v.getUint16(p + 6, true), flags: v.getUint16(p + 8, true),
      method: v.getUint16(p + 10, true), time: v.getUint16(p + 12, true), date: v.getUint16(p + 14, true),
      crc: v.getUint32(p + 16, true), usize: v.getUint32(p + 24, true), name: text.decode(z.subarray(p + 46, p + 46 + nl)),
      extra: [...z.subarray(p + 46 + nl, p + 46 + nl + el)], comment: text.decode(z.subarray(p + 46 + nl + el, p + 46 + nl + el + cl)),
      attrs: v.getUint32(p + 38, true), localExtra: [...z.subarray(local + 30 + lnl, local + 30 + lnl + lel)],
    }
  })
  return { comment: text.decode(z.subarray(e + 22, e + 22 + v.getUint16(e + 20, true))), entries }
}

test('zip metadata matches fflate\'s field by field, and Python reads it back', t => {
  const d = sample()
  const mtime = new Date(2021, 2, 4, 5, 6, 8)
  const unix = 0o100644 * 65536
  const files = {
    'plain.txt': d,
    'stored.bin': [d, { level: 0, mtime: new Date(2019, 11, 31, 23, 59, 58), comment: 'stored ✓', attrs: unix, os: 3 }],
    'extra.txt': [d, { level: 9, extra: { 0x5455: Uint8Array.of(1, 0x10, 0x20, 0x30, 0x40), 0xCAFE: new Uint8Array(0) }, comment: 'two fields' }],
    'ünïcødé/文件.txt': [d.subarray(0, 100), { mtime: '2022-07-08T09:10:12' }],
    'dir/': new Uint8Array(0),
  } as const
  const ours = k.zip(files as k.ZipInput, { level: 6, mtime, comment: 'archive ✓' })
  // fflate writes no archive comment: its top-level comment is a default for every entry
  const theirs = f.zipSync(files as unknown as f.Zippable, { level: 6, mtime })
  const a = centralDirectory(ours), b = centralDirectory(theirs)
  assert.equal(a.entries.length, b.entries.length)
  for (const [i, e] of a.entries.entries()) {
    // A directory: kokyu stores it as Unix mode 0o755 with the DOS directory attribute, as Info-ZIP and Python do;
    // fflate deflates the empty entry and sets no attributes
    if (e.name === 'dir/') {
      assert.deepEqual([e.method, e.madeBy >> 8, e.attrs, b.entries[i]!.method, b.entries[i]!.attrs], [0, 3, (0o040755 << 16 | 0x10) >>> 0, 8, 0])
      continue
    }
    // Without attrs, kokyu writes a Unix host with mode 0o644, as Python and Info-ZIP do on Unix; fflate writes an
    // MS-DOS host with none. Every other field matches.
    if (e.name !== 'stored.bin') {
      assert.deepEqual([e.madeBy >> 8, e.attrs], [3, 0o100644 * 65536], e.name)
      assert.deepEqual([b.entries[i]!.madeBy >> 8, b.entries[i]!.attrs], [0, 0], e.name)
    }
    const same = e.name === 'stored.bin' ? e : { ...e, madeBy: b.entries[i]!.madeBy, attrs: b.entries[i]!.attrs }
    assert.deepEqual(same, b.entries[i], `central record ${e.name}`)
  }
  assert.deepEqual([a.comment, b.comment], ['archive ✓', ''])

  // An independent reader: Python's zipfile
  if (!has('python3', '-V')) { t.skip('no python3 on this machine'); return }
  const script = 'import io, sys, zipfile, json\nz = zipfile.ZipFile(io.BytesIO(sys.stdin.buffer.read()))\nprint(json.dumps({"comment": z.comment.decode(), "files": [[i.filename, list(i.date_time), i.comment.decode(), i.external_attr, i.create_system, i.extra.hex()] for i in z.infolist()]}))'
  const read = JSON.parse(python(script, ours).toString()) as { comment: string; files: [string, number[], string, number, number, string][] }
  assert.equal(read.comment, 'archive ✓')
  const info = Object.fromEntries(read.files.map(x => [x[0], x]))
  assert.deepEqual(info['plain.txt']!.slice(1, 3), [[2021, 3, 4, 5, 6, 8], ''])
  assert.deepEqual(info['stored.bin']!.slice(1), [[2019, 12, 31, 23, 59, 58], 'stored ✓', unix, 3, ''])
  // Each field is id, length, data, little-endian: 0x5455 with 5 bytes, then 0xCAFE empty
  assert.deepEqual([info['extra.txt']![2], info['extra.txt']![5]], ['two fields', '555405000110203040feca0000'])
  assert.deepEqual(info['ünïcødé/文件.txt']![1], [2022, 7, 8, 9, 10, 12])
  assert.equal(info['dir/']![3], (0o040755 << 16 | 0x10) >>> 0)
})

test('unzip filters and stream entries see what fflate\'s do', () => {
  const files = { 'a.txt': sample(), 'b/c.bin': binary.subarray(0, 5000), 'empty.txt': new Uint8Array(0), 'd/': new Uint8Array(0) }
  const streamed = ffZip(Object.entries(files), n => n === 'b/c.bin')
  for (const [what, archive] of [['kokyu zip', k.zip(files)], ['fflate zipSync', f.zipSync(files)], ['fflate Zip stream', streamed]] as const) {
    type Info = { name: string; size: number | undefined; originalSize: number | undefined; compression: number }
    const seen = (unzip: (filter: (i: f.UnzipFileInfo) => boolean) => unknown): Info[] => {
      const out: Info[] = []
      unzip(i => { out.push({ name: i.name, size: i.size, originalSize: i.originalSize, compression: i.compression }); return i.name !== 'a.txt' })
      return out
    }
    const theirs = seen(filter => f.unzipSync(archive, { filter }))
    assert.deepEqual(seen(filter => k.unzip(archive, { filter })), theirs, `unzip filter, ${what}`)
    assert.deepEqual(Object.keys(k.unzip(archive, { filter: i => i.name !== 'a.txt' })).sort(), Object.keys(f.unzipSync(archive, { filter: i => i.name !== 'a.txt' })).sort(), `kept entries, ${what}`)
    // The streaming readers only know what each local header says: no sizes when a data descriptor follows
    type StreamInfo = { compression: number; size: number | undefined; originalSize: number | undefined }
    const ffStreamInfo: Record<string, StreamInfo> = {}
    const u = new f.Unzip(file => { ffStreamInfo[file.name] = { compression: file.compression, size: file.size, originalSize: file.originalSize } })
    u.push(archive, true)
    const ourStreamInfo: Record<string, StreamInfo> = {}
    for (const c of k.unzipStream().finish(archive)) ourStreamInfo[c.entry.name] = { compression: c.entry.compression, size: c.entry.size, originalSize: c.entry.originalSize }
    assert.deepEqual(ourStreamInfo, ffStreamInfo, `stream entry info, ${what}`)
  }
})

test('out buffers behave like fflate\'s', () => {
  const d = sample(), raw = f.deflateSync(d), z = f.zlibSync(d), gz = f.gzipSync(d)
  const cases: [string, (out: Uint8Array) => Uint8Array, (out: Uint8Array) => Uint8Array][] = [
    ['inflate', out => k.inflateInto(raw, out), out => f.inflateSync(raw, { out })],
    ['unzlib', out => k.unzlibInto(z, out), out => f.unzlibSync(z, { out })],
    ['gunzip', out => k.gunzipInto(gz, out), out => f.gunzipSync(gz, { out })],
    ['decompress', out => k.decompressInto(gz, out), out => f.decompressSync(gz, { out })],
  ]
  for (const [name, ours, theirs] of cases) {
    for (const size of [d.length, d.length + 100]) {
      const a = new Uint8Array(size), b = new Uint8Array(size)
      const got = ours(a), want = theirs(b)
      same(got, want, `${name} into ${size} bytes`)
      assert.equal(got.buffer, a.buffer, `${name} decodes in place`)
    }
    // Too small: kokyu refuses
    assert.ok(throwsCode(() => ours(new Uint8Array(d.length - 1)), 'too-large'), `${name} into too small a buffer`)
  }
})

test('every function gives the same result for Buffers, odd-offset views and plain arrays', async () => {
  for (const n of [5000, 40_000]) {
    const plain = fullText.slice(100, 100 + n)
    const big = new Uint8Array(n + 7); big.set(plain, 3)
    const shapes: [string, Uint8Array][] = [['Buffer', Buffer.from(plain)], ['odd-offset view', big.subarray(3, 3 + n)], ['plain', plain]]
    // A fixed time, since the default is the current one and could tick between calls
    const mtime = new Date(2020, 0, 1)
    const want = { raw: k.deflate(plain), z: k.zlib(plain), gz: k.gzip(plain), zip: k.zip({ a: plain }, { mtime }) }
    for (const [shape, d] of shapes) {
      const at = `${shape}, ${n} B`
      same(k.deflate(d), want.raw, `deflate, ${at}`)
      same(k.zlib(d), want.z, `zlib, ${at}`)
      same(k.gzip(d), want.gz, `gzip, ${at}`)
      same(k.zip({ a: d }, { mtime }), want.zip, `zip, ${at}`)
      same(chunked(k.deflateStream(), d, 3000), want.raw, `deflateStream, ${at}`)
      same(await k.deflateAsync(d), want.raw, `deflateAsync, ${at}`)
      same(await k.zipAsync({ a: d }, { mtime }), want.zip, `zipAsync, ${at}`)
      same(cat(await chunkedAsync(k.gzipStreamAsync(), d, 3000)), want.gz, `gzipStreamAsync, ${at}`)
      // Decoders get the compressed forms in the same shapes
      const as = (b: Uint8Array): Uint8Array => shape === 'Buffer' ? Buffer.from(b) : shape === 'plain' ? b.slice() : (() => { const x = new Uint8Array(b.length + 5); x.set(b, 1); return x.subarray(1, 1 + b.length) })()
      same(k.inflate(as(want.raw)), plain, `inflate, ${at}`)
      same(k.unzlib(as(want.z)), plain, `unzlib, ${at}`)
      same(k.gunzip(as(want.gz)), plain, `gunzip, ${at}`)
      same(k.decompress(as(want.gz)), plain, `decompress, ${at}`)
      same(k.unzip(as(want.zip))['a']!, plain, `unzip, ${at}`)
      same(await k.inflateAsync(as(want.raw)), plain, `inflateAsync, ${at}`)
      same(await k.unzipAsync(as(want.zip)).then(r => r['a']!), plain, `unzipAsync, ${at}`)
      same(chunked(k.gunzipStream(), as(want.gz), 1000), plain, `gunzipStream, ${at}`)
      assert.equal(k.decodeText(d), f.strFromU8(d), `decodeText, ${at}`)
    }
    // Async calls copy a Buffer rather than move it (a Buffer's slice() is a view, so it can't be how they copy)
    const kept = Buffer.from(plain)
    same(f.inflateSync(await k.deflateAsync(kept)), plain, `deflateAsync with a Buffer, ${n} B`)
    same(f.inflateSync(cat(await chunkedAsync(k.deflateStreamAsync(), kept, 20_000))), plain, `deflateStreamAsync with a Buffer, ${n} B`)
    assert.equal(kept.length, n, 'the Buffer is still there')
    // Moving a view that doesn't cover its whole buffer copies it instead, leaving the caller's buffer alone
    const view = big.subarray(3, 3 + n)
    same(f.inflateSync(await k.deflateAsync(view, { transfer: true })), plain, `deflateAsync with transfer on a view, ${n} B`)
    assert.equal(big.length, n + 7)
  }
})

// One Buffer refilled for every push, as Node's stream readers do, then scrubbed: nothing returned or held
// back may still point into it
function pushReused<O>(s: { push(c: Uint8Array): O; finish(): O }, data: Uint8Array, size: number): O[] {
  const buf = Buffer.alloc(size), out: O[] = []
  for (let o = 0; o < data.length; o += size) {
    const n = Math.min(size, data.length - o)
    buf.set(data.subarray(o, o + n))
    out.push(s.push(buf.subarray(0, n)))
    buf.fill(0xEE)
  }
  out.push(s.finish())
  return out
}

test('streams never keep or return views of a reused Buffer', () => {
  const d = cat([fullText.subarray(0, 60_000), binary.subarray(0, 20_000)])
  const files = { 'a.txt': d, 'stored.bin': binary.subarray(0, 9000), 'c.txt': d.subarray(0, 5000) }
  const archives = [k.zip(files, { level: 6 }), f.zipSync(files, { level: 0 }), (() => {
    const z = k.zipStream(), out: Uint8Array[] = []
    for (const [n, data] of Object.entries(files)) out.push(...pushReused(z.file(n, { level: n === 'stored.bin' ? 0 : 6 }), data, 777))
    return cat([...out, z.finish()])
  })()]
  for (const size of [7, 777, 5000]) {
    same(cat(pushReused(k.inflateStream(), f.deflateSync(d), size)), d, `inflateStream, chunks of ${size}`)
    same(cat(pushReused(k.unzlibStream(), f.zlibSync(d), size)), d, `unzlibStream, chunks of ${size}`)
    same(cat(pushReused(k.gunzipStream(), f.gzipSync(d), size)), d, `gunzipStream, chunks of ${size}`)
    same(cat(pushReused(k.decompressStream(), f.gzipSync(d), size)), d, `decompressStream, chunks of ${size}`)
    same(f.inflateSync(cat(pushReused(k.deflateStream(), d, size))), d, `deflateStream, chunks of ${size}`)
    for (const [i, archive] of archives.entries()) sameFiles(joinChunks(pushReused(k.unzipStream(), archive, size).flat()), files, `unzipStream archive ${i}, chunks of ${size}`)
    assert.equal(pushReused(k.decodeTextStream(), k.encodeText('héllo wörld 🎉 '.repeat(500)), size).join(''), 'héllo wörld 🎉 '.repeat(500), `decodeTextStream, chunks of ${size}`)
  }
  // An archive read from a Buffer gives entries of their own
  const fromBuffer = k.unzip(Buffer.from(archives[1]!))['stored.bin']!
  assert.ok(fromBuffer.byteOffset === 0 && fromBuffer.byteLength === fromBuffer.buffer.byteLength, 'unzip entry owns its buffer')
})

const BOUNDARIES = [0, 1, 257, 258, 259, 16_383, 16_384, 16_385, 32_767, 32_768, 32_769, 65_535, 65_536, 65_537, 262_143, 262_144, 262_145]

test('boundary sizes round-trip with fflate on every path, raw deflate at every level', () => {
  const text = fullText.subarray(0, 262_145), noise = randomBytes(262_145, 21)
  for (const n of BOUNDARIES) {
    for (const [kind, src] of [['text', text], ['random', noise]] as const) {
      const d = src.subarray(0, n)
      for (let level = 0; level <= 9; level++) {
        const lv = level as k.Level, at = `${kind} ${n} B, level ${level}`
        const ours = k.deflate(d, { level: lv })
        same(f.inflateSync(ours), d, `deflate → fflate, ${at}`)
        same(chunked(k.deflateStream({ level: lv }), d, 20_000), ours, `deflateStream equals deflate, ${at}`)
        same(k.inflate(f.deflateSync(d, { level: lv })), d, `fflate → inflate, ${at}`)
      }
      same(f.gunzipSync(k.gzip(d)), d, `gzip → fflate, ${kind} ${n} B`)
      same(k.unzlib(f.zlibSync(d)), d, `fflate zlib → unzlib, ${kind} ${n} B`)
      same(chunked(k.inflateStream(), f.deflateSync(d), 4096), d, `fflate → inflateStream, ${kind} ${n} B`)
    }
  }
})

test('at every truncation point kokyu accepts and rejects what node:zlib does, never with other bytes', () => {
  const d = cat([fullText.subarray(0, 1500), randomBytes(300, 4), new Uint8Array(400)])
  const forms: [string, Uint8Array, (b: Uint8Array) => Uint8Array, (b: Uint8Array) => Uint8Array, () => k.Stream][] = []
  for (const level of [0, 6] as const) {
    forms.push(
      [`raw level ${level}`, k.deflate(d, { level }), b => zl.inflateRawSync(b), b => k.inflate(b), () => k.inflateStream()],
      [`zlib level ${level}`, f.zlibSync(d, { level }), b => zl.inflateSync(b), b => k.unzlib(b), () => k.unzlibStream()],
      [`gzip level ${level}`, f.gzipSync(d, { level, mtime: 1e12 }), b => zl.gunzipSync(b), b => k.gunzip(b), () => k.gunzipStream()],
    )
  }
  for (const [name, packed, referee, ours, stream] of forms) {
    for (let cut = 0; cut <= packed.length; cut++) {
      const b = packed.subarray(0, cut)
      const z = attempt(() => referee(b))
      const one = attempt(() => ours(b))
      const s = attempt(() => chunked(stream(), b, 50))
      assert.equal(one.ok, z.ok, `${name} cut at ${cut}: zlib ${z.ok ? 'accepts' : 'rejects'}, kokyu ${one.ok ? 'accepts' : 'rejects'}`)
      assert.equal(s.ok, z.ok, `${name} stream cut at ${cut}`)
      if (one.ok && z.ok) same(one.value, z.value, `${name} cut at ${cut}`)
      if (s.ok && z.ok) same(s.value, z.value, `${name} stream cut at ${cut}`)
    }
  }
  // A cut zip never comes back with wrong bytes or missing entries. The streaming reader may finish once the
  // central directory has begun, since every entry comes before it; anywhere earlier it must fail.
  const files = { 'a.txt': d, 'b.bin': d.subarray(0, 500) }, archive = k.zip(files)
  const directory = new DataView(archive.buffer, archive.byteOffset).getUint32(archive.length - 6, true)
  for (let cut = 0; cut < archive.length; cut++) {
    assert.equal(attempt(() => k.unzip(archive.subarray(0, cut))).ok, false, `zip cut at ${cut} decoded`)
    const streamed = attempt(() => unzipInPieces(archive.subarray(0, cut), 64))
    if (cut < directory + 4) assert.equal(streamed.ok, false, `zip stream cut at ${cut}, before the central directory`)
    else if (streamed.ok) sameFiles(streamed.value, files, `zip stream cut at ${cut}`)
  }
})

test('like fflate, decoders don\'t check checksums', () => {
  const d = sample()
  const gz = f.gzipSync(d); gz[gz.length - 8]! ^= 1
  const z = f.zlibSync(d); z[z.length - 1]! ^= 1
  const zip = f.zipSync({ 'a.txt': d })
  zip[centralRecords(zip).at[0]! + 16]! ^= 1
  same(k.gunzip(gz), f.gunzipSync(gz), 'a bad CRC-32 in gzip')
  same(k.unzlib(z), f.unzlibSync(z), 'a bad Adler-32 in zlib')
  same(k.unzip(zip)['a.txt']!, f.unzipSync(zip)['a.txt']!, 'a bad CRC-32 in zip')
})

test('deliberate differences from fflate stay deliberate', () => {
  const d = sample()

  // A zip name that's valid UTF-8 but lacks the UTF-8 flag: kokyu reads UTF-8, fflate reads latin1
  const named = k.zip({ 'ünï.txt': d })
  const v = new DataView(named.buffer)
  const central = centralRecords(named).at[0]!
  v.setUint16(6, v.getUint16(6, true) & ~0x800, true)
  v.setUint16(central + 8, v.getUint16(central + 8, true) & ~0x800, true)
  assert.deepEqual(Object.keys(k.unzip(named)), ['ünï.txt'])
  assert.deepEqual(Object.keys(f.unzipSync(named)), ['Ã¼nÃ¯.txt'])

  // gzip: no timestamp unless asked (fflate stamps the current time), OS byte 255 (unknown) where fflate writes 3
  const ours = k.gzip(d), theirs = f.gzipSync(d)
  assert.equal(gzipFields(ours).mtime, 0)
  assert.notEqual(gzipFields(theirs).mtime, 0)
  assert.equal(ours[9], 255)
  // fflate's gunzipSync stops after the first member (its Gunzip stream doesn't); kokyu reads them all, as gzip(1) does
  const multi = cat([f.gzipSync(d), f.gzipSync(d)])
  assert.equal(f.gunzipSync(multi).length, d.length)
  assert.equal(k.gunzip(multi).length, 2 * d.length)
  // A zip stream cut between entries: fflate's Unzip accepts it silently, kokyu's unzipStream says it's truncated
  const two = k.zip({ 'a.txt': d, 'b.txt': d }, { level: 0 })
  const cut = two.subarray(0, two.indexOf(0x50, 30 + 5 + d.length))
  const names: string[] = []
  const u = new f.Unzip(file => { names.push(file.name); file.ondata = () => {}; file.start() })
  u.push(cut, true)
  assert.deepEqual(names, ['a.txt'])
  assert.ok(throwsCode(() => k.unzipStream().finish(cut), 'truncated'))
  // A text stream that ends mid-character: kokyu writes U+FFFD, as strFromU8 and TextDecoder do; DecodeUTF8 throws
  const split = new Uint8Array([0x61, 0xE2, 0x82])
  const text = k.decodeTextStream()
  assert.equal(text.push(split) + text.finish(), 'a�')
  assert.equal(f.strFromU8(split), 'a�')
  assert.throws(() => new f.DecodeUTF8(() => {}).push(split, true), /invalid UTF-8/)
  // fflate's `mem` option has no counterpart: kokyu sizes its own tables and ignores it
  same(k.deflate(d, { mem: 12 } as k.DeflateOptions), k.deflate(d), 'mem is ignored')
})
