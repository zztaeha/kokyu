// The edges: rare header fields, ZIP64 written by another tool, entries kokyu can't decode, size limits, output
// buffers, streams used after finishing, the async API's flush, abort and transfer paths, and unchecked checksums.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import * as zl from 'node:zlib'
import * as f from 'fflate'
import * as k from '../engine/index.js'
import {
  attempt, cat, centralRecords, chunked, codeOf, codeOfAsync, failureCost, fullText, has, joinChunks, pieces, python, readable, same,
  sameFiles, sample, unzipInPieces,
} from './helpers.js'

const d = sample.subarray(0, 6000)
const hasPython = has('python3', '-V')

test('gzip headers with extra, name, comment and header CRC decode as node:zlib decodes them', () => {
  const base = k.gzip(d)
  // Header fields go between the fixed 10 bytes and the deflate data
  const build = (flags: number, fields: Uint8Array, method = 8, crc = 0): Uint8Array => {
    const head = base.slice(0, 10)
    head[2] = method
    head[3] = flags
    let all = cat([head, fields])
    if (flags & 2) {
      const sum = (k.crc32(all) & 0xFFFF) ^ crc
      all = cat([all, Uint8Array.of(sum & 0xFF, sum >> 8)])
    }
    return cat([all, base.subarray(10)])
  }
  const text = (s: string): Uint8Array => cat([k.encodeText(s), new Uint8Array(1)])
  const extra = Uint8Array.of(5, 0, 1, 2, 3, 4, 5)
  const cases: [string, Uint8Array][] = [
    ['extra field', build(4, extra)],
    ['name and comment', build(8 | 16, cat([text('a.txt'), text('a comment')]))],
    ['every field with a header CRC', build(4 | 8 | 16 | 2, cat([extra, text('a.txt'), text('a comment')]))],
    ['a reserved flag', build(0x20, new Uint8Array(0))],
    ['another compression method', build(0, new Uint8Array(0), 7)],
  ]
  for (const [what, gz] of cases) {
    const z = attempt(() => zl.gunzipSync(gz))
    const want = z.ok ? 'ok' : 'rejected'
    for (const [how, run] of [['gunzip', () => k.gunzip(gz)], ['gunzipStream', () => chunked(k.gunzipStream(), gz, 1)]] as const) {
      const got = attempt(run)
      assert.equal(got.ok ? 'ok' : 'rejected', want, `${how}, ${what}: zlib ${want}`)
      if (got.ok && z.ok) same(got.value, z.value, `${how}, ${what}`)
    }
  }
  assert.equal(codeOf(() => k.gunzip(cases[3]![1])), 'invalid-header')
  assert.equal(codeOf(() => k.gunzip(cases[4]![1])), 'unsupported')
  // A wrong header CRC: zlib rejects it; kokyu, like fflate, doesn't check it
  const wrongCrc = build(2, new Uint8Array(0), 8, 1)
  assert.equal(attempt(() => zl.gunzipSync(wrongCrc)).ok, false)
  same(k.gunzip(wrongCrc), d, 'kokyu with a wrong header CRC')
  same(f.gunzipSync(wrongCrc), d, 'fflate with a wrong header CRC')
  // Options a header can't hold
  assert.equal(codeOf(() => k.gzip(d, { filename: 'a\u0000b' })), 'invalid-option')
  assert.equal(codeOf(() => k.gzip(d, { mtime: new Date('1969-12-31T23:59:59Z') })), 'invalid-option')
  assert.equal(codeOf(() => k.gzip(d, { mtime: 0x100000000 * 1000 })), 'invalid-option')
  assert.equal(codeOf(() => k.gzip(d, { mtime: 0xFFFFFFFF * 1000 })), 'ok', 'the last time MTIME holds')
  assert.equal(k.gzip(d, { filename: '' })[3], 0, 'an empty filename writes no name field, as fflate does')
})

test('zlib headers match node:zlib at every level, and a dictionary the data never asked for is ignored as node does', () => {
  for (let level = 0; level <= 9; level++) {
    const ours = k.zlib(d, { level: level as k.Level }), theirs = zl.deflateSync(d, { level })
    assert.deepEqual([...ours.subarray(0, 2)], [...theirs.subarray(0, 2)], `level ${level}`)
  }
  const dictionary = fullText.subarray(0, 1000)
  same(zl.inflateSync(k.zlib(d), { dictionary }), d, 'node ignores it')
  same(k.unzlib(k.zlib(d), { dictionary }), d, 'kokyu ignores it')
  // Data that refers back into the dictionary under a header that doesn't ask for one: ignoring it must fail
  const unasked = cat([k.zlib(new Uint8Array(0)).subarray(0, 2), k.deflate(d, { dictionary }), new Uint8Array(4)])
  assert.throws(() => zl.inflateSync(unasked, { dictionary }))
  for (const [what, run] of [['unzlib', () => k.unzlib(unasked, { dictionary })], ['unzlibStream', () => k.unzlibStream({ dictionary }).finish(unasked)]] as const) {
    assert.equal(codeOf(run), 'invalid-data', `${what} ignores the dictionary`)
  }
})

test('ZIP64 archives written by Python read the same everywhere', t => {
  if (!hasPython) { t.skip('no python3 on this machine'); return }
  // A seekable archive with ZIP64 members, an unseekable one (data descriptors, the first with 8-byte sizes), and
  // one with 65,536 entries, which needs the ZIP64 end record and locator
  const script = `
import base64, io, json, sys, zipfile
text = ("kokyu zip64 " * 1000).encode()
data = bytes(range(256)) * 40
a = io.BytesIO()
with zipfile.ZipFile(a, "w", zipfile.ZIP_DEFLATED) as z:
    with z.open("text.txt", "w", force_zip64=True) as f: f.write(text)
    info = zipfile.ZipInfo("bin.dat")
    with z.open(info, "w", force_zip64=True) as f: f.write(data)
class Sink:
    def __init__(self): self.buf = bytearray()
    def write(self, b): self.buf += b; return len(b)
    def flush(self): pass
b = Sink()
with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as z:
    with z.open("text.txt", "w", force_zip64=True) as f: f.write(text)
    with z.open("more.txt", "w") as f: f.write(text[:999])
c = io.BytesIO()
with zipfile.ZipFile(c, "w") as z:
    for i in range(65536): z.writestr(str(i), b"")
print(json.dumps({"a": base64.b64encode(a.getvalue()).decode(), "b": base64.b64encode(bytes(b.buf)).decode(), "c": base64.b64encode(c.getvalue()).decode()}))
`
  const out = JSON.parse(python(script).toString()) as Record<'a' | 'b' | 'c', string>
  const fixture = (key: 'a' | 'b' | 'c'): Uint8Array => new Uint8Array(Buffer.from(out[key], 'base64'))
  const a = fixture('a'), b = fixture('b'), c = fixture('c')
  const text = k.encodeText('kokyu zip64 '.repeat(1000)), bin = Uint8Array.from({ length: 10_240 }, (_, i) => i & 255)
  for (const [what, got] of [['unzip', k.unzip(a)], ['fflate', f.unzipSync(a)], ['unzipStream', unzipInPieces(a, 1)], ['unzipStream, 1000-byte chunks', unzipInPieces(a, 1000)]] as const) {
    same(got['text.txt']!, text, `ZIP64 members, ${what}`)
    same(got['bin.dat']!, bin, `ZIP64 members, ${what}`)
  }
  for (const [what, got] of [['unzip', k.unzip(b)], ['fflate', f.unzipSync(b)], ['unzipStream', unzipInPieces(b, 1)], ['unzipStream, 777-byte chunks', unzipInPieces(b, 777)]] as const) {
    same(got['text.txt']!, text, `ZIP64 descriptors, ${what}`)
    same(got['more.txt']!, text.subarray(0, 999), `ZIP64 descriptors, ${what}`)
  }
  // Python marks the header sizes 0xFFFFFFFF; the ZIP64 field alone says the descriptor is 64-bit, so a header
  // that says 0 instead (APPNOTE 4.3.9.2) must read the same
  const zeroed = b.slice(), zv = new DataView(zeroed.buffer)
  zv.setUint32(18, 0, true)
  zv.setUint32(22, 0, true)
  same(unzipInPieces(zeroed, 1)['text.txt']!, text, 'ZIP64 field with header sizes of 0')
  assert.equal(k.zipEntries(c).length, 65_536)
  assert.equal(Object.keys(k.unzip(c)).length, 65_536)
  assert.equal(Object.keys(unzipInPieces(c, 65_536)).length, 65_536)
})

// Each entry's central record and local header, by name
function records(z: Uint8Array): Record<string, { central: number; local: number }> {
  const v = new DataView(z.buffer, z.byteOffset, z.byteLength)
  const name = (p: number): string => new TextDecoder().decode(z.subarray(p + 46, p + 46 + v.getUint16(p + 28, true)))
  return Object.fromEntries(centralRecords(z).at.map(p => [name(p), { central: p, local: v.getUint32(p + 42, true) }]))
}

// Marks an entry's compression method 12 (bzip2), which kokyu can't decode, in both of its headers
const toMethod12 = (v: DataView, at: { central: number; local: number }): void => {
  v.setUint16(at.local + 8, 12, true)
  v.setUint16(at.central + 10, 12, true)
}

test('zip entries kokyu can\'t decode fail unless a filter skips them and their sizes are known, and padding after the data is skipped', () => {
  const files = { 'a.txt': d, 'b.bin': d.subarray(0, 3000) }
  const keepA = { filter: (e: k.ZipEntryInfo) => e.name === 'a.txt' }
  const patched = (edit: (v: DataView, at: { central: number; local: number }) => void, archive: Uint8Array = k.zip(files)): Uint8Array => {
    const z = archive.slice()
    edit(new DataView(z.buffer), records(z)['b.bin']!)
    return z
  }
  const encrypted = patched((v, at) => { v.setUint16(at.local + 6, v.getUint16(at.local + 6, true) | 1, true); v.setUint16(at.central + 8, v.getUint16(at.central + 8, true) | 1, true) })
  const method12 = patched(toMethod12)
  for (const [what, z] of [['encrypted', encrypted], ['method 12', method12]] as const) {
    assert.equal(codeOf(() => k.unzip(z)), 'unsupported', `unzip, ${what}`)
    assert.equal(codeOf(() => k.unzipStream().finish(z)), 'unsupported', `unzipStream, ${what}`)
    same(k.unzip(z, keepA)['a.txt']!, d, `unzip skips it, ${what}`)
    const chunks = k.unzipStream(keepA).finish(z)
    same(cat(chunks.map(c => c.data)), d, `unzipStream skips it, ${what}`)
    assert.deepEqual(k.zipEntries(z).map(e => e.name), ['a.txt', 'b.bin'])
  }
  // With a data descriptor, only decoding finds where an entry ends, so even a skipped one must be decodable
  const streamedZip = (() => { const z = k.zipStream(), out: Uint8Array[] = []; for (const [n, x] of Object.entries(files)) out.push(z.file(n).finish(x)); return cat([...out, z.finish()]) })()
  const described = patched(toMethod12, streamedZip)
  same(k.unzip(described, keepA)['a.txt']!, d, 'unzip reads sizes from the central directory')
  assert.equal(codeOf(() => k.unzipStream(keepA).finish(described)), 'unsupported')

  // A size deflate could never produce is refused before anything is allocated for it
  const huge = patched((v, at) => v.setUint32(at.central + 24, v.getUint32(at.central + 20, true) * 1032 + 2000, true))
  const refused = failureCost(() => k.unzip(huge))
  assert.equal(refused.code, 'invalid-data')
  assert.ok(refused.largest < 1 << 16, `allocated ${refused.largest} bytes`)

  // Bytes after the deflate data but inside the compressed size: skipped by every reader, as Python skips them
  const one = k.zip({ 'a.txt': d }), v = new DataView(one.buffer)
  const csize = v.getUint32(18, true), end = 30 + 5 + csize
  const padded = cat([one.subarray(0, end), new Uint8Array(7).fill(0xAB), one.subarray(end)])
  const pv = new DataView(padded.buffer)
  pv.setUint32(18, csize + 7, true)
  pv.setUint32(end + 7 + 20, csize + 7, true)
  pv.setUint32(padded.length - 6, end + 7, true)
  same(k.unzip(padded)['a.txt']!, d, 'padded entry, unzip')
  same(f.unzipSync(padded)['a.txt']!, d, 'padded entry, fflate')
  for (const size of [1, 1000]) sameFiles(unzipInPieces(padded, size), { 'a.txt': d }, `padded entry, unzipStream in ${size}-byte chunks`)
  // The padding and the central directory in the same chunk as the end of the deflate data
  same(cat(k.unzipStream().finish(padded).map(c => c.data)), d, 'padded entry, unzipStream in one finish()')
  if (hasPython) {
    const read = python('import io, sys, zipfile\nsys.stdout.buffer.write(zipfile.ZipFile(io.BytesIO(sys.stdin.buffer.read())).read("a.txt"))', padded)
    same(read, d, 'Python reads the padded entry')
  }

  // A stored entry with a descriptor, cut before its descriptor
  const stored = (() => { const z = k.zipStream(), w = z.file('a.txt', { level: 0 }); return cat([w.finish(d), z.finish()]) })()
  assert.equal(codeOf(() => k.unzipStream().finish(stored.subarray(0, 30 + 5 + 1000))), 'truncated')
  // A compressed size that stops short of the deflate data's end
  const short = one.slice()
  new DataView(short.buffer).setUint32(18, csize - 5, true)
  assert.equal(codeOf(() => k.unzipStream().finish(short)), 'truncated')
})

test('zip writing limits fail with the right code', () => {
  const long = 'x'.repeat(65_536)
  for (const [what, run] of [
    ['a name over 65,535 bytes', () => k.zip({ [long]: d })],
    ['an entry comment over 65,535 bytes', () => k.zip({ a: [d, { comment: long }] })],
    ['an archive comment over 16,361 bytes, past where bsdtar looks for the end', () => k.zip({ a: d }, { comment: 'x'.repeat(16_362) })],
    ['a streamed archive comment over 16,361 bytes', () => k.zipStream({ comment: 'x'.repeat(16_362) })],
    ['an extra field over 65,535 bytes', () => k.zip({ a: [d, { extra: { 1: new Uint8Array(65_536) } }] })],
    ['extra fields adding up to more', () => k.zip({ a: [d, { extra: { 2: new Uint8Array(40_000), 3: new Uint8Array(40_000) } }] })],
    ['an extra field id past 16 bits', () => k.zip({ a: [d, { extra: { 70_000: new Uint8Array(1) } }] })],
    ['a negative extra field id', () => k.zip({ a: [d, { extra: { [-1]: new Uint8Array(1) } }] })],
    ['an extra field that isn\'t bytes', () => k.zip({ a: [d, { extra: { 1: 'text' as never } }] })],
    ['a streamed entry with too long a name', () => k.zipStream().file(long)],
    ['a date before 1980', () => k.zip({ a: [d, { mtime: '1979-12-31T12:00:00' }] })],
    ['a date after 2107', () => k.zip({ a: [d, { mtime: '2108-01-01T00:00:00' }] })],
  ] as const) assert.equal(codeOf(run), 'invalid-option', what)
  // Each limit itself is allowed
  const longest = 'x'.repeat(65_535)
  for (const [what, run] of [
    ['a name of 65,535 bytes', () => k.zip({ [longest]: d })],
    ['an entry comment of 65,535 bytes', () => k.zip({ a: [d, { comment: longest }] })],
    ['an archive comment of 16,361 bytes', () => k.zip({ a: d }, { comment: 'x'.repeat(16_361) })],
    ['extra fields adding up to 65,535 bytes', () => k.zip({ a: [d, { extra: { 2: new Uint8Array(65_531) } }] })],
    ['an extra field id of 0', () => k.zip({ a: [d, { extra: { 0: new Uint8Array(1) } }] })],
    ['an extra field id of 0xFFFF', () => k.zip({ a: [d, { extra: { 0xFFFF: new Uint8Array(1) } }] })],
    ['a streamed entry with a name of 65,535 bytes', () => k.zipStream().file(longest)],
    ['the first date of 1980', () => k.zip({ a: [d, { mtime: '1980-01-01T00:00:00' }] })],
    ['the last date of 2107', () => k.zip({ a: [d, { mtime: '2107-12-31T23:59:58' }] })],
  ] as const) assert.equal(codeOf(run), 'ok', what)
  // 65,535 entries is the count that means "see the ZIP64 record", so a plain archive holds at most 65,534
  const many = (n: number): Record<string, Uint8Array> => Object.fromEntries(Array.from({ length: n }, (_, i) => [`f${i}`, new Uint8Array(0)]))
  const most = k.zip(many(65_534))
  assert.equal(Object.keys(f.unzipSync(most)).length, 65_534)
  assert.equal(codeOf(() => k.zip(many(65_535))), 'too-large')
})

test('out buffers with dictionaries, several gzip members, and every format decompress detects', () => {
  const dictionary = fullText.subarray(0, 4000), body = fullText.subarray(4000, 20_000)
  const packed = k.deflate(body, { dictionary }), out = new Uint8Array(body.length)
  const got = k.inflateInto(packed, out, { dictionary })
  same(got, body, 'inflate with a dictionary into out')
  assert.equal(got.buffer, out.buffer)
  assert.equal(codeOf(() => k.inflateInto(packed, new Uint8Array(body.length - 1), { dictionary })), 'too-large')
  same(k.unzlibInto(k.zlib(body, { dictionary }), new Uint8Array(body.length), { dictionary }), body, 'unzlib with a dictionary into out')
  // With a dictionary the output can't go straight into out, but out's size still stops a bomb
  const bomb = k.deflate(new Uint8Array(5_000_000), { dictionary })
  const { code, largest } = failureCost(() => k.inflateInto(bomb, new Uint8Array(10), { dictionary }))
  assert.equal(code, 'too-large')
  assert.ok(largest < 1 << 16, `allocated ${largest} bytes`)
  // Several members land one after another
  const multi = cat([k.gzip(body.subarray(0, 5000)), f.gzipSync(body.subarray(5000))]), into = new Uint8Array(body.length + 10)
  const all = k.gunzipInto(multi, into)
  same(all, body, 'gunzip of two members into out')
  assert.equal(all.buffer, into.buffer)
  assert.equal(codeOf(() => k.gunzipInto(multi, new Uint8Array(body.length - 1))), 'too-large')
  for (const [what, p] of [['gzip', k.gzip(body)], ['zlib', k.zlib(body)], ['raw', k.deflate(body)]] as const) {
    const o = new Uint8Array(body.length)
    same(k.decompressInto(p, o), body, `decompress ${what} into out`)
    assert.equal(codeOf(() => k.decompressInto(p, new Uint8Array(body.length - 1))), 'too-large', `decompress ${what} into too small a buffer`)
  }
})

test('no stream can be used after it finishes', async () => {
  const cases: [string, () => { push(c: never): unknown; finish(c?: never): unknown }, unknown][] = [
    ['deflateStream', () => k.deflateStream(), d], ['zlibStream', () => k.zlibStream(), d], ['gzipStream', () => k.gzipStream(), d],
    ['inflateStream', () => k.inflateStream(), k.deflate(d)], ['unzlibStream', () => k.unzlibStream(), k.zlib(d)],
    ['gunzipStream', () => k.gunzipStream(), k.gzip(d)], ['decompressStream', () => k.decompressStream(), k.gzip(d)],
    ['unzipStream', () => k.unzipStream(), k.zip({ a: d })], ['encodeTextStream', () => k.encodeTextStream(), 'abc'],
    ['decodeTextStream', () => k.decodeTextStream(), d], ['latin1 decodeTextStream', () => k.decodeTextStream({ latin1: true }), d],
  ]
  for (const [name, make, input] of cases) {
    const s = make()
    s.finish(input as never)
    assert.equal(codeOf(() => s.push(input as never)), 'invalid-state', `${name} push`)
    assert.equal(codeOf(() => s.finish()), 'invalid-state', `${name} finish`)
  }
  // A compressor's flush is guarded the same way, and a chunk refused for its type leaves the stream as it was
  for (const [name, make, unpack] of [['deflateStream', k.deflateStream, k.inflate], ['zlibStream', k.zlibStream, k.unzlib], ['gzipStream', k.gzipStream, k.gunzip]] as const) {
    const done = make()
    done.finish(d)
    assert.equal(codeOf(() => done.flush()), 'invalid-state', `${name} flush after finish`)
    const s = make()
    assert.equal(codeOf(() => s.push('not bytes' as never)), 'invalid-option', `${name} bad chunk`)
    assert.equal(codeOf(() => s.finish('not bytes' as never)), 'invalid-option', `${name} bad last chunk`)
    assert.deepEqual(unpack(cat([s.push(d), s.flush(), s.finish(d)])), cat([d, d]), `${name} after refused chunks`)
  }
  // A call that failed midway leaves the stream broken: every later call rethrows
  const broken = k.inflateStream()
  assert.equal(codeOf(() => broken.push(Uint8Array.of(0xFF))), 'invalid-data')
  assert.equal(codeOf(() => broken.finish(k.deflate(d))), 'invalid-data')
  // A zip entry's writer belongs to that entry alone
  const z = k.zipStream(), first = z.file('a.txt')
  first.finish(d)
  z.file('b.txt')
  assert.equal(codeOf(() => first.push(d)), 'invalid-state', 'a finished entry\'s writer')
  const az = k.zipStreamAsync(), afirst = az.file('a.txt')
  void afirst.finish(d)
  az.file('b.txt')
  await assert.rejects(afirst.push(d), { name: 'KokyuError', code: 'invalid-state' })
  // Async streams, inline and in a worker
  for (const size of [1000, 100_000]) {
    const body = fullText.subarray(0, size)
    for (const [name, s, input] of [['gzipStreamAsync', k.gzipStreamAsync(), body], ['gunzipStreamAsync', k.gunzipStreamAsync(), k.gzip(body)], ['unzipStreamAsync', k.unzipStreamAsync(), k.zip({ a: body })]] as const) {
      await (s as k.AsyncStream<Uint8Array, unknown>).finish(input)
      await assert.rejects((s as k.AsyncStream<Uint8Array, unknown>).push(input), { code: 'invalid-state' }, `${name}, ${size} B`)
    }
  }
})

test('a finished stream lets go of its buffers and tables, though the caller still holds it', () => {
  // In a child with gc() to call. A stream that kept its coder would hold 4 MB (a decoder's peak buffer) or 700 KB.
  const script = `const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
const text = Uint8Array.from({ length: 4 << 20 }, (_, i) => 97 + (i * i >>> 7) % 26), z = k.zlib(text)
// A few rounds: V8 can free array buffers on another thread after gc() returns
const held = async () => { for (let i = 0; i < 4; i++) { gc(); await new Promise(r => setTimeout(r, 10)) } return process.memoryUsage().arrayBuffers }
const cases = {
  inflate: () => { const s = k.inflateStream(); s.push(z.subarray(2)); s.finish(); return s },
  unzlib: () => { const s = k.unzlibStream(); s.finish(z); return s },
  decompress: () => { const s = k.decompressStream(); s.push(z); s.finish(); return s },
  deflate: () => { const s = k.deflateStream(); s.push(text.subarray(0, 200000)); s.finish(); return s },
}
const out = {}
for (const [name, make] of Object.entries(cases)) { const before = await held(), keep = Array.from({ length: 20 }, make); out[name] = (await held() - before) / keep.length }
console.log(JSON.stringify(out))`
  const got = JSON.parse(String(execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', script], { timeout: 60_000 }))) as Record<string, number>
  for (const [name, bytes] of Object.entries(got)) assert.ok(bytes < 64 << 10, `a finished ${name} stream holds ${bytes} B`)
})

test('async streams: flush, abort once in a worker, queued jobs, and what crosses to a worker', async () => {
  const body = fullText.subarray(0, 100_000)
  for (const [make, decode] of [[k.deflateStreamAsync, zl.inflateRawSync], [k.zlibStreamAsync, zl.inflateSync], [k.gzipStreamAsync, zl.gunzipSync]] as const) {
    const s = make()
    const head = cat(await Promise.all([s.push(body.subarray(0, 60_000)), s.flush()]))
    same(decode(head, { finishFlush: zl.constants.Z_SYNC_FLUSH }), body.subarray(0, 60_000), 'flushed async output decodes so far')
    same(decode(cat([head, ...await Promise.all([s.push(body.subarray(60_000)), s.finish()])])), body, 'and all of it after finish')
  }

  // Abort after the stream has moved to a worker: what's pending and anything later rejects
  const c = new AbortController(), s = k.gzipStreamAsync({ signal: c.signal })
  await s.push(body)
  const pending = s.push(fullText.subarray(0, 1_000_000))
  c.abort()
  await assert.rejects(pending, e => e === c.signal.reason)
  await assert.rejects(s.push(body), e => e === c.signal.reason)

  // A job still waiting for a worker can be aborted; the others finish
  const big = fullText.subarray(0, 1_000_000), controls = Array.from({ length: 10 }, () => new AbortController())
  const jobs = controls.map(ctl => k.deflateAsync(big, { level: 9, signal: ctl.signal }))
  controls[9]!.abort()
  await assert.rejects(jobs[9]!, e => e === controls[9]!.signal.reason)
  for (const job of jobs.slice(0, 9)) same(f.inflateSync(await job), big, 'the jobs not aborted')

  // signal for zip and unzip, and transfer for unzip
  const done = new AbortController()
  done.abort()
  await assert.rejects(k.zipAsync({ a: body }, { signal: done.signal }), { name: 'AbortError' })
  await assert.rejects(k.unzipAsync(k.zip({ a: body }), { signal: done.signal }), { name: 'AbortError' })
  const archive = k.zip({ a: body })
  same((await k.unzipAsync(archive, { transfer: true }))['a']!, body, 'unzipAsync with transfer')
  assert.equal(archive.length, 0)
  // Options the engine never reads stay behind, even ones a worker couldn't receive
  same(await k.deflateAsync(body, { onProgress: () => {} } as k.DeflateOptions), k.deflate(body), 'an unknown option')
  // An option the engine reads but a worker can't receive fails the call instead of leaving it pending
  await assert.rejects(k.zipAsync({ a: [body, { extra: { 1: (() => {}) as never } }] }), { code: 'invalid-option' })

  // A filter runs once per entry, inline or with a worker
  for (const size of [1000, 100_000]) {
    let calls = 0
    await k.unzipAsync(k.zip({ a: fullText.subarray(0, size), b: d }), { filter: () => { calls++; return true } })
    assert.equal(calls, 2, `filter calls, ${size} B`)
  }

  // unzipStreamAsync filters on this side, so an entry it can't decode, with its sizes in its header, fails only when
  // the filter keeps it
  const files = { 'a.txt': body, 'b.bin': d }
  const z = k.zip(files, { level: 0 })
  toMethod12(new DataView(z.buffer), records(z)['b.bin']!)
  for (const size of [z.length, 1000]) {
    const keep = k.unzipStreamAsync({ filter: e => e.name === 'a.txt' })
    same(cat((await Promise.all(pieces(keep, z, size))).flat().map(ch => ch.data)), body, 'skipped by the filter')
    const all = k.unzipStreamAsync({ filter: () => true })
    await assert.rejects(Promise.all(pieces(all, z, size)), { code: 'unsupported' })
  }
})

test('latin1 text streams and Web streams of arrays and strings', async () => {
  const latin = String.fromCharCode(...Array.from({ length: 256 }, (_, i) => i)).repeat(20), bytes = k.encodeText(latin, { latin1: true })
  const enc = k.encodeTextStream({ latin1: true }), encoded: Uint8Array[] = []
  for (let o = 0; o < latin.length; o += 37) encoded.push(enc.push(latin.slice(o, o + 37)))
  same(cat([...encoded, enc.finish()]), bytes, 'latin1 encodeTextStream')
  assert.equal(pieces(k.decodeTextStream({ latin1: true }), bytes, 41).join(''), latin, 'latin1 decodeTextStream')

  const files = { 'a.txt': sample, 'b.txt': sample.subarray(0, 100) }
  const chunks: k.UnzipChunk[] = []
  for await (const ch of readable(k.zip(files), 999).pipeThrough(k.toTransformStream(k.unzipStream()))) chunks.push(ch)
  sameFiles(joinChunks(chunks), files, 'unzip through a TransformStream')
  let text = ''
  for await (const s of readable(k.encodeText('héllo 🎉 '.repeat(300)), 999).pipeThrough(k.toTransformStream(k.decodeTextStream()))) text += s
  assert.equal(text, 'héllo 🎉 '.repeat(300))
})

test('one-shot decodes share their tables, so a paused stream keeps its own', () => {
  // Two texts with different code tables, each long enough for several dynamic blocks
  const a = fullText.subarray(0, 300_000), b = fullText.subarray(300_000, 600_000)
  const za = k.deflate(a), zb = k.deflate(b)
  for (const cut of [1000, za.length >> 1, za.length - 1000]) {
    const s = k.inflateStream()
    const head = s.push(za.subarray(0, cut))
    same(k.inflate(zb), b, 'one-shot inflate between the pushes')
    same(cat([head, s.finish(za.subarray(cut))]), a, `stream paused at ${cut} of ${za.length}`)
  }
})

test('decoders don\'t check checksums, inline or in a worker, except to find where a stored streamed entry ends', async () => {
  const viaStreamAsync = async (s: k.AsyncStream, b: Uint8Array): Promise<Uint8Array> => cat(await Promise.all(pieces(s, b, 1000)))
  const paths = (one: (b: Uint8Array) => Uint8Array, stream: () => k.Stream,
    oneAsync: (b: Uint8Array) => Promise<Uint8Array>, streamAsync: () => k.AsyncStream) => [
    ['one-shot', async (b: Uint8Array) => one(b)],
    ['stream', async (b: Uint8Array) => chunked(stream(), b, 1000)],
    ['async', (b: Uint8Array) => oneAsync(b)],
    ['async stream', (b: Uint8Array) => viaStreamAsync(streamAsync(), b)],
  ] as const
  const unzipped = (files: Record<string, Uint8Array>): Uint8Array => cat(Object.keys(files).sort().map(n => files[n]!))
  const fromChunks = (chunks: k.UnzipChunk[]): Uint8Array => unzipped(joinChunks(chunks))
  const zipPaths = [
    ['one-shot', async (b: Uint8Array) => unzipped(k.unzip(b))],
    ['stream', async (b: Uint8Array) => unzipped(unzipInPieces(b, 1000))],
    ['async', async (b: Uint8Array) => unzipped(await k.unzipAsync(b))],
    ['async stream', async (b: Uint8Array) => fromChunks((await Promise.all(pieces(k.unzipStreamAsync(), b, 1000))).flat())],
  ] as const
  // Each entry's CRC-32 fields: local header (+14) and central record (+16)
  const breakCrcs = (z: Uint8Array): Uint8Array => {
    const b = z.slice(), v = new DataView(b.buffer)
    for (let p = 0; p + 4 <= b.length; p++) {
      const sig = v.getUint32(p, true)
      const at = sig === 0x04034b50 ? 14 : sig === 0x02014b50 ? 16 : -1
      if (at >= 0) b[p + at]! ^= 1
    }
    return b
  }
  // Async paths run inline below 16 KB of input: 2,000 bytes of text stay inline, 200,000 (37 KB packed) go to a worker
  for (const size of [2000, 200_000]) {
    const x = fullText.subarray(0, size)
    const z = k.zlib(x); z[z.length - 1]! ^= 1
    const g = k.gzip(x); g[g.length - 8]! ^= 1
    const cases = [
      ['zlib', z, paths(k.unzlib, k.unzlibStream, k.unzlibAsync, k.unzlibStreamAsync)],
      ['gzip', g, paths(k.gunzip, k.gunzipStream, k.gunzipAsync, k.gunzipStreamAsync)],
      ['decompress of zlib', z, paths(k.decompress, k.decompressStream, k.decompressAsync, k.decompressStreamAsync)],
      ['decompress of gzip', g, paths(k.decompress, k.decompressStream, k.decompressAsync, k.decompressStreamAsync)],
    ] as const
    for (const [format, bad, list] of cases) {
      for (const [path, run] of list) same(await run(bad), x, `${format} with a broken checksum, ${path}, ${size} B`)
    }
    // A wrong gzip size still fails: it's a length, not a checksum
    const sized = k.gzip(x); sized[sized.length - 4]! ^= 1
    for (const [path, run] of cases[1][2]) assert.equal(await codeOfAsync(() => run(sized)), 'invalid-data', `gzip size, ${path}, ${size} B`)

    // A deflated and a stored entry with broken CRCs, then a stream-written archive whose stored entry ends at a
    // data descriptor: there the CRC is how the reader finds the entry's end, so it's still computed
    const want = cat([x, x])
    const archive = breakCrcs(k.zip({ 'a.txt': x, 'b.txt': [x, { level: 0 }] }))
    const zs = k.zipStream()
    const streamed = cat([zs.file('a.txt').finish(x), zs.file('b.txt', { level: 0 }).finish(x), zs.finish()])
    for (const [path, run] of zipPaths) {
      same(await run(archive), want, `zip with broken CRCs, ${path}, ${size} B`)
      same(await run(streamed), want, `zip with descriptors, ${path}, ${size} B`)
    }
  }
})
