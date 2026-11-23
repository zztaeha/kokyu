import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as zl from 'node:zlib'
import * as ff from 'fflate'
import * as k from '../engine/index.js'
import { deflater, buildCodes } from '../engine/deflate.js'
import { inflater, buildTable } from '../engine/inflate.js'
import { binary, bytes, cat, chunked, fullText, largestAllocation, randomBytes, rng, sample, throwsCode, totalAllocation } from './helpers.js'

const LEVELS = [0, 1, 6, 9] as const
const corpus: [string, Uint8Array][] = [
  ['empty', new Uint8Array(0)],
  ['1 byte', new Uint8Array([42])],
  ['text 300 B', fullText.subarray(0, 300)],
  ['text 200 KB', fullText.subarray(0, 200_000)],
  ['random 70 KB', randomBytes(70_000)],
  ['zeros 300 KB', new Uint8Array(300_000)],
]

test('every format round-trips through fflate and node:zlib, both directions', () => {
  for (const [name, d] of corpus) {
    for (const level of LEVELS) {
      const at = `${name}, level ${level}`
      assert.deepEqual(ff.inflateSync(k.deflate(d, { level })), d, `deflate ours→fflate ${at}`)
      assert.deepEqual(k.inflate(ff.deflateSync(d, { level })), d, `deflate fflate→ours ${at}`)
      assert.deepEqual(bytes(zl.inflateSync(k.zlib(d, { level }))), d, `zlib ours→node ${at}`)
      assert.deepEqual(k.unzlib(zl.deflateSync(d, { level })), d, `zlib node→ours ${at}`)
      assert.deepEqual(ff.gunzipSync(k.gzip(d, { level })), d, `gzip ours→fflate ${at}`)
      assert.deepEqual(bytes(zl.gunzipSync(k.gzip(d, { level, filename: 'a.txt', mtime: 1e12 }))), d, `gzip ours→node ${at}`)
      assert.deepEqual(k.gunzip(zl.gzipSync(d, { level })), d, `gzip node→ours ${at}`)
    }
    for (const packed of [ff.gzipSync(d), ff.zlibSync(d), ff.deflateSync(d)]) assert.deepEqual(k.decompress(packed), d, `decompress ${name}`)
  }
})

test('Huffman lengths are optimal, capped at the limit, and complete', () => {
  // Total cost of an optimal unrestricted code: the sum of every merge
  const optimal = (w: number[]): number => {
    const q = w.filter(x => x > 0).sort((a, b) => a - b)
    let cost = 0
    while (q.length > 1) {
      const m = q.shift()! + q.shift()!
      cost += m
      q.splice(q.findIndex(x => x >= m) >>> 0, 0, m)
    }
    return cost
  }
  const fib = [1, 1]
  while (fib.length < 30) fib.push(fib.at(-1)! + fib.at(-2)!)
  const r = rng(5)
  // Weights, length limit, and whether the result must be optimal (the limit can't bind, and two or more weights)
  const cases: [number[], number, boolean][] = [
    [fib, 15, false], [fib.slice(0, 19), 7, false], [[0, 0, 7, 0], 15, false], [[3, 9], 15, true],
    [Array.from({ length: 286 }, () => (r() * 1000) | 0), 15, false], [Array.from({ length: 19 }, () => (r() * 50) | 0), 7, false],
    // Within 10x of each other, 286 weights can't reach depth 15
    [Array.from({ length: 286 }, () => 100 + ((r() * 900) | 0)), 15, true],
  ]
  for (const [w, maxBits, opt] of cases) {
    const lengths = buildCodes(Uint32Array.from(w), maxBits)
    const used = w.filter(x => x > 0).length
    let kraft = 0, cost = 0
    w.forEach((x, i) => { if (x) { kraft += 2 ** -lengths[i]!; cost += x * lengths[i]! } })
    assert.ok(Math.max(...lengths) <= maxBits, `capped at ${maxBits}`)
    assert.equal(kraft, used === 1 ? 0.5 : 1, 'complete')
    if (opt) assert.equal(cost, optimal(w), 'optimal')
  }
})

test('chunking never changes deflate output', () => {
  // Ends inside long repeats, so held matches reach the last byte: nothing may read past it
  const tails: [string, Uint8Array][] = [130, 257, 300].map(n => [`ends in a ${n}-byte repeat`, cat([sample, sample.subarray(40_000, 40_000 + n)])])
  for (const [name, d] of [...corpus, ...tails]) {
    for (let level = 0; level <= 9; level++) {
      const oneShot = k.deflate(d, { level: level as k.Level })
      for (const max of [5, 70_000]) {
        const s = deflater(level)
        const streamed = chunked({ push: c => s.push(c), finish: () => s.push(new Uint8Array(0), true) }, d, max)
        const at = `${name}, level ${level}, chunks ≤${max}`
        if (level > 0) assert.deepEqual(streamed, oneShot, at)
        assert.deepEqual(ff.inflateSync(streamed), d, `${at} decodes`)
      }
    }
  }
})

test('one-shot compressors allocate their output once, with the header and trailer written in place', () => {
  // Stored output is the most a compressor gives; growing to it by doubling, then joining the framing, took about 5×
  const d = randomBytes(1 << 20)
  for (const [name, fn] of [['deflate', k.deflate], ['zlib', k.zlib], ['gzip', k.gzip]] as const) {
    const total = totalAllocation(() => fn(d))
    assert.ok(total <= d.length * 1.1, `${name} allocated ${total} B for ${d.length} B`)
  }
})

test('stored blocks in a row share headers, one per 64 KB piece, however the input arrives', () => {
  const d = randomBytes(1 << 20)
  for (const level of [1, 6, 9] as const) {
    const oneShot = k.deflate(d, { level })
    assert.ok(oneShot.length <= d.length + 5 * Math.ceil(d.length / 65535), `level ${level}: ${oneShot.length - d.length} B of headers`)
    assert.deepEqual(chunked(k.deflateStream({ level }), d, 5000), oneShot, `level ${level} streamed`)
  }
})

test('stored data decodes into a buffer of exactly its size, and stored blocks followed by coded ones still decode', () => {
  const d = randomBytes(300_000)
  for (const packed of [k.deflate(d, { level: 0 }), bytes(zl.deflateRawSync(d))]) {
    assert.equal(largestAllocation(() => k.inflate(packed)), d.length)
    assert.deepEqual(k.inflate(packed), d)
  }
  // A stored block that isn't the last, then a whole deflate stream: the size walk stops at the coded block
  const head = sample.subarray(0, 1000), mixed = cat([new Uint8Array([0, 0xe8, 0x03, 0x17, 0xfc]), head, k.deflate(sample)])
  assert.deepEqual(k.inflate(mixed), cat([head, sample]))
})

test('a stream that takes an earlier stream\'s tables compresses as it would with fresh ones, beside a stream left open', () => {
  const d = sample.subarray(0, 200_000), want = k.deflate(d)
  // Takes a set of its own and never finishes, so it never gives it back
  k.deflateStream().push(d)
  // Each stream before leaves its chains for the next; other data in them must count for nothing
  for (const [i, before] of [fullText.subarray(300_000, 500_000), binary, d].entries()) {
    chunked(k.deflateStream(), before, 70_000)
    assert.deepEqual(chunked(k.deflateStream(), d, 70_000, i), want, `stream after ${i}`)
  }
  // Two at once: the second takes the spare the first will leave, and they never share one
  const a = k.deflateStream(), b = k.deflateStream(), outA = [a.push(d.subarray(0, 120_000))], outB = [b.push(d.subarray(0, 90_000))]
  outA.push(a.finish(d.subarray(120_000)))
  outB.push(b.push(d.subarray(90_000)), b.finish())
  assert.deepEqual(cat(outA), want)
  assert.deepEqual(cat(outB), want)
})

test('a dictionary on reused tables compresses as on fresh ones, though its last positions never go in the chains', () => {
  // The dictionary ends with z's first 15 bytes and holds all of z at 1000; the data is z. The search finds the
  // 22-byte match at the dictionary's end, then switches to a chain through its last 3 positions: empty in fresh
  // tables, but the earlier stream left links there, to where the 52-byte match at 1000 starts.
  const z = randomBytes(52, 3)
  z.copyWithin(15, 0, 7)
  const dictionary = randomBytes(30_000, 4), d = cat([z, randomBytes(2000, 5)]), earlier = randomBytes(65_536, 6)
  dictionary.set(z.subarray(0, 15), 29_985)
  dictionary.set(z, 1000)
  earlier.copyWithin(62_765, 1012, 1018)
  // A stream left open takes any spare set, so this call gets fresh tables
  k.deflateStream().push(binary.subarray(0, 70_000))
  const fresh = k.deflate(d, { dictionary })
  // A finished stream leaves its tables for the next call
  const s = k.deflateStream()
  s.push(earlier)
  s.finish()
  assert.deepEqual(k.deflate(d, { dictionary }), fresh)
  assert.deepEqual(k.inflate(fresh, { dictionary }), d)
})

test('a block that closes on a held match leaves the match to the next block, which may go out stored', () => {
  // Random bytes go out stored; a 300-byte repeat makes the search try every position after it. A 5-byte match is then
  // held and beaten as the block fills, so the next block must start at the held match's first byte.
  const base = randomBytes(40_000), y = 16_200
  base.set(base.subarray(y - 1000, y - 700), y)
  for (let x = y + 301; x < y + 600; x++) {
    const d = base.slice()
    d.set(d.subarray(x, x + 5), x - 100)
    d.set(d.subarray(x + 1, x + 8), x - 299)
    for (const level of [5, 6, 7] as const) assert.deepEqual(bytes(zl.inflateRawSync(k.deflate(d, { level }))), d, `match at ${x}, level ${level}`)
  }
})

// Matching heuristics only ever change sizes, which a round trip can't see
test('a run codes as length 258\'s own symbol, so it costs what zlib\'s does', () => {
  // 258 also fits the code below it with 5 extra bits: valid, but every run would cost 5 bits more
  const zeros = new Uint8Array(100_000)
  assert.ok(k.deflate(zeros).length <= zl.deflateRawSync(zeros).length)
})

test('each level compresses at least as well as the one below, and level 9 within 1% of zlib\'s', () => {
  for (const [name, d, slack] of [['text', fullText.subarray(0, 300_000), 1], ['machine code', binary, 1.01]] as const) {
    let prev = Infinity
    for (let level = 1; level <= 9; level++) {
      const size = k.deflate(d, { level: level as k.Level }).length
      assert.ok(size <= prev, `${name}: level ${level} gave ${size} bytes, level ${level - 1} gave ${prev}`)
      prev = size
    }
    const zlib9 = zl.deflateRawSync(d, { level: 9 }).length
    assert.ok(prev <= zlib9 * slack, `${name}: level 9 gave ${prev} bytes, zlib's level 9 gave ${zlib9}`)
  }
})

test('level 9 stays within 1% of zlib\'s on text made of a few short words, where a match needs many candidates', () => {
  // Every word starts thousands of matches, and only a rare few go on into the next words: the search has to switch
  // to a sparser chain to reach them
  const r = rng(2026), words = ['데이터', '압축', 'データ', '圧縮', '数据', 'данные', 'сжатие', 'بيانات', 'données', 'naïve', 'stream', '🚀']
  let s = ''
  while (s.length < 300_000) s += Array.from({ length: 4 + ((r() * 8) | 0) }, () => words[(r() * words.length) | 0]).join(' ') + '. '
  const d = new TextEncoder().encode(s), size = k.deflate(d, { level: 9 }).length, zlib9 = zl.deflateRawSync(d, { level: 9 }).length
  assert.ok(size <= zlib9 * 1.01, `level 9 gave ${size} bytes, zlib's level 9 gave ${zlib9}`)
})

test('streams round-trip with fflate and node:zlib at any chunk size, down to 1 byte', () => {
  for (const [name, d] of corpus.filter(([n]) => !n.startsWith('zeros'))) {
    for (const max of [1, 9, 4096, 100_000]) {
      const at = `${name}, chunks ≤${max}`
      assert.deepEqual(ff.inflateSync(chunked(k.deflateStream(), d, max)), d, `deflateStream ${at}`)
      assert.deepEqual(ff.unzlibSync(chunked(k.zlibStream(), d, max)), d, `zlibStream ${at}`)
      assert.deepEqual(bytes(zl.gunzipSync(chunked(k.gzipStream(), d, max))), d, `gzipStream ${at}`)
      assert.deepEqual(chunked(k.inflateStream(), ff.deflateSync(d), max), d, `inflateStream ${at}`)
      assert.deepEqual(chunked(k.unzlibStream(), zl.deflateSync(d), max), d, `unzlibStream ${at}`)
      assert.deepEqual(chunked(k.gunzipStream(), zl.gzipSync(d), max), d, `gunzipStream ${at}`)
      assert.deepEqual(chunked(k.decompressStream(), ff.gzipSync(d), max), d, `decompressStream gzip ${at}`)
      assert.deepEqual(chunked(k.decompressStream(), ff.deflateSync(d), max), d, `decompressStream raw ${at}`)
    }
  }
})

test('stream outputs are never written to again after they are returned', () => {
  const s = k.inflateStream()
  const packed = ff.deflateSync(sample)
  const first = s.push(packed.subarray(0, 2000))
  const snapshot = first.slice()
  s.finish(packed.subarray(2000))
  assert.deepEqual(first, snapshot)
})

test('flush makes everything so far decodable without ending the stream', () => {
  for (const make of [k.deflateStream, k.zlibStream]) {
    const s = make()
    const head = cat([s.push(sample.subarray(0, 1000)), s.flush()])
    const decode = make === k.zlibStream ? zl.inflateSync : zl.inflateRawSync
    assert.deepEqual(bytes(decode(head, { finishFlush: zl.constants.Z_SYNC_FLUSH })), sample.subarray(0, 1000))
    const all = cat([head, s.push(sample.subarray(1000)), s.finish()])
    assert.deepEqual(bytes(decode(all)), sample)
  }
})

test('preset dictionaries work with fflate and node:zlib', () => {
  const dict = sample.subarray(0, 8000), d = sample.subarray(8000, 12_000)
  // Each compressor must use it: a round trip alone passes even when one ignores it
  const packers: [string, (o: k.DeflateOptions) => Uint8Array][] = [
    ['deflate', o => k.deflate(d, o)], ['zlib', o => k.zlib(d, o)],
    ['deflateStream', o => chunked(k.deflateStream(o), d, 1000)], ['zlibStream', o => chunked(k.zlibStream(o), d, 1000)],
  ]
  for (const [what, pack] of packers) assert.ok(pack({ dictionary: dict }).length < pack({}).length, `${what} uses the dictionary`)
  assert.deepEqual(ff.inflateSync(k.deflate(d, { dictionary: dict }), { dictionary: dict }), d)
  assert.deepEqual(k.inflate(ff.deflateSync(d, { dictionary: dict }), { dictionary: dict }), d)
  assert.deepEqual(bytes(zl.inflateSync(k.zlib(d, { dictionary: dict }), { dictionary: dict })), d)
  assert.deepEqual(k.unzlib(zl.deflateSync(d, { dictionary: dict }), { dictionary: dict }), d)
  assert.deepEqual(chunked(k.unzlibStream({ dictionary: dict }), zl.deflateSync(d, { dictionary: dict }), 5), d)
  assert.ok(throwsCode(() => k.unzlib(zl.deflateSync(d, { dictionary: dict })), 'invalid-option'))
  assert.ok(throwsCode(() => k.unzlib(zl.deflateSync(d, { dictionary: dict }), { dictionary: sample.subarray(0, 10) }), 'invalid-option'))
})

test('gzip: multiple members and header fields', () => {
  const a = sample.subarray(0, 1000), b = sample.subarray(1000, 4000)
  const multi = cat([zl.gzipSync(a), zl.gzipSync(b)])
  assert.deepEqual(k.gunzip(multi), cat([a, b]))
  assert.deepEqual(chunked(k.gunzipStream(), multi, 3), cat([a, b]))
  // One-shot and streaming agree on what follows the last member: ignored unless it starts another
  for (const tail of [[0x1f], [0], [0x1f, 0x8b]]) {
    const d = cat([multi, new Uint8Array(tail)])
    const result = (f: () => Uint8Array): Uint8Array | string => { try { return f() } catch (e) { return (e as { code: string }).code } }
    assert.deepEqual(result(() => chunked(k.gunzipStream(), d, 3)), result(() => k.gunzip(d)), `tail ${tail}`)
  }
  assert.deepEqual(k.gunzip(ff.gzipSync(a, { filename: 'x', mtime: 1e12 })), a)
})

test('gunzip never trusts a junk trailer for a big buffer, and lands honest sizes exactly', () => {
  const withJunk = cat([k.gzip(sample), new Uint8Array([0, 0xFF, 0xFF, 0xFF, 0x7F])])
  const junk = largestAllocation(() => assert.deepEqual(k.gunzip(withJunk), sample))
  assert.ok(junk <= withJunk.length * 8, `allocated ${junk} bytes for ${withJunk.length} of input`)
  // Past 8x the buffer grows onto exactly the claimed size instead of doubling past it
  const zeros = new Uint8Array(300_000), packed = k.gzip(zeros)
  assert.equal(largestAllocation(() => assert.deepEqual(k.gunzip(packed), zeros)), zeros.length)
})

test('decoding into a caller\'s buffer never writes past the result', () => {
  // Junk after the data keeps the fast loop running to the very last match
  for (let i = 0; i < 40; i++) {
    const d = sample.subarray(i * 997, i * 997 + 3000 + i * 37)
    const out = new Uint8Array(d.length + 64).fill(0xAA)
    assert.deepEqual(k.gunzipInto(cat([k.gzip(d), new Uint8Array(32)]), out), d)
    assert.ok(out.subarray(d.length).every(b => b === 0xAA), `bytes past the result changed, input ${i}`)
  }
})

test('results own their whole buffer, and empty results are never shared', () => {
  const owns = (b: Uint8Array, what: string): void => assert.ok(b.byteOffset === 0 && b.byteLength === b.buffer.byteLength, what)
  const raw = k.deflate(sample), z = k.zlib(sample), gz = k.gzip(sample)
  for (const [what, b] of Object.entries({ raw, z, gz, inflate: k.inflate(raw), unzlib: k.unzlib(z), gunzip: k.gunzip(gz), zip: k.zip({ a: sample }) })) owns(b, what)
  for (const b of Object.values(k.unzip(k.zip({ a: sample, b: new Uint8Array(0) })))) owns(b, 'unzip entry')
  const ds = k.deflateStream()
  for (const b of [ds.push(sample), ds.flush(), ds.push(sample), ds.finish()]) owns(b, 'deflateStream')
  const is = k.inflateStream()
  for (let o = 0; o < raw.length; o += 1000) owns(is.push(raw.subarray(o, o + 1000)), 'inflateStream')
  owns(is.finish(), 'inflateStream finish')
  const us = k.unzipStream()
  for (const c of us.finish(k.zip({ a: sample }))) owns(c.data, 'unzipStream')

  // A caller moving an empty result elsewhere must leave later calls working
  for (const empty of [k.gunzipStream().push(gz.subarray(0, 5)), k.unzlibStream().push(z.subarray(0, 1)), k.encodeTextStream().push('')]) {
    assert.equal(empty.length, 0)
    structuredClone(empty, { transfer: [empty.buffer] })
  }
  const after = k.deflateStream()
  assert.deepEqual(k.inflate(cat([after.push(sample), after.finish()])), sample)
})

test('bad input fails with a KokyuError and the right code', () => {
  const gz = k.gzip(sample)
  const flipped = gz.slice(); flipped[gz.length - 2]! ^= 1
  assert.ok(throwsCode(() => k.gunzip(flipped), 'invalid-data'))
  assert.ok(throwsCode(() => k.gunzip(gz.subarray(0, gz.length - 3)), 'truncated'))
  assert.ok(throwsCode(() => k.unzlib(new Uint8Array([0x78, 0x9c, 1, 2])), 'truncated'))
  assert.ok(throwsCode(() => k.unzlib(new Uint8Array([1, 2, 3, 4])), 'invalid-header'))
  assert.ok(throwsCode(() => k.gunzip(sample), 'invalid-header'))
  assert.ok(throwsCode(() => k.deflate(sample, { level: 11 as 9 }), 'invalid-option'))
  assert.ok(throwsCode(() => k.gunzipStream().finish(), 'truncated'))
  // Random corruption must never escape as anything but a KokyuError (or decode to something)
  const packed = ff.deflateSync(sample)
  const r = randomBytes(400, 3)
  for (let i = 0; i < 400; i++) {
    const x = packed.slice()
    x[(r[i]! * 97) % x.length]! ^= 1 << (i % 8)
    try { k.inflate(x) } catch (e) { assert.ok(e instanceof k.KokyuError, String(e)) }
  }
})

test('unused bit patterns stay invalid, whatever the previous table left behind', () => {
  const tables = [new Uint16Array(1 << 15)], t = tables[0]!
  // A complete code first, so every entry it reaches holds a valid symbol
  buildTable(new Uint8Array(8).fill(3), 0, tables, 0)
  // One distance code of one bit: the other bit pattern must decode as no symbol any alphabet has
  assert.equal(buildTable(Uint8Array.of(0, 1), 1, tables, 0), 1)
  assert.equal(t[0]! >> 4, 1)
  assert.ok(t[1]! >> 4 >= 288, 'an unused pattern decoded as a code')
})

test('an inflater stops at the end of its data and reports what follows', () => {
  const packed = ff.deflateSync(sample)
  const inf = inflater()
  inf.push(cat([packed, new Uint8Array([9, 8, 7])]), true)
  assert.equal(inf.consumed, packed.length)
  assert.deepEqual(inf.rest(), new Uint8Array([9, 8, 7]))
})

const naiveAdler = (d: Uint8Array, start = 1): number => {
  let a = start & 0xFFFF, b = start >>> 16
  for (const x of d) { a = (a + x) % 65521; b = (b + a) % 65521 }
  return ((b << 16) | a) >>> 0
}

test('checksums match node:zlib and a reference adler32', () => {
  for (const [, d] of corpus) {
    assert.equal(k.crc32(d), zl.crc32(d))
    assert.equal(k.crc32(d.subarray(100), k.crc32(d.subarray(0, 100))), zl.crc32(d))
    assert.equal(k.adler32(d), naiveAdler(d))
    assert.equal(k.adler32(d.subarray(100), k.adler32(d.subarray(0, 100))), naiveAdler(d))
  }
  // Every alignment and the lengths around each unrolled step and reduction boundary
  const r = randomBytes(80_000)
  for (let off = 0; off < 8; off++) {
    for (const len of [0, 1, 3, 4, 15, 16, 17, 31, 33, 3799, 3800, 3801, 7601, 70_001]) {
      const d = r.subarray(off, off + len)
      assert.equal(k.crc32(d), zl.crc32(d), `crc32 offset ${off}, length ${len}`)
      assert.equal(k.adler32(d), naiveAdler(d), `adler32 offset ${off}, length ${len}`)
    }
  }
  // The largest sums a reduction can start from, then the largest bytes: the bound that keeps b in int32
  const ff = new Uint8Array(20_000).fill(0xFF)
  assert.equal(k.adler32(ff, 0xFFF0FFF0), naiveAdler(ff, 0xFFF0FFF0))
  // crc32 switches to Chorba at 512 bytes and ends it on a 41–80 byte tail: every length across both, then a
  // running CRC split anywhere
  for (let off = 0; off < 4; off++) {
    for (let len = 0; len <= 3000; len++) {
      const d = r.subarray(off, off + len)
      if (k.crc32(d) !== zl.crc32(d)) assert.fail(`crc32 offset ${off}, length ${len}`)
    }
  }
  // The first split is at 0, which checks the whole buffer
  const big = randomBytes(1 << 20)
  for (let t = 0; t < 200; t++) {
    const cut = (t * 5237) % big.length
    assert.equal(k.crc32(big.subarray(cut), k.crc32(big.subarray(0, cut))), zl.crc32(big), `crc32 split at ${cut}`)
  }
})
