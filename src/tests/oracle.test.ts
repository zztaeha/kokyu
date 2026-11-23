// kokyu against fflate as the oracle, across many kinds of data. Compressed bytes are allowed to
// differ (kokyu finds different matches); everything that comes back out must be byte-identical.

import { test, type TestContext } from 'node:test'
import assert from 'node:assert/strict'
import * as zl from 'node:zlib'
import * as f from 'fflate'
import * as k from '../engine/index.js'
import {
  attempt, binary, cat, chunked, equal, ffStream, ffZip, fromFflate, fullText, joinChunks, pushChunked, randomBytes, rng, roundTrip, same,
  sameFiles, spans,
} from './helpers.js'

// node:zlib, the reference implementation, settles whether a stream is valid
const referee = {
  raw: (b: Uint8Array): Uint8Array => zl.inflateRawSync(b),
  zlib: (b: Uint8Array): Uint8Array => zl.inflateSync(b),
  gzip: (b: Uint8Array): Uint8Array => zl.gunzipSync(b),
}

// What fflate wrote that zlib couldn't read back, reported per test
const fflateBugs: string[] = []

function reportFflateBugs(t: TestContext): void {
  const shown = fflateBugs.slice(0, 6).join('; ') + (fflateBugs.length > 6 ? `; and ${fflateBugs.length - 6} more` : '')
  if (fflateBugs.length) t.diagnostic(`fflate wrote ${fflateBugs.length} streams zlib can't read back, kokyu returned the input or an error for each: ${shown}`)
  fflateBugs.length = 0
}

// Data
const r = rng(2024)
const pick = <T>(xs: readonly T[]): T => xs[(r() * xs.length) | 0]!
const utf8 = (s: string): Uint8Array => new TextEncoder().encode(s)
const repeat = (unit: Uint8Array, n: number): Uint8Array => Uint8Array.from({ length: n }, (_, i) => unit[i % unit.length]!)

const words = ['kokyu', '呼吸', 'дыхание', 'respiración', 'نفس', '숨', '🌬️', 'Atem', 'ἀναπνοή', 'श्वास', 'breath', '👨‍👩‍👧']
let multilingual = ''
while (multilingual.length < 60_000) multilingual += pick(words) + (r() < 0.1 ? '\n' : ' ')

const names = ['Ada', 'Linus', 'Grace', 'Ken', 'Barbara', 'Dennis', 'Margaret', 'Alan']
const records: string[] = []
for (let i = 0; i < 1500; i++) {
  records.push(JSON.stringify({ id: i, name: `${pick(names)} ${pick(names)}`, active: r() < 0.5, score: Math.round(r() * 1e4) / 100, tags: [pick(words), pick(words)] }))
}
const json = utf8(`[${records.join(',\n')}]`)
const csv = utf8(Array.from({ length: 4000 }, (_, i) => `${i},${(r() * 1e6).toFixed(3)},${Math.round(r() * 100)},${pick(names)}`).join('\n'))

// A 256x256 RGBA gradient with a little noise, like an image's raw pixels
const gradient = new Uint8Array(256 * 256 * 4)
for (let y = 0, o = 0; y < 256; y++) {
  for (let x = 0; x < 256; x++, o += 4) {
    gradient[o] = x; gradient[o + 1] = y; gradient[o + 2] = (x + y) >> 1; gradient[o + 3] = r() < 0.05 ? (r() * 256) | 0 : 255
  }
}
const sparse = new Uint8Array(200_000).map(() => r() < 0.01 ? (r() * 256) | 0 : 0)
const skewed = new Uint8Array(150_000).map(() => Math.clz32((r() * 0x100000000) >>> 0))
const window = randomBytes(32_768, 5), pastWindow = randomBytes(32_769, 6)

const data: [string, Uint8Array][] = [
  ['empty', new Uint8Array(0)],
  ...[1, 2, 3, 4, 5].map(n => [`${n} bytes`, randomBytes(n, n)] as [string, Uint8Array]),
  ['one byte repeated', new Uint8Array(100_000).fill(0x61)],
  ['zeros 65535', new Uint8Array(65_535)],
  ['zeros 65537', new Uint8Array(65_537)],
  ['random 100 KB', randomBytes(100_000, 7)],
  ['random 65536', randomBytes(65_536, 8)],
  ...[2, 3, 7, 258, 259].map(p => [`period ${p}`, repeat(randomBytes(p, p), 40_000)] as [string, Uint8Array]),
  ['match at the window edge', cat([window, window.subarray(0, 1000)])],
  ['repeat just past the window', cat([pastWindow, pastWindow.subarray(0, 1000)])],
  ['source text 200 KB', fullText.subarray(0, 200_000)],
  ['JSON', json],
  ['CSV', csv],
  ['multilingual UTF-8', utf8(multilingual)],
  ['image pixels', gradient],
  ['sparse', sparse],
  ['every byte value', repeat(Uint8Array.from({ length: 256 }, (_, i) => i), 102_400)],
  ['skewed bytes', skewed],
  ['executable', binary],
  ['mixed regimes', cat([fullText.subarray(0, 40_000), randomBytes(40_000, 9), new Uint8Array(40_000), json.subarray(0, 40_000), fullText.subarray(300_000, 340_000)])],
]
const large = cat([fullText.subarray(0, 1_500_000), binary, randomBytes(300_000, 10), new Uint8Array(500_000), json])

const LEVELS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] as const

test('every kind of data round-trips through fflate at every level, both directions', t => {
  const cases: [string, Uint8Array, readonly k.Level[]][] = [...data.map(([n, d]) => [n, d, LEVELS] as [string, Uint8Array, readonly k.Level[]]), ['large mixed 3 MB', large, [0, 1, 6, 9]]]
  for (const [name, d, levels] of cases) {
    for (const level of levels) {
      const at = `${name}, level ${level}`
      const ours = { raw: k.deflate(d, { level }), zlib: k.zlib(d, { level }), gzip: k.gzip(d, { level }) }
      roundTrip(`kokyu deflate → fflate, ${at}`, () => f.inflateSync(ours.raw), d)
      roundTrip(`kokyu zlib → fflate, ${at}`, () => f.unzlibSync(ours.zlib), d)
      roundTrip(`kokyu gzip → fflate, ${at}`, () => f.gunzipSync(ours.gzip), d)
      for (const kind of ['raw', 'zlib', 'gzip'] as const) roundTrip(`kokyu ${kind} → zlib, ${at}`, () => referee[kind](ours[kind]), d)
      const raw = f.deflateSync(d, { level }), z = f.zlibSync(d, { level }), gz = f.gzipSync(d, { level })
      fromFflate(`fflate deflate → kokyu, ${at}`, raw, referee.raw, b => k.inflate(b), d, fflateBugs)
      fromFflate(`fflate zlib → kokyu, ${at}`, z, referee.zlib, b => k.unzlib(b), d, fflateBugs)
      fromFflate(`fflate gzip → kokyu, ${at}`, gz, referee.gzip, b => k.gunzip(b), d, fflateBugs)
      fromFflate(`fflate zlib → kokyu decompress, ${at}`, z, referee.zlib, b => k.decompress(b), d, fflateBugs)
      fromFflate(`fflate gzip → kokyu decompress, ${at}`, gz, referee.gzip, b => k.decompress(b), d, fflateBugs)
    }
  }
  reportFflateBugs(t)
})

test('streams agree with fflate at random chunk sizes, both directions', t => {
  for (const [i, [name, d]] of [...data, ['large mixed 3 MB', large] as [string, Uint8Array]].entries()) {
    for (const max of d.length > 1e6 ? [65_536] : [7, 1000, 65_536]) {
      const level = ([1, 6, 9] as const)[i % 3]!
      const at = `${name}, chunks ≤${max}`
      roundTrip(`kokyu deflateStream → fflate, ${at}`, () => f.inflateSync(chunked(k.deflateStream({ level }), d, max)), d)
      roundTrip(`kokyu zlibStream → fflate, ${at}`, () => f.unzlibSync(chunked(k.zlibStream({ level }), d, max)), d)
      roundTrip(`kokyu gzipStream → fflate, ${at}`, () => f.gunzipSync(chunked(k.gzipStream({ level }), d, max)), d)
      const raw = ffStream(new f.Deflate({ level }), d, max)
      const z = ffStream(new f.Zlib({ level }), d, max)
      const gz = ffStream(new f.Gzip({ level }), d, max)
      fromFflate(`fflate Deflate → kokyu inflateStream, ${at}`, raw, referee.raw, b => chunked(k.inflateStream(), b, max), d, fflateBugs)
      fromFflate(`fflate Zlib → kokyu unzlibStream, ${at}`, z, referee.zlib, b => chunked(k.unzlibStream(), b, max), d, fflateBugs)
      fromFflate(`fflate Gzip → kokyu gunzipStream, ${at}`, gz, referee.gzip, b => chunked(k.gunzipStream(), b, max), d, fflateBugs)
      fromFflate(`fflate Gzip → kokyu decompressStream, ${at}`, gz, referee.gzip, b => chunked(k.decompressStream(), b, max), d, fflateBugs)
    }
  }
  reportFflateBugs(t)
})

test('zip archives move between kokyu and fflate with every kind of data', () => {
  const files = new Map(data.map(([name, d], i) => [`${i % 3 ? 'données/日本語/' : ''}${i} ${name}.bin`, d]))
  const zippable = (): f.Zippable => Object.fromEntries([...files].map(([n, d], i) => [n, [d, { level: LEVELS[i % 10]! }]]))
  const want = Object.fromEntries(files)
  const check = (got: Record<string, Uint8Array>, what: string): void => sameFiles(got, want, what)
  const streamed = (archive: Uint8Array, max: number): Record<string, Uint8Array> => joinChunks(pushChunked(k.unzipStream(), archive, max, 4).flat())

  check(f.unzipSync(k.zip(zippable() as k.ZipInput)), 'kokyu zip → fflate unzipSync')
  const theirs = f.zipSync(zippable())
  check(k.unzip(theirs), 'fflate zipSync → kokyu unzip')
  check(streamed(theirs, 4096), 'fflate zipSync → kokyu unzipStream')

  // fflate's streaming writer: data descriptors, deflated and stored entries
  const theirStream = ffZip(files, (_, i) => i % 2 === 1)
  check(k.unzip(theirStream), 'fflate Zip stream → kokyu unzip')
  check(streamed(theirStream, 777), 'fflate Zip stream → kokyu unzipStream')

  // kokyu's streaming writer read by fflate, one-shot and streaming
  const zs = k.zipStream(), parts: Uint8Array[] = []
  for (const [i, [n, d]] of [...files].entries()) parts.push(chunked(zs.file(n, { level: i % 2 ? 0 : 6 }), d, 50_000))
  parts.push(zs.finish())
  const ours = cat(parts)
  check(f.unzipSync(ours), 'kokyu zipStream → fflate unzipSync')
  const got: Record<string, Uint8Array[]> = {}
  const u = new f.Unzip(file => {
    file.ondata = (err, c) => { if (err) throw err; (got[file.name] ??= []).push(c) }
    file.start()
  })
  u.register(f.UnzipInflate)
  u.push(ours, true)
  check(Object.fromEntries(Object.entries(got).map(([n, p]) => [n, cat(p)])), 'kokyu zipStream → fflate Unzip stream')
})

test('preset dictionaries work both ways', () => {
  const dictionary = fullText.subarray(100_000, 132_768)
  for (const d of [fullText.subarray(0, 50_000), json, new Uint8Array(0)]) {
    for (const level of [1, 6, 9] as const) {
      same(f.inflateSync(k.deflate(d, { level, dictionary }), { dictionary }), d, `kokyu deflate → fflate, level ${level}`)
      same(k.inflate(f.deflateSync(d, { level, dictionary }), { dictionary }), d, `fflate deflate → kokyu, level ${level}`)
      same(f.unzlibSync(k.zlib(d, { level, dictionary }), { dictionary }), d, `kokyu zlib → fflate, level ${level}`)
      same(k.unzlib(f.zlibSync(d, { level, dictionary }), { dictionary }), d, `fflate zlib → kokyu, level ${level}`)
    }
  }
})

test('text encodes and decodes byte-identically to fflate', () => {
  const strings = ['', 'hello', multilingual, '👨‍👩‍👧 family 🏳️‍🌈', '﻿with a BOM', 'nul\u0000inside', 'lone \uD800 high', 'lone \uDC00 low']
  for (const s of strings) same(k.encodeText(s), f.strToU8(s), `encodeText ${JSON.stringify(s.slice(0, 20))}`)
  for (const [name, d] of data) {
    assert.equal(k.decodeText(d), f.strFromU8(d), `decodeText ${name}`)
    assert.equal(k.decodeText(d, { latin1: true }), f.strFromU8(d, true), `latin1 decodeText ${name}`)
  }
  const latin = String.fromCharCode(...Array.from({ length: 256 }, (_, i) => i))
  same(k.encodeText(latin, { latin1: true }), f.strToU8(latin, true), 'latin1 encodeText')

  // Streams split inside multi-byte characters and surrogate pairs must match the one-shot result
  const whole = utf8(multilingual)
  for (const max of [1, 3, 1000]) {
    const dec = k.decodeTextStream(), next = rng(max)
    let out = ''
    for (const [a, b] of spans(whole.length, max, next)) out += dec.push(whole.subarray(a, b))
    assert.equal(out + dec.finish(), f.strFromU8(whole), `decodeTextStream, chunks ≤${max}`)
    const enc = k.encodeTextStream(), parts: Uint8Array[] = []
    for (const [a, b] of spans(multilingual.length, max, next)) parts.push(enc.push(multilingual.slice(a, b)))
    parts.push(enc.finish())
    same(cat(parts), f.strToU8(multilingual), `encodeTextStream, chunks ≤${max}`)
  }
})

// Flips 1 to 3 random bits
function corrupt(d: Uint8Array, next: () => number): Uint8Array {
  const out = d.slice()
  for (let j = 0, flips = 1 + Math.floor(next() * 3); j < flips; j++) out[Math.floor(next() * out.length)]! ^= 1 << Math.floor(next() * 8)
  return out
}
test('corrupted deflate data: kokyu accepts and rejects exactly what zlib does', t => {
  const next = rng(11)
  const sources = [fullText.subarray(0, 3000), randomBytes(2000, 3), new Uint8Array(4000), json.subarray(0, 20_000), binary.subarray(0, 8000)]
  let fflateLenient = 0
  for (let i = 0; i < 3000; i++) {
    const src = sources[i % sources.length]!
    const level = (1 + (i % 9)) as k.Level
    const bad = corrupt(i & 1 ? f.deflateSync(src, { level }) : k.deflate(src, { level }), next)
    const z = attempt(() => zl.inflateRawSync(bad))
    const ours = attempt(() => k.inflate(bad))
    const theirs = attempt(() => f.inflateSync(bad))
    assert.equal(ours.ok, z.ok, `case ${i}: zlib ${z.ok ? 'accepts' : 'rejects'} it, kokyu ${ours.ok ? 'accepts' : 'rejects'} it`)
    if (ours.ok && z.ok) same(ours.value, z.value, `case ${i}: kokyu and zlib output`)
    if (ours.ok && theirs.ok) same(ours.value, theirs.value, `case ${i}: kokyu and fflate output`)
    if (theirs.ok && !z.ok) fflateLenient++
  }
  t.diagnostic(`fflate decoded ${fflateLenient} of 3000 corrupted streams that zlib and kokyu reject`)
})

// Neither library checks checksums, so a corrupted file may decode to wrong bytes; kokyu must still only ever
// fail with its own errors. The counts keep the trade visible.
test('corrupted gzip, zlib and zip fail only with KokyuErrors', t => {
  const next = rng(12)
  const src = [fullText.subarray(0, 5000), json.subarray(0, 8000), binary.subarray(0, 6000)]
  const formats: [string, (d: Uint8Array) => Uint8Array, (d: Uint8Array) => Uint8Array[], (d: Uint8Array) => Uint8Array[]][] = [
    ['gzip', d => k.gzip(d), d => [k.gunzip(d)], d => [f.gunzipSync(d)]],
    ['zlib', d => k.zlib(d), d => [k.unzlib(d)], d => [f.unzlibSync(d)]],
    ['zip', d => k.zip({ 'a.txt': d, 'b.txt': d.subarray(0, 1000) }), d => Object.values(k.unzip(d)), d => Object.values(f.unzipSync(d))],
  ]
  for (const [name, pack, ours, theirs] of formats) {
    let kokyuWrong = 0, fflateWrong = 0
    for (let i = 0; i < 1000; i++) {
      const d = src[i % src.length]!
      const want = name === 'zip' ? [d, d.subarray(0, 1000)] : [d]
      const wrong = (got: Uint8Array[]): boolean => got.some((b, j) => !want[j] || !equal(b, want[j]!))
      const bad = corrupt(pack(d), next)
      try {
        if (wrong(ours(bad))) kokyuWrong++
      } catch (e) {
        assert.ok(e instanceof k.KokyuError, `${name} case ${i}: ${String(e)}`)
      }
      const other = attempt(() => theirs(bad))
      if (other.ok && wrong(other.value)) fflateWrong++
    }
    t.diagnostic(`${name}: of 1000 corrupted files, wrong bytes came back from kokyu for ${kokyuWrong}, from fflate for ${fflateWrong}`)
  }
})
