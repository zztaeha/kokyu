// Seeded differential fuzzing against fflate and node:zlib: random data, sizes, levels, chunkings and formats.
// FUZZ=<cases> runs more than the quick default; a failing case reproduces from its number alone.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import * as zl from 'node:zlib'
import * as f from 'fflate'
import * as k from '../engine/index.js'
import { attempt, binary, cat, chunked, chunkedAsync, fromFflate, fullText, rng, same, sameFiles, unzipInPieces } from './helpers.js'

const CASES = Number(process.env['FUZZ'] ?? 300)
// Sizes where blocks, windows, match lengths and the async inline limit change behavior
const EDGES = [0, 1, 2, 3, 4, 257, 258, 259, 16_384, 32_768, 65_535, 65_536, 262_144]
const source = cat([fullText.subarray(0, 600_000), binary])

// Real text and machine code, noise, runs and short repeats, cut into segments of random length
function data(r: () => number): Uint8Array {
  const roll = r()
  const size = roll < 0.4 ? Math.max(0, EDGES[(r() * EDGES.length) | 0]! + ((r() * 3) | 0) - 1) : (r() * (roll < 0.9 ? 20_000 : 300_000)) | 0
  const out = new Uint8Array(size)
  for (let o = 0; o < size;) {
    const n = Math.min(size - o, 1 + ((r() * (r() < 0.5 ? 64 : 40_000)) | 0))
    const kind = (r() * 4) | 0
    if (kind === 0) { const at = (r() * (source.length - n)) | 0; out.set(source.subarray(at, at + n), o) }
    else if (kind === 1) for (let i = 0; i < n; i++) out[o + i] = (r() * 256) | 0
    else if (kind === 2) out.fill((r() * 256) | 0, o, o + n)
    else { const p = 1 + ((r() * 300) | 0), at = (r() * (source.length - p)) | 0; for (let i = 0; i < n; i++) out[o + i] = source[at + (i % p)]! }
    o += n
  }
  return out
}

const level = (r: () => number): k.Level => ((r() * 10) | 0) as k.Level

test(`differential fuzz: ${CASES} cases against fflate and node:zlib`, async () => {
  for (let c = 0; c < CASES; c++) {
    const r = rng(1000 + c), d = data(r), lv = level(r), max = 1 + ((r() * 70_000) | 0)
    const at = `case ${c} (${d.length} B, level ${lv}, chunks ≤${max})`

    // kokyu's output, read by fflate and zlib; every kokyu path gives the same bytes
    const raw = k.deflate(d, { level: lv })
    same(f.inflateSync(raw), d, `deflate → fflate, ${at}`)
    same(zl.inflateRawSync(raw), d, `deflate → zlib, ${at}`)
    same(chunked(k.deflateStream({ level: lv }), d, max, c), raw, `deflateStream equals deflate, ${at}`)
    const format = (r() * 3) | 0
    if (format === 0) {
      const z = k.zlib(d, { level: lv })
      same(f.unzlibSync(z), d, `zlib → fflate, ${at}`)
      same(zl.inflateSync(z), d, `zlib → zlib, ${at}`)
      same(chunked(k.zlibStream({ level: lv }), d, max, c), z, `zlibStream equals zlib, ${at}`)
    } else if (format === 1) {
      const gz = k.gzip(d, { level: lv })
      same(f.gunzipSync(gz), d, `gzip → fflate, ${at}`)
      same(zl.gunzipSync(gz), d, `gzip → zlib, ${at}`)
      same(chunked(k.gzipStream({ level: lv }), d, max, c), gz, `gzipStream equals gzip, ${at}`)
    } else {
      const half = (d.length * r()) | 0, files = { 'a.bin': d.subarray(0, half), 'b/c.bin': d.subarray(half) }
      const ours = k.zip(files, { level: lv }), theirs = f.zipSync(files, { level: level(r) })
      sameFiles(f.unzipSync(ours), files, `zip → fflate, ${at}`)
      sameFiles(k.unzip(theirs), files, `fflate zip → unzip, ${at}`)
      sameFiles(unzipInPieces(theirs, max), files, `fflate zip → unzipStream, ${at}`)
    }

    // A flushed stream still decodes, wherever the flushes fall
    const s = k.deflateStream({ level: lv }), flushed: Uint8Array[] = []
    for (let o = 0; o < d.length; o += max) flushed.push(s.push(d.subarray(o, o + max)), r() < 0.5 ? s.flush() : new Uint8Array(0))
    flushed.push(s.finish())
    same(f.inflateSync(cat(flushed)), d, `flushed deflateStream → fflate, ${at}`)

    // fflate's output, read by every kokyu path
    const theirs = f.deflateSync(d, { level: level(r) })
    same(zl.inflateRawSync(theirs), d, `fflate deflate → zlib, ${at}`)
    same(k.inflate(theirs), d, `fflate → inflate, ${at}`)
    same(chunked(k.inflateStream(), theirs, max, c), d, `fflate → inflateStream, ${at}`)
    const tgz = f.gzipSync(d, { level: level(r) }), tz = f.zlibSync(d, { level: level(r) }), pick = r() < 0.5
    fromFflate(`fflate gzip → gunzip, ${at}`, tgz, zl.gunzipSync, k.gunzip, d)
    fromFflate(`fflate zlib → unzlib, ${at}`, tz, zl.inflateSync, k.unzlib, d)
    fromFflate(`fflate → decompressStream, ${at}`, pick ? tgz : tz, pick ? zl.gunzipSync : zl.inflateSync, b => chunked(k.decompressStream(), b, max, c), d)

    // Text decoding agrees with fflate on any bytes, valid UTF-8 or not
    if (d.length < 50_000) assert.equal(k.decodeText(d), f.strFromU8(d), `decodeText, ${at}`)

    // Every tenth case also goes through the async API, inline or in a worker by size
    if (c % 10 === 0) {
      same(await k.deflateAsync(d, { level: lv }), raw, `deflateAsync equals deflate, ${at}`)
      same(await k.inflateAsync(theirs), d, `fflate → inflateAsync, ${at}`)
      if (attempt(() => zl.gunzipSync(tgz)).ok) same(cat(await chunkedAsync(k.gunzipStreamAsync(), tgz, max, c)), d, `fflate gzip → gunzipStreamAsync, ${at}`)
    }
  }
})
