// Web-standard checks of every API family, run unchanged in Node, Bun, Deno and browsers.
// Returns the failures; an empty list means the runtime passed.

import * as f from 'fflate'
import * as k from '../engine/index.js'
import { cat, equal, joinChunks, pieces, randomBytes, readable, rng, sameEntries } from './common.js'

// Compressible text, large enough that async calls go to a worker rather than run inline
function makeText(n: number): Uint8Array {
  const words = ['kokyu', 'deflate', 'stream', 'window', 'huffman', 'zip', 'breath', 'chunk', 'worker', 'pool']
  const r = rng(9)
  let s = ''
  while (s.length < n) s += words[(r() * words.length) | 0] + (r() < 0.1 ? '\n' : ' ')
  return new TextEncoder().encode(s.slice(0, n))
}

const text = makeText(200_000)
const small = text.subarray(0, 2000)
const noise = randomBytes(50_000, 4)
// Kept referenced, so collecting it can't close its worker session: one left open keeps Deno from exiting
const failed: unknown[] = []

export async function run(): Promise<string[]> {
  const failures: string[] = []
  const check = async (name: string, fn: () => boolean | Promise<boolean>): Promise<void> => {
    try {
      if (!(await fn())) failures.push(name)
    } catch (e) {
      failures.push(`${name}: ${e instanceof Error ? e.message : String(e)}`)
    }
  }

  for (const d of [small, text, noise]) {
    const n = `${d.length} B`
    await check(`deflate ↔ fflate ${n}`, () => equal(f.inflateSync(k.deflate(d)), d) && equal(k.inflate(f.deflateSync(d)), d))
    await check(`zlib ↔ fflate ${n}`, () => equal(f.unzlibSync(k.zlib(d)), d) && equal(k.unzlib(f.zlibSync(d)), d))
    await check(`gzip ↔ fflate ${n}`, () => equal(f.gunzipSync(k.gzip(d)), d) && equal(k.gunzip(f.gzipSync(d)), d))
    await check(`decompress ${n}`, () => equal(k.decompress(f.gzipSync(d)), d) && equal(k.decompress(f.zlibSync(d)), d))
    await check(`streams ${n}`, () =>
      equal(f.gunzipSync(cat(pieces(k.gzipStream(), d, 9999))), d) && equal(cat(pieces(k.gunzipStream(), f.gzipSync(d), 777)), d))
  }

  const files = { 'a.txt': text, 'dir/ünï.bin': noise, 'empty.txt': new Uint8Array(0) }
  const same = (got: Record<string, Uint8Array>) => sameEntries(got, files)
  await check('zip ↔ fflate', () => same(f.unzipSync(k.zip(files))) && same(k.unzip(f.zipSync(files))))
  await check('zip streams', () => {
    const z = k.zipStream(), out: Uint8Array[] = []
    for (const [name, data] of Object.entries(files)) out.push(...pieces(z.file(name), data, 9999))
    out.push(z.finish())
    const archive = cat(out)
    return same(f.unzipSync(archive)) && same(joinChunks(pieces(k.unzipStream(), archive, 5000).flat()))
  })
  await check('text', () => {
    const s = 'héllo 🎉 ' + new TextDecoder().decode(small)
    return k.decodeText(k.encodeText(s)) === s && equal(k.encodeText(s), f.strToU8(s))
  })

  // All async one-shots, and the raw, zlib and decompress streams, against fflate: inline when small, else in a worker
  for (const d of [small, text]) {
    const n = `${d.length} B`
    await check(`deflateAsync ${n}`, async () => equal(await k.deflateAsync(d), k.deflate(d)))
    await check(`gzip/gunzipAsync ${n}`, async () => equal(await k.gunzipAsync(await k.gzipAsync(d)), d))
    await check(`zip/unzipAsync ${n}`, async () => (await k.unzipAsync(await k.zipAsync({ 'x.txt': d })))['x.txt']?.length === d.length)
    await check(`async one-shots ↔ fflate ${n}`, async () =>
      equal(f.inflateSync(await k.deflateAsync(d)), d) && equal(await k.inflateAsync(f.deflateSync(d)), d) &&
      equal(f.unzlibSync(await k.zlibAsync(d)), d) && equal(await k.unzlibAsync(f.zlibSync(d)), d) &&
      equal(f.gunzipSync(await k.gzipAsync(d)), d) && equal(await k.gunzipAsync(f.gzipSync(d)), d) &&
      equal(await k.decompressAsync(f.zlibSync(d)), d) && equal(await k.decompressAsync(f.deflateSync(d)), d) &&
      equal(f.unzipSync(await k.zipAsync({ a: d }))['a']!, d) && equal((await k.unzipAsync(f.zipSync({ a: d })))['a']!, d))
    await check(`async streams ↔ fflate ${n}`, async () => {
      const all = <O>(s: { push(c: Uint8Array): Promise<O>; finish(): Promise<O> }, x: Uint8Array): Promise<O[]> => Promise.all(pieces(s, x, 4000))
      return equal(f.inflateSync(cat(await all(k.deflateStreamAsync(), d))), d) && equal(cat(await all(k.inflateStreamAsync(), f.deflateSync(d))), d) &&
        equal(f.unzlibSync(cat(await all(k.zlibStreamAsync(), d))), d) && equal(cat(await all(k.unzlibStreamAsync(), f.zlibSync(d))), d) &&
        equal(cat(await all(k.decompressStreamAsync(), f.gzipSync(d))), d)
    })
  }
  await check('parallel async jobs', async () =>
    (await Promise.all(Array.from({ length: 8 }, () => k.gzipAsync(text)))).every(g => equal(f.gunzipSync(g), text)))
  await check('async streams', async () => {
    const gz = cat(await Promise.all(pieces(k.gzipStreamAsync(), text, 65536)))
    return equal(f.gunzipSync(gz), text) && equal(cat(await Promise.all(pieces(k.gunzipStreamAsync(), gz, 4096))), text)
  })
  await check('async zip streams', async () => {
    const z = k.zipStreamAsync()
    const parts = [z.file('a.txt').finish(text), z.file('b.bin', { level: 0 }).finish(noise), z.finish()]
    const archive = cat(await Promise.all(parts))
    const u = k.unzipStreamAsync({ filter: e => e.name === 'a.txt' })
    const got = [...await u.push(archive), ...await u.finish()]
    return equal(f.unzipSync(archive)['b.bin']!, noise) && equal(cat(got.map(c => c.data)), text)
  })
  await check('async zip calls settle in call order', async () => {
    // Small enough to run inline, where every call settles in one tick; written as they settle, the archive must read
    const z = k.zipStreamAsync(), order: string[] = [], written: Uint8Array[] = []
    const a = z.file('a.txt'), calls: [string, Promise<Uint8Array>][] = [['a.push', a.push(small)], ['a.finish', a.finish()]]
    calls.push(['b.finish', z.file('b.txt', { level: 0 }).finish(small)], ['finish', z.finish()])
    await Promise.all(calls.map(([name, p]) => p.then(bytes => { order.push(name); written.push(bytes) })))
    return order.join() === calls.map(c => c[0]).join() && Object.keys(k.unzip(cat(written))).join() === 'a.txt,b.txt'
  })
  await check('worker errors keep their code', async () => {
    try { await k.gunzipAsync(cat([new Uint8Array(20), text])); return false } catch (e) { return (e as { code?: string }).code === 'invalid-header' }
  })
  await check('abort', async () => {
    const c = new AbortController()
    const p = k.deflateAsync(text, { level: 9, signal: c.signal })
    c.abort()
    try { await p; return false } catch (e) { return (e as Error).name === 'AbortError' }
  })
  await check('pool works after abort', async () => equal(f.gunzipSync(await k.gzipAsync(text)), text))
  await check('a failed stream closes its worker session', async () => {
    const s = k.inflateStreamAsync()
    failed.push(s)
    try { await s.push(noise); return false } catch (e) { return (e as { code?: string }).code === 'invalid-data' }
  })
  await check('transfer moves, default copies', async () => {
    const kept = new Uint8Array(text), moved = new Uint8Array(text)
    await k.gzipAsync(kept)
    await k.gzipAsync(moved, { transfer: true })
    return kept.length === text.length && moved.length === 0
  })
  await check('a canceled pipe closes its worker session', async () => {
    const reader = readable(text, 20_000).pipeThrough(k.toTransformStream(k.gzipStreamAsync())).getReader()
    await reader.read()
    await reader.cancel()
    return true
  })
  await check('toTransformStream', async () => {
    const out: Uint8Array[] = []
    for await (const x of readable(text, 10_000).pipeThrough(k.toTransformStream(k.gzipStreamAsync())).pipeThrough(k.toTransformStream(k.gunzipStream()))) out.push(x)
    return equal(cat(out), text)
  })
  return failures
}
