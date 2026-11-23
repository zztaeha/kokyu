import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { readdirSync, readFileSync } from 'node:fs'
import * as f from 'fflate'
import type { FlateStreamHandler } from 'fflate'
import * as k from '../engine/index.js'
import { cat, equal, joinChunks, pieces, rng } from './common.js'

export { cat, equal, joinChunks, pieces, randomBytes, readable, rng, sameEntries } from './common.js'

const tsRoot = new URL('../../node_modules/@typescript/', import.meta.url)
const tsPkg = readdirSync(tsRoot).find(n => n.startsWith('typescript-'))
if (!tsPkg) throw new Error('TypeScript platform package not found; run npm install')
const text = new Uint8Array(readFileSync(new URL(`${tsPkg}/lib/lib.dom.d.ts`, tsRoot)))

export const sample: Uint8Array = text.subarray(0, 50_000)
export const fullText: Uint8Array = text
// The middle of the native tsc executable: real machine code and data tables
const exe = readFileSync(new URL(`${tsPkg}/lib/tsc${process.platform === 'win32' ? '.exe' : ''}`, tsRoot))
export const machineCode = (n: number): Uint8Array => new Uint8Array(exe.subarray(exe.length >> 1, (exe.length >> 1) + n))
export const binary: Uint8Array = machineCode(300_000)

export const u16 = (v: number): number[] => [v & 255, (v >>> 8) & 255]
export const u32 = (v: number): number[] => [v & 255, (v >>> 8) & 255, (v >>> 16) & 255, (v >>> 24) & 255]

export const bytes = (b: Uint8Array): Uint8Array => new Uint8Array(b.buffer, b.byteOffset, b.byteLength)

// Random-sized spans of 1 to `max` covering `length`; the last may run past the end
export function* spans(length: number, max: number, next: () => number): Generator<[number, number]> {
  for (let o = 0; o < length;) {
    const end = o + 1 + Math.floor(next() * max)
    yield [o, end]
    o = end
  }
}

// A stream fed `data` in random-sized pieces of up to `max` bytes (1 means one byte at a time), then finished
export function pushChunked<O>(s: { push(c: Uint8Array): O; finish(): O }, data: Uint8Array, max: number, seed: number): O[] {
  const out: O[] = []
  for (const [a, b] of spans(data.length, max, rng(seed))) out.push(s.push(data.subarray(a, b)))
  out.push(s.finish())
  return out
}

export const chunked = (s: k.Stream, data: Uint8Array, max: number, seed = 7): Uint8Array => cat(pushChunked(s, data, max, seed))

// An archive read by unzipStream in pieces of `size` bytes
export const unzipInPieces = (archive: Uint8Array, size: number): Record<string, Uint8Array> =>
  joinChunks(pieces(k.unzipStream(), archive, size).flat())

// A call's error code: 'ok' when it doesn't throw, the error's type when it has no code
const codeOfError = (e: unknown): string => (e as { code?: string }).code ?? (e as Error).constructor.name
export function codeOf(fn: () => unknown): string {
  try { fn(); return 'ok' } catch (e) { return codeOfError(e) }
}
export async function codeOfAsync(fn: () => unknown): Promise<string> {
  try { await fn(); return 'ok' } catch (e) { return codeOfError(e) }
}
export const throwsCode = (fn: () => unknown, code: string): boolean => codeOf(fn) === code

// What a call allocates as Uint8Arrays: the largest one made from a length, and the total, counting copies made with
// `new Uint8Array(view)` but not `slice()`'s, which bypass the constructor
function allocations(fn: () => void): { largest: number; total: number } {
  const scope = globalThis as { Uint8Array: Uint8ArrayConstructor }
  const real = scope.Uint8Array
  let largest = 0, total = 0
  scope.Uint8Array = new Proxy(real, {
    construct(target, args) {
      const a = args[0] as unknown
      if (typeof a === 'number') largest = Math.max(largest, a)
      total += typeof a === 'number' ? a : ArrayBuffer.isView(a) ? a.byteLength : 0
      return Reflect.construct(target, args) as object
    },
  })
  try {
    fn()
  } finally {
    scope.Uint8Array = real
  }
  return { largest, total }
}
export const largestAllocation = (fn: () => void): number => allocations(fn).largest
export const totalAllocation = (fn: () => void): number => allocations(fn).total
// A call that should fail: its error code, and the largest array it allocated before failing
export function failureCost(fn: () => unknown): { code: string; largest: number } {
  let code = ''
  const largest = largestAllocation(() => { code = codeOf(fn) })
  return { code, largest }
}

export function same(got: Uint8Array, want: Uint8Array, what: string): void {
  if (equal(got, want)) return
  let i = 0
  while (i < got.length && i < want.length && got[i] === want[i]) i++
  assert.fail(`${what}: got ${got.length} bytes, expected ${want.length}, first difference at byte ${i}`)
}

export function sameFiles(got: Record<string, Uint8Array>, want: Record<string, Uint8Array>, what = 'files'): void {
  assert.deepEqual(Object.keys(got).sort(), Object.keys(want).sort(), `${what}: entry names`)
  for (const [n, d] of Object.entries(want)) same(got[n]!, d, `${what}: ${n}`)
}

export const attempt = <T>(fn: () => T): { ok: true; value: T } | { ok: false } => {
  try { return { ok: true, value: fn() } } catch { return { ok: false } }
}

// Runs one direction of a round trip; a throw is reported with the same context as a mismatch
export function roundTrip(what: string, run: () => Uint8Array, want: Uint8Array): void {
  let got: Uint8Array
  try {
    got = run()
  } catch (e) {
    return assert.fail(`${what}: threw ${(e as Error).message}`)
  }
  same(got, want, what)
}

// fflate's output, refereed by zlib: when zlib reads it back exactly, kokyu must too. Otherwise fflate wrote a broken
// stream (listed in `bugs`), which kokyu may still decode, but only to the input. fflate does this at level 0 on any
// multiple of 65,535 bytes (a stray block before the trailer: kokyu reads zlib's, not checking checksums, and refuses
// gzip's on its size), and in its streams on sparse or skewed bytes (distances reaching before the start, refused too).
export function fromFflate(what: string, packed: Uint8Array, check: (b: Uint8Array) => Uint8Array, decode: (b: Uint8Array) => Uint8Array,
  want: Uint8Array, bugs?: string[]): void {
  const z = attempt(() => check(packed))
  if (z.ok && equal(z.value, want)) return roundTrip(what, () => decode(packed), want)
  bugs?.push(what)
  const ours = attempt(() => decode(packed))
  if (z.ok) {
    assert.ok(ours.ok, `${what}: fflate's output is broken; zlib decodes it, kokyu rejects it`)
    same(ours.value, z.value, `${what}: kokyu and zlib disagree on fflate's broken output`)
  } else if (ours.ok) {
    same(ours.value, want, `${what}: zlib rejects fflate's broken output, kokyu decodes it to other bytes`)
  }
}

// fflate's sync streams, fed in random chunks of up to `max` bytes
export function ffStream(s: { ondata: FlateStreamHandler; push(c: Uint8Array, final?: boolean): void }, d: Uint8Array, max: number): Uint8Array {
  const out: Uint8Array[] = []
  s.ondata = c => { out.push(c.slice()) }
  for (const [a, b] of spans(d.length, max, rng(3))) s.push(d.subarray(a, b), b >= d.length)
  if (!d.length) s.push(new Uint8Array(0), true)
  return cat(out)
}

// fflate's streaming zip writer, with the entries `stored` picks written uncompressed
export function ffZip(files: Iterable<[string, Uint8Array]>, stored: (name: string, i: number) => boolean): Uint8Array {
  const out: Uint8Array[] = []
  const z = new f.Zip((err, c) => { if (err) throw err; out.push(c) })
  let i = 0
  for (const [n, d] of files) {
    const entry = stored(n, i++) ? new f.ZipPassThrough(n) : new f.ZipDeflate(n, { level: 6 })
    z.add(entry)
    entry.push(d, true)
  }
  z.end()
  return cat(out)
}

// What an fflate worker stream or Zip hands its callback, every chunk up to the final one
export const ffChunks = (start: (cb: f.AsyncFlateStreamHandler) => void): Promise<Uint8Array[]> =>
  new Promise((resolve, reject) => {
    const out: Uint8Array[] = []
    start((err, c, final) => { if (err) return reject(err); out.push(c); if (final) resolve(out) })
  })

// kokyu's async streams, every push sent before any result is awaited
export const chunkedAsync = <O>(s: { push(c: Uint8Array): Promise<O>; finish(): Promise<O> }, d: Uint8Array, max: number, seed = 7): Promise<O[]> =>
  Promise.all(pushChunked(s, d, max, seed))

// Where a zip's end record and each of its central records start
export function centralRecords(z: Uint8Array): { end: number; at: number[] } {
  const v = new DataView(z.buffer, z.byteOffset, z.byteLength), at: number[] = []
  let e = z.length - 22
  while (v.getUint32(e, true) !== 0x06054b50) e--
  for (let i = 0, p = v.getUint32(e + 16, true); i < v.getUint16(e + 10, true); i++) {
    at.push(p)
    p += 46 + v.getUint16(p + 28, true) + v.getUint16(p + 30, true) + v.getUint16(p + 32, true)
  }
  return { end: e, at }
}

// Whether a command-line tool is installed, for the tests that check kokyu against it
export const has = (cmd: string, flag: string): boolean => {
  try { execFileSync(cmd, [flag], { stdio: 'ignore' }); return true } catch { return false }
}
// Python's output for a script, run isolated from the environment
export const python = (script: string, input?: Uint8Array): Buffer =>
  execFileSync('python3', ['-I', '-c', script], { input, maxBuffer: 1 << 26 })

// Deflate data written field by field, least-significant bit first, so it can break any rule. A field marked as a
// Huffman code goes out most-significant bit first.
export type Field = [value: number, bits: number, huffman?: boolean]
export function bitsOf(fields: Field[]): Uint8Array {
  const out: number[] = []
  let acc = 0, n = 0
  for (const [v, len, huffman] of fields) {
    for (let i = 0; i < len; i++) {
      acc |= ((v >> (huffman ? len - 1 - i : i)) & 1) << n
      if (++n === 8) { out.push(acc); acc = n = 0 }
    }
  }
  if (n) out.push(acc)
  return Uint8Array.from(out)
}

// Timing for bench.ts and versus.ts. Sync calls run in batches of at least 1 ms so timer overhead doesn't swamp
// small inputs; async ones are timed from call to result. Each run is the median of 150 ms of samples.
export const now = (): number => performance.now()
const median = (xs: number[]): number => xs.sort((a, b) => a - b)[xs.length >> 1]!
export const time = (ms: number): string => ms < 1 ? `${(ms * 1000).toFixed(1)} µs` : `${ms.toFixed(ms < 100 ? 2 : 0)} ms`

export async function measure(run: () => unknown): Promise<number> {
  const samples: number[] = []
  const first = run()
  if (first instanceof Promise) {
    await first
    for (const end = now() + 150; samples.length < 5 || now() < end;) {
      const t = now()
      await run()
      samples.push(now() - t)
    }
    return median(samples)
  }
  let n = 0
  const warm = now()
  while (now() - warm < 30) { run(); n++ }
  const batch = Math.max(1, Math.ceil(n / (now() - warm)))
  for (const end = now() + 150; samples.length < 5 || now() < end;) {
    const t = now()
    for (let j = 0; j < batch; j++) run()
    samples.push((now() - t) / batch)
  }
  return median(samples)
}

// kokyu's and fflate's medians over seven alternating rounds, so drift on the machine can't favor either
export async function race(ours: () => unknown, theirs: () => unknown): Promise<[number, number]> {
  const a: number[] = [], b: number[] = []
  for (let round = 0; round < 7; round++) { b.push(await measure(theirs)); a.push(await measure(ours)) }
  return [median(a), median(b)]
}
