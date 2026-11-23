// What the encoder and decoder must agree on (RFC 1951), plus the errors and byte helpers every format uses.

/**
 * Why a call failed: `truncated` (the data ends early), `invalid-data` (corrupt data), `invalid-header` (a bad zlib or
 * gzip header), `too-large` (output passes `out`, `maxSize` or, in `unzip`, an entry's stated size, or a zip being
 * written needs ZIP64), `unsupported` (a feature kokyu doesn't read, such as zip encryption), `invalid-option` (a bad
 * argument), `invalid-state` (a stream used after it finished, or out of order) or `worker` (a pool worker failed).
 */
export type KokyuErrorCode =
  | 'truncated' | 'invalid-data' | 'invalid-header' | 'too-large'
  | 'unsupported' | 'invalid-option' | 'invalid-state' | 'worker'

/**
 * Every error kokyu raises itself. Others pass through: an abort rejects with the signal's reason, and a filter's
 * throw is rethrown. Branch on `code`; the message is for people.
 */
export class KokyuError extends Error {
  /** What went wrong, stable across versions */
  readonly code: KokyuErrorCode
  constructor(code: KokyuErrorCode, message: string) {
    super(`kokyu: ${message}`)
    this.name = 'KokyuError'
    this.code = code
  }
}

export const fail = (code: KokyuErrorCode, message: string): never => { throw new KokyuError(code, message) }
export const FINISHED = 'the stream is already finished'
export const BUSY = 'a stream can\'t be called from inside its own call'

// A caller's value in a message: a number as it is, anything else only by its type, so no text of theirs reaches a log
export const shown = (v: unknown): string => typeof v === 'number' ? String(v) : typeof v

// Internal only: a caller who moved it to a worker would detach it for every later use
export const EMPTY: Uint8Array<ArrayBuffer> = new Uint8Array(0)

/** Compression level: 0 stores without compressing, 1 is fastest, 9 is smallest. Every compressor defaults to 6. */
export type Level = 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

/** A sync stream: push input and get back the output it completes, then finish with the last of it. */
export interface Stream<In = Uint8Array, Out = Uint8Array<ArrayBuffer>> {
  /** Adds a chunk and returns the output it completes, possibly empty. Chunks can be any size. */
  push(chunk: In): Out
  /** Adds an optional last chunk and returns the rest of the output. Using the stream after this fails. */
  finish(chunk?: In): Out
}

// A Uint8Array from any realm (an iframe's, a vm context's), Buffer included. Anything else would read as no bytes (an
// ArrayBuffer) or the wrong ones (a Uint16Array), so it's refused rather than misread.
export const isBytes = (v: unknown): v is Uint8Array =>
  ArrayBuffer.isView(v) && (v as { [Symbol.toStringTag]?: unknown })[Symbol.toStringTag] === 'Uint8Array'
// A view whose buffer was transferred away, or resized out from under it: it reads as empty, but any use of it throws
export const unreadable = (v: Uint8Array): boolean => { if (v.length) return false; try { v.at(0); return false } catch { return true } }
// v itself, checked: a caller with a narrower type for v can cast the result back to it
export const bytes = (v: unknown, what = 'input'): Uint8Array =>
  !isBytes(v) ? fail('invalid-option', `${what} must be a Uint8Array`)
    : unreadable(v) ? fail('invalid-option', `${what} can't be read: its buffer is detached or resized`) : v

export function checkLevel(level: number): void {
  if (!Number.isInteger(level) || level < 0 || level > 9) fail('invalid-option', `level must be 0 to 9, got ${shown(level)}`)
}

// The caller's output cap, Infinity for none
export const checkMaxSize = (max: unknown): number =>
  max == null || max === Infinity ? Infinity : Number.isSafeInteger(max) && (max as number) >= 0 ? max as number
    : fail('invalid-option', `maxSize must be a whole number of bytes, 0 or more, got ${shown(max)}`)

// Pinned to one length, which a growable SharedArrayBuffer could change between the reads that size and copy it
export const dictionaryOf = (v: Uint8Array | null | undefined): Uint8Array | undefined =>
  v == null ? undefined : bytes(v, 'dictionary').subarray(0, v.length)

// Using a finished stream is a mistake, so it fails instead of coding more. A call that threw leaves the stream
// mid-unit, so every later call rethrows that error, but a chunk refused by `check` leaves it as it was.
export function finishOnce<In, Out>(s: Stream<In, Out>, check?: (c: In) => In): Stream<In, Out>
export function finishOnce<In, Out>(s: Stream<In, Out> & { flush(): Out }, check?: (c: In) => In): Stream<In, Out> & { flush(): Out }
export function finishOnce<In, Out>(s: Stream<In, Out> & { flush?(): Out }, check = bytes as (c: In) => In): Stream<In, Out> & { flush?(): Out } {
  let ended = false, broken = false, busy = false, error: unknown
  // 0 push, 1 flush, 2 finish
  const call = (how: number, c?: In): Out => {
    if (broken) throw error
    if (ended) fail('invalid-state', FINISHED)
    // Reading a chunk can run the caller's code, a getter say, and a call it makes from there is refused
    if (busy) fail('invalid-state', BUSY)
    busy = true
    let started = false
    try {
      if (how === 0 || c !== undefined) c = check(c!)
      started = true
      ended = how === 2
      return how === 2 ? s.finish(c) : how ? s.flush!() : s.push(c!)
    } catch (e) {
      if (started) {
        broken = true
        error = e
      }
      throw e
    } finally {
      busy = false
      // Never called again, so its buffers, tables and last chunk go now, not once the caller drops the stream
      if (ended || broken) s = null!
    }
  }
  const guarded: Stream<In, Out> & { flush?(): Out } = { push: c => call(0, c), finish: c => call(2, c) }
  if (s.flush) guarded.flush = () => call(1)
  return guarded
}

// A stream from one function that codes a chunk, told whether it's the last
export const streamOf = <In, Out>(run: (chunk: In, last: boolean) => Out, empty: In, check?: (c: In) => In): Stream<In, Out> =>
  finishOnce({ push: c => run(c, false), finish: (c = empty) => run(c, true) }, check)

export function concat<T extends Uint8Array>(parts: T[]): T | Uint8Array<ArrayBuffer> {
  if (parts.length === 1) return parts[0]!
  let size = 0
  for (const p of parts) size += p.length
  const out = new Uint8Array(size)
  let o = 0
  for (const p of parts) { out.set(p, o); o += p.length }
  return out
}

export const readU16LE = (b: Uint8Array, o: number): number => b[o]! | (b[o + 1]! << 8)
export const readU32LE = (b: Uint8Array, o: number): number =>
  (b[o]! | (b[o + 1]! << 8) | (b[o + 2]! << 16) | (b[o + 3]! << 24)) >>> 0
export const readU64LE = (b: Uint8Array, o: number): number => readU32LE(b, o) + readU32LE(b, o + 4) * 0x100000000

// A Uint8Array keeps a stored value's low 8 bits, so the writers need no masks
export function writeU16LE(b: Uint8Array, o: number, v: number): void {
  b[o] = v; b[o + 1] = v >>> 8
}

export function writeU32LE(b: Uint8Array, o: number, v: number): void {
  b[o] = v; b[o + 1] = v >>> 8; b[o + 2] = v >>> 16; b[o + 3] = v >>> 24
}

// How far back a match may reach
export const WINDOW = 32768

// Extra bits and base values for the length codes 257-285 and distance codes 0-29 (RFC 1951 section 3.2.5):
// each base follows the last plus the values its extra bits span. Length code 285 is 258 with no extra bits.
export const LEN_EXTRA: Uint8Array = Uint8Array.from({ length: 29 }, (_, i) => i < 8 || i > 27 ? 0 : (i >> 2) - 1)
export const DIST_EXTRA: Uint8Array = Uint8Array.from({ length: 30 }, (_, i) => i < 4 ? 0 : (i >> 1) - 1)
const bases = (extra: Uint8Array, first: number): Uint16Array => {
  const b = new Uint16Array(extra.length)
  for (let i = 0; i < b.length; i++) b[i] = i ? b[i - 1]! + (1 << extra[i - 1]!) : first
  return b
}
export const LEN_BASE: Uint16Array = bases(LEN_EXTRA, 3)
LEN_BASE[28] = 258
export const DIST_BASE: Uint16Array = bases(DIST_EXTRA, 1)

// The code-length alphabet is transmitted in this order so that the trailing
// entries can be omitted when unused (RFC 1951 section 3.2.7).
export const CLEN_ORDER: Uint8Array = new Uint8Array([16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15])

// The fixed code's lengths (section 3.2.6): literal/length symbols 0-287, distances 0-29
export const FIXED_LIT_LENGTHS: Uint8Array = new Uint8Array(288).fill(8, 0, 144).fill(9, 144, 256).fill(7, 256, 280).fill(8, 280, 288)
export const FIXED_DIST_LENGTHS: Uint8Array = new Uint8Array(30).fill(5)

// Each byte with its bits in reverse order
const REV8 = new Uint8Array(256)
for (let i = 0; i < 256; i++) {
  let r = 0
  for (let b = 0; b < 8; b++) r |= ((i >> b) & 1) << (7 - b)
  REV8[i] = r
}

// How many codes of each length the lengths last passed to canonicalCodes hold; inflate checks a code's shape by them
export const CODE_COUNTS: Uint16Array = new Uint16Array(16)
const NEXT = new Uint16Array(16)

// The canonical code (section 3.2.2) of each length, bit-reversed: Huffman codes go out
// most-significant bit first in a stream that packs everything else least-significant first
export function canonicalCodes(lengths: Uint8Array, codes: Uint16Array = new Uint16Array(lengths.length)): Uint16Array {
  const n = lengths.length, counts = CODE_COUNTS, next = NEXT
  counts.fill(0)
  for (let i = 0; i < n; i++) counts[lengths[i]!]!++
  counts[0] = 0
  for (let len = 1, code = 0; len < 16; len++) {
    code = (code + counts[len - 1]!) << 1
    next[len] = code
  }
  for (let i = 0; i < n; i++) {
    const len = lengths[i]!
    if (!len) continue
    const c = next[len]!
    next[len] = c + 1
    codes[i] = ((REV8[c & 0xFF]! << 8) | REV8[c >> 8]!) >> (16 - len)
  }
  return codes
}

// A copy of b[from, to) in a buffer of its own. Never slice() for this: a Node Buffer is a Uint8Array whose
// slice() returns a view, so caller data would stay shared.
export const copy = (b: Uint8Array, from = 0, to: number = b.length): Uint8Array<ArrayBuffer> => new Uint8Array(b.subarray(from, to))

export const viewOf = (b: Uint8Array): DataView => new DataView(b.buffer, b.byteOffset, b.byteLength)

// a then b, without a copy when a is empty. b is pinned to one length, which a growable SharedArrayBuffer could
// change between concat's sum and its copy.
export const join = (a: Uint8Array, b: Uint8Array): Uint8Array => a.length ? concat([a, b.subarray(0, b.length)]) : b

// The most bytes JSC (Bun, Safari) lets one typed array hold. A size guess or a doubling never asks for more while the
// need fits: from a size that isn't a power of two, doubling can jump past it.
export const MAX_BYTES: number = 2 ** 32

// The first len bytes of buf, in a new array of size bytes
export const resize = (buf: Uint8Array, len: number, size: number): Uint8Array<ArrayBuffer> => {
  const next = new Uint8Array(size)
  next.set(buf.subarray(0, len))
  return next
}

// buf, or a copy of its first len bytes doubled in size until n more fit
export function grow<T extends Uint8Array>(buf: T, len: number, n: number): T | Uint8Array<ArrayBuffer> {
  if (len + n <= buf.length) return buf
  let size = buf.length || 1
  while (size < len + n) size *= 2
  return resize(buf, len, len + n <= MAX_BYTES ? Math.min(size, MAX_BYTES) : size)
}

// The first n bytes of an array kokyu made and won't use again, as an array of their own. transfer() frees the old
// buffer at once, and an engine that shrinks in place skips the copy. Older engines slice.
export const shrink = (b: Uint8Array<ArrayBuffer>, n: number): Uint8Array<ArrayBuffer> => {
  const transfer = (b.buffer as { transfer?(n: number): ArrayBuffer }).transfer
  return n === b.length ? b : transfer ? new Uint8Array(transfer.call(b.buffer, n)) : b.slice(0, n)
}

export const readU32BE = (b: Uint8Array, o: number): number =>
  ((b[o]! << 24) | (b[o + 1]! << 16) | (b[o + 2]! << 8) | b[o + 3]!) >>> 0

export function writeU32BE(b: Uint8Array, o: number, v: number): void {
  b[o] = v >>> 24; b[o + 1] = v >>> 16; b[o + 2] = v >>> 8; b[o + 3] = v
}
