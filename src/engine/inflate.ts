// DEFLATE decompression (RFC 1951), resumable: input can arrive in any chunking, and a unit cut off by the end of
// a chunk is decoded again from its start on the next push.

import {
  LEN_BASE, LEN_EXTRA, DIST_BASE, DIST_EXTRA, CLEN_ORDER, FIXED_LIT_LENGTHS, FIXED_DIST_LENGTHS, CODE_COUNTS, canonicalCodes, grow,
  EMPTY, MAX_BYTES, WINDOW, bytes, copy, dictionaryOf, fail, resize, shrink, viewOf,
} from './shared.js'

// Bit patterns no code uses decode as this symbol, which both block alphabets reject, after reading the table's full
// width. A pattern made of bits past the end of the input then reads as the input running out, not as bad data.
const UNUSED = 4095
const TOO_LARGE = 'output exceeds its size limit'
const BAD_CODE = 'invalid Huffman code'
const BAD_REPEAT = 'invalid code length repeat'
const bad = (message: string): never => fail('invalid-data', message)

const REVERSED = new Uint16Array(288)
// Symbols sorted by code length, and where each length starts among them
const BY_LENGTH = new Uint16Array(288), OFFSETS = new Uint16Array(16)

// A Huffman code as a flat table in tables[k], indexed by the next `width` bits, entries symbol << 4 | length; returns
// the width. As in zlib: never over-subscribed, incomplete only where allowed (2: always, 1: one 1-bit code or none).
export function buildTable(lengths: Uint8Array, incomplete: number, tables: Uint16Array[], k: number): number {
  const codes = canonicalCodes(lengths, REVERSED), counts = CODE_COUNTS
  let max = 15, left = 1
  while (max && !counts[max]) max--
  for (let l = 1; l < 16; l++) if ((left = (left << 1) - counts[l]!) < 0) bad(BAD_CODE)
  if (left && incomplete < (max > 1 ? 2 : 1)) bad(BAD_CODE)
  const size = 1 << max
  // Grown to the width a block needs, never to the full 15 bits up front: zeroing 128 KB costs a small stream ~2 µs
  let t = tables[k]!
  if (t.length < size) tables[k] = t = new Uint16Array(size)
  for (let l = 1, o = 0; l <= max; l++) { OFFSETS[l] = o; o += counts[l]! }
  for (let s = 0; s < lengths.length; s++) { const l = lengths[s]!; if (l) BY_LENGTH[OFFSETS[l]!++] = s }
  // Width by width, as libdeflate does it: doubling copies every shorter code into the new top half, then each code
  // of the new width takes its one slot. Filling every slot of each code let 28-byte blocks cost 40–50x zlib's time.
  t[0] = UNUSED << 4 | max
  for (let l = 1, i = 0; l <= max; l++) {
    t.copyWithin(1 << (l - 1), 0, 1 << (l - 1))
    for (const e = i + counts[l]!; i < e; i++) { const s = BY_LENGTH[i]!; t[codes[s]!] = s << 4 | l }
  }
  return max
}

const EMPTY16 = new Uint16Array(0)
let FIXED: Uint16Array[] | undefined
// Dynamic tables for one-shot decodes, which finish within the call that starts them
const SCRATCH = [EMPTY16, EMPTY16, EMPTY16]

const HEADER = 0, STORED = 1, CODES = 2, DONE = 3

// Thrown when a chunk ends inside a block header; the header is read again once more input arrives
const UNDERFLOW = new Error('underflow')

// Two input bytes from p, as zero bits past the end
const u16 = (s: Uint8Array, p: number, n: number): number => p + 1 < n ? s[p]! | s[p + 1]! << 8 : p < n ? s[p]! : 0

interface InflaterOptions<B extends ArrayBufferLike = ArrayBuffer> {
  dictionary?: Uint8Array | undefined
  // A destination for the output, and a hard cap: a stream that would write more than it holds fails
  out?: Uint8Array<B> | undefined
  // Size of the first buffer
  sizeHint?: number | undefined
  // The size a header or trailer claims. Growth stops on it exactly instead of doubling past, but it's
  // never trusted for more than doubling would allocate: the claim may be wrong.
  expected?: number
  // Fail once the output, not counting a dictionary, passes this many bytes in all. Checked as the buffer grows,
  // which caps memory, and exactly at the end of every push.
  limit?: number | undefined
  // Build block tables in shared scratch; only for a decode that finishes in the same call (inflateRawEnd)
  shared?: boolean
  // Decode only to find where the data ends: every symbol is checked but nothing is written, so skipping costs
  // what the input does, not what it decodes to. `skipped` counts the bytes passed over; every push returns EMPTY.
  discard?: boolean
}

// The fields are as of the last push: plain fields, since an object literal with getters costs ~340 ns to create
// B is the buffer type of `out`: without one, every output is in an ArrayBuffer of the inflater's own
export interface Inflater<B extends ArrayBufferLike = ArrayBuffer> {
  // Decompressed bytes for `chunk`. The caller owns every array returned; the inflater never writes to one again.
  push(chunk: Uint8Array, final?: boolean): Uint8Array<B | ArrayBuffer>
  // Once finished, the input bytes after the end of the deflate data: a trailer, or the next member
  rest(): Uint8Array
  // Offset in the last pushed chunk just past the deflate data (exact when it was the only push)
  readonly consumed: number
  // In discard mode, the bytes decoded so far
  readonly skipped: number
  readonly finished: boolean
}

// Options count only when set on the object itself: one put on Object.prototype (pollution) must not send output into
// its buffer, switch on discard or shared tables, or size a huge first buffer
const own = <O extends object, K extends keyof O>(o: O, k: K): O[K] | undefined => Object.hasOwn(o, k) ? o[k] : undefined

export function inflater<B extends ArrayBufferLike = ArrayBuffer>(opts: InflaterOptions<B> = {}): Inflater<B> {
  const dict = dictionaryOf(own(opts, 'dictionary'))
  // Checked by the public call it came through
  const given = own(opts, 'out')
  const tail = dict?.subarray(-WINDOW) ?? EMPTY
  const expected = own(opts, 'expected') ?? 0, discard = own(opts, 'discard') === true
  // A dictionary goes ahead of the output, so it decodes into a buffer of its own; inflateRawEnd copies it out
  const fixed = !!given && !tail.length
  // The dictionary sits at the front of the output buffer, ahead of what the limit counts
  const limit = (own(opts, 'limit') ?? Infinity) + tail.length
  let out: Uint8Array<B | ArrayBuffer> = fixed ? given! : new Uint8Array(discard ? 0 : Math.min(tail.length + Math.max(own(opts, 'sizeHint') ?? 0, 1 << 10), limit, MAX_BYTES))
  out.set(tail)
  // `dropped` counts what a stream moved out of the buffer, so the limit holds across pushes. Writing stops at `top`,
  // out's end or the limit's if that comes first, and calls room(), so passing the limit fails at once.
  let len = tail.length, start = len, dropped = 0, top = Math.min(out.length, limit), src: Uint8Array = EMPTY, pos = 0, acc = 0, nbits = 0
  let state = HEADER, finalBlock = false, stored = 0, lbits = 0, dbits = 0
  // Set by each block header before any symbol is read
  let lit: Uint16Array = SCRATCH[0]!, dist: Uint16Array = SCRATCH[1]!, tables = own(opts, 'shared') ? SCRATCH : null

  // Exact reads for block headers, which are decoded again whole when a chunk cuts one off
  const bits = (n: number): number => {
    while (nbits < n) {
      if (pos >= src.length) throw UNDERFLOW
      acc |= src[pos++]! << nbits
      nbits += 8
    }
    const v = acc & ((1 << n) - 1)
    acc >>>= n
    nbits -= n
    return v
  }

  const room = (n: number): Uint8Array<B | ArrayBuffer> => {
    const need = len + n
    if (fixed || dropped + need > limit) fail('too-large', TOO_LARGE)
    out = expected >= need && expected <= Math.max(need, out.length * 2) ? resize(out, len, expected) : grow(out, len, n)
    top = Math.min(out.length, limit - dropped)
    return out
  }

  const header = (): void => {
    finalBlock = bits(1) === 1
    const type = bits(2)
    if (type === 0) {
      bits(nbits & 7)
      const n = bits(16)
      if ((n ^ 0xFFFF) !== bits(16)) bad('stored block length check failed')
      // bits() reads no byte it doesn't need, so nothing is buffered here and the stored bytes start at pos
      stored = n
      state = STORED
      return
    }
    if (type === 1) {
      if (!FIXED) {
        FIXED = [EMPTY16, EMPTY16]
        buildTable(FIXED_LIT_LENGTHS, 2, FIXED, 0)
        buildTable(FIXED_DIST_LENGTHS, 2, FIXED, 1)
      }
      [lit, dist] = FIXED as [Uint16Array, Uint16Array]
      lbits = 9
      dbits = 5
    } else if (type === 2) {
      const hlit = bits(5) + 257, hdist = bits(5) + 1, hclen = bits(4) + 4, n = hlit + hdist
      if (hlit > 286 || hdist > 30) bad('too many length or distance codes')
      const t = tables ??= [EMPTY16, EMPTY16, EMPTY16]
      const clens = new Uint8Array(19), lengths = new Uint8Array(n)
      for (let i = 0; i < hclen; i++) clens[CLEN_ORDER[i]!] = bits(3)
      const cbits = buildTable(clens, 0, t, 2), ct = t[2]!
      for (let i = 0; i < n;) {
        while (nbits < cbits && pos < src.length) { acc |= src[pos++]! << nbits; nbits += 8 }
        const e = ct[acc & ((1 << cbits) - 1)]!, sym = e >> 4
        if ((e & 15) > nbits) throw UNDERFLOW
        acc >>>= e & 15
        nbits -= e & 15
        if (sym < 16) { lengths[i++] = sym; continue }
        if (sym === 16 && !i) bad(BAD_REPEAT)
        const repeat = sym === 16 ? 3 + bits(2) : sym === 17 ? 3 + bits(3) : 11 + bits(7)
        if (i + repeat > n) bad(BAD_REPEAT)
        lengths.fill(sym === 16 ? lengths[i - 1]! : 0, i, i + repeat)
        i += repeat
      }
      if (!lengths[256]) bad('no end-of-block code')
      lbits = buildTable(lengths.subarray(0, hlit), 1, t, 0)
      dbits = buildTable(lengths.subarray(hlit), 1, t, 1)
      lit = t[0]!
      dist = t[1]!
    } else {
      bad('reserved block type')
    }
    state = CODES
  }

  // Symbols until the block ends (true) or the input runs out (false). Reads past the end see zero bits; each literal
  // or match is checked against the real end before anything is written, so a cut-off one is decoded again next push.
  const codes = (): boolean => {
    const s = src, n = s.length, end = n * 8, lt = lit, dt = dist, lm = (1 << lbits) - 1, dm = (1 << dbits) - 1
    // Word copies write up to 3 bytes past a match, which a caller's own buffer must never see
    const words = !fixed
    let p = pos, a = acc, b = nbits, o = out, l = len, t = top, p0 = p, a0 = a, b0 = b, done = false
    let view = viewOf(o)
    for (;;) {
      p0 = p; a0 = a; b0 = b
      if (b < 15) { a |= u16(s, p, n) << b; p += 2; b += 16 }
      const e = lt[a & lm]!, sym = e >> 4
      a >>>= e & 15
      b -= e & 15
      // A distance of 0 marks a bad length or distance symbol, reported once the input is known to hold it
      let length = 0, distance = 0
      if (sym > 256 && sym < 286) {
        const li = sym - 257, lx = LEN_EXTRA[li]!
        if (b < lx) { a |= u16(s, p, n) << b; p += 2; b += 16 }
        length = LEN_BASE[li]! + (a & ((1 << lx) - 1))
        a >>>= lx
        b -= lx
        if (b < 15) { a |= u16(s, p, n) << b; p += 2; b += 16 }
        const d = dt[a & dm]!, ds = d >> 4
        a >>>= d & 15
        b -= d & 15
        if (ds < 30) {
          const dx = DIST_EXTRA[ds]!
          if (b < dx) { a |= u16(s, p, n) << b; p += 2; b += 16 }
          distance = DIST_BASE[ds]! + (a & ((1 << dx) - 1))
          a >>>= dx
          b -= dx
        }
      }
      if (p > n && p * 8 - b > end) break
      if (sym < 256) {
        if (!discard) {
          if (l === t) {
            len = l
            o = room(1)
            t = top
            view = viewOf(o)
          }
          o[l] = sym
        }
        l++
        continue
      }
      if (sym === 256) { done = true; break }
      // A bad length symbol leaves the distance unread, so one check covers both
      if (!distance) bad('invalid length or distance symbol')
      if (distance > l) bad('distance reaches before the output')
      if (discard) { l += length; continue }
      if (l + length > t) {
        len = l
        o = room(length)
        t = top
        view = viewOf(o)
      }
      let from = l - distance
      // Four bytes at a time whenever the source stays that far behind, overlapping or not. The last word may run
      // up to 3 bytes past the match, into room the output hasn't reached yet.
      if (words && distance >= 4 && l + length + 3 <= o.length) {
        for (let k = 0; k < length; k += 4) view.setUint32(l + k, view.getUint32(from + k, true), true)
        l += length
      } else {
        const to = l + length
        while (l < to) o[l++] = o[from++]!
      }
    }
    if (!done) { p = p0; a = a0; b = b0 }
    // Whole bytes still in the accumulator go back to the input, which also drops any read past its end
    pos = p - (b >> 3)
    nbits = b & 7
    acc = a & ((1 << nbits) - 1)
    len = l
    return done
  }

  const self = {
    consumed: 0,
    skipped: 0,
    finished: false,
    rest: () => src.subarray(pos),
    push(chunk: Uint8Array, final = false): Uint8Array<B | ArrayBuffer> {
      bytes(chunk)
      if (state === DONE) return new Uint8Array(0)
      if (pos < src.length) {
        // One read of its length, with the copy pinned to it: a view of a growable SharedArrayBuffer can grow meanwhile
        const left = src.length - pos, n = chunk.length, merged = new Uint8Array(left + n)
        merged.set(src.subarray(pos))
        merged.set(chunk.subarray(0, n), left)
        src = merged
      } else {
        src = chunk
      }
      pos = 0
      for (;;) {
        if (state === HEADER) {
          const p = pos, a = acc, b = nbits
          try {
            header()
            continue
          } catch (e) {
            if (e !== UNDERFLOW) throw e
            pos = p; acc = a; nbits = b
            break
          }
        }
        if (state === STORED) {
          const n = Math.min(stored, src.length - pos)
          if (!discard) {
            if (len + n > top) room(n)
            out.set(src.subarray(pos, pos + n), len)
          }
          len += n
          pos += n
          stored -= n
          if (stored) break
        } else if (!codes()) {
          break
        }
        // Padding bits after the last block end the data; bytes past them belong to whatever follows
        if (finalBlock) {
          state = DONE
          break
        }
        state = HEADER
      }
      if (final && state !== DONE) fail('truncated', 'deflate data ends early')
      if (dropped + len > limit) fail('too-large', TOO_LARGE)
      // A cut-off unit's bytes are copied, and a chunk read to its end isn't kept: callers often reuse the chunk they
      // just pushed, and a view of a resizable buffer would show its next contents at its next length
      if (state !== DONE) {
        src = pos < src.length ? copy(src, pos) : EMPTY
        pos = 0
      }
      self.consumed = pos
      self.skipped = len
      self.finished = state === DONE
      // Everything decoded since the last call, in an array of its own (or a view of the caller's `out`). A stream
      // keeps decoding into the same buffer, with the last 32 KB moved to the front as history.
      if (discard) return EMPTY
      const result = fixed ? out.subarray(start, len) : state === DONE && !start ? shrink(out as Uint8Array<ArrayBuffer>, len) : out.slice(start, len)
      if (state !== DONE && !fixed && len > WINDOW) {
        out.copyWithin(0, len - WINDOW, len)
        dropped += len - WINDOW
        len = WINDOW
        top = Math.min(out.length, limit - dropped)
      }
      start = len
      return result
    },
  }
  return self
}

// The output size of data that is stored blocks up to its final one, read from their headers alone, so incompressible
// data decodes into a buffer of exactly its size and needs no trimming copy. Other data gets undefined.
function storedSize(d: Uint8Array): number | undefined {
  for (let p = 0, size = 0, n = 0; p + 4 < d.length && !(d[p]! & 6); p += 5 + n) {
    size += n = d[p + 1]! | d[p + 2]! << 8
    if (d[p]! & 1) return size
  }
  return undefined
}

// Set while a one-shot decode uses SCRATCH: one started inside it (by a getter on its input) builds tables of its own
let busy = false

// One-shot decode. `end` is the offset just past the deflate data, where a trailer would start.
export function inflateRawEnd<B extends ArrayBufferLike = ArrayBuffer>(src: Uint8Array, opts: InflaterOptions<B>): { bytes: Uint8Array<B | ArrayBuffer>; end: number } {
  bytes(src)
  // A dictionary keeps the decode out of `out`, so it's copied there afterwards, out's size capping it too
  const out = own(opts, 'out'), into = out && own(opts, 'dictionary')?.length ? out : undefined
  const limit = into ? Math.min(into.length, own(opts, 'limit') ?? Infinity) : own(opts, 'limit')
  const shared = !busy
  const inf = inflater({ ...opts, sizeHint: own(opts, 'sizeHint') ?? storedSize(src) ?? src.length * 4, limit, shared })
  busy = true
  let result: Uint8Array<B | ArrayBuffer>
  try {
    result = inf.push(src, true)
  } finally {
    if (shared) busy = false
  }
  if (into) {
    into.set(result)
    result = into.subarray(0, result.length)
  }
  return { bytes: result, end: inf.consumed }
}

