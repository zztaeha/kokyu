// The deflate family: raw (RFC 1951), zlib (RFC 1950) and gzip (RFC 1952), one-shot and
// streaming, plus decompress(), which detects the format from the first bytes.

import { deflater, type Deflater } from './deflate.js'
import { inflater, inflateRawEnd, type Inflater } from './inflate.js'
import { crc32, adler32 } from './checksum.js'
import { encodeText } from './text.js'
import {
  EMPTY, bytes, checkLevel, checkMaxSize, concat, copy, dictionaryOf, fail, finishOnce, join, readU16LE, readU32BE, readU32LE, streamOf, writeU32BE,
  writeU32LE, type Level, type Stream,
} from './shared.js'

/** Options for compressing to raw deflate or zlib */
export interface DeflateOptions {
  /** 0 stores, 1 is fastest, 9 is smallest. Default 6. */
  level?: Level | undefined
  /** Bytes the input likely repeats, such as a common header, for smaller output. The decoder needs the same bytes. */
  dictionary?: Uint8Array | undefined
}

/** Options for decompressing raw deflate or zlib data */
export interface InflateOptions {
  /** The preset dictionary the data was compressed with */
  dictionary?: Uint8Array | undefined
  /** Fail with `too-large` once the output would pass this many bytes. Set it for untrusted input. No cap by default. */
  maxSize?: number | undefined
}

/** Options for decompressing gzip data */
export interface GunzipOptions {
  /** Fail with `too-large` once the output would pass this many bytes. Set it for untrusted input. No cap by default. */
  maxSize?: number | undefined
}

/** Options for compressing to gzip */
export interface GzipOptions {
  /** 0 stores, 1 is fastest, 9 is smallest. Default 6. */
  level?: Level | undefined
  /** The original file name, stored in the header. Latin1 characters only. */
  filename?: string | undefined
  /** Modification time for the header. Default none (0), so the same input always gives the same bytes. */
  mtime?: Date | string | number | undefined
}

/** A compression stream, which can also flush */
export interface CompressStream extends Stream {
  /** Returns everything pushed so far as output a decoder can read in full, without ending the stream */
  flush(): Uint8Array<ArrayBuffer>
}

const u32be = (v: number): Uint8Array<ArrayBuffer> => { const b = new Uint8Array(4); writeU32BE(b, 0, v); return b }

function gzipTrailer(crc: number, size: number): Uint8Array<ArrayBuffer> {
  const t = new Uint8Array(8)
  writeU32LE(t, 0, crc)
  writeU32LE(t, 4, size)
  return t
}

// The trailer's ISIZE holds the size mod 2^32
const checkSize = (d: Uint8Array, at: number, size: number): void => {
  if (readU32LE(d, at) !== size >>> 0) fail('invalid-data', 'gzip size mismatch')
}

// Checked and copied when a stream is made, as every stream takes its options: the caller may reuse the buffer
const ownDictionary = (v: Uint8Array | null | undefined): Uint8Array | undefined => { const d = dictionaryOf(v); return d && copy(d) }

/**
 * Compresses to raw deflate (RFC 1951), the format inside zlib, gzip and zip.
 * @example const packed = deflate(encodeText('hello'), { level: 9 })
 */
export const deflate = (data: Uint8Array, opts: DeflateOptions = {}): Uint8Array<ArrayBuffer> =>
  deflater(opts?.level ?? 6, opts?.dictionary).push(data, true)

// Each format decodes in one function that takes an optional out: without it the output has an ArrayBuffer of its own
function decodeRaw<B extends ArrayBufferLike = ArrayBuffer>(data: Uint8Array, opts: InflateOptions, out?: Uint8Array<B>): Uint8Array<B | ArrayBuffer> {
  const max = checkMaxSize(opts?.maxSize)
  return inflateRawEnd(data, { dictionary: opts?.dictionary, out, limit: max }).bytes
}

// Two SharedArrayBuffer objects can be one memory (one posted back from a worker, or wasm memory before and after a
// grow), at the same offsets
const sameMemory = (a: ArrayBufferLike, b: ArrayBufferLike): boolean =>
  a === b || a[Symbol.toStringTag] === 'SharedArrayBuffer' && b[Symbol.toStringTag] === 'SharedArrayBuffer'

// out is checked before anything reads it, so a missing one fails rather than reading as no out and allocating. Input
// sharing out's memory is read from a copy: output written over bytes not yet read would decode as garbage.
function into<B extends ArrayBufferLike>(data: Uint8Array, out: Uint8Array<B>, decode: (data: Uint8Array, out: Uint8Array<B>) => Uint8Array): Uint8Array<B> {
  bytes(out, 'out')
  bytes(data)
  const shared = sameMemory(data.buffer, out.buffer) && data.byteOffset < out.byteOffset + out.length && out.byteOffset < data.byteOffset + data.length
  return out.subarray(0, decode(shared ? copy(data) : data, out).length)
}

/**
 * Decompresses raw deflate data. Throws a KokyuError on bad or truncated data.
 * @example const text = decodeText(inflate(packed, { maxSize: 10_000_000 }))
 * @see inflateInto, to decode into a buffer you already have
 */
export function inflate(data: Uint8Array, opts: InflateOptions = {}): Uint8Array<ArrayBuffer> { return decodeRaw(data, opts) }

/**
 * Decompresses raw deflate data into `out` and returns the view of `out` that holds it. Output that doesn't fit
 * fails with `too-large`.
 * @example const view = inflateInto(packed, buffer)
 */
export function inflateInto<B extends ArrayBufferLike>(data: Uint8Array, out: Uint8Array<B>, opts: InflateOptions = {}): Uint8Array<B> {
  return into(data, out, (d, o) => decodeRaw(d, opts, o))
}

// Zlib

function zlibHeader(level: number, dictionary: Uint8Array | undefined): Uint8Array<ArrayBuffer> {
  const dict = !!dictionary?.length
  const h = new Uint8Array(dict ? 6 : 2)
  const cmf = 0x78
  let flg = ((level >= 7 ? 3 : level >= 6 ? 2 : level >= 2 ? 1 : 0) << 6) | (dict ? 0x20 : 0)
  flg |= 31 - ((cmf << 8) | flg) % 31
  h[0] = cmf
  h[1] = flg
  if (dict) writeU32BE(h, 2, adler32(dictionary!))
  return h
}

const isZlib = (d: Uint8Array): boolean =>
  d.length >= 2 && (d[0]! & 0x0F) === 8 && d[0]! >> 4 <= 7 && ((d[0]! << 8) | d[1]!) % 31 === 0
// Past the end, a read is undefined and so matches nothing
const isGzip = (d: Uint8Array, p = 0): boolean => d[p] === 0x1f && d[p + 1] === 0x8b

// Header length, or -1 when more bytes are needed
function zlibHeaderLength(d: Uint8Array, dictionary: Uint8Array | undefined): number {
  if (d.length < 2) return -1
  if (!isZlib(d)) fail('invalid-header', 'not a zlib stream')
  if (!(d[1]! & 0x20)) return 2
  if (!dictionary) return fail('invalid-option', 'this zlib stream needs a preset dictionary')
  if (d.length < 6) return -1
  if (readU32BE(d, 2) !== adler32(dictionary)) fail('invalid-option', 'the preset dictionary does not match this stream')
  return 6
}

/**
 * Compresses to zlib (RFC 1950): deflate with a short header and an Adler-32 checksum.
 * @example const packed = zlib(encodeText('hello'))
 */
export function zlib(data: Uint8Array, opts: DeflateOptions = {}): Uint8Array<ArrayBuffer> {
  return zlibStream(opts).finish(bytes(data))
}

function decodeZlib<B extends ArrayBufferLike = ArrayBuffer>(data: Uint8Array, opts: InflateOptions, out?: Uint8Array<B>): Uint8Array<B | ArrayBuffer> {
  bytes(data)
  opts ??= {}
  // Checked even when the data needs none, as every other call and stream checks it
  const dictionary = dictionaryOf(opts.dictionary)
  const max = checkMaxSize(opts.maxSize)
  const h = zlibHeaderLength(data, dictionary)
  if (h < 0) fail('truncated', 'zlib header ends early')
  const { bytes: result, end } = inflateRawEnd(data.subarray(h), { out, limit: max, dictionary: h === 6 ? dictionary : undefined })
  const t = h + end
  if (t + 4 > data.length) fail('truncated', 'zlib trailer is missing')
  return result
}

/**
 * Decompresses zlib data. Throws a KokyuError on bad or truncated data, or a missing or wrong dictionary.
 * @example const text = decodeText(unzlib(packed))
 * @see unzlibInto, to decode into a buffer you already have
 */
export function unzlib(data: Uint8Array, opts: InflateOptions = {}): Uint8Array<ArrayBuffer> { return decodeZlib(data, opts) }

/**
 * Decompresses zlib data into `out` and returns the view of `out` that holds it. Output that doesn't fit fails
 * with `too-large`.
 * @example const view = unzlibInto(packed, buffer)
 */
export function unzlibInto<B extends ArrayBufferLike>(data: Uint8Array, out: Uint8Array<B>, opts: InflateOptions = {}): Uint8Array<B> {
  return into(data, out, (d, o) => decodeZlib(d, opts, o))
}

// Gzip

function gzipHeader(level: number, opts: GzipOptions): Uint8Array<ArrayBuffer> {
  // Each option read once: a getter giving two answers could otherwise flag a name it then leaves out
  const { filename, mtime } = opts
  const name = filename ? encodeText(filename, { latin1: true }) : null
  if (name?.includes(0)) fail('invalid-option', 'gzip filenames cannot contain NUL')
  const h = new Uint8Array(10 + (name ? name.length + 1 : 0))
  h[0] = 0x1f
  h[1] = 0x8b
  h[2] = 8
  h[3] = name ? 8 : 0
  const ms = mtime === undefined ? 0 : new Date(mtime).getTime()
  const seconds = Math.floor(ms / 1000)
  if (!(seconds >= 0 && seconds <= 0xFFFFFFFF)) fail('invalid-option', 'gzip mtime must fall between 1970 and 2106')
  writeU32LE(h, 4, seconds)
  h[8] = level === 9 ? 2 : level === 1 ? 4 : 0
  h[9] = 255
  if (name) h.set(name, 10)
  return h
}

const zeroEnd = (d: Uint8Array, at: number): number => { const z = d.indexOf(0, at); return z < 0 ? -1 : z + 1 }

// The flags of the gzip header starting at `p`, 0 if not there yet. Each fixed byte is checked once present, as zlib's
// header is, so data that isn't gzip fails as such however short it is.
function gzipFlags(d: Uint8Array, p: number): number {
  const n = d.length - p
  if (n > 0 && d[p] !== 0x1f || n > 1 && d[p + 1] !== 0x8b) fail('invalid-header', 'not a gzip stream')
  if (n > 2 && d[p + 2] !== 8) fail('unsupported', 'gzip data that isn\'t deflate-compressed')
  const flg = d[p + 3] ?? 0
  if (flg & 0xE0) fail('invalid-header', 'reserved gzip flags are set')
  return flg
}

// Offset just past a gzip header starting at `p`, or -1 when more bytes are needed
function gzipHeaderEnd(d: Uint8Array, p: number): number {
  const flg = gzipFlags(d, p)
  if (d.length - p < 10) return -1
  let q = p + 10
  if (flg & 4) { if (q + 2 > d.length) return -1; q += 2 + readU16LE(d, q) }
  if (flg & 8) { q = zeroEnd(d, q); if (q < 0) return -1 }
  if (flg & 16) { q = zeroEnd(d, q); if (q < 0) return -1 }
  if (flg & 2) q += 2
  return q > d.length ? -1 : q
}

/**
 * Compresses to gzip (RFC 1952), the format of .gz files and HTTP `Content-Encoding: gzip`.
 * @example const gz = gzip(encodeText('hello'), { filename: 'hello.txt' })
 */
export function gzip(data: Uint8Array, opts: GzipOptions = {}): Uint8Array<ArrayBuffer> {
  return gzipStream(opts).finish(bytes(data))
}

function decodeGzip<B extends ArrayBufferLike = ArrayBuffer>(data: Uint8Array, opts: GunzipOptions, out?: Uint8Array<B>): Uint8Array<B | ArrayBuffer> {
  bytes(data)
  opts ??= {}
  const max = checkMaxSize(opts.maxSize)
  const parts: Uint8Array<B | ArrayBuffer>[] = []
  let p = 0, total = 0
  do {
    const h = gzipHeaderEnd(data, p)
    if (h < 0) fail('truncated', 'gzip header ends early')
    // The last member's ISIZE, unless the end of the file is junk gunzip ignores. Trusted for at most 8x the
    // input up front, the ratio of nearly all real data, so junk can't force a huge allocation.
    const isize = p === 0 && data.length >= 18 ? readU32LE(data, data.length - 4) : 0
    // Later members start small: a guess from all the input left would allocate it again for every member
    const sizeHint = p === 0 ? Math.min(isize, data.length * 8) || undefined : 1 << 10
    const member = inflateRawEnd(data.subarray(h), { out: out?.subarray(total), limit: max - total, sizeHint, expected: isize })
    const t = h + member.end
    if (t + 8 > data.length) fail('truncated', 'gzip trailer is missing')
    checkSize(data, t + 4, member.bytes.length)
    parts.push(member.bytes)
    total += member.bytes.length
    p = t + 8
  } while (isGzip(data, p))
  return out ? out.subarray(0, total) : concat(parts)
}

/**
 * Decompresses gzip data. Concatenated members decode as one, as gzip(1) does, and trailing junk is ignored.
 * @example const text = decodeText(gunzip(gz, { maxSize: 10_000_000 }))
 * @see gunzipInto, to decode into a buffer you already have
 */
export function gunzip(data: Uint8Array, opts: GunzipOptions = {}): Uint8Array<ArrayBuffer> { return decodeGzip(data, opts) }

/**
 * Decompresses gzip data into `out` and returns the view of `out` that holds it, every member included. Output that
 * doesn't fit fails with `too-large`.
 * @example const view = gunzipInto(gz, buffer)
 */
export function gunzipInto<B extends ArrayBufferLike>(data: Uint8Array, out: Uint8Array<B>, opts: GunzipOptions = {}): Uint8Array<B> {
  return into(data, out, (d, o) => decodeGzip(d, opts, o))
}

function decodeAny<B extends ArrayBufferLike = ArrayBuffer>(data: Uint8Array, opts: InflateOptions, out?: Uint8Array<B>): Uint8Array<B | ArrayBuffer> {
  bytes(data)
  opts ??= {}
  // Gzip has no use for a dictionary, but a malformed one is refused here as everywhere else
  dictionaryOf(opts.dictionary)
  if (isGzip(data)) return decodeGzip(data, opts, out)
  if (isZlib(data)) return decodeZlib(data, opts, out)
  return decodeRaw(data, opts, out)
}

/**
 * Decompresses gzip, zlib or raw deflate, whichever the data starts as. The dictionary applies to zlib and raw deflate.
 * @example const text = decodeText(decompress(data))
 * @see decompressInto, to decode into a buffer you already have
 */
export function decompress(data: Uint8Array, opts: InflateOptions = {}): Uint8Array<ArrayBuffer> { return decodeAny(data, opts) }

/**
 * Decompresses gzip, zlib or raw deflate into `out` and returns the view of `out` that holds it. Output that doesn't
 * fit fails with `too-large`.
 * @example const view = decompressInto(data, buffer)
 */
export function decompressInto<B extends ArrayBufferLike>(data: Uint8Array, out: Uint8Array<B>, opts: InflateOptions = {}): Uint8Array<B> {
  return into(data, out, (d, o) => decodeAny(d, opts, o))
}

// Streams

/** Compresses to raw deflate as a stream */
export function deflateStream(opts: DeflateOptions = {}): CompressStream {
  const d = deflater(opts?.level ?? 6, opts?.dictionary)
  return finishOnce({ push: c => d.push(c), flush: () => d.flush(), finish: (c = EMPTY) => d.push(c, true) })
}

// Unguarded: inflateStream and decompressStream guard it
const rawStream = (dictionary: Uint8Array | undefined, limit: number): Stream => {
  const inf = inflater({ dictionary, limit })
  return { push: c => inf.push(c), finish: (c = EMPTY) => inf.push(c, true) }
}

/** Decompresses raw deflate as a stream. finish() fails with `truncated` if the data hasn't ended. */
export const inflateStream = (opts: InflateOptions = {}): Stream => finishOnce(rawStream(opts?.dictionary, checkMaxSize(opts?.maxSize)))

// A deflate stream made with its format's header, which goes out with the first output, and ended by a checksum trailer
function framed(d: Deflater, update: (c: Uint8Array) => void, trailer: () => Uint8Array<ArrayBuffer>): CompressStream {
  return finishOnce({
    push: c => { update(c); return d.push(c) },
    flush: () => d.flush(),
    finish: (c = EMPTY) => { update(c); return d.push(c, true, trailer()) },
  })
}

/** Compresses to zlib as a stream */
export function zlibStream(opts: DeflateOptions = {}): CompressStream {
  opts ??= {}
  // Read once and pinned to its length, so the header's DICTID is always of the dictionary the data was compressed with
  const level = opts.level ?? 6, dictionary = dictionaryOf(opts.dictionary)
  // Checked before the header is built from it, as the deflater would
  checkLevel(level)
  let sum = 1
  return framed(deflater(level, dictionary, zlibHeader(level, dictionary)), c => { sum = adler32(c, sum) }, () => u32be(sum))
}

/** Compresses to gzip as a stream */
export function gzipStream(opts: GzipOptions = {}): CompressStream {
  opts ??= {}
  const level = opts.level ?? 6
  checkLevel(level)
  let crc = 0, size = 0
  return framed(deflater(level, undefined, gzipHeader(level, opts)), c => { crc = crc32(c, crc); size += c.length }, () => gzipTrailer(crc, size))
}

class UnzlibStream implements Stream {
  private pending: Uint8Array = EMPTY
  private inf: Inflater | null = null
  private done = false

  // The dictionary is the stream's own copy (ownDictionary)
  constructor(private readonly dictionary: Uint8Array | undefined, private readonly max: number) {}

  push(chunk: Uint8Array): Uint8Array<ArrayBuffer> { return this.run(chunk, false) }

  finish(chunk: Uint8Array = EMPTY): Uint8Array<ArrayBuffer> {
    const out = this.run(chunk, true)
    if (!this.done) fail('truncated', 'zlib data ends early')
    return out
  }

  private run(chunk: Uint8Array, last: boolean): Uint8Array<ArrayBuffer> {
    bytes(chunk)
    let out: Uint8Array<ArrayBuffer> = new Uint8Array(0)
    if (this.done) return out
    let data = join(this.pending, chunk)
    this.pending = EMPTY
    if (!this.inf) {
      const h = zlibHeaderLength(data, this.dictionary)
      if (h < 0) { this.pending = copy(data); return out }
      this.inf = inflater({ dictionary: h === 6 ? this.dictionary : undefined, limit: this.max })
      data = data.subarray(h)
    }
    if (!this.inf.finished) {
      out = this.inf.push(data, last)
      if (!this.inf.finished) return out
      data = this.inf.rest()
    }
    // The Adler-32 trailer only marks the end; like fflate, kokyu doesn't check it
    if (data.length < 4) { this.pending = copy(data); return out }
    this.done = true
    return out
  }
}

/** Decompresses zlib as a stream */
export function unzlibStream(opts: InflateOptions = {}): Stream {
  const max = checkMaxSize(opts?.maxSize)
  return finishOnce(new UnzlibStream(ownDictionary(opts?.dictionary), max))
}

class GunzipStream implements Stream {
  private pending: Uint8Array = EMPTY
  private inf: Inflater | null = null
  private size = 0
  private total = 0
  private members = 0
  private trailing = false
  // Header fields still to read (FEXTRA, FNAME, FCOMMENT, FHCRC flag bits), or -1 outside a header
  private fields = -1
  private skip = 0

  constructor(private readonly max: number) {}

  push(chunk: Uint8Array): Uint8Array<ArrayBuffer> { return this.run(chunk, false) }

  finish(chunk: Uint8Array = EMPTY): Uint8Array<ArrayBuffer> {
    const out = this.run(chunk, true)
    if (this.inf || this.fields >= 0 || this.members === 0 || this.pending.length) fail('truncated', 'gzip data ends early')
    return out
  }

  // Consumes header bytes as they arrive, holding only the fixed part and FEXTRA's length until whole, so not even an
  // endless name or comment is held or rescanned. Returns the offset past the header, or -1 when all of `d` was header.
  private header(d: Uint8Array): number {
    let q = 0
    if (this.fields < 0) {
      const flg = gzipFlags(d, 0)
      if (d.length < 10) { this.pending = copy(d); return -1 }
      this.fields = flg & (4 | 8 | 16 | 2)
      q = 10
    }
    for (;;) {
      const n = Math.min(this.skip, d.length - q)
      this.skip -= n
      q += n
      if (this.skip) return -1
      if (this.fields & 4) {
        if (d.length - q < 2) { this.pending = copy(d, q); return -1 }
        this.skip = readU16LE(d, q)
        q += 2
        this.fields &= ~4
      } else if (this.fields & (8 | 16)) {
        q = zeroEnd(d, q)
        if (q < 0) return -1
        this.fields &= this.fields & 8 ? ~8 : ~16
      } else if (this.fields & 2) {
        this.skip = 2
        this.fields &= ~2
      } else {
        this.fields = -1
        return q
      }
    }
  }

  private run(chunk: Uint8Array, last: boolean): Uint8Array<ArrayBuffer> {
    bytes(chunk)
    const outs: Uint8Array<ArrayBuffer>[] = []
    let data = join(this.pending, chunk)
    this.pending = EMPTY
    while (!this.trailing) {
      if (!this.inf) {
        if (!data.length) break
        // After a member, anything that doesn't start another one is ignored, as gzip(1) does;
        // so is a lone final byte, too short to start one
        if (this.fields < 0 && this.members > 0 && (data[0] !== 0x1f || (data.length > 1 ? data[1] !== 0x8b : last))) { this.trailing = true; break }
        const h = this.header(data)
        if (h < 0) break
        // Each member may use what the ones before left of the cap
        this.inf = inflater({ limit: this.max - this.total })
        this.size = 0
        data = data.subarray(h)
      }
      if (!this.inf.finished) {
        const out = this.inf.push(data, last)
        this.size += out.length
        this.total += out.length
        if (out.length) outs.push(out)
        if (!this.inf.finished) break
        data = this.inf.rest()
      }
      if (data.length < 8) { this.pending = copy(data); break }
      checkSize(data, 4, this.size)
      this.members++
      this.inf = null
      data = data.subarray(8)
    }
    return concat(outs)
  }
}

/** Decompresses gzip as a stream, concatenated members included */
export const gunzipStream = (opts: GunzipOptions = {}): Stream => finishOnce(new GunzipStream(checkMaxSize(opts?.maxSize)))

/** Decompresses gzip, zlib or raw deflate as a stream, whichever the first bytes show */
export function decompressStream(opts: InflateOptions = {}): Stream {
  opts ??= {}
  const max = checkMaxSize(opts.maxSize)
  // Taken now, though the format that reads it is only known once two bytes arrive
  const dictionary = ownDictionary(opts.dictionary)
  let inner: Stream | null = null
  let pending: Uint8Array = EMPTY
  const route = (chunk: Uint8Array, last: boolean): Uint8Array<ArrayBuffer> => {
    bytes(chunk)
    if (!inner) {
      const data = join(pending, chunk)
      if (data.length < 2 && !last) { pending = copy(data); return new Uint8Array(0) }
      inner = isGzip(data) ? new GunzipStream(max) : isZlib(data) ? new UnzlibStream(dictionary, max) : rawStream(dictionary, max)
      chunk = data
    }
    return last ? inner.finish(chunk) : inner.push(chunk)
  }
  return streamOf(route, EMPTY)
}
