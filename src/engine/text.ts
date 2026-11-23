// Strings to bytes and back, UTF-8 by default or latin1 (one byte per character).

import { BUSY, EMPTY, FINISHED, bytes, concat, copy, fail, streamOf, type Stream } from './shared.js'

/** Options for converting between text and bytes */
export interface TextOptions {
  /** One byte per character, U+0000 to U+00FF, instead of UTF-8. Default false. */
  latin1?: boolean | undefined
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()
// Zip names and comments keep a leading U+FEFF: it's part of the name, as kokyu writes it and Python reads it, and
// dropping it could make two names one
const names = new TextDecoder('utf-8', { ignoreBOM: true })
const strict = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })
// A stream settles its encoding when made, so a caller reusing the options object can't switch it midway
const LATIN1: TextOptions = { latin1: true }

// Text as TextEncoder reads it: undefined is its default, '', and anything else goes through ToString, which
// refuses a Symbol. Every encoding reads the same text.
const str = (s: unknown): string => typeof s === 'string' ? s : s === undefined ? '' : `${s as string}`

/**
 * Encodes text as UTF-8 bytes, or latin1 with `{ latin1: true }`.
 * @example const packed = gzip(encodeText('héllo'))
 */
export function encodeText(text: string, opts: TextOptions = {}): Uint8Array<ArrayBuffer> {
  if (!opts?.latin1) return encoder.encode(text)
  text = str(text)
  const out = new Uint8Array(text.length)
  for (let i = 0; i < text.length; i++) {
    const c = text.charCodeAt(i)
    if (c > 0xFF) fail('invalid-option', 'latin1 text can only hold characters up to U+00FF')
    out[i] = c
  }
  return out
}

/** Decodes UTF-8 bytes to text, invalid sequences becoming U+FFFD, or latin1 with `{ latin1: true }` */
export function decodeText(data: Uint8Array, opts: TextOptions = {}): string {
  bytes(data)
  if (!opts?.latin1) return decoder.decode(data)
  let s = ''
  // Chunked so the spread never exceeds the engine's argument limit
  for (let i = 0; i < data.length; i += 8192) s += String.fromCharCode(...data.subarray(i, i + 8192))
  return s
}

// Bytes 0x80 to 0xFF in IBM code page 437, the encoding APPNOTE (appendix D) gives names without the UTF-8 flag
const CP437 = '\u00c7\u00fc\u00e9\u00e2\u00e4\u00e0\u00e5\u00e7\u00ea\u00eb\u00e8\u00ef\u00ee\u00ec\u00c4\u00c5\u00c9\u00e6\u00c6\u00f4\u00f6\u00f2\u00fb\u00f9\u00ff\u00d6\u00dc\u00a2\u00a3\u00a5\u20a7\u0192\u00e1\u00ed\u00f3\u00fa\u00f1\u00d1\u00aa\u00ba\u00bf\u2310\u00ac\u00bd\u00bc\u00a1\u00ab\u00bb\u2591\u2592\u2593\u2502\u2524\u2561\u2562\u2556\u2555\u2563\u2551\u2557\u255d\u255c\u255b\u2510\u2514\u2534\u252c\u251c\u2500\u253c\u255e\u255f\u255a\u2554\u2569\u2566\u2560\u2550\u256c\u2567\u2568\u2564\u2565\u2559\u2558\u2552\u2553\u256b\u256a\u2518\u250c\u2588\u2584\u258c\u2590\u2580\u03b1\u00df\u0393\u03c0\u03a3\u03c3\u00b5\u03c4\u03a6\u0398\u03a9\u03b4\u221e\u03c6\u03b5\u2229\u2261\u00b1\u2265\u2264\u2320\u2321\u00f7\u2248\u00b0\u2219\u00b7\u221a\u207f\u00b2\u25a0\u00a0'

// Zip names without the UTF-8 flag are still usually UTF-8; anything that isn't valid UTF-8 is read as code page 437,
// as Python and Info-ZIP read it
export function decodeName(data: Uint8Array, utf8: boolean): string {
  if (utf8) return names.decode(data)
  try {
    return strict.decode(data)
  } catch {
    let s = ''
    for (const b of data) s += b < 0x80 ? String.fromCharCode(b) : CP437[b - 0x80]!
    return s
  }
}

// A class rather than closures in finishOnce: building one per stream is a measurable share of a 1 KB encode
class Utf8Encoder implements Stream<string, Uint8Array<ArrayBuffer>> {
  // A high surrogate at the end of a chunk waits for its pair, or it would encode as U+FFFD
  private held = ''
  private done = false
  private busy = false

  push(s: string): Uint8Array<ArrayBuffer> {
    return this.run(s, false)
  }

  finish(s = ''): Uint8Array<ArrayBuffer> {
    return this.run(s, true)
  }

  private run(s: string, last: boolean): Uint8Array<ArrayBuffer> {
    if (this.done) fail('invalid-state', FINISHED)
    if (this.busy) fail('invalid-state', BUSY)
    // Only text that isn't a string runs the caller's code as it's read, and a call it makes from there is refused
    if (typeof s !== 'string') {
      this.busy = true
      try {
        s = str(s)
      } finally {
        this.busy = false
      }
    }
    this.done = last
    // Only when something is held: JSC turns '' + s into a rope that its encoder then copies
    if (this.held) {
      s = this.held + s
      this.held = ''
    }
    const c = s.charCodeAt(s.length - 1)
    if (!last && c >= 0xD800 && c <= 0xDBFF) {
      this.held = s[s.length - 1]!
      s = s.slice(0, -1)
    }
    // An empty finish() is the common case, and encode('') still costs a native call
    return s ? encoder.encode(s) : new Uint8Array(0)
  }
}

/** Encodes text pushed in pieces; a character split between pushes still encodes correctly */
export function encodeTextStream(opts: TextOptions = {}): Stream<string, Uint8Array<ArrayBuffer>> {
  if (opts?.latin1) return streamOf(s => encodeText(s, LATIN1), '', str)
  return new Utf8Encoder()
}

// Bytes in the UTF-8 sequence a byte starts: 1 for ASCII, continuation bytes (10xxxxxx) and invalid leads
const seqLen = (b: number): number => b < 0xC2 ? 1 : b < 0xE0 ? 2 : b < 0xF0 ? 3 : b < 0xF5 ? 4 : 1

// Plain decode() per chunk, holding back only a character cut off at the end: stream mode costs 1.7x
// more (measured). ignoreBOM keeps a U+FEFF inside the text; the stream's leading one is dropped here.
/** Decodes bytes pushed in pieces; a character split between chunks still decodes correctly */
export function decodeTextStream(opts: TextOptions = {}): Stream<Uint8Array, string> {
  if (opts?.latin1) return streamOf(b => decodeText(b, LATIN1), EMPTY)
  const d = new TextDecoder('utf-8', { ignoreBOM: true })
  let held: Uint8Array = EMPTY, started = false
  const run = (b: Uint8Array, last: boolean): string => {
    bytes(b)
    let s = ''
    if (held.length) {
      // Complete the held character with the continuation bytes that open this chunk
      const need = seqLen(held[0]!) - held.length
      let k = 0
      while (k < need && k < b.length && (b[k]! & 0xC0) === 0x80) k++
      if (k < need && k === b.length && !last) { held = concat([held, b.subarray(0, k)]); return '' }
      s = d.decode(concat([held, b.subarray(0, k)]))
      b = b.subarray(k)
    }
    let cut = b.length
    for (let j = b.length - 1; !last && j >= 0 && j >= b.length - 3; j--) {
      if ((b[j]! & 0xC0) === 0x80) continue
      if (seqLen(b[j]!) > b.length - j) cut = j
      break
    }
    held = cut < b.length ? copy(b, cut) : EMPTY
    if (cut > 0) s += d.decode(cut === b.length ? b : b.subarray(0, cut))
    if (!started && s) {
      started = true
      if (s.charCodeAt(0) === 0xFEFF) s = s.slice(1)
    }
    return s
  }
  return streamOf(run, EMPTY)
}
