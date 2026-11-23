// DEFLATE compression (RFC 1951) as a resumable stream. Deterministic: the same bytes and level always give the same
// output however the input is chunked (flush() aside), except that a level-0 stream may end in one more empty block.

import {
  LEN_BASE, LEN_EXTRA, DIST_BASE, DIST_EXTRA, CLEN_ORDER, FIXED_LIT_LENGTHS, FIXED_DIST_LENGTHS, canonicalCodes, grow,
  EMPTY, MAX_BYTES, WINDOW, bytes, checkLevel, dictionaryOf, resize, shrink, viewOf,
} from './shared.js'

const MIN_MATCH = 3
const MAX_MATCH = 258
const WMASK = WINDOW - 1
// 2^16 buckets, twice the window: with as many as the window, 62% of positions in incompressible data find a live
// entry to try and fail. Fixed, not sized to the input as fflate's is, so chunking can't change the output.
const HASH_BITS = 16
// Positions are hashed on 4 bytes. Measured smaller and faster than 3: 3-byte matches far back cost
// more bits than the literals they replace, and greedy parsing takes them anyway.
const HASH_LEN = 4
// A block closes when its symbols fill, or at MAX_SPAN raw bytes, which keeps what a stored fallback needs inside a
// streaming window
const BLOCK = 1 << 14
const MAX_SPAN = 1 << 18
const SKIP_MAX = 32
// A stream compresses nothing until it holds this much, so one that ends sooner finishes through the
// shared tables like a one-shot call instead of allocating 448 KB of its own. Output is unchanged.
const DEFER = 1 << 16
const NONE = 0, SYNC = 1, FINAL = 2

// Per level from 1 (0 stores only): chain depth, the length that ends a search, a held length that quarters the next
// search, the length that goes out without looking ahead (3 is greedy), and the positions of a found match scanned for
// a chain to switch to (0: none). Tuned on synthetic data and Silesia together, speed on real files first.
const LEVELS: readonly (readonly [chain: number, nice: number, good: number, lazy: number, scan: number])[] = [
  [6, 32, 4, 3, 0],
  [8, 32, 4, 3, 0],
  [12, 64, 4, 3, 0],
  [12, 64, 8, 16, 16],
  [16, 64, 8, 32, 16],
  [48, 128, 4, 16, 16],
  [48, 258, 8, 32, 16],
  [48, 258, 8, 258, 16],
  [96, 258, 8, 258, 16],
]

// Past the first few, length codes come four to each count of extra bits and distance codes two, so a code is the
// top bit's position plus the bits just below it. 258 has a code of its own.
const lenCode = (len: number): number => {
  const v = len - 3, top = 31 - Math.clz32(v)
  return v < 8 ? v : len === 258 ? 28 : 4 * top - 4 + ((v >> (top - 2)) & 3)
}
const distCode = (dist: number): number => {
  const v = dist - 1, top = 31 - Math.clz32(v)
  return v < 4 ? v : 2 * top + ((v >> (top - 1)) & 1)
}

// Scratch for buildCodes, which never nests, so one set serves every call
const KEYS = new Uint32Array(320)
const DEPTH = new Int32Array(320)

// Huffman code lengths of at most maxBits: Moffat and Katajainen's in-place algorithm, ties broken by symbol so output
// is deterministic. A code too deep is rebuilt from halved weights, which flattens it at a cost of a few bits at most.
export function buildCodes(freqs: Uint32Array, maxBits: number): Uint8Array {
  const n = freqs.length, lengths = new Uint8Array(n), a = DEPTH
  for (let shift = 0; ; shift++) {
    let used = 0
    for (let s = 0; s < n; s++) if (freqs[s]) KEYS[used++] = ((freqs[s]! >>> shift) || 1) * 512 + s
    // A lone symbol still needs a one-bit code, or the decoder sees an empty tree
    if (used < 2) {
      if (used) lengths[KEYS[0]! & 511] = 1
      return lengths
    }
    const sorted = KEYS.subarray(0, used).sort()
    for (let i = 0; i < used; i++) a[i] = sorted[i]! >>> 9
    // Pair the two lightest of leaves (from s) and internal nodes (from r); a[t] becomes the new node
    for (let t = 0, s = 0, r = 0; t < used - 1; t++) {
      let sum = 0
      for (let k = 0; k < 2; k++) {
        if (s >= used || (r < t && a[r]! < a[s]!)) { sum += a[r]!; a[r++] = t } else sum += a[s++]!
      }
      a[t] = sum
    }
    // Internal node depths from parent links, then leaf depths level by level
    a[used - 2] = 0
    for (let t = used - 3; t >= 0; t--) a[t] = a[a[t]!]! + 1
    for (let avail = 1, inner = 0, depth = 0, t = used - 2, x = used - 1; avail > 0; avail = 2 * inner, inner = 0, depth++) {
      while (t >= 0 && a[t] === depth) { inner++; t-- }
      while (avail > inner) { a[x--] = depth; avail-- }
    }
    if (a[0]! <= maxBits) {
      for (let i = 0; i < used; i++) lengths[sorted[i]! & 511] = a[i]!
      return lengths
    }
  }
}

// A dynamic block's two code-length tables, run-length encoded into the 19-symbol alphabet that precedes them:
// symbols and their extra bits (value | count << 8). Scratch, read before the next block plans.
const PACKED_SYMS = new Uint8Array(286 + 30)
const PACKED_EXTRA = new Uint16Array(286 + 30)
const CLEN_FREQS = new Uint32Array(19)

let FIXED_CODES: Uint16Array[] | undefined

// Multiplicative hash of the four bytes at a position: every input bit reaches the top bits it keeps
const hashOf = (w: number): number => Math.imul(w, 0x9E3779B1) >>> (32 - HASH_BITS)

// Chains hold position + base + 1, so 0 is empty and new tables need no fill. Calls sharing one set raise
// the base past all they stored, which drops every older entry below the search limit without clearing.
interface Tables { head: Int32Array; prev: Int32Array; syms: Int32Array; base: number }

const newTables = (): Tables => ({ head: new Int32Array(1 << HASH_BITS), prev: new Int32Array(WINDOW), syms: new Int32Array(BLOCK), base: 0 })

// Positions [from, to) into the hash chains
const hashRange = ({ head, prev, base }: Tables, view: DataView, from: number, to: number): void => {
  for (let j = from; j < to; j++) {
    const h = hashOf(view.getUint32(j))
    prev[j & WMASK] = head[h]!
    head[h] = base + j + 1
  }
}

let shared: Tables | null = null
let sharedBusy = false

// A finished stream's tables, for the next stream that needs a set of its own: zip entries and other streams in turn
// skip allocating 448 KB each. Links are cleared as well as heads: a dictionary's last positions never go in, and a
// chain switch can read theirs.
let spare: Tables | null = null
const ownTables = (): Tables => {
  const s = spare
  if (!s) return newTables()
  spare = null
  s.head.fill(0)
  s.prev.fill(0)
  s.base = 0
  return s
}

interface LoopState { pos: number; n: number; extra: number; held: number; heldDist: number; miss: number; blockStart: number }

// The match loop, outside the deflater's closure: V8 runs it 10 to 25% slower as a closure
function matchLoop(
  st: LoopState, tb: Tables, data: Uint8Array, end: number, mode: number, level: number, lf: Uint32Array, df: Uint32Array,
  emit: (upTo: number, symbols: number, extra: number) => void,
): void {
  const { head, prev, syms, base } = tb
  const [chain, nice, good, lazy, scan] = LEVELS[level - 1]!
  const view = viewOf(data), top = end - HASH_LEN + 1
  // Before the final chunk, stop where a full-length match and the hashes inside it still
  // have every byte they read, so chunking never changes the output
  const stop = mode === NONE ? end - MAX_MATCH - HASH_LEN : end
  let i = st.pos, k = st.n, x = st.extra, h0 = st.held, hd = st.heldDist, m = st.miss, bs = st.blockStart
  while (i < stop) {
    let bestLen = h0 || MIN_MATCH - 1, bestDist = 0
    if (i + HASH_LEN <= end) {
      const h = hashOf(view.getUint32(i)), max = end - i < MAX_MATCH ? end - i : MAX_MATCH
      // A held match can already reach the end, and then nothing longer fits
      if (bestLen < max) {
        // A lookahead search goes half as deep, and a quarter once the held match is good (libdeflate)
        let depth = h0 ? h0 >= good ? chain >> 2 : chain >> 1 : chain
        // Entries at or below the limit are out of the window, empty, or from an earlier call
        const limit = base + (i > WINDOW ? i - WINDOW : 0)
        // One cheap rejection before the full compare: the four bytes ending where a longer match would
        // pass the current best (at the start, the four hashed). Most candidates die here.
        // The chain followed is that of the position j bytes into each candidate: 0 until a switch
        let at = bestLen > 3 ? bestLen - 3 : 0, want = view.getUint32(i + at), v = head[h]!, j = 0
        while (v > limit + j && depth-- > 0) {
          // The next link is loaded before the compare, so its memory fetch overlaps the compare's work
          const p = v - base - 1, c = p - j
          v = prev[p & WMASK]!
          if (view.getUint32(c + at) === want) {
            // Four bytes a step; at the first difference, the XOR's leading zeros count the bytes still equal
            let l = 0
            for (;;) {
              if (l + 4 > max) {
                while (l < max && data[c + l] === data[i + l]) l++
                break
              }
              const d = view.getUint32(c + l) ^ view.getUint32(i + l)
              if (d) { l += Math.clz32(d) >> 3; break }
              l += 4
            }
            if (l > bestLen) {
              bestLen = l
              bestDist = i - c
              if (l >= nice || l === max) break
              at = l - 3
              want = view.getUint32(i + at)
              // A longer match holds every 4-byte string of this one at the same offset, so it lies on each of their
              // chains. Go on along the one whose next entry reaches back farthest (fast-zlib's chain switching); an
              // empty one means there is none. A string must end inside the match, and start before i to be chained.
              for (let q = 0, n = Math.min(l - 3, i - c, scan); q < n; q++) {
                const e = prev[(c + q) & WMASK]!
                if (e - q < v - j) { v = e; j = q }
              }
            }
          }
        }
      }
      prev[i & WMASK] = head[h]!
      head[h] = base + i + 1
    }

    // Literals go out for [litFrom, litTo), then a match if len is set
    let litFrom = i, litTo = i, len = 0, dist = 0, next = i + 1
    if (h0) {
      // libdeflate's test: the match here must pay for the literal it costs the held one, 4 per byte of length
      // gained against a bit per doubling of distance
      if (bestDist && 4 * (bestLen - h0) + Math.clz32(bestDist) - Math.clz32(hd) > 2) {
        litFrom = i - 1
      } else {
        len = h0
        dist = hd
        next = i - 1 + h0
      }
      h0 = 0
    }
    if (!len) {
      if (!bestDist) {
        // zstd's skip: every 256 misses in a row widen the step by one, up to every 32nd position. Skipped bytes are
        // literals but still hashed, so a later repeat is found; `>>>` as `>>` goes negative at 2^31.
        litTo = next = i + Math.min(1 + (m++ >>> 8), SKIP_MAX, BLOCK - 2 - k, end - i)
      } else if (bestLen >= lazy) {
        len = bestLen
        dist = bestDist
        next = i + bestLen
        m = 0
      } else {
        h0 = bestLen
        hd = bestDist
        m = 0
      }
    }

    for (let q = litFrom; q < litTo; q++) {
      const b = data[q]!
      syms[k++] = b
      lf[b] = lf[b]! + 1
    }
    if (len) {
      const lc = lenCode(len), dc = distCode(dist)
      syms[k++] = 1 << 28 | lc << 23 | (len - LEN_BASE[lc]!) << 18 | dc << 13 | (dist - DIST_BASE[dc]!)
      lf[257 + lc] = lf[257 + lc]! + 1
      df[dc] = df[dc]! + 1
      x += LEN_EXTRA[lc]! + DIST_EXTRA[dc]!
    }
    // Positions inside a match or a skip still enter the hash chains, so later matches can reach back into them and a
    // chain switch can follow their links
    hashRange(tb, view, i + 1, next < top ? next : top)
    i = next

    // Symbols are recorded up to here; a held match belongs to whatever block comes next.
    // A step adds at most a literal and a match, and a skip stops at BLOCK - 2, so syms never overflows.
    const upTo = h0 ? i - 1 : i
    if (k >= BLOCK - 2 || upTo - bs >= MAX_SPAN) {
      emit(upTo, k, x)
      k = x = 0
      bs = upTo
    }
  }

  st.pos = i
  st.n = k
  st.extra = x
  st.held = h0
  st.heldDist = hd
  st.miss = m
}

export interface Deflater {
  // Compressed bytes for `chunk`. Output trails input until the final push: by the open block, sent once it holds
  // BLOCK symbols or MAX_SPAN bytes, and in a stream by its first DEFER bytes, which wait uncompressed.
  // The final push ends with `tail`, a format's trailer.
  push(chunk: Uint8Array, final?: boolean, tail?: Uint8Array): Uint8Array<ArrayBuffer>
  // Everything pushed so far, byte-aligned and decodable, with the window kept for what follows.
  flush(): Uint8Array<ArrayBuffer>
}

// `head` goes out ahead of the compressed bytes: a format's header, written into the same output so a one-shot's
// header, data and trailer take no joining copy
export function deflater(level: number, dictionary?: Uint8Array, head: Uint8Array = EMPTY): Deflater {
  checkLevel(level)
  dictionary = dictionaryOf(dictionary)
  const litFreq = new Uint32Array(286), distFreq = new Uint32Array(30)
  // The bit writer: bits packed least-significant first into out, reserved ahead so the hot loops write unchecked
  let out = new Uint8Array(1 << 12), o = 0, acc = 0, nb = 0
  // Blocks written stored join one run, kept in buf from `run` until whole 64 KB pieces fill, so incompressible data
  // costs a stored header per piece rather than per block. Nothing waits while run equals blockStart.
  let t: Tables | null = null, buf: Uint8Array = EMPTY, end = 0, pos = 0, blockStart = 0, run = 0, n = 0, extra = 0
  // A match found at the position before pos, held back in case the one at pos is better
  let held = 0, heldDist = 0, miss = 0
  litFreq[256] = 1

  const reserve = (k: number): void => { out = grow(out, o, k) }
  const write = (v: number, bits: number): void => {
    acc |= v << nb
    nb += bits
    while (nb >= 8) { out[o++] = acc; acc >>>= 8; nb -= 8 }
  }
  const align = (): void => { if (nb) { out[o++] = acc; acc = nb = 0 } }

  // Stored blocks hold at most 65535 bytes, so longer spans become several. An empty span still
  // emits one block, which is what a sync flush and an empty final block need.
  const stored = (from: number, to: number, final: boolean): void => {
    do {
      const e = Math.min(from + 65535, to), k = e - from
      reserve(k + 8)
      write(+(final && e === to), 3)
      align()
      write(k, 16)
      write(k ^ 0xFFFF, 16)
      out.set(buf.subarray(from, e), o)
      o += k
      from = e
    } while (from < to)
  }

  // Codes go into a local accumulator that spills 16 bits at a time, so each add stays under 31 bits. A symbol is a
  // literal byte, or a match packed as 1<<28 | length code<<23 | its extra<<18 | distance code<<13 | its extra.
  const symbols = (syms: Int32Array, ll: Uint8Array, lc: Uint16Array, dl: Uint8Array, dc: Uint16Array): void => {
    const buf = out, count = n
    let p = o, a = acc, b = nb
    for (let i = 0; i < count; i++) {
      const s = syms[i]!
      if (s < 256) {
        a |= lc[s]! << b
        b += ll[s]!
      } else {
        const c = (s >>> 23) & 31, k = (s >>> 13) & 31
        a |= lc[257 + c]! << b
        b += ll[257 + c]!
        if (b >= 16) { buf[p++] = a; buf[p++] = a >>> 8; a >>>= 16; b -= 16 }
        a |= ((s >>> 18) & 31) << b
        b += LEN_EXTRA[c]!
        if (b >= 16) { buf[p++] = a; buf[p++] = a >>> 8; a >>>= 16; b -= 16 }
        a |= dc[k]! << b
        b += dl[k]!
        if (b >= 16) { buf[p++] = a; buf[p++] = a >>> 8; a >>>= 16; b -= 16 }
        a |= (s & 8191) << b
        b += DIST_EXTRA[k]!
      }
      if (b >= 16) { buf[p++] = a; buf[p++] = a >>> 8; a >>>= 16; b -= 16 }
    }
    o = p
    acc = a
    nb = b
    write(lc[256]!, ll[256]!)
  }

  // Prices stored, fixed and dynamic from the block's counts, kept while matching, and writes only the cheapest
  const emitBlock = (final: boolean, upTo: number): void => {
    const lit = buildCodes(litFreq, 15), dist = buildCodes(distFreq, 15)
    let hlit = 286, hdist = 30, hclen = 19, packed = 0
    while (hlit > 257 && !lit[hlit - 1]) hlit--
    while (hdist > 1 && !dist[hdist - 1]) hdist--
    const all = new Uint8Array(hlit + hdist)
    all.set(lit.subarray(0, hlit))
    all.set(dist.subarray(0, hdist), hlit)
    CLEN_FREQS.fill(0)
    const pack = (s: number, e = 0, bits = 0): void => {
      PACKED_SYMS[packed] = s
      PACKED_EXTRA[packed++] = e | bits << 8
      CLEN_FREQS[s]!++
    }
    for (let i = 0; i < all.length;) {
      const v = all[i]!
      let run = 1
      while (i + run < all.length && all[i + run] === v) run++
      i += run
      if (v) { pack(v); run-- }
      // Zeros repeat as 17 or 18, other lengths as 16 after one written plainly
      while (run >= 3) {
        const k = Math.min(run, v ? 6 : 138)
        if (v) pack(16, k - 3, 2)
        else if (k <= 10) pack(17, k - 3, 3)
        else pack(18, k - 11, 7)
        run -= k
      }
      while (run-- > 0) pack(v)
    }
    const clen = buildCodes(CLEN_FREQS, 7)
    while (hclen > 4 && !clen[CLEN_ORDER[hclen - 1]!]) hclen--
    let dyn = 17 + hclen * 3 + extra, fix = 3 + extra
    for (let i = 0; i < packed; i++) dyn += clen[PACKED_SYMS[i]!]! + (PACKED_EXTRA[i]! >> 8)
    for (let s = 0; s < 286; s++) { dyn += litFreq[s]! * lit[s]!; fix += litFreq[s]! * FIXED_LIT_LENGTHS[s]! }
    for (let s = 0; s < 30; s++) { dyn += distFreq[s]! * dist[s]!; fix += distFreq[s]! * 5 }
    const fixed = fix <= dyn
    if ((upTo - blockStart + 5) * 8 <= Math.min(dyn, fix)) {
      const cut = final ? upTo : run + Math.floor((upTo - run) / 65535) * 65535
      if (cut > run || final) stored(run, cut, final)
      run = cut
    } else {
      if (run < blockStart) stored(run, blockStart, false)
      run = upTo
      reserve((Math.min(dyn, fix) >> 3) + 8)
      write(+final | (fixed ? 2 : 4), 3)
      if (!fixed) {
        write(hlit - 257 | (hdist - 1) << 5 | (hclen - 4) << 10, 14)
        for (let i = 0; i < hclen; i++) write(clen[CLEN_ORDER[i]!]!, 3)
        const cc = canonicalCodes(clen)
        for (let i = 0; i < packed; i++) {
          const s = PACKED_SYMS[i]!, e = PACKED_EXTRA[i]!
          write(cc[s]!, clen[s]!)
          write(e & 0xFF, e >> 8)
        }
      }
      const [lc, dc] = fixed ? FIXED_CODES ??= [canonicalCodes(FIXED_LIT_LENGTHS), canonicalCodes(FIXED_DIST_LENGTHS)] : [canonicalCodes(lit), canonicalCodes(dist)]
      symbols(t!.syms, fixed ? FIXED_LIT_LENGTHS : lit, lc!, fixed ? FIXED_DIST_LENGTHS : dist, dc!)
    }
    n = extra = 0
    blockStart = upTo
    litFreq.fill(0)
    litFreq[256] = 1
    distFreq.fill(0)
  }

  const compress = (mode: number): void => {
    if (!level) {
      // Stored-only: whole 64 KB blocks as they fill, the remainder on sync or final
      const to = mode === NONE ? blockStart + Math.floor((end - blockStart) / 65535) * 65535 : end
      if (to > blockStart || mode === FINAL) stored(blockStart, to, mode === FINAL)
      blockStart = to
      if (mode === SYNC) stored(to, to, false)
      return
    }
    const st: LoopState = { pos, n, extra, held, heldDist, miss, blockStart }
    matchLoop(st, t ??= ownTables(), buf, end, mode, level, litFreq, distFreq, (upTo, k, x) => { n = k; extra = x; emitBlock(false, upTo) })
    ;({ pos, n, extra, held, heldDist, miss } = st)
    if (mode === FINAL) {
      emitBlock(true, end)
    } else if (mode === SYNC) {
      if (n) emitBlock(false, end)
      if (run < end) stored(run, end, false)
      run = end
      stored(end, end, false)
    }
  }

  // The length is read once and the copy pinned to it: a view of a growable SharedArrayBuffer can grow meanwhile
  const append = (chunk: Uint8Array): void => {
    const n = chunk.length
    if (end + n > buf.length) {
      // Slide by whole windows so `prev`, indexed by position & WMASK, keeps its layout. Level 0 never looks back, so
      // it keeps no window and has no tables to line up.
      const keep = level ? Math.min(run, pos - WINDOW) : blockStart
      const shift = level ? keep - (keep & WMASK) : keep
      if (shift > 0) {
        buf.copyWithin(0, shift, end)
        end -= shift
        pos -= shift
        blockStart -= shift
        run -= shift
        if (t) for (const a of [t.head, t.prev]) for (let j = 0; j < a.length; j++) a[j] = Math.max(0, a[j]! - shift)
      }
      buf = grow(buf, end, n)
    }
    buf.set(chunk.subarray(0, n), end)
    end += n
  }

  // The whole bytes written so far, as an array of their own; a partial byte stays for the next write
  const take = (last: boolean, tail: Uint8Array): Uint8Array<ArrayBuffer> => {
    reserve(1 + tail.length)
    if (last) {
      align()
      out.set(tail, o)
      o += tail.length
    }
    const r = last ? shrink(out, o) : out.slice(0, o)
    // The finished buffer is the caller's now, or detached
    if (last) out = EMPTY
    o = 0
    return r
  }

  reserve(head.length)
  out.set(head)
  o = head.length

  if (dictionary?.length && level) {
    const d = dictionary.subarray(-WINDOW)
    append(d)
    hashRange(t = ownTables(), viewOf(buf), 0, d.length - HASH_LEN + 1)
    pos = blockStart = run = d.length
  }

  return {
    push(chunk, final = false, tail = EMPTY) {
      bytes(chunk)
      // A lone final chunk is read in place, and its output sized once to what stored blocks would take, which is
      // the most it can be: no copies as it grows, and pages it never writes cost no memory
      if (final && !end) {
        buf = chunk
        end = chunk.length
        const need = o + end + (end >>> 11) + 32 + tail.length
        if (need > out.length) out = resize(out, o, Math.min(need, MAX_BYTES))
      } else {
        append(chunk)
      }
      if (final || t || end >= DEFER) {
        // Compressing everything in one call borrows the shared tables, and lets go of them after: a stream still
        // holding them would write into the next borrower's
        let borrowed = false
        if (final && level && !t && !sharedBusy) {
          t = shared ??= newTables()
          if (t.base + end + 2 > 0x7FFFFFFF) { t.head.fill(0); t.base = 0 }
          sharedBusy = borrowed = true
        }
        try {
          compress(final ? FINAL : NONE)
        } finally {
          if (borrowed) {
            t!.base += end + 1
            sharedBusy = false
          } else if (final) {
            spare ??= t
          }
          if (final) t = null
        }
      }
      return take(final, tail)
    },
    flush() {
      compress(SYNC)
      return take(false, EMPTY)
    },
  }
}
