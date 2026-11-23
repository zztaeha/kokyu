// CRC-32 (gzip, zip) and Adler-32 (zlib). Both take the previous value so they can run over a stream.

import { bytes } from './shared.js'

// Slicing-by-16: sixteen derived tables fold sixteen bytes per step
const CRC = new Int32Array(256 * 16)
for (let n = 0; n < 256; n++) {
  let c = n
  for (let k = 0; k < 8; k++) c = c & 1 ? 0xEDB88320 ^ (c >>> 1) : c >>> 1
  CRC[n] = c
}
for (let n = 0; n < 256; n++) {
  for (let t = 1; t < 16; t++) {
    const prev = CRC[(t - 1) * 256 + n]!
    CRC[t * 256 + n] = (prev >>> 8) ^ CRC[prev & 0xFF]!
  }
}

// Bytes are read four at a time as aligned words, which assumes little-endian; elsewhere the byte loop does it all.
// Signed words on purpose: JSC (Bun, Safari) read a Uint32Array view 10x slower once text has a byte over 0x7F.
const LITTLE = new Uint8Array(Uint32Array.of(1).buffer)[0] === 1
const NO_WORDS = new Int32Array(0)
// Below this, slicing-by-16 alone is faster than Chorba
const CHORBA_MIN = 512
const TAIL = new Int32Array(20)
const TAIL8 = new Uint8Array(TAIL.buffer)

// Slicing-by-16 over the aligned words u[0, to)
function words16(u: Int32Array, to: number, c: number): number {
  for (let k = 0; k < to; k += 4) {
    const a = c ^ u[k]!, b = u[k + 1]!, d = u[k + 2]!, e = u[k + 3]!
    c = CRC[3840 + (a & 0xFF)]! ^ CRC[3584 + ((a >>> 8) & 0xFF)]! ^ CRC[3328 + ((a >>> 16) & 0xFF)]! ^ CRC[3072 + (a >>> 24)]! ^
      CRC[2816 + (b & 0xFF)]! ^ CRC[2560 + ((b >>> 8) & 0xFF)]! ^ CRC[2304 + ((b >>> 16) & 0xFF)]! ^ CRC[2048 + (b >>> 24)]! ^
      CRC[1792 + (d & 0xFF)]! ^ CRC[1536 + ((d >>> 8) & 0xFF)]! ^ CRC[1280 + ((d >>> 16) & 0xFF)]! ^ CRC[1024 + (d >>> 24)]! ^
      CRC[768 + (e & 0xFF)]! ^ CRC[512 + ((e >>> 8) & 0xFF)]! ^ CRC[256 + ((e >>> 16) & 0xFF)]! ^ CRC[e >>> 24]!
  }
  return c
}

/** CRC-32 of the data, as gzip and zip use it. To continue over more data, pass the previous result as `crc`. */
export function crc32(data: Uint8Array, crc = 0): number {
  bytes(data)
  // The word offsets below are int32 math, so a longer input goes through in 1 GiB pieces
  if (data.length > 1 << 30) {
    for (let i = 0; i < data.length; i += 1 << 30) crc = crc32(data.subarray(i, i + (1 << 30)), crc)
    return crc
  }
  let c = ~crc
  // Read once and passed on: a view of a growable SharedArrayBuffer can grow while this runs
  const n = data.length
  let i = Math.min(n, (4 - (data.byteOffset & 3)) & 3)
  for (let k = 0; k < i; k++) c = CRC[(c ^ data[k]!) & 0xFF]! ^ (c >>> 8)
  const len = n - i
  const u = LITTLE && len >= 16 ? new Int32Array(data.buffer, data.byteOffset + i, len >> 2) : NO_WORDS
  if (len >= CHORBA_MIN && LITTLE) return chorba(data, u, i, c, n)
  const w = u.length >> 2 << 2
  c = words16(u, w, c)
  for (i += w << 2; i < n; i++) c = CRC[(c ^ data[i]!) & 0xFF]! ^ (c >>> 8)
  return ~c >>> 0
}

// Chorba (arXiv 2412.16398, as zlib-ng's 32-bit "small" variant): folds 40 bytes a step with shifts and XORs
// against a sparse multiple of the polynomial, with no table lookups. The last 41–80 bytes take the carries and
// go through slicing-by-16 from a zero register, since the running CRC was folded into the first word.
function chorba(data: Uint8Array, input: Int32Array, start: number, c: number, n: number): number {
  const len = n - start
  let next1 = c, next2 = 0, next3 = 0, next4 = 0, next5 = 0, next6 = 0, next7 = 0, next8 = 0, next9 = 0, next10 = 0
  let j = 0
  for (; j + 80 < len; j += 40) {
    const w = j >> 2
    const in1 = input[w]! ^ next1, in2 = input[w + 1]! ^ next2, in3 = input[w + 2]! ^ next3, in4 = input[w + 3]! ^ next4
    const a1 = in1 << 17, a2 = (in1 >>> 15) ^ (in1 << 23), a3 = (in1 >>> 9) ^ (in1 << 19), a4 = in1 >>> 13, a6 = in1 << 12, a7 = in1 >>> 20
    const b1 = in2 << 17, b2 = (in2 >>> 15) ^ (in2 << 23), b3 = (in2 >>> 9) ^ (in2 << 19), b4 = in2 >>> 13, b6 = in2 << 12, b7 = in2 >>> 20
    const c1 = in3 << 17, c2 = (in3 >>> 15) ^ (in3 << 23), c3 = (in3 >>> 9) ^ (in3 << 19), c4 = in3 >>> 13, c6 = in3 << 12, c7 = in3 >>> 20
    const d1 = in4 << 17, d2 = (in4 >>> 15) ^ (in4 << 23), d3 = (in4 >>> 9) ^ (in4 << 19), d4 = in4 >>> 13, d6 = in4 << 12, d7 = in4 >>> 20
    const in5 = input[w + 4]! ^ next5 ^ a1, in6 = input[w + 5]! ^ next6 ^ a2 ^ b1
    const in7 = input[w + 6]! ^ next7 ^ a3 ^ b2 ^ c1, in8 = input[w + 7]! ^ next8 ^ a4 ^ b3 ^ c2 ^ d1
    const e1 = in5 << 17, e2 = (in5 >>> 15) ^ (in5 << 23), e3 = (in5 >>> 9) ^ (in5 << 19), e4 = in5 >>> 13, e6 = in5 << 12, e7 = in5 >>> 20
    const f1 = in6 << 17, f2 = (in6 >>> 15) ^ (in6 << 23), f3 = (in6 >>> 9) ^ (in6 << 19), f4 = in6 >>> 13, f6 = in6 << 12, f7 = in6 >>> 20
    const g1 = in7 << 17, g2 = (in7 >>> 15) ^ (in7 << 23), g3 = (in7 >>> 9) ^ (in7 << 19), g4 = in7 >>> 13, g6 = in7 << 12, g7 = in7 >>> 20
    const h1 = in8 << 17, h2 = (in8 >>> 15) ^ (in8 << 23), h3 = (in8 >>> 9) ^ (in8 << 19), h4 = in8 >>> 13, h6 = in8 << 12, h7 = in8 >>> 20
    const in9 = input[w + 8]! ^ next9 ^ b4 ^ c3 ^ d2 ^ e1
    const in10 = input[w + 9]! ^ next10 ^ a6 ^ c4 ^ d3 ^ e2 ^ f1
    const i1 = in9 << 17, i2 = (in9 >>> 15) ^ (in9 << 23), i3 = (in9 >>> 9) ^ (in9 << 19), i4 = in9 >>> 13, i6 = in9 << 12, i7 = in9 >>> 20
    const j1 = in10 << 17, j2 = (in10 >>> 15) ^ (in10 << 23), j3 = (in10 >>> 9) ^ (in10 << 19), j4 = in10 >>> 13, j6 = in10 << 12, j7 = in10 >>> 20
    next1 = a7 ^ b6 ^ d4 ^ e3 ^ f2 ^ g1
    next2 = b7 ^ c6 ^ e4 ^ f3 ^ g2 ^ h1
    next3 = c7 ^ d6 ^ f4 ^ g3 ^ h2 ^ i1
    next4 = d7 ^ e6 ^ g4 ^ h3 ^ i2 ^ j1
    next5 = e7 ^ f6 ^ h4 ^ i3 ^ j2
    next6 = f7 ^ g6 ^ i4 ^ j3
    next7 = g7 ^ h6 ^ j4
    next8 = h7 ^ i6
    next9 = i7 ^ j6
    next10 = j7
  }
  const rest = len - j
  TAIL8.set(data.subarray(start + j, n))
  TAIL[0] = TAIL[0]! ^ next1; TAIL[1] = TAIL[1]! ^ next2; TAIL[2] = TAIL[2]! ^ next3; TAIL[3] = TAIL[3]! ^ next4
  TAIL[4] = TAIL[4]! ^ next5; TAIL[5] = TAIL[5]! ^ next6; TAIL[6] = TAIL[6]! ^ next7; TAIL[7] = TAIL[7]! ^ next8
  TAIL[8] = TAIL[8]! ^ next9; TAIL[9] = TAIL[9]! ^ next10
  const w = rest >> 4 << 2
  let r = words16(TAIL, w, 0)
  for (let k = w << 2; k < rest; k++) r = CRC[(r ^ TAIL8[k]!) & 0xFF]! ^ (r >>> 8)
  return ~r >>> 0
}

// 65521 is the largest prime below 2^16. Reducing every 3800 bytes keeps b under 2^31, so V8 keeps both sums
// as int32; past that they become doubles. Unrolled, this beat weighted 16-byte sums by 1.45x (measured).
/** Adler-32 of the data, as zlib uses it. To continue over more data, pass the previous result as `adler`. */
export function adler32(data: Uint8Array, adler = 1): number {
  bytes(data)
  let a = adler & 0xFFFF, b = adler >>> 16
  let i = 0
  const n = data.length
  while (i < n) {
    const end = Math.min(i + 3800, n)
    const blocks = end - ((end - i) & 15)
    for (; i < blocks; i += 16) {
      a = (a + data[i]!) | 0; b = (b + a) | 0; a = (a + data[i + 1]!) | 0; b = (b + a) | 0
      a = (a + data[i + 2]!) | 0; b = (b + a) | 0; a = (a + data[i + 3]!) | 0; b = (b + a) | 0
      a = (a + data[i + 4]!) | 0; b = (b + a) | 0; a = (a + data[i + 5]!) | 0; b = (b + a) | 0
      a = (a + data[i + 6]!) | 0; b = (b + a) | 0; a = (a + data[i + 7]!) | 0; b = (b + a) | 0
      a = (a + data[i + 8]!) | 0; b = (b + a) | 0; a = (a + data[i + 9]!) | 0; b = (b + a) | 0
      a = (a + data[i + 10]!) | 0; b = (b + a) | 0; a = (a + data[i + 11]!) | 0; b = (b + a) | 0
      a = (a + data[i + 12]!) | 0; b = (b + a) | 0; a = (a + data[i + 13]!) | 0; b = (b + a) | 0
      a = (a + data[i + 14]!) | 0; b = (b + a) | 0; a = (a + data[i + 15]!) | 0; b = (b + a) | 0
    }
    for (; i < end; i++) { a = (a + data[i]!) | 0; b = (b + a) | 0 }
    a %= 65521
    b %= 65521
  }
  return ((b << 16) | a) >>> 0
}
