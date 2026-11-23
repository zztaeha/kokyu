// Test helpers with no Node dependency, shared by helpers.ts and portable.ts, which also runs in browsers

import type { UnzipChunk } from '../engine/index.js'

// Seeded so a failure reproduces exactly. The seed is hashed first (murmur3's finalizer), since xorshift from a
// small state starts with small numbers.
export function rng(seed: number): () => number {
  let s = seed >>> 0
  s = Math.imul(s ^ (s >>> 16), 0x85EBCA6B)
  s = Math.imul(s ^ (s >>> 13), 0xC2B2AE35)
  s = (s ^ (s >>> 16)) >>> 0 || 1
  return () => {
    s ^= s << 13; s ^= s >>> 17; s ^= s << 5
    return (s >>> 0) / 0x100000000
  }
}

export function randomBytes(n: number, seed = 1): Uint8Array {
  const r = rng(seed)
  const out = new Uint8Array(n)
  for (let i = 0; i < n; i++) out[i] = (r() * 256) | 0
  return out
}

export function cat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let o = 0
  for (const p of parts) { out.set(p, o); o += p.length }
  return out
}

// Node's Buffer.compare where there is one: on megabytes it's far faster than comparing byte by byte
const buffer = (globalThis as { Buffer?: { compare(a: Uint8Array, b: Uint8Array): number } }).Buffer
export const equal = (a: Uint8Array, b: Uint8Array): boolean =>
  a.length === b.length && (buffer ? buffer.compare(a, b) === 0 : a.every((v, i) => v === b[i]))

// A stream fed `data` in pieces of `size` bytes, then finished, with every result in order
export function pieces<T>(s: { push(c: Uint8Array): T; finish(): T }, data: Uint8Array, size: number): T[] {
  const out: T[] = []
  for (let o = 0; o < data.length; o += size) out.push(s.push(data.subarray(o, o + size)))
  out.push(s.finish())
  return out
}

// Each entry's bytes from an unzip stream's chunks, for the entries whose last chunk arrived
export function joinChunks(chunks: UnzipChunk[]): Record<string, Uint8Array> {
  const parts = new Map<string, Uint8Array[]>(), got = new Map<string, Uint8Array>()
  for (const c of chunks) {
    let p = parts.get(c.entry.name)
    if (!p) parts.set(c.entry.name, p = [])
    p.push(c.data)
    if (c.final) got.set(c.entry.name, cat(p))
  }
  return Object.fromEntries(got)
}

// The same entries with the same bytes, as a yes or no
export const sameEntries = (got: Record<string, Uint8Array>, want: Record<string, Uint8Array>): boolean =>
  Object.keys(got).length === Object.keys(want).length && Object.keys(want).every(n => got[n] && equal(got[n], want[n]!))

// A web stream of `data` in pieces of `size` bytes
export const readable = (data: Uint8Array, size: number): ReadableStream<Uint8Array> => new ReadableStream({
  start(c) { for (let o = 0; o < data.length; o += size) c.enqueue(data.subarray(o, o + size)); c.close() },
})
