// The streams behind the async stream API, by name: opened in a pool worker, or on the calling thread
// when a whole stream is small enough to finish inline.

import {
  deflateStream, inflateStream, zlibStream, unzlibStream, gzipStream, gunzipStream, decompressStream,
} from './formats.js'
import { ZipWriter, UnzipReader } from './zip.js'

// A stream opened now: each call runs one of its methods or, when opening it failed, rethrows why. `local` builds
// it instead, for a stream on this thread that differs from what a worker builds from opts.
export function session(kind: string, opts: unknown, local?: (() => object) | null): (method: string, args: unknown[]) => unknown {
  let s: Record<string, (...args: unknown[]) => unknown> | undefined, failure: unknown
  try {
    s = (local ? local() : streams[kind]!(opts as never)) as typeof s
  } catch (e) {
    failure = e
  }
  return (method, args) => { if (!s) throw failure; return s[method]!.apply(s, args) }
}

const streams: Record<string, (opts: never) => object> = {
  deflate: deflateStream, inflate: inflateStream, zlib: zlibStream, unzlib: unzlibStream, gzip: gzipStream,
  gunzip: gunzipStream, decompress: decompressStream,
  zip: opts => new ZipWriter(opts),
  unzip: (opts: { ask: boolean; maxSize: number | undefined }) => new UnzipReader({ maxSize: opts.maxSize }, opts.ask),
}
