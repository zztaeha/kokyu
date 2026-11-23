// kokyu: deflate, zlib, gzip and zip for every JavaScript runtime, with zero dependencies.
// Every format comes four ways: name() sync, nameAsync() off the main thread, nameStream() and nameStreamAsync().

export { KokyuError, type KokyuErrorCode, type Level, type Stream } from './shared.js'
export { crc32, adler32 } from './checksum.js'
export {
  deflate, inflate, zlib, unzlib, gzip, gunzip, decompress, inflateInto, unzlibInto, gunzipInto, decompressInto,
  deflateStream, inflateStream, zlibStream, unzlibStream, gzipStream, gunzipStream, decompressStream,
  type DeflateOptions, type InflateOptions, type GzipOptions, type GunzipOptions, type CompressStream,
} from './formats.js'
export {
  zip, unzip, zipEntries, zipStream, unzipStream,
  type ZipInput, type ZipOptions, type ZipEntryOptions, type ZipEntryInfo, type ZipEntry, type UnzipOptions,
  type UnzipStreamOptions, type UnzipChunk, type ZipStream, type UnzipStream,
} from './zip.js'
export { encodeText, decodeText, encodeTextStream, decodeTextStream, type TextOptions } from './text.js'
export {
  deflateAsync, inflateAsync, zlibAsync, unzlibAsync, gzipAsync, gunzipAsync, decompressAsync, zipAsync, unzipAsync,
  deflateStreamAsync, inflateStreamAsync, zlibStreamAsync, unzlibStreamAsync, gzipStreamAsync, gunzipStreamAsync,
  decompressStreamAsync, zipStreamAsync, unzipStreamAsync,
  type AsyncOptions, type AsyncStream, type AsyncCompressStream, type AsyncZipStream, type AsyncUnzipStream,
} from './pool.js'

import { ENTRY } from './zip.js'

type Element<O> = O extends readonly (infer E)[] ? E : O

/**
 * Wraps any kokyu stream, sync or async, as a web TransformStream. A pipe that fails or is canceled finishes the
 * stream, except a zip entry's, which stays open so its archive can go on; if a cancel may have lost any of the
 * entry's bytes, the archive refuses to finish. For a zip entry it's a readable and writable pair, which sees every
 * cancel, rather than a TransformStream instance. Bytes once read are out of its sight: an aborted `pipeTo` can drop
 * some its destination hadn't written yet, so don't finish an archive whose output pipe was aborted.
 * @example const gz = file.stream().pipeThrough(toTransformStream(gzipStream()))
 */
export function toTransformStream<In, Out>(stream: {
  push(chunk: In): Out | Promise<Out>
  finish(): Out | Promise<Out>
}): TransformStream<In, Element<Out>> {
  let canceled = false, unread = 0
  // A zip entry's hook, called when a cancel drops bytes it returned: its archive counts them, so it can't be finished
  const lose = (stream as { [ENTRY]?: () => void })[ENTRY]
  const emit = (out: Out, controller: TransformStreamDefaultController<Element<Out>>): void => {
    if (canceled) { if ((out as { length: number }).length) lose?.(); return }
    if (Array.isArray(out)) for (const piece of out) controller.enqueue(piece as Element<Out>)
    else if ((out as { length: number }).length) { controller.enqueue(out as Element<Out>); unread++ }
  }
  // A pipe that fails or is canceled midway still finishes the stream, since a refused chunk leaves it open, and an
  // async stream left open keeps its worker session, and in Deno the process alive. A zip entry stays open: finishing
  // it would write a cut-off entry, its last bytes lost, into the archive the caller may still finish.
  const end = (): void => { if (!lose) Promise.resolve().then(() => stream.finish()).catch(() => {}) }
  const transformer: Transformer<In, Element<Out>> & { cancel(): void } = {
    async transform(chunk, controller) {
      try {
        emit(await stream.push(chunk), controller)
      } catch (e) {
        end()
        throw e
      }
    },
    async flush(controller) { emit(await stream.finish(), controller) },
    // Nothing is read after a cancel, so a push under way emits nothing
    cancel() {
      canceled = true
      end()
    },
  }
  const ts = new TransformStream(transformer)
  if (!lose) return ts
  // flush runs whatever the demand, so an entry's last bytes can wait unread, and a cancel during or after flush never
  // reaches the transformer. A readable of its own sees every cancel, and whether anything returned was never read.
  const reader = ts.readable.getReader()
  // A chunk whose read was released before it came is kept here while nobody holds the readable, for the next pull. If
  // a new reader holds it by then, the chunk waits in this readable's queue (`held`), where no read of it can be seen.
  let held = false, kept: { value: Element<Out> } | null = null
  const readable: ReadableStream<Element<Out>> = new ReadableStream<Element<Out>>({
    async pull(c) {
      held = false
      if (kept) { c.enqueue(kept.value); kept = null; return }
      const r = await reader.read()
      if (r.done) { if (!canceled) c.close(); return }
      unread--
      if (!readable.locked) kept = r
      else { c.enqueue(r.value); held = c.desiredSize! < 0 }
    },
    cancel(reason) {
      canceled = true
      if (unread || held || kept) lose()
      return reader.cancel(reason)
    },
  }, { highWaterMark: 0 })
  return { readable, writable: ts.writable }
}
