// Runs inside a pool worker: one-shot calls, and the streams opened by the async stream API.

import { deflate, inflate, zlib, unzlib, gzip, gunzip, decompress } from './formats.js'
import { zipList, unzip } from './zip.js'
import { session } from './streams.js'
import { FINISHED, KokyuError } from './shared.js'
import type { Message, Reply } from './pool.js'

type Fn = (...args: never[]) => unknown

const calls: Record<string, Fn> = {
  deflate, inflate, zlib, unzlib, gzip, gunzip, decompress, zip: zipList,
  // Entries are kept by their place in the central directory: names can repeat
  unzipSome: (data: Uint8Array, kept: number[] | null, maxSize: number | undefined) => {
    const keep = kept && new Set(kept)
    let i = 0
    return unzip(data, { filter: keep ? () => keep.has(i++) : undefined, maxSize })
  },
}

const sessions = new Map<number, (method: string, args: unknown[]) => unknown>()

function handle(m: Message): Reply | null {
  if (m.op === 'close') { sessions.delete(m.sid); return null }
  if (m.op === 'open') {
    sessions.set(m.sid, session(m.kind, m.opts))
    return null
  }
  try {
    let value: unknown
    if (m.op === 'call') {
      value = (calls[m.fn] as (...args: unknown[]) => unknown)(...m.args)
    } else {
      const run = sessions.get(m.sid)
      if (!run) throw new KokyuError('invalid-state', FINISHED)
      value = run(m.method, m.args)
      // Streams copy whatever they keep between calls and return arrays of their own, so a chunk is garbage once its
      // call returns. Detaching it frees it now rather than whenever the GC next runs.
      for (const a of m.args) if (a instanceof Uint8Array && a.buffer instanceof ArrayBuffer) (a.buffer as { transfer?(n: number): ArrayBuffer }).transfer?.(0)
    }
    return { id: m.id, ok: true, value }
  } catch (e) {
    return { id: m.id, ok: false, code: e instanceof KokyuError ? e.code : undefined, message: e instanceof Error ? e.message : String(e) }
  }
}

// Every array a call returns is fresh and unreferenced afterwards, so all of them move instead of copying.
// Empty ones stay behind: there's nothing to gain from moving them.
function transferables(value: unknown): ArrayBuffer[] {
  const found = new Set<ArrayBuffer>()
  const walk = (v: unknown): void => {
    if (v instanceof Uint8Array) {
      if (v.byteLength && v.buffer instanceof ArrayBuffer) found.add(v.buffer)
    } else if (v && typeof v === 'object') {
      for (const x of Object.values(v)) walk(x)
    }
  }
  walk(value)
  return [...found]
}

const answer = (m: Message, post: (r: Reply, t: ArrayBuffer[]) => void): void => {
  const r = handle(m)
  if (r) post(r, r.ok ? transferables(r.value) : [])
}

const scope = globalThis as unknown as {
  postMessage?: (message: unknown, transfer: Transferable[]) => void
  onmessage: ((e: MessageEvent<Message>) => void) | null
}

if (typeof scope.postMessage === 'function') {
  scope.onmessage = e => answer(e.data, (r, t) => scope.postMessage!(r, t))
} else {
  const spec: string = 'node:worker_threads'
  // No top-level await: Vite bundles workers in a format that can't hold one.
  import(spec).then(({ parentPort }: { parentPort: { on(ev: 'message', fn: (m: Message) => void): void; postMessage(m: unknown, t: ArrayBuffer[]): void } }) =>
    parentPort.on('message', m => answer(m, (r, t) => parentPort.postMessage(r, t))))
}
