// The async API. Small inputs run inline; larger ones go to a pool of workers that start on
// first use, are reused across calls, and never keep a Node process alive while idle.

import {
  deflate, inflate, zlib, unzlib, gzip, gunzip, decompress, type DeflateOptions, type InflateOptions, type GzipOptions,
  type GunzipOptions,
} from './formats.js'
import {
  UnzipReader, unzip, zipEntries, zipFiles, zipList, ENTRY, ENTRY_DONE, ENTRY_OPEN, ENTRY_OPEN_AT_END, LOST, type ZipInput, type ZipOptions,
  type ZipEntryOptions, type ZipEntryInfo, type UnzipOptions, type UnzipStreamOptions, type UnzipChunk, type AskChunk,
} from './zip.js'
import { session } from './streams.js'
import { BUSY, FINISHED, KokyuError, bytes, checkMaxSize, copy, isBytes, unreadable, type KokyuErrorCode } from './shared.js'

/**
 * Options every async call takes. One-shot inputs under 16 KB run on this thread, larger ones in a worker pool.
 * In Node a worker keeps the time zone it started with, so set `TZ` before the first async call: zip times use it.
 */
export interface AsyncOptions {
  /** Cancels the call or stream: each of its promises not yet settled rejects with the signal's reason */
  signal?: AbortSignal | undefined
  /**
   * Lets a worker take the input's buffer instead of copying it, which empties the caller's array. Inputs small
   * enough to run on this thread, and views of a larger buffer, are never moved.
   */
  transfer?: boolean | undefined
}

/** An async stream: like Stream, but each call returns a promise. Calls can be made without awaiting; they run in order. */
export interface AsyncStream<In = Uint8Array, Out = Uint8Array<ArrayBuffer>> {
  /** Adds a chunk and resolves to the output it completes, possibly empty */
  push(chunk: In): Promise<Out>
  /** Adds an optional last chunk and resolves to the rest of the output. Using the stream after this fails. */
  finish(chunk?: In): Promise<Out>
}

/** An async compression stream, which can also flush */
export interface AsyncCompressStream extends AsyncStream {
  /** Resolves to everything pushed so far as output a decoder can read in full, without ending the stream */
  flush(): Promise<Uint8Array<ArrayBuffer>>
}

/**
 * A zip archive written as an async stream, one entry at a time. Its calls and its entries' settle in call order, so
 * each result can be written as it comes.
 */
export interface AsyncZipStream {
  /**
   * Starts an entry and returns its stream: push the entry's data, then finish it before the next file() or finish().
   * The name follows the same rules as a {@link ZipInput} path.
   */
  file(name: string, opts?: ZipEntryOptions): AsyncStream
  /** Ends the archive and resolves to its last bytes */
  finish(): Promise<Uint8Array<ArrayBuffer>>
}

/** A zip read as an async stream: push the archive's bytes and get back chunks of its entries */
export type AsyncUnzipStream = AsyncStream<Uint8Array, UnzipChunk[]>

export type Message =
  | { op: 'call'; id: number; fn: string; args: unknown[] }
  | { op: 'open'; sid: number; kind: string; opts: unknown }
  | { op: 'method'; id: number; sid: number; method: string; args: unknown[] }
  | { op: 'close'; sid: number }

export type Reply =
  | { id: number; ok: true; value: unknown }
  | { id: number; ok: false; code: KokyuErrorCode | undefined; message: string }

// Inputs under 16 KB run inline, where a worker round trip (about what deflating 1 KB of text costs) would be a real
// share of compressing them. Decoding takes as long as its output, which input size doesn't bound.
const INLINE_LIMIT = 16 << 10

const hardware = (globalThis as { navigator?: { hardwareConcurrency?: number } }).navigator?.hardwareConcurrency ?? 2
const MAX_WORKERS = Math.max(1, Math.min(4, hardware - 1))

interface Port {
  post(message: unknown, transfer: Transferable[]): void
  terminate(): void
  ref(): void
  unref(): void
  // The worker can't be unref'd and would keep the process running (Deno), so it's retired when idle
  pinsProcess: boolean
}

const IDLE_RETIRE_MS = 500

interface NodeWorker {
  on(event: string, fn: (value: unknown) => void): void
  postMessage(message: unknown, transfer: readonly Transferable[]): void
  terminate(): Promise<number>
  ref(): void
  unref(): void
}

// Workers inherit the parent's Node flags, except under an inline -e/-p script, whose --input-type would stop the
// worker file from loading
function workerOptions(): { execArgv?: string[] } {
  const parent = (globalThis as { process?: { execArgv?: string[] } }).process?.execArgv ?? []
  return parent.some(a => /^(-e|-p|--eval|--print|--input-type)\b/.test(a)) ? { execArgv: [] } : {}
}

async function spawn(onReply: (r: Reply) => void, onCrash: (e: unknown) => void): Promise<Port> {
  // Browsers, Deno and Bun: a module worker, written the way bundlers recognize and bundle
  if (typeof Worker !== 'undefined') {
    const w = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' })
    w.onmessage = e => onReply(e.data as Reply)
    w.onerror = e => { e.preventDefault(); onCrash(new KokyuError('worker', e.message || 'worker failed to start')) }
    // Bun's workers can be unref'd like Node's; Deno's can't
    const h = w as Worker & { ref?(): void; unref?(): void }
    h.unref?.()
    return {
      post: (m, t) => w.postMessage(m, t), terminate: () => w.terminate(), ref: () => h.ref?.(), unref: () => h.unref?.(),
      pinsProcess: !h.unref && 'Deno' in globalThis,
    }
  }
  // Node. The specifier is a variable so browser bundles never try to resolve it.
  const spec: string = 'node:worker_threads'
  const threads = await import(spec) as { Worker: new (url: URL, opts: { execArgv?: string[] }) => NodeWorker }
  const w = new threads.Worker(new URL('./worker.js', import.meta.url), workerOptions())
  w.on('message', m => onReply(m as Reply))
  w.on('error', onCrash)
  w.on('exit', () => onCrash(new KokyuError('worker', 'worker exited')))
  w.unref()
  return { post: (m, t) => w.postMessage(m, t), terminate: () => { void w.terminate() }, ref: () => w.ref(), unref: () => w.unref(), pinsProcess: false }
}

interface Slot {
  port: Promise<Port>
  // Running a one-shot job; each worker takes one at a time so an abort can stop it cleanly
  busy: boolean
  sessions: number
  inflight: number
  dead: boolean
  idle: ReturnType<typeof setTimeout> | undefined
}

interface Pending {
  slot: Slot
  // Null for a one-shot job
  session: Session | null
  resolve(value: unknown): void
  reject(error: unknown): void
}

interface Job {
  fn: string
  args: unknown[]
  transfer: Transferable[]
  slot: Slot | null
  resolve(value: unknown): void
  reject(error: unknown): void
}

class Pool {
  private readonly slots: Slot[] = []
  private readonly queue: Job[] = []
  private readonly pending = new Map<number, Pending>()
  // Streams whose open couldn't be posted, and why
  private readonly unsent = new Map<number, string>()
  private nextId = 1

  call(fn: string, args: unknown[], transfer: Transferable[], signal: AbortSignal | undefined): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const job: Job = { fn, args, transfer, slot: null, resolve, reject }
      if (signal) {
        // A getter or filter run while making the call can abort it, and then the event has already fired
        if (signal.aborted) return reject(signal.reason)
        const onAbort = (): void => this.abort(job, signal.reason)
        signal.addEventListener('abort', onAbort, { once: true })
        const done = (): void => signal.removeEventListener('abort', onAbort)
        job.resolve = v => { done(); resolve(v) }
        job.reject = e => { done(); reject(e) }
      }
      this.queue.push(job)
      this.pump()
    })
  }

  // A stream's state lives in one worker; its messages are pipelined to it in order
  open(kind: string, opts: unknown): Session {
    let slot = this.slots.reduce<Slot | undefined>((best, s) => !best || s.sessions < best.sessions ? s : best, undefined)
    if (!slot || (slot.sessions > 0 && this.slots.length < MAX_WORKERS)) slot = this.spawnSlot()
    slot.sessions++
    const sid = this.nextId++
    this.post(slot, { op: 'open', sid, kind, opts }, [])
    return new Session(this, slot, sid)
  }

  request(session: Session, method: string, args: unknown[], transfer: Transferable[]): Promise<unknown> {
    const { slot, sid } = session
    if (slot.dead) return Promise.reject(new KokyuError('worker', 'the worker running this stream stopped'))
    return new Promise((resolve, reject) => {
      const id = this.track(slot, resolve, reject, session)
      this.post(slot, { op: 'method', id, sid, method, args }, transfer)
    })
  }

  // With a reason, the session's calls still pending reject with it
  closeSession(session: Session, reason: unknown): void {
    const { slot, sid } = session
    if (reason !== undefined) for (const id of session.calls) this.pending.get(id)?.reject(reason)
    slot.sessions--
    // A dead worker gets no close message, the one place an unsent open is otherwise forgotten
    if (slot.dead) this.unsent.delete(sid)
    else this.post(slot, { op: 'close', sid }, [])
    this.rest(slot)
  }

  // With nothing in flight a worker stops holding the process open. One that can't be unref'd is
  // retired instead, unless an open stream's state lives in it.
  private rest(slot: Slot): void {
    if (slot.inflight || slot.dead) return
    void slot.port.then(port => {
      if (!port.pinsProcess) return port.unref()
      if (slot.sessions) return
      clearTimeout(slot.idle)
      slot.idle = setTimeout(() => { if (!slot.inflight && !slot.sessions) this.retire(slot, null) }, IDLE_RETIRE_MS)
    }, () => {})
  }

  private pump(): void {
    while (this.queue.length) {
      let slot = this.slots.find(s => !s.busy)
      if (!slot && this.slots.length < MAX_WORKERS) slot = this.spawnSlot()
      if (!slot) return
      const job = this.queue.shift()!
      slot.busy = true
      job.slot = slot
      const id = this.track(slot, job.resolve, job.reject)
      this.post(slot, { op: 'call', id, fn: job.fn, args: job.args }, job.transfer)
    }
  }

  private track(slot: Slot, resolve: (v: unknown) => void, reject: (e: unknown) => void, session: Session | null = null): number {
    const id = this.nextId++
    this.pending.set(id, { slot, session, resolve, reject })
    session?.calls.add(id)
    if (slot.inflight++ === 0) {
      clearTimeout(slot.idle)
      void slot.port.then(p => p.ref(), () => {})
    }
    return id
  }

  // A message that can't be posted fails its call rather than leaving it pending forever. When a stream's open
  // couldn't be sent, each of its calls fails with that reason.
  private post(slot: Slot, message: Message, transfer: Transferable[]): void {
    void slot.port.then(p => {
      let why = message.op === 'method' ? this.unsent.get(message.sid) : undefined
      if (message.op === 'close') this.unsent.delete(message.sid)
      if (!why) {
        try {
          p.post(message, transfer)
          return
        } catch (e) {
          // The detail comes from the caller's value, which may not even turn into text: the call is answered anyway
          why = 'kokyu: this call can\'t be sent to a worker'
          try { why += ` (${e instanceof Error ? e.message : String(e)})` } catch {}
        }
      }
      if (message.op === 'open') this.unsent.set(message.sid, why)
      if ('id' in message) this.reply(slot, { id: message.id, ok: false, code: 'invalid-option', message: why })
    }, () => {})
  }

  private spawnSlot(): Slot {
    const slot: Slot = { port: null!, busy: false, sessions: 0, inflight: 0, dead: false, idle: undefined }
    slot.port = spawn(r => this.reply(slot, r), e => this.crash(slot, e))
    slot.port.catch(e => this.crash(slot, e))
    this.slots.push(slot)
    return slot
  }

  private reply(slot: Slot, r: Reply): void {
    const p = this.pending.get(r.id)
    if (!p) return
    this.pending.delete(r.id)
    p.session?.calls.delete(r.id)
    slot.inflight--
    this.rest(slot)
    if (r.ok) p.resolve(r.value)
    else p.reject(r.code ? Object.assign(new KokyuError(r.code, ''), { message: r.message }) : new Error(r.message))
    if (!p.session) {
      slot.busy = false
      this.pump()
    }
  }

  private abort(job: Job, reason: unknown): void {
    job.reject(reason)
    if (!job.slot) this.queue.splice(this.queue.indexOf(job), 1)
    // Nothing else lives in this worker, so stop the work outright; otherwise let it finish unseen. A closed stream's
    // calls may still be in flight there, and would reject with this job's reason.
    else if (job.slot.sessions === 0 && job.slot.inflight === 1) this.retire(job.slot, reason)
  }

  // However a worker fails to start or dies, its calls reject with a KokyuError coded 'worker', in every runtime
  private crash(slot: Slot, error: unknown): void {
    if (slot.dead) return
    const why = error instanceof Error ? error.message : String(error)
    this.retire(slot, error instanceof KokyuError ? error : new KokyuError('worker', `the worker failed: ${why}`))
  }

  private retire(slot: Slot, reason: unknown): void {
    slot.dead = true
    clearTimeout(slot.idle)
    this.slots.splice(this.slots.indexOf(slot), 1)
    void slot.port.then(p => p.terminate(), () => {})
    for (const [id, p] of this.pending) {
      if (p.slot !== slot) continue
      this.pending.delete(id)
      p.reject(reason)
    }
    // Not now: an abort retires from inside its signal's dispatch, and the other jobs that signal cancels are still
    // queued, so pumping now would start a worker for each of them only to stop it
    queueMicrotask(() => this.pump())
  }
}

// A call refused before anything is sent: its stream or entry has finished, or it came out of order
const refuse = (why: string): Promise<never> => Promise.reject(new KokyuError('invalid-state', why))

class Session {
  private closed = false
  // Its calls in flight, so closing it with a reason touches only them, not every pending call in the pool
  readonly calls = new Set<number>()
  private readonly pool: Pool
  readonly slot: Slot
  readonly sid: number

  constructor(pool: Pool, slot: Slot, sid: number) {
    this.pool = pool
    this.slot = slot
    this.sid = sid
  }

  call(method: string, args: unknown[], transfer: Transferable[]): Promise<unknown> {
    if (this.closed) return refuse(FINISHED)
    return this.pool.request(this, method, args, transfer)
  }

  close(reason?: unknown): void {
    if (this.closed) return
    this.closed = true
    this.pool.closeSession(this, reason)
  }
}

let pool: Pool | null = null
const getPool = (): Pool => pool ??= new Pool()

interface Queued { method: string; args: unknown[]; transfer: Transferable[]; resolve(v: unknown): void; reject(e: unknown): void }

// A stream opened, fed and finished within one tick and under INLINE_LIMIT runs on this thread, as
// small one-shot calls do. Anything else opens a worker session with its queued calls replayed in order.
class LazySession {
  private queue: Queued[] | null = []
  private size = 0
  private session: Session | null = null
  private readonly kind: string
  // Needed only until the stream runs here or in a worker: a held stream would keep its dictionary copy for good
  private opts: unknown

  constructor(kind: string, opts: unknown) {
    this.kind = kind
    this.opts = opts
  }

  // `local` comes with each call rather than being kept here: the registry's entry holds this, and a filter in `local`
  // can lead back to the stream, which would then never be collected
  call(method: string, args: unknown[], transfer: Transferable[], local?: () => object): Promise<unknown> {
    if (this.session) return this.session.call(method, args, transfer)
    if (!this.queue) return refuse(FINISHED)
    if (args[0] instanceof Uint8Array) this.size += args[0].length
    // Decided from the first call, so a stream that's never used never opens a worker session
    if (!this.queue.length) queueMicrotask(() => this.decide(local))
    return new Promise((resolve, reject) => {
      this.queue!.push({ method, args, transfer, resolve, reject })
      if (this.size >= INLINE_LIMIT) this.decide(local)
    })
  }

  close(reason?: unknown): void {
    if (this.session) return this.session.close(reason)
    for (const c of this.queue ?? []) c.reject(reason)
    this.queue = this.opts = null
  }

  private decide(local: (() => object) | undefined): void {
    const queue = this.queue, opts = this.opts
    if (!queue) return
    this.queue = this.opts = null
    if (this.size >= INLINE_LIMIT || !queue.some(c => c.method === 'finish')) {
      this.session = getPool().open(this.kind, opts)
      for (const c of queue) this.session.call(c.method, c.args, c.transfer).then(c.resolve, c.reject)
      return
    }
    const run = session(this.kind, opts, local)
    for (const c of queue) {
      try {
        c.resolve(run(c.method, c.args))
      } catch (e) {
        c.reject(e)
      }
    }
  }
}

// Only a view covering its whole buffer can be moved; moving a slice would take its neighbors too. An ArrayBuffer from
// any realm can move, shared memory can't.
const movable = (b: Uint8Array): Transferable[] =>
  b.byteOffset === 0 && b.byteLength === b.buffer.byteLength && b.byteLength > 0
    && Object.prototype.toString.call(b.buffer) === '[object ArrayBuffer]' ? [b.buffer as ArrayBuffer] : []

// The caller's buffer moves to one kokyu owns without a copy, emptying the caller's array; one that can't move
// (WebAssembly memory, say) is copied. Only what movable() passed gets here, so its buffer is an ArrayBuffer.
const move = (b: Uint8Array): Uint8Array<ArrayBuffer> => {
  try { return new Uint8Array(structuredClone(b.buffer as ArrayBuffer, { transfer: [b.buffer] })) } catch { return copy(bytes(b)) }
}

// What a worker gets, taken at the call since posting comes later: the buffer if it may move whole, else a copy of the
// view (a posted one clones all of it). Checked again here and in move(): an option getter run since can detach it.
const own = (b: Uint8Array, transfer: boolean | undefined): Uint8Array<ArrayBuffer> => transfer && movable(b).length ? move(b) : copy(bytes(b))

// Only options the engine reads travel, so a stray callback or the like can't stop a call from being sent. Values are
// taken now, since posting comes later: a buffer or Date the caller changes after the call must not leak in.
const OPTIONS = ['level', 'dictionary', 'filename', 'mtime', 'comment', 'maxSize']
const ENTRY_OPTIONS = ['level', 'mtime', 'comment', 'attrs', 'os', 'extra', 'mode']
// An object is read now as the sync call reads it, since a structured clone drops its prototype (a toString, a Date of
// another realm): a time through new Date(), text through ToString, kept an object so an empty text stays truthy.
// A view that can't be read goes as it is, for the call to refuse as the sync one does.
const take = (v: unknown, key = ''): unknown =>
  isBytes(v) ? (unreadable(v) ? v : copy(v))
    : typeof v !== 'object' && typeof v !== 'function' || v === null ? v
      : key === 'mtime' ? new Date(v as Date)
        : key === 'comment' || key === 'filename' ? Object(String(v)) : v
// Options of any type read as the sync calls read them, by plain [[Get]]: `in` would miss a get-only Proxy's values
const pick = (o: object, keys: string[]): Record<string, unknown> => {
  const r: Record<string, unknown> = {}
  for (const key of keys) {
    const v = (o as Record<string, unknown>)[key]
    if (v === undefined) continue
    r[key] = key === 'extra' && v !== null && typeof v === 'object'
      ? Object.fromEntries(Object.entries(v).map(([id, b]) => [id, take(b)]))
      : take(v, key)
  }
  return r
}

const strip = <T extends AsyncOptions>(opts: T): Omit<T, 'signal' | 'transfer'> => pick(opts, OPTIONS) as Omit<T, 'signal' | 'transfer'>

type Work<T> = [size: number, local: () => T, toWorker: () => [unknown[], Uint8Array[]]]

// A one-shot call, whose every failure comes back through its promise. `start` checks the arguments, whose errors win
// over an aborted signal; after that, an abort that lands before the promise settles rejects it with its reason,
// whatever it would have settled with.
async function run<T>(fn: string, opts: AsyncOptions, start: () => Work<T>): Promise<T> {
  const [size, local, toWorker] = start(), signal = opts.signal
  // The work, or the move to a worker, is done before this returns. Its outcome, result or failure, is awaited all the
  // same, so an abort made right after the call rejects it inline as in a worker.
  const work = new Promise<T>(resolve => {
    if (signal?.aborted) throw signal.reason
    if (size < INLINE_LIMIT) return resolve(local())
    const [args, buffers] = toWorker()
    // A buffer listed twice can't be moved at all
    resolve(getPool().call(fn, args, [...new Set(buffers.flatMap(movable))], signal) as Promise<T>)
  })
  try {
    const done = await work
    if (signal?.aborted) throw signal.reason
    return done
  } catch (e) {
    throw signal?.aborted ? signal.reason : e
  }
}

function oneShot<O extends object>(
  fn: string, local: (data: Uint8Array, opts: O) => Uint8Array<ArrayBuffer>, data: Uint8Array, opts: O & AsyncOptions = {} as O & AsyncOptions,
): Promise<Uint8Array<ArrayBuffer>> {
  opts ??= {} as O & AsyncOptions
  return run(fn, opts, () => {
    const o = strip(opts) as O
    bytes(data)
    return [data.length, () => local(data, o), () => { const d = own(data, opts.transfer); return [[d, o], [d]] }]
  })
}

/** deflate() as a promise, run in a worker for inputs of 16 KB or more */
export const deflateAsync = (data: Uint8Array, opts?: DeflateOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('deflate', deflate, data, opts)
/** inflate() as a promise, run in a worker for inputs of 16 KB or more */
export const inflateAsync = (data: Uint8Array, opts?: InflateOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('inflate', inflate, data, opts)
/** zlib() as a promise, run in a worker for inputs of 16 KB or more */
export const zlibAsync = (data: Uint8Array, opts?: DeflateOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('zlib', zlib, data, opts)
/** unzlib() as a promise, run in a worker for inputs of 16 KB or more */
export const unzlibAsync = (data: Uint8Array, opts?: InflateOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('unzlib', unzlib, data, opts)
/** gzip() as a promise, run in a worker for inputs of 16 KB or more */
export const gzipAsync = (data: Uint8Array, opts?: GzipOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('gzip', gzip, data, opts)
/** gunzip() as a promise, run in a worker for inputs of 16 KB or more */
export const gunzipAsync = (data: Uint8Array, opts?: GunzipOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('gunzip', gunzip, data, opts)
/** decompress() as a promise, run in a worker for inputs of 16 KB or more */
export const decompressAsync = (data: Uint8Array, opts?: InflateOptions & AsyncOptions): Promise<Uint8Array<ArrayBuffer>> => oneShot('decompress', decompress, data, opts)

/** zip() as a promise, run in a worker for inputs of 16 KB or more */
export function zipAsync(input: ZipInput, opts: ZipOptions & AsyncOptions = {}): Promise<Uint8Array<ArrayBuffer>> {
  opts ??= {}
  return run('zip', opts, () => {
    const files = zipFiles(input), o = strip(opts)
    let size = 0
    for (const f of files) size += f[1].length
    return [size, () => zipList(files, o), () => {
      // Moving a buffer detaches every view of it, so options and copies are taken first, and each buffer moves once.
      // An option getter can detach or shrink an entry's bytes after zipFiles() checks them, so they're checked again.
      const picked = files.map(f => pick(f[2], ENTRY_OPTIONS)), transfer = opts.transfer
      const copies = files.map(([, b]) => transfer && movable(b).length ? null : copy(bytes(b)))
      const moved = new Map<ArrayBufferLike, Uint8Array>()
      const owned = files.map(([, b], i) => {
        let m = copies[i] ?? moved.get(b.buffer)
        if (!m) moved.set(b.buffer, m = move(b))
        return m
      })
      return [[files.map(([path], i) => [path, owned[i], picked[i]]), o, Date.now()], owned]
    }]
  })
}

/** unzip() as a promise, run in a worker for archives of 16 KB or more. The filter runs on the calling thread. */
export function unzipAsync(data: Uint8Array, opts: UnzipOptions & AsyncOptions = {}): Promise<Record<string, Uint8Array<ArrayBuffer>>> {
  opts ??= {}
  return run('unzipSome', opts, () => {
    const filter = opts.filter, maxSize = checkMaxSize(opts.maxSize)
    bytes(data)
    return [data.length, () => unzip(data, { filter, maxSize }), () => {
      // The filter reads the bytes the worker gets: shared memory another thread rewrites could list others later
      const d = own(data, opts.transfer)
      const kept = filter ? zipEntries(d).flatMap((e, i) => filter(e) ? [i] : []) : null
      return [[d, kept, maxSize], [d]]
    }]
  })
}

type Send = (method: string, chunk?: Uint8Array, shape?: (reply: unknown) => unknown) => Promise<unknown>
type Call = (method: string, args: unknown[]) => Promise<unknown>
type Stop = (why: string, ...error: [unknown?]) => Promise<never>
type Lose = (why: string) => void
type Live = <T>(p: Promise<T>) => Promise<T>

// A stream call's promise, settled one step after `p`: an abort that lands before then rejects it with its reason,
// whatever it would have settled with. Thrown, as a rejection returned would settle it two steps later.
const lastly = <T>(p: Promise<T>, signal: AbortSignal | undefined, check: (value: T) => T = v => v): Promise<T> =>
  p.then(v => { if (signal?.aborted) throw signal.reason; return check(v) }, (e: unknown) => { throw signal?.aborted ? signal.reason : e })

// Closes a stream's session and stops listening for its abort. Built outside openStream: its closures share one scope,
// so one held by the registry or a long-lived signal would keep the stream itself alive.
function closer(session: LazySession, signal: AbortSignal | undefined): () => void {
  const onAbort = (e: Event): void => session.close((e.target as AbortSignal).reason)
  signal?.addEventListener('abort', onAbort, { once: true })
  // Held weakly where the registry runs, and by no closure here, as one would share it with this: the caller's own
  // listeners on it can lead back to a stream dropped unfinished
  const ref = signal && (dropped ? new WeakRef(signal) : { signal, deref() { return this.signal } })
  return () => { session.close(); ref?.deref()?.removeEventListener('abort', onAbort) }
}

// A stream dropped unfinished would keep its state in a worker for good, so collecting it closes the session
const dropped = typeof FinalizationRegistry === 'function' ? new FinalizationRegistry<() => void>(close => close()) : null

// `local` builds the stream when it runs on this thread, if that differs from the worker's. With `settle`, which may
// make calls of its own before a reply is complete, each call goes out only once the one before has settled.
function openStream<T extends object>(
  kind: string, opts: AsyncOptions, streamOpts: unknown, build: (send: Send, call: Call, stop: Stop, lose: Lose, live: Live) => T,
  local?: () => object, settle?: (reply: unknown, call: Call) => Promise<unknown>,
): T {
  const session = new LazySession(kind, streamOpts)
  // Taken now, as the stream's other options are: a later change must not start moving the caller's chunks
  const { signal, transfer } = opts
  const close = closer(session, signal)
  let previous: Promise<unknown> = Promise.resolve(), finished = false, failed = false, failure: unknown, busy = false
  // A call queued behind one that failed gets the same error, as the sync stream gives it
  const call = (method: string, a: unknown[], transfer: Transferable[] = []): Promise<unknown> =>
    signal?.aborted ? Promise.reject(signal.reason) : failed ? Promise.reject(failure) : session.call(method, a, transfer, local)
  const start = async (method: string, chunk?: Uint8Array): Promise<unknown> => {
    if (signal?.aborted) return Promise.reject(signal.reason)
    // Refused before the chunk is taken, so a call that can't run never empties the caller's array
    if (failed) return Promise.reject(failure)
    if (finished) return refuse(FINISHED)
    // Taking the chunk can run the caller's code, a getter say: a call it makes on this stream would run first, so
    // it's refused, as in the sync stream
    if (busy) return refuse(BUSY)
    busy = true
    let c: Uint8Array<ArrayBuffer> | undefined
    // A chunk small enough to run on this thread is copied, never moved: whether the stream will isn't known yet.
    // Only push needs one; a missing one is refused here, as in the sync stream, which stays usable.
    try { c = chunk === undefined && method !== 'push' ? undefined : own(bytes(chunk), transfer && chunk!.length >= INLINE_LIMIT) } finally { busy = false }
    // Only now: a finish whose chunk is refused leaves the stream open, as a refused zip entry finish does
    if (method === 'finish') finished = true
    const go = (): Promise<unknown> => live(call(method, c ? [c] : [], c ? movable(c) : []))
    let r = settle ? previous.then(go).then(v => settle(v, call)) : go()
    // Holding no result: a held stream would keep its last one
    if (settle) previous = r.then(() => {}, () => {})
    // A failed call fails every later one, so the session closes now, not when the stream is collected: in Deno an
    // open session keeps the process alive. A zip writer stays usable after a refused entry, so it stays open.
    if (kind !== 'zip') r = r.catch((e: unknown) => { if (!signal?.aborted) { failed = true; failure = e; close() } throw e })
    if (method === 'finish') r = r.finally(close)
    // Until a call settles it holds the stream, so collecting the stream never closes a session mid-call
    return r.finally(() => stream)
  }
  // The final step checks the abort, not the stream's failure: inline, a later call's failure can be known before this
  // call settles. A reply's last reshaping goes inside that step.
  const send: Send = (method, chunk, shape) => lastly(start(method, chunk), signal, shape)
  // A result once it's ready, or waiting its turn: a call sent before the stream failed (a zip archive lost while it
  // was in flight) fails with it too
  const live: Live = p => lastly(p, signal, v => { if (failed) throw failure; return v })
  // A call refused before anything is sent, for `why` or an earlier call's error, which after an abort rejects with
  // its reason, as every other call does
  const stop: Stop = (why, ...error) =>
    signal?.aborted ? Promise.reject(signal.reason) : error.length ? Promise.reject(error[0]) : failed ? Promise.reject(failure) : refuse(why)
  // Fails every later call with `why` and closes the session now, for a zip archive that can't be finished
  const lose: Lose = why => { failed = true; failure = new KokyuError('invalid-state', why); close() }
  const stream = build(send, call, stop, lose, live)
  dropped?.register(stream, close)
  return stream
}

// An async stream's methods over its sends; a compressor's adds flush
type Out = Promise<Uint8Array<ArrayBuffer>>
const plain = (send: Send): AsyncStream => ({ push: c => send('push', c) as Out, finish: c => send('finish', c) as Out })
const compressing = (send: Send): AsyncCompressStream => ({ ...plain(send), flush: () => send('flush') as Out })

function streamAsync<T extends object>(kind: string, opts: AsyncOptions & object, build: (send: Send, call: Call, stop: Stop, lose: Lose, live: Live) => T): T {
  opts ??= {}
  let o: unknown
  // An option that can't be read (its toString throws, say) fails every call, as any other bad option does
  try { o = strip(opts) } catch (e) { const signal = opts.signal, f = (): Promise<never> => lastly(Promise.reject(e), signal); return build(f, f, f, () => {}, p => lastly(p, signal)) }
  return openStream(kind, opts, o, build)
}

/** deflateStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const deflateStreamAsync = (opts: DeflateOptions & AsyncOptions = {}): AsyncCompressStream => streamAsync('deflate', opts, compressing)
/** zlibStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const zlibStreamAsync = (opts: DeflateOptions & AsyncOptions = {}): AsyncCompressStream => streamAsync('zlib', opts, compressing)
/** gzipStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const gzipStreamAsync = (opts: GzipOptions & AsyncOptions = {}): AsyncCompressStream => streamAsync('gzip', opts, compressing)
/** inflateStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const inflateStreamAsync = (opts: InflateOptions & AsyncOptions = {}): AsyncStream => streamAsync('inflate', opts, plain)
/** unzlibStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const unzlibStreamAsync = (opts: InflateOptions & AsyncOptions = {}): AsyncStream => streamAsync('unzlib', opts, plain)
/** gunzipStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const gunzipStreamAsync = (opts: GunzipOptions & AsyncOptions = {}): AsyncStream => streamAsync('gunzip', opts, plain)
/** decompressStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export const decompressStreamAsync = (opts: InflateOptions & AsyncOptions = {}): AsyncStream => streamAsync('decompress', opts, plain)

/** zipStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once */
export function zipStreamAsync(opts: ZipOptions & AsyncOptions = {}): AsyncZipStream {
  // The entry being written, checked here as zipStream() checks it before anything is sent: a refused call leaves
  // the open entry and the stream as they were
  let current: object | null = null, busy = false
  // Every call settles in call order. Run inline, all of them settle in one tick, and JSC would settle the archive's
  // finish() ahead of the entry calls made before it: written as they settle, the central directory would come first.
  let turn: Promise<unknown> = Promise.resolve()
  return streamAsync('zip', opts, (send, call, stop, lose, live) => {
    const inOrder = <T>(p: Promise<T>): Promise<T> => {
      const before = turn
      // Watched now, so a result waiting its turn is never reported unhandled; the promise returned still can be
      p.catch(() => {})
      turn = before.then(() => p).then(() => {}, () => {})
      // A result or refusal still waiting its turn when an abort lands rejects with the abort's reason, as a call in
      // flight does
      return live(before.then(() => p))
    }
    return {
      file(name, fileOpts = {}) {
        if (current) return { push: () => inOrder(stop(ENTRY_OPEN)), finish: () => inOrder(stop(ENTRY_OPEN)) }
        // Open while its options are read, so a getter's file() or finish() is refused, as zipStream() refuses it.
        // Options that can't be read refuse the entry and leave none open.
        const entry = current = {}
        let picked: Record<string, unknown>
        try { picked = pick(fileOpts ?? {}, ENTRY_OPTIONS) } catch (e) { current = null; return { push: () => inOrder(Promise.reject(e)), finish: () => inOrder(Promise.reject(e)) } }
        // Stamped now, as zipStream() stamps it, not when the worker gets to it
        const opened = call('openFile', [name, picked, Date.now()])
        // An entry the worker refused isn't the open one
        opened.catch(() => { if (current === entry) current = null })
        // Sent right away so the worker keeps busy; an error from opening the entry wins over the push's own
        const write = async (method: string, c: Uint8Array | undefined, last: boolean): Promise<Uint8Array<ArrayBuffer>> => {
          if (current !== entry) return opened.then(() => stop(ENTRY_DONE), (e: unknown) => stop(ENTRY_DONE, e))
          // A call from the caller's code run while the chunk is read is refused, as zipStream() refuses it; the entry
          // stays open meanwhile, so file() and finish() refuse it too
          if (busy) return stop(BUSY)
          busy = true
          try {
            // Checked before the entry counts as finished: a refused chunk leaves it open, as in zipStream()
            if (c !== undefined) bytes(c)
            const sent = Promise.all([opened, send(method, c)])
            if (last) current = null
            return sent.then(([, v]) => v as Uint8Array<ArrayBuffer>)
          } finally {
            busy = false
          }
        }
        return { push: c => inOrder(write('pushFile', c, false)), finish: c => inOrder(write('finishFile', c, true)), [ENTRY]: () => lose(LOST) } as AsyncStream
      },
      finish: () => inOrder(current ? stop(ENTRY_OPEN_AT_END) : send('finish') as Promise<Uint8Array<ArrayBuffer>>),
    }
  })
}

/**
 * unzipStream() with promises, run in a worker unless the whole stream is under 16 KB and finished at once.
 * The filter runs on the calling thread.
 */
export function unzipStreamAsync(opts: UnzipStreamOptions & AsyncOptions = {}): AsyncUnzipStream {
  // With a filter, the worker stops at each entry's header until the filter here answers, so a skipped entry is decoded
  // only when nothing else finds its end. A stream small enough to run on this thread uses the filter directly.
  opts ??= {}
  const filter = opts.filter, maxSize = opts.maxSize
  // A filter that throws drops the rest of its call's reply and fails the stream, so every later call rethrows its
  // error, as in unzipStream
  const settle = filter && (async (reply: unknown, call: Call): Promise<UnzipChunk[]> => {
    const out: UnzipChunk[] = []
    for (let chunks = reply as AskChunk[]; ;) {
      const ask = chunks[chunks.length - 1]?.ask ? chunks.pop() : undefined
      out.push(...chunks)
      if (!ask) return out
      // Sent as the truth value unzipStream() would act on: the answer itself may not survive a post
      chunks = await call('decide', [!!filter(ask.entry)]) as AskChunk[]
    }
  })
  // A worker's replies are copies, so one entry's chunks from two replies would be two objects: each chunk gets the
  // object its entry started with, as in unzipStream(). Entries are contiguous, so the open one is the only candidate.
  let open: ZipEntryInfo | null = null
  const same = (chunks: unknown): UnzipChunk[] => {
    for (const c of chunks as UnzipChunk[]) {
      c.entry = open ??= c.entry
      if (c.final) open = null
    }
    return chunks as UnzipChunk[]
  }
  return openStream('unzip', opts, { ask: !!filter, maxSize }, send => ({
    push: c => send('push', c, same) as Promise<UnzipChunk[]>,
    finish: c => send('finish', c, same) as Promise<UnzipChunk[]>,
  }), filter && (() => new UnzipReader({ filter, maxSize })), settle)
}
