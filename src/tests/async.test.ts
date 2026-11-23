import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { inspect } from 'node:util'
import * as k from '../engine/index.js'
import { cat, pieces, readable, sample } from './helpers.js'

// Large enough to go to a worker rather than run inline
const big = cat([sample, sample, sample, sample])
const small = sample.subarray(0, 1000)

test('async one-shots round-trip inline and in workers, and deflateAsync matches deflate', async () => {
  for (const d of [small, big]) {
    assert.deepEqual(await k.deflateAsync(d, { level: 6 }), k.deflate(d))
    assert.deepEqual(await k.inflateAsync(k.deflate(d)), d)
    assert.deepEqual(await k.unzlibAsync(await k.zlibAsync(d)), d)
    assert.deepEqual(await k.gunzipAsync(await k.gzipAsync(d)), d)
    assert.deepEqual(await k.decompressAsync(k.gzip(d)), d)
    const files = { 'a.txt': d, 'b/c.txt': small }
    const r = await k.unzipAsync(await k.zipAsync(files))
    assert.deepEqual(r['a.txt'], d)
    assert.deepEqual(Object.keys(await k.unzipAsync(k.zip(files), { filter: e => e.name === 'a.txt' })), ['a.txt'])
  }
})

test('unzipAsync\'s filter judges the bytes its worker decodes, even when shared memory changes under the call', async () => {
  const fill = (c: number): [Uint8Array, { level: 0 }] => [new Uint8Array(20000).fill(c), { level: 0 }]
  const mtime = new Date(2020, 0, 1)
  const a = k.zip({ 'okay.txt': fill(1), 'evil.txt': fill(2) }, { mtime }), b = k.zip({ 'evil.txt': fill(2), 'okay.txt': fill(1) }, { mtime })
  // Another thread rewriting the archive just after the filter has seen it
  const data = a.slice()
  const filter = (e: k.ZipEntry): boolean => { if (e.name === 'evil.txt') data.set(b); return e.name !== 'evil.txt' }
  assert.deepEqual(Object.keys(await k.unzipAsync(data, { filter })), ['okay.txt'])
})

test('many parallel jobs share the pool', async () => {
  const results = await Promise.all(Array.from({ length: 12 }, () => k.gzipAsync(big)))
  for (const r of results) assert.deepEqual(k.gunzip(r), big)
})

test('async streams pipeline pushes and keep order', async () => {
  const gz = cat(await Promise.all(pieces(k.gzipStreamAsync({ level: 6 }), big, 65536)))
  assert.deepEqual(k.gunzip(gz), big)
  assert.deepEqual(cat(await Promise.all(pieces(k.gunzipStreamAsync(), gz, 4096))), big)
})

test('streams in a worker give the same bytes after it frees each chunk, and leave the caller\'s alone', async () => {
  const before = big.slice()
  for (const [s, decode] of [[k.deflateStreamAsync(), k.inflate], [k.zlibStreamAsync(), k.unzlib], [k.gzipStreamAsync(), k.gunzip]] as const) {
    assert.deepEqual(decode(cat(await Promise.all(pieces(s, big, 40_000)))), big)
  }
  const z = k.zipStreamAsync(), parts = [...pieces(z.file('a.txt'), big, 40_000), z.finish()]
  assert.deepEqual(k.unzip(cat(await Promise.all(parts)))['a.txt'], big)
  // Decoders keep the tail of a chunk for the next one, so each chunk here ends inside a trailer or a block
  const gz = k.gzip(big), zl = k.zlib(big), stored = k.zip({ 'a.txt': [big, { level: 0 }], 'b.txt': small })
  for (const [s, packed, at, want] of [
    [k.gunzipStreamAsync(), cat([gz, k.gzip(small)]), gz.length - 4, cat([big, small])],
    [k.unzlibStreamAsync(), zl, zl.length - 2, big],
    [k.inflateStreamAsync(), k.deflate(big), 70_001, big],
  ] as const) {
    assert.deepEqual(cat(await Promise.all([s.push(packed.subarray(0, at)), s.push(packed.subarray(at)), s.finish()])), want)
  }
  const u = k.unzipStreamAsync(), chunks = (await Promise.all(pieces(u, stored, 16_385))).flat()
  assert.deepEqual(cat(chunks.filter(c => c.entry.name === 'a.txt').map(c => c.data)), big)
  assert.deepEqual(cat(chunks.filter(c => c.entry.name === 'b.txt').map(c => c.data)), small)
  assert.deepEqual(big, before)
})

test('async zip streams, with the unzip filter applied on this side', async () => {
  const z = k.zipStreamAsync(), parts: Promise<Uint8Array>[] = []
  const w = z.file('big.txt')
  parts.push(w.push(big), w.finish())
  parts.push(z.file('s.txt', { level: 0 }).finish(small))
  parts.push(z.finish())
  const archive = cat(await Promise.all(parts))
  assert.deepEqual(k.unzip(archive)['s.txt'], small)
  const u = k.unzipStreamAsync({ filter: e => e.name === 'big.txt' })
  const chunks = [...await u.push(archive), ...await u.finish()]
  assert.ok(chunks.every(c => c.entry.name === 'big.txt'))
  assert.deepEqual(cat(chunks.map(c => c.data)), big)
})

test('small streams finished within one tick run inline and match the worker path', async () => {
  const s = k.deflateStreamAsync({ level: 6 })
  const buf = small.slice()
  const a = s.push(buf)
  // Reusing the buffer before the tick ends must not change what was pushed
  buf.fill(0)
  assert.deepEqual(cat(await Promise.all([a, s.finish()])), k.deflate(small))

  // An await between pushes leaves the stream open past the tick, so it goes to a worker
  const w = k.deflateStreamAsync({ level: 6 })
  const first = await w.push(small)
  assert.deepEqual(cat([first, await w.finish()]), k.deflate(small))

  const z = k.zipStreamAsync()
  const archive = cat(await Promise.all([z.file('a.txt').finish(small), z.finish()]))
  assert.deepEqual(k.unzip(archive)['a.txt'], small)
  const u = k.unzipStreamAsync()
  const chunks = (await Promise.all([u.push(archive), u.finish()])).flat()
  assert.deepEqual(cat(chunks.map(c => c.data)), small)

  const bad = k.unzlibStreamAsync()
  const failed = bad.push(new Uint8Array([1, 2, 3, 4]))
  bad.finish().catch(() => {})
  await assert.rejects(failed, { code: 'invalid-header' })

  const c = new AbortController()
  const q = k.gzipStreamAsync({ signal: c.signal })
  const queued = [q.push(small), q.finish()]
  c.abort()
  for (const p of queued) await assert.rejects(p, e => e === c.signal.reason)
})

test('worker errors arrive as KokyuErrors with their code', async () => {
  await assert.rejects(k.unzlibStreamAsync().push(new Uint8Array([1, 2, 3, 4])), { name: 'KokyuError', code: 'invalid-header' })
  await assert.rejects(k.gunzipAsync(cat([new Uint8Array(20), big])), { name: 'KokyuError', code: 'invalid-header' })
})

test('abort rejects with the signal reason, and the pool keeps working', async () => {
  const c = new AbortController()
  c.abort()
  await assert.rejects(k.gzipAsync(big, { signal: c.signal }), e => e === c.signal.reason)
  // Aborted with its job already given to a worker, which the pool retires as it would mid-run: a timer instead could
  // fire after the result when the thread stalls, and the call would rightly resolve
  const running = new AbortController()
  const p = k.deflateAsync(big, { level: 9, signal: running.signal })
  running.abort()
  await assert.rejects(p, e => e === running.signal.reason)
  assert.deepEqual(k.gunzip(await k.gzipAsync(big)), big)
})

test('inputs are copied by default and moved with transfer', async () => {
  const kept = new Uint8Array(big)
  await k.gzipAsync(kept)
  assert.equal(kept.length, big.length)
  const moved = new Uint8Array(big)
  assert.deepEqual(k.gunzip(await k.gzipAsync(moved, { transfer: true })), big)
  assert.equal(moved.length, 0)
})

test('a buffer reused as soon as a call returns never changes the result', async () => {
  for (const d of [small, big]) {
    const buf = new Uint8Array(d), files = { 'a.txt': new Uint8Array(d) }
    const gz = k.gzipAsync(buf), zipped = k.zipAsync(files)
    buf.fill(0)
    files['a.txt'].fill(0)
    assert.deepEqual(k.gunzip(await gz), d)
    assert.deepEqual(k.unzip(await zipped)['a.txt'], d)
    // One buffer refilled for every push, as a file read loop does
    const s = k.deflateStreamAsync(), chunk = new Uint8Array(d.length >> 2), parts: Promise<Uint8Array>[] = []
    for (let o = 0; o < d.length; o += chunk.length) {
      const n = Math.min(chunk.length, d.length - o)
      chunk.set(d.subarray(o, o + n))
      parts.push(s.push(chunk.subarray(0, n)))
    }
    parts.push(s.finish())
    assert.deepEqual(k.inflate(cat(await Promise.all(parts))), d)
    // One options object changed between files
    const opts: k.ZipEntryOptions = { level: 6 }, z = k.zipStreamAsync(), files2 = [z.file('a.txt', opts).finish(d)]
    opts.level = 0
    files2.push(z.file('b.txt', opts).finish(d), z.finish())
    // Level 0 is stored blocks, larger than the data
    assert.deepEqual(k.zipEntries(cat(await Promise.all(files2))).map(e => e.size! < e.originalSize!), [true, false])
  }
})

test('an option that can\'t be read rejects an async stream\'s calls, and a zip entry refused for one leaves the stream usable', async () => {
  const throwing = { toString(): string { throw new Error('caller toString') } } as never
  for (const open of [() => k.gzipStreamAsync({ filename: throwing }), () => k.zipStreamAsync({ comment: throwing })]) {
    const s = open()
    await assert.rejects(s.finish(), /caller toString/)
  }
  const z = k.zipStreamAsync(), getter = { get mtime(): Date { throw new Error('caller getter') } }
  await assert.rejects(z.file('x', getter).finish(small), /caller getter/)
  await assert.rejects(z.file('y', { comment: throwing }).finish(small), /caller toString/)
  const archive = cat([await z.file('a').finish(small), await z.finish()])
  assert.deepEqual(Object.keys(k.unzip(archive)), ['a'])
})

test('an async decode stream that failed gives every later call the same error, calls queued behind it too', async () => {
  const broken = k.deflate(big).slice()
  broken[2] = ~broken[2]!
  const archive = k.zip({ a: big }, { level: 9 }), bad = archive.slice()
  bad[40] = ~bad[40]!
  const cases = [[() => k.inflateStreamAsync(), broken], [() => k.unzipStreamAsync(), bad], [() => k.unzipStreamAsync({ filter: () => true }), bad]] as const
  for (const [open, data] of cases) {
    const s = open(), half = data.length >> 1
    const calls = await Promise.allSettled([s.push(data.subarray(0, half)), s.push(data.subarray(half)), s.finish()])
    const later = await Promise.allSettled([s.push(small)])
    const errors = [...calls, ...later].map(r => r.status === 'rejected' ? `${r.reason.code}: ${r.reason.message}` : 'resolved')
    assert.equal(new Set(errors).size, 1, errors.join(' | '))
    assert.match(errors[0]!, /^invalid-data/)
  }
})

test('toTransformStream pipes sync and async streams', async () => {
  const parts: Uint8Array[] = []
  for await (const x of readable(big, 10_000).pipeThrough(k.toTransformStream(k.gzipStream())).pipeThrough(k.toTransformStream(k.gunzipStreamAsync()))) parts.push(x)
  assert.deepEqual(cat(parts), big)
})

test('toTransformStream finishes an async stream when the pipe is canceled, closing its worker session', async () => {
  const s = k.gzipStreamAsync(), source = new ReadableStream<Uint8Array>({ pull(c) { c.enqueue(big) } })
  const reader = source.pipeThrough(k.toTransformStream(s)).getReader()
  await reader.read()
  await reader.cancel()
  await assert.rejects(s.push(small), (e: k.KokyuError) => e.code === 'invalid-state')
})

test('toTransformStream finishes an async stream when a chunk it refuses fails the pipe, closing its worker session', async () => {
  const s = k.gzipStreamAsync()
  let i = 0
  const source = new ReadableStream<Uint8Array>({ pull(c) { c.enqueue((i++ ? 'text' : big) as Uint8Array) } })
  await assert.rejects(source.pipeThrough(k.toTransformStream(s)).pipeTo(new WritableStream()), (e: k.KokyuError) => e.code === 'invalid-option')
  await new Promise(r => setTimeout(r, 0))
  await assert.rejects(s.push(small), (e: k.KokyuError) => e.code === 'invalid-state')
})

test('toTransformStream leaves a zip entry open when its pipe fails, so the archive can\'t finish without it', async () => {
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.txt')
    let i = 0
    const source = new ReadableStream<Uint8Array>({ pull(c) { c.enqueue((i++ ? 'text' : big) as Uint8Array) } })
    await assert.rejects(source.pipeThrough(k.toTransformStream(entry)).pipeTo(new WritableStream()), (e: k.KokyuError) => e.code === 'invalid-option')
    await new Promise(r => setTimeout(r, 0))
    await assert.rejects(async () => zs.finish(), (e: k.KokyuError) => e.code === 'invalid-state')
    await entry.finish()
    await zs.finish()
  }
})

test('a zip archive refuses to finish once a canceled pipe drops bytes its entry returned, and its session closes', async () => {
  const lost = (e: k.KokyuError): boolean => e.code === 'invalid-state' && e.message.includes('canceled pipe')
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.bin', { level: 0 })
    let release!: () => void
    const held = new Promise<void>(r => { release = r })
    // A caller's wrapper, spread from the entry, whose push is held until the pipe is canceled
    const slow = { ...entry, push: async (c: Uint8Array) => { const r = await entry.push(c); await held; return r } }
    const ts = k.toTransformStream(slow), reader = ts.readable.getReader(), written = ts.writable.getWriter().write(big)
    const read = reader.read()
    await new Promise(r => setTimeout(r, 20))
    await reader.cancel('stop')
    release()
    await Promise.allSettled([read, written])
    await new Promise(r => setTimeout(r, 0))
    // With the entry still open, then finished
    await assert.rejects(async () => zs.finish(), lost)
    await assert.rejects(async () => zs.file('b.txt').finish(small), lost)
    await assert.rejects(async () => entry.finish(), lost)
    await assert.rejects(async () => zs.finish(), lost)
  }
  // In a child, counting the close messages the pool posts, with the archive still held
  const script = `import { Worker } from 'node:worker_threads'
let closed = 0
const post = Worker.prototype.postMessage
Worker.prototype.postMessage = function (m, t) { if (m?.op === 'close') closed++; return post.call(this, m, t) }
const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
const zs = k.zipStreamAsync(), entry = zs.file('a.bin', { level: 0 })
let release
const held = new Promise(r => { release = r })
const ts = k.toTransformStream({ ...entry, push: async c => { const r = await entry.push(c); await held; return r } })
const reader = ts.readable.getReader(), written = ts.writable.getWriter().write(new Uint8Array(200000)), read = reader.read()
await new Promise(r => setTimeout(r, 20))
await reader.cancel('stop')
release()
await Promise.allSettled([read, written])
await new Promise(r => setTimeout(r, 50))
globalThis.archive = zs
console.log(closed)`
  assert.equal(String(execFileSync(process.execPath, ['--input-type=module', '-e', script], { timeout: 60_000 })).trim(), '1')
})

test('a zip archive refuses to finish once a cancel during the pipe\'s flush drops the entry\'s last bytes', async () => {
  const lost = (e: k.KokyuError): boolean => e.code === 'invalid-state' && e.message.includes('canceled pipe')
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.bin')
    let reader!: ReadableStreamDefaultReader<Uint8Array>
    // The readable side is canceled as flush asks for the entry's last bytes, so the spec never calls cancel()
    const ts = k.toTransformStream({ ...entry, finish: () => { const r = entry.finish(); void reader.cancel('stop').catch(() => {}); return r } })
    reader = ts.readable.getReader()
    const writer = ts.writable.getWriter()
    void writer.write(big).catch(() => {})
    void writer.close().catch(() => {})
    for (;;) if ((await reader.read().catch(() => ({ done: true }))).done) break
    await new Promise(r => setTimeout(r, 20))
    await assert.rejects(async () => zs.finish(), lost)
  }
})

test('a zip archive refuses to finish once a cancel throws away an entry\'s last bytes, queued but never read', async () => {
  const lost = (e: k.KokyuError): boolean => e.code === 'invalid-state' && e.message.includes('canceled pipe')
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const ts = k.toTransformStream(zs.file('a.bin', { level: 0 })), writer = ts.writable.getWriter(), reader = ts.readable.getReader()
    const first = reader.read()
    await writer.write(big)
    await first
    // flush has run and queued the entry's last bytes, which nothing reads
    await writer.close()
    await reader.cancel('stop')
    await assert.rejects(async () => zs.finish(), lost)
  }
  // A cancel with everything read so far loses nothing: the entry stays open, and the archive finishes and reads back
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.bin', { level: 0 }), ts = k.toTransformStream(entry), reader = ts.readable.getReader()
    const first = reader.read()
    await ts.writable.getWriter().write(big)
    const got = await first
    await reader.cancel('stop')
    const z = cat([got.value!, await entry.finish(), await zs.finish()])
    assert.equal(k.unzip(z)['a.bin']!.length, big.length)
  }
})

test('a zip archive refuses to finish once a cancel throws away a chunk whose read was released as it came', async () => {
  const lost = (e: k.KokyuError): boolean => e.code === 'invalid-state' && e.message.includes('canceled pipe')
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.bin', { level: 0 })
    let release!: () => void
    const held = new Promise<void>(r => { release = r })
    const ts = k.toTransformStream({ ...entry, push: async (c: Uint8Array) => { const r = await entry.push(c); await held; return r } })
    const reader = ts.readable.getReader(), written = ts.writable.getWriter().write(big), read = reader.read().catch(() => {})
    await new Promise(r => setTimeout(r, 20))
    // Its read let go while the chunk is on the way, the chunk lands in the readable's own queue
    reader.releaseLock()
    release()
    await Promise.allSettled([read, written])
    await new Promise(r => setTimeout(r, 20))
    await ts.readable.cancel('stop')
    await assert.rejects(async () => zs.finish(), lost)
  }
})

test('a chunk whose read was released before it came counts as read once a new reader takes it', async () => {
  for (const zs of [k.zipStream(), k.zipStreamAsync()]) {
    const entry = zs.file('a.bin', { level: 0 }), ts = k.toTransformStream(entry), writer = ts.writable.getWriter()
    // Once the readable has started, a read pulls from the entry; it's let go before the chunk comes
    await new Promise(r => setTimeout(r, 0))
    const first = ts.readable.getReader()
    void first.read().catch(() => {})
    first.releaseLock()
    // The chunk comes while nobody holds the readable, then a new reader takes it and cancels: nothing was lost
    await writer.write(big)
    const next = ts.readable.getReader(), got = await next.read()
    await next.cancel('done')
    const z = cat([got.value!, await entry.finish(), await zs.finish()])
    assert.equal(k.unzip(z)['a.bin']!.length, big.length)
  }
})

test('a zip stream refusal still waiting its turn when an abort lands rejects with the abort\'s reason', async () => {
  const ac = new AbortController(), reason = new Error('stop'), zs = k.zipStreamAsync({ signal: ac.signal }), entry = zs.file('a.bin')
  // Each refusal is made before the abort, behind a push still on its way
  const calls = [entry.push(big), zs.finish(), zs.file('b.txt').push(small), entry.push('text' as never)]
  ac.abort(reason)
  for (const p of calls) await assert.rejects(p, e => e === reason)
})

test('calls refused for options that can\'t be read reject with an abort\'s reason after it, and with what was thrown before', async () => {
  const ac = new AbortController(), reason = new Error('stop')
  const unreadable = { get level(): never { throw new Error('getter') } }
  const entry = k.zipStreamAsync({ signal: ac.signal }).file('a.txt', unreadable as never)
  const stream = k.deflateStreamAsync({ signal: ac.signal, get level(): never { throw new Error('getter') } } as never)
  ac.abort(reason)
  for (const p of [entry.push(small), entry.finish(), stream.push(small)]) await assert.rejects(p, e => e === reason)
  // With no abort, what the getter threw, undefined included
  await assert.rejects(k.zipStreamAsync().file('a.txt', { get level(): never { throw undefined } } as never).push(small), e => e === undefined)
})

test('calls already sent when a canceled pipe loses an async entry\'s bytes fail with it', async () => {
  const lost = (e: k.KokyuError): boolean => e.code === 'invalid-state' && e.message.includes('canceled pipe')
  const zs = k.zipStreamAsync(), entry = zs.file('a.bin', { level: 0 })
  let release!: () => void
  const held = new Promise<void>(r => { release = r })
  const ts = k.toTransformStream({ ...entry, push: async (c: Uint8Array) => { const r = await entry.push(c); await held; return r } })
  const reader = ts.readable.getReader(), written = ts.writable.getWriter().write(big), read = reader.read()
  await new Promise(r => setTimeout(r, 20))
  await reader.cancel('stop')
  // Both reach the worker before the dropped push is known to be lost
  const sent = [entry.finish(), zs.finish()]
  release()
  await Promise.allSettled([read, written])
  for (const p of sent) await assert.rejects(p, lost)
})

test('a held async stream lets go of its last result', () => {
  // In a child with gc() to call, each result dropped as it comes while its stream stays held
  const script = `const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
let collected = 0
const seen = new FinalizationRegistry(() => collected++)
const data = new Uint8Array(4 << 20).map((_, i) => i * 7), archive = k.zip({ 'a.bin': [data, { level: 0 }] })
const zs = k.zipStreamAsync(), u = k.unzipStreamAsync({ filter: () => true })
seen.register(await zs.file('a.bin', { level: 0 }).finish(data), 0)
seen.register(await u.finish(archive), 0)
for (let i = 0; i < 6; i++) { gc(); await new Promise(r => setTimeout(r, 50)) }
globalThis.held = [zs, u]
console.log(collected)`
  assert.equal(String(execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', script], { timeout: 60_000 })).trim(), '2')
})

test('zipStreamAsync calls still waiting their turn when an abort lands reject with its reason, inline as in a worker', async () => {
  for (const d of [small, big]) {
    const ac = new AbortController(), reason = new Error('stop'), zs = k.zipStreamAsync({ signal: ac.signal }), entry = zs.file('a.bin')
    // Inline, the work behind all of them is done before the first settles
    const first = entry.push(d), rest = [entry.finish(d), zs.file('b.bin').finish(d), zs.finish()]
    await first
    ac.abort(reason)
    for (const p of rest) await assert.rejects(p, e => e === reason)
  }
})

test('async stream calls still under way when an abort lands reject with its reason, inline as in a worker', async () => {
  const archive = k.zip({ 'a.txt': sample })
  const unzipping = (o: k.AsyncOptions): k.AsyncUnzipStream => k.unzipStreamAsync(o)
  for (const d of [small, big]) for (const make of [k.deflateStreamAsync, k.gzipStreamAsync, unzipping]) {
    const ac = new AbortController(), reason = new Error('stop'), s = make({ signal: ac.signal })
    // Inline, the work behind both is done before the first settles
    const data = make === unzipping ? archive : d, first = s.push(data.subarray(0, 100)), last = s.finish(data.subarray(100))
    await first
    ac.abort(reason)
    await assert.rejects(last, e => e === reason)
  }
  // The exact step the abort lands on, in the window right before each call settles: results, failures and refusals
  const gz = k.gzip(small)
  const sequences: Record<string, (signal: AbortSignal) => Promise<unknown>[]> = {
    unzip: signal => { const s = k.unzipStreamAsync({ signal }); return [s.push(archive.subarray(0, 100)), s.finish(archive.subarray(100))] },
    'a failing gunzip': signal => { const s = k.gunzipStreamAsync({ signal }); return [s.push(gz.subarray(0, 100)), s.finish()] },
    'one-shots': signal => [k.deflateAsync(small, { signal }), k.unzipAsync(archive, { signal }), k.gunzipAsync(gz.subarray(1), { signal })],
    zip: signal => { const zs = k.zipStreamAsync({ signal }), e = zs.file('a'); return [e.push(small), zs.file('b').push(small), e.finish(), zs.finish()] },
    'zip, options unreadable': signal => {
      const zs = k.zipStreamAsync({ signal, get level(): never { throw new Error('getter') } })
      return [zs.file('a').push(small), zs.finish()]
    },
  }
  for (const [name, sequence] of Object.entries(sequences)) for (let at = 0; at < 30; at++) {
    const ac = new AbortController(), reason = new Error('stop'), calls = sequence(ac.signal)
    const settled = calls.map(p => p.then(() => 'ok', e => e === reason ? 'reason' : 'other'))
    for (let i = 0; i < at; i++) await Promise.resolve()
    // Read from the promises themselves: a reaction of their own would run a step late
    const pending = calls.map(p => inspect(p).includes('<pending>'))
    ac.abort(reason)
    const got = await Promise.all(settled)
    got.forEach((g, i) => { if (pending[i]) assert.equal(g, 'reason', `${name}: call ${i} pending at step ${at} settled ${g}`) })
  }
})

test('an abort that lands before a call settles rejects it with its reason, whatever the call would have settled with', async () => {
  for (const d of [small, big]) {
    const gz = k.gzip(d, { level: 0 }), z = k.zip({ 'a.txt': d }, { level: 0 })
    // Aborted right after the call: inline, the work is done by then, failed or not, but the promise hasn't settled
    for (const call of [
      (signal: AbortSignal) => k.deflateAsync(d, { signal }),
      (signal: AbortSignal) => k.gunzipAsync(gz, { signal }),
      (signal: AbortSignal) => k.zipAsync({ 'a.txt': d }, { signal }),
      (signal: AbortSignal) => k.unzipAsync(z, { signal }),
      (signal: AbortSignal) => k.gunzipAsync(gz.subarray(0, gz.length >> 1), { signal }),
      (signal: AbortSignal) => k.deflateAsync(d, { signal, level: 99 as never }),
      // In a worker, the filter and the entry list it reads fail on this thread, before the call returns
      (signal: AbortSignal) => k.unzipAsync(z.subarray(1), { signal, filter: () => true }),
      (signal: AbortSignal) => k.deflateStreamAsync({ signal, get level(): never { throw new Error('getter') } }).push(d),
    ]) {
      const ac = new AbortController(), reason = new Error('stop'), p = call(ac.signal)
      ac.abort(reason)
      await assert.rejects(p, e => e === reason)
    }
    // A finish whose own work fails, cut short, still under way when the abort lands
    for (const [make, data] of [[k.gunzipStreamAsync, gz], [k.unzipStreamAsync, z]] as const) {
      const ac = new AbortController(), reason = new Error('stop'), s = make({ signal: ac.signal })
      const first = s.push(data.subarray(0, data.length >> 1)), last = s.finish()
      await first
      assert.ok(inspect(last).includes('<pending>'))
      ac.abort(reason)
      await assert.rejects(last, e => e === reason)
    }
    // A filter or getter that aborts its own call: kokyu's failure after it, or the filter's own throw, gives way
    for (const thrown of [false, true]) {
      for (const call of [
        (signal: AbortSignal, filter: () => boolean) => k.unzipAsync(z, { signal, filter, maxSize: 10 }),
        (signal: AbortSignal, filter: () => boolean) => k.unzipStreamAsync({ signal, filter, maxSize: 10 }).finish(z),
      ]) {
        const ac = new AbortController(), reason = new Error('stop')
        const filter = (): boolean => { ac.abort(reason); if (thrown) throw new Error('mine'); return true }
        await assert.rejects(call(ac.signal, filter), e => e === reason)
      }
    }
    const ac = new AbortController(), reason = new Error('stop')
    const stops = { get comment(): string { ac.abort(reason); return '' } }
    await assert.rejects(k.zipAsync({ a: [d, stops], b: [d, { level: 99 as never }] }, { signal: ac.signal }), e => e === reason)
  }
  // Arguments are checked first: a bad one wins over a signal aborted before the call
  const ac = new AbortController()
  ac.abort()
  await assert.rejects(k.deflateAsync(null as never, { signal: ac.signal }), { code: 'invalid-option' })
})

test('a call whose options can\'t be sent to a worker is answered, even when why can\'t be put into words', { timeout: 20_000 }, async () => {
  const trap = (): never => { throw new Error('trap') }
  // Sending runs the getter; what it throws can't be read as text
  for (const thrown of [() => Object.create(null), () => new Proxy({}, { get: trap, getPrototypeOf: trap })]) {
    const level = { get x(): never { throw thrown() } } as never
    for (const call of [() => k.deflateAsync(big, { level }), () => k.zipAsync({ a: [big, { level }] }), () => k.deflateStreamAsync({ level }).push(big)]) {
      await assert.rejects(call(), { code: 'invalid-option' })
    }
  }
})

test('zipStreamAsync refuses file() and finish() called from an entry option getter, as zipStream does, and the archive still reads back', async () => {
  for (const inner of ['file', 'finish']) {
    const zs = k.zipStreamAsync({ mtime: new Date(2020, 0, 1) })
    let refused: Promise<unknown> = Promise.resolve()
    const opts = { get level() { refused = (inner === 'file' ? zs.file('x').push(new Uint8Array(0)) : zs.finish()).catch(e => e); return 1 as const } }
    const e = zs.file('outer', opts)
    const parts = await Promise.all([e.push(big), e.finish(), zs.finish()])
    assert.equal((await refused as k.KokyuError).code, 'invalid-state', inner)
    assert.deepEqual(Object.keys(k.unzip(cat(parts))), ['outer'], inner)
  }
})

test('async calls read options as the sync calls do, from a Proxy that only traps get too', async () => {
  const proxied = <T extends object>(o: T): T => new Proxy({}, { get: (_, key) => (o as Record<PropertyKey, unknown>)[key] }) as T
  const mtime = new Date(2020, 0, 1)
  for (const d of [small, big]) {
    await assert.rejects(k.inflateAsync(k.deflate(d), proxied({ maxSize: 100 })), { code: 'too-large' })
    await assert.rejects(k.deflateAsync(d, proxied({ level: 99 as never })), { code: 'invalid-option' })
    await assert.rejects(k.unzlibStreamAsync(proxied({ maxSize: 100 })).finish(k.zlib(d)), { code: 'too-large' })
    const zs = k.zipStreamAsync(proxied({ mtime })), e = zs.file('a', proxied({ level: 0 as const }))
    const sync = k.zipStream({ mtime }), se = sync.file('a', { level: 0 })
    assert.deepEqual(cat(await Promise.all([e.push(d), e.finish(), zs.finish()])), cat([se.push(d), se.finish(), sync.finish()]))
  }
})

test('a chunk whose getter calls its own async stream gets that call refused, as the sync stream does', async () => {
  const length = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(Uint8Array.prototype), 'length')!.get!
  // The chunk, and what the call made on its `at`th length read got
  const trapped = (data: Uint8Array, act: () => Promise<unknown>, at = 1): [Uint8Array, () => Promise<unknown>] => {
    const chunk = new Uint8Array(data)
    let reads = 0, inner: Promise<unknown> = Promise.resolve('not called')
    Object.defineProperty(chunk, 'length', { get() { if (++reads === at) inner = act().then(() => 'accepted', (e: k.KokyuError) => e.code); return length.call(this) } })
    return [chunk, () => inner]
  }
  for (const d of [small, big]) {
    const s = k.deflateStreamAsync(), [c, inner] = trapped(d, () => s.push(Uint8Array.of(1, 2, 3)))
    assert.deepEqual(k.inflate(cat(await Promise.all([s.push(c), s.finish()]))), d)
    assert.equal(await inner(), 'invalid-state')
    // A zip entry's chunk whose getter finishes its entry or opens another, read as it's checked or as it's sent:
    // refused, and the bytes stay in that entry
    for (const last of [false, true]) for (const at of [1, 2]) {
      const zs = k.zipStreamAsync(), a = zs.file('a')
      const [ca, innerA] = trapped(d, () => Promise.all([...last ? [] : [a.finish()], zs.file('x').push(d)]), at)
      const parts = await Promise.all(last ? [a.finish(ca), zs.finish()] : [a.push(ca), a.finish(), zs.finish()])
      assert.equal(await innerA(), 'invalid-state')
      assert.deepEqual(k.unzip(cat(parts)), { a: d })
    }
  }
})

test('a held async stream lets go of its copy of the options once it runs', () => {
  const script = `const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
const dictionary = new Uint8Array(1 << 20).fill(7), held = []
const settle = async () => { for (let i = 0; i < 4; i++) { gc(); await new Promise(r => setTimeout(r, 20)) } }
await settle()
const before = process.memoryUsage().arrayBuffers
for (const data of [new Uint8Array(100), new Uint8Array(20000)]) {
  for (let i = 0; i < 10; i++) { const s = k.zlibStreamAsync({ dictionary }); await s.finish(data); held.push(s) }
}
// And streams closed before their first call, by an abort
const ac = new AbortController()
for (let i = 0; i < 10; i++) held.push(k.zlibStreamAsync({ dictionary, signal: ac.signal }))
ac.abort()
await settle()
console.log(Math.round((process.memoryUsage().arrayBuffers - before) / 2 ** 20))`
  const kept = Number(String(execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', script], { timeout: 60_000 })).trim())
  assert.ok(kept < 5, `30 held streams keep ${kept} MB`)
})

test('async zip entries without an mtime are stamped at the call, as sync ones are, not when a worker gets to them', async () => {
  // Date.now() is stubbed on this thread only, so a time a worker took would show
  const real = Date.now, at = new Date(2001, 2, 3, 4, 5, 6).getTime()
  Date.now = () => at
  try {
    const zs = k.zipStreamAsync(), parts = [await zs.file('b.bin').finish(big), await zs.finish()]
    for (const z of [await k.zipAsync({ 'a.bin': big }), cat(parts)]) assert.equal(k.zipEntries(z)[0]!.mtime.getTime(), at)
  } finally {
    Date.now = real
  }
})

test('a pipe canceled or aborted while an async push is under way stops cleanly, with the right error', async () => {
  const data = new Uint8Array(8 << 20).map((_, i) => (i * 2654435761) >>> 24)
  let finished = false
  const s = k.gzipStreamAsync(), ts = k.toTransformStream({ push: (c: Uint8Array) => s.push(c), finish: () => { finished = true; return s.finish() } })
  const written = ts.writable.getWriter().write(data), reader = ts.readable.getReader()
  reader.read().catch(() => {})
  await new Promise(r => setTimeout(r, 2))
  // The push still under way must not enqueue into the canceled side, which made cancel() reject
  await reader.cancel('stop')
  await written
  assert.ok(finished)
  let o = 0
  const source = new ReadableStream<Uint8Array>({ pull(c) { if (o >= data.length) c.close(); else c.enqueue(data.subarray(o, o += 2 << 20)) } })
  const abort = new AbortController(), sink = new WritableStream({ write() { setTimeout(() => abort.abort(), 1) } })
  await assert.rejects(source.pipeThrough(k.toTransformStream(k.gzipStreamAsync())).pipeTo(sink, { signal: abort.signal }), { name: 'AbortError' })
})

test('aborting a job never rejects a failed stream\'s calls still in flight on the same worker', async () => {
  const bad = new Uint8Array(64 << 10).fill(0xFF)
  for (let i = 0; i < 5; i++) {
    const s = k.inflateStreamAsync(), abort = new AbortController()
    let other: Promise<unknown> = Promise.resolve()
    // The caller answers the stream's failure by starting other work, then canceling it
    const pushed = s.push(bad).catch((e: Error) => {
      other = k.deflateAsync(new Uint8Array(1 << 20), { signal: abort.signal }).catch(() => {})
      abort.abort(new Error('unrelated job canceled'))
      return e
    })
    const finished = s.finish().then(() => null, (e: Error) => e)
    const [pe, fe] = await Promise.all([pushed, finished])
    await other
    assert.equal(fe?.message, (pe as Error).message)
  }
})

test('a zipStreamAsync aborted with an entry open rejects every later call with the abort reason', async () => {
  const abort = new AbortController(), z = k.zipStreamAsync({ signal: abort.signal }), w = z.file('a')
  await w.push(small)
  const reason = new Error('stopped')
  abort.abort(reason)
  for (const call of [() => z.finish(), () => z.file('b').push(small), () => w.push(small)]) await assert.rejects(call(), e => e === reason)
})

test('a zipStreamAsync entry whose name was refused rejects with the abort reason once aborted, as every call does', async () => {
  for (const data of [small, big]) {
    const abort = new AbortController(), z = k.zipStreamAsync({ signal: abort.signal }), bad = z.file('../evil')
    await assert.rejects(bad.push(data), { code: 'invalid-option' })
    const reason = new Error('stopped')
    abort.abort(reason)
    await assert.rejects(bad.finish(data), e => e === reason)
  }
})

test('async streams and calls agree with the sync ones after a missing chunk, a failed finish, or an abort from their own callbacks', async () => {
  // A push with no chunk is refused and leaves the stream usable
  const s = k.deflateStreamAsync()
  await assert.rejects(s.push(undefined as never), { code: 'invalid-option' })
  assert.deepEqual(k.inflate(cat([await s.push(small), await s.finish()])), small)
  // After a finish that failed, later calls give its error, as the sync stream rethrows it
  const bad = Uint8Array.of(0x1F, 0x8B, 8, 0, 0, 0, 0, 0, 0, 3, 0xFF), g = k.gunzipStreamAsync()
  await assert.rejects(g.finish(bad), { code: 'invalid-data' })
  await assert.rejects(g.push(small), { code: 'invalid-data' })
  // A filter or option getter that aborts the call's own signal rejects it inline, as it does in a worker
  const z = k.zip({ 'a.txt': small })
  for (const call of [
    (signal: AbortSignal, stop: () => void) => k.unzipAsync(z, { signal, filter: () => { stop(); return true } }),
    (signal: AbortSignal, stop: () => void) => k.unzipStreamAsync({ signal, filter: () => { stop(); return true } }).finish(z),
    (signal: AbortSignal, stop: () => void) => k.zipAsync({ a: [small, { get comment(): string { stop(); return '' } }] }, { signal }),
  ]) {
    const abort = new AbortController(), reason = new Error('stopped')
    await assert.rejects(call(abort.signal, () => abort.abort(reason)), e => e === reason)
  }
})

test('callable options read in a worker as the sync calls read them', async () => {
  const label = Object.assign(() => {}, { toString: () => 'release 1.2' }) as never
  class Stamp { static valueOf(): number { return Date.UTC(2020, 5, 1) } }
  const data = new Uint8Array(20000), entry = { mtime: Stamp as never, comment: label }
  assert.deepEqual((await k.gzipAsync(data, { filename: label })).subarray(10, 22), k.gzip(data, { filename: label }).subarray(10, 22))
  const read = (z: Uint8Array): unknown => k.zipEntries(z).map(e => [e.mtime.getTime(), e.comment])
  assert.deepEqual(read(await k.zipAsync({ a: [data, entry] }, { comment: label })), read(k.zip({ a: [data, entry] }, { comment: label })))
})

test('aborting thousands of streams on one signal costs time in proportion to them', async () => {
  // Each stream closed by the abort scanned every pending call in the pool: 8000 took about 250 ms
  const chunk = new Uint8Array(16384), many = new AbortController(), pushed: Promise<unknown>[] = []
  for (let i = 0; i < 8000; i++) pushed.push(k.deflateStreamAsync({ signal: many.signal }).push(chunk).catch(() => {}))
  await Promise.resolve()
  const t = performance.now()
  many.abort()
  const took = performance.now() - t
  await Promise.all(pushed)
  assert.ok(took < 60, `abort took ${took.toFixed(0)} ms`)
})

test('aborting a batch of queued jobs on one signal doesn\'t start a worker for each of them', () => {
  // An aborted job retired its worker, and the next queued job, which the same abort was about to cancel, started one.
  // In a child, so the pool starts clean: a worker with an open session elsewhere is never retired.
  const script = `import { Worker } from 'node:worker_threads'
let ended = 0
const stop = Worker.prototype.terminate
Worker.prototype.terminate = function () { ended++; return stop.call(this) }
const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
const chunk = new Uint8Array(16384)
await k.deflateAsync(new Uint8Array(20000))
const batch = new AbortController(), jobs = Array.from({ length: 200 }, () => k.deflateAsync(chunk, { signal: batch.signal }).catch(() => {}))
await new Promise(r => setTimeout(r, 0))
batch.abort()
await Promise.all(jobs)
await Promise.all(Array.from({ length: 8 }, () => k.deflateAsync(chunk)))
console.log(ended)`
  const ended = Number(String(execFileSync(process.execPath, ['--input-type=module', '-e', script], { timeout: 60_000 })).trim())
  assert.ok(ended < 20, `${ended} workers stopped for 200 aborted jobs`)
})

test('an async stream is collected once dropped, at any point, even when its filter or the caller\'s abort listener can reach it', () => {
  // In a child with gc() to call. A filter or listener sharing a scope with something that uses the stream reaches it.
  const script = `const k = await import('${new URL('../engine/index.js', import.meta.url).href}')
let collected = 0
const seen = new FinalizationRegistry(() => collected++)
const settle = async () => { for (let i = 0; i < 6; i++) { gc(); await new Promise(r => setTimeout(r, 50)) } }
const archive = k.zip({ 'a.bin': new Uint8Array(40000).map((_, i) => i * 31), 'skip.bin': new Uint8Array(10) })
const listening = () => { const ac = new AbortController(), s = k.deflateStreamAsync({ signal: ac.signal }); ac.signal.addEventListener('abort', () => s.finish().catch(() => {})); return s }
// A filter that reaches its stream, in a run that goes as far as 'push', 'finish' or 'abort', or not at all
const filtered = async (upTo) => {
  const ac = new AbortController(), wanted = new Set(['a.bin']), s = k.unzipStreamAsync({ signal: ac.signal, filter: e => wanted.has(e.name) }), feed = c => s.push(c)
  if (upTo === 'abort') { ac.abort(); await feed(archive).catch(() => {}) } else if (upTo) await feed(archive)
  if (upTo === 'finish') await s.finish()
  seen.register(s, 0)
}
const cases = {
  filter: () => filtered('finish'),
  filterPushed: () => filtered('push'),
  filterAborted: () => filtered('abort'),
  filterUnused: () => filtered(''),
  listener: async () => { const s = listening(); await s.push(new Uint8Array(20000)); await s.finish(); seen.register(s, 0) },
  dropped: async () => { const s = listening(); await s.push(new Uint8Array(20000)); seen.register(s, 0) },
}
const out = {}
for (const [name, run] of Object.entries(cases)) { await settle(); collected = 0; for (let i = 0; i < 30; i++) await run(); await settle(); out[name] = collected }
console.log(JSON.stringify(out))`
  const got = JSON.parse(String(execFileSync(process.execPath, ['--expose-gc', '--input-type=module', '-e', script], { timeout: 60_000 }))) as Record<string, number>
  for (const [name, n] of Object.entries(got)) assert.ok(n >= 27, `${name}: ${n} of 30 streams collected`)
})

test('an idle pool never keeps the process alive', () => {
  const script = `import('${new URL('../engine/index.js', import.meta.url).href}').then(k => k.gzipAsync(new Uint8Array(1 << 20)))`
  // A child still running at the timeout makes execFileSync throw
  execFileSync(process.execPath, ['--input-type=module', '-e', script], { timeout: 10_000 })
})
