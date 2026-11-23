import * as f from 'fflate'
import * as k from '../engine/index.js'
import { cat, equal, ffChunks, fullText, joinChunks, machineCode, measure, race, randomBytes, sameEntries, time } from './helpers.js'

const KB = 1024
const CHUNK = 64 * KB
const ZIP_FILE = 16 * KB

interface Input {
  raw: Uint8Array
  str: string
  deflated: Uint8Array
  zlibbed: Uint8Array
  gzipped: Uint8Array
  files: Record<string, Uint8Array>
  zipped: Uint8Array
  stored: Uint8Array
}

type Run = (i: Input) => unknown
interface Op {
  name: string
  fflate: Run
  kokyu: Run
  // Is kokyu's output right? A fast wrong answer doesn't get timed.
  check: (out: unknown, i: Input) => boolean
  // Report compressed sizes for this row
  size?: true
}
type Pushable = { push(chunk: Uint8Array, final?: boolean): void }
type Callback = (err: f.FlateError | null, out: unknown) => void

// Inputs: real text and binary from the installed TypeScript platform package (see helpers.ts)
const text = (n: number): Uint8Array => new Uint8Array(fullText.subarray(0, n))

const columns: [string, Uint8Array][] = [
  ['text 1 KB', text(KB)],
  ['text 64 KB', text(64 * KB)],
  ['text 1 MB', text(1024 * KB)],
  ['binary 1 MB', machineCode(1024 * KB)],
  ['random 1 MB', randomBytes(1024 * KB, 0x9E3779B9)],
]

function prepare(raw: Uint8Array): Input {
  const files: Record<string, Uint8Array> = {}
  for (let o = 0; o < raw.length; o += ZIP_FILE) files[`f${o / ZIP_FILE}.bin`] = raw.subarray(o, o + ZIP_FILE)
  return {
    raw,
    str: f.strFromU8(raw),
    deflated: f.deflateSync(raw, { level: 6 }),
    zlibbed: f.zlibSync(raw, { level: 6 }),
    gzipped: f.gzipSync(raw, { level: 6 }),
    files,
    zipped: f.zipSync(files, { level: 6 }),
    stored: f.zipSync(files, { level: 0 }),
  }
}

const decodes = (decode: (b: Uint8Array) => Uint8Array) => (out: unknown, i: Input) => equal(decode(out as Uint8Array), i.raw)
const equalsRaw = (out: unknown, i: Input) => equal(out as Uint8Array, i.raw)
const joined = (decode: (b: Uint8Array) => Uint8Array) => (out: unknown, i: Input) => equal(decode(cat(out as Uint8Array[])), i.raw)
const joinedRaw = (out: unknown, i: Input) => equal(cat(out as Uint8Array[]), i.raw)
const zipHolds = (out: unknown, i: Input) => sameEntries(f.unzipSync(out as Uint8Array), i.files)
const zipPartsHold = (out: unknown, i: Input) => sameEntries(f.unzipSync(cat(out as Uint8Array[])), i.files)
const unzipped = (out: unknown, i: Input) => sameEntries(out as Record<string, Uint8Array>, i.files)
const unzipChunks = (out: unknown, i: Input) => sameEntries(joinChunks(out as k.UnzipChunk[]), i.files)

// fflate's worker streams transfer each pushed chunk, detaching its buffer, so those paths push copies
function pushAll(s: Pushable, data: Uint8Array, copy = false): void {
  for (let o = 0; o < data.length; o += CHUNK) {
    s.push(copy ? data.slice(o, o + CHUNK) : data.subarray(o, o + CHUNK), o + CHUNK >= data.length)
  }
}

const call = <T>(pick: (i: Input) => T, fn: (data: T, cb: Callback) => unknown): Run => i =>
  new Promise((resolve, reject) => { fn(pick(i), (err, out) => err ? reject(err) : resolve(out)) })

const stream = (make: (cb: f.FlateStreamHandler) => Pushable, pick: (i: Input) => Uint8Array): Run => i => {
  const out: Uint8Array[] = []
  pushAll(make(c => { out.push(c) }), pick(i))
  return out
}

const asyncStream = (make: (cb: f.AsyncFlateStreamHandler) => Pushable, pick: (i: Input) => Uint8Array): Run => i =>
  ffChunks(cb => pushAll(make(cb), pick(i), true))

type ZipFile = f.ZipInputFile & Pushable
function zipInto(zip: f.Zip, i: Input, make: (name: string) => ZipFile, copy = false): void {
  for (const [name, data] of Object.entries(i.files)) {
    const file = make(name)
    zip.add(file)
    file.push(copy ? data.slice() : data, true)
  }
  zip.end()
}

// Sync file types emit everything before end() returns
const zipStream = (make: (name: string) => ZipFile): Run => i => {
  const out: Uint8Array[] = []
  zipInto(new f.Zip((err, c) => { if (err) throw err; out.push(c) }), i, make)
  return out
}

const asyncZipStream = (make: (name: string) => ZipFile): Run => i => ffChunks(cb => zipInto(new f.Zip(cb), i, make, true))

const unzipStream = (decoder: f.UnzipDecoderConstructor, pick: (i: Input) => Uint8Array): Run => i => {
  const out: Uint8Array[] = []
  const unzip = new f.Unzip(file => {
    file.ondata = (err, c) => { if (err) throw err; out.push(c) }
    file.start()
  })
  unzip.register(decoder)
  pushAll(unzip, pick(i))
  return out
}

const asyncUnzipStream: Run = i =>
  new Promise((resolve, reject) => {
    const out: Uint8Array[] = []
    let left = Object.keys(i.files).length
    const unzip = new f.Unzip(file => {
      file.ondata = (err, c, final) => {
        if (err) return reject(err)
        out.push(c)
        if (final && --left === 0) resolve(out)
      }
      file.start()
    })
    unzip.register(f.AsyncUnzipInflate)
    pushAll(unzip, i.zipped, true)
  })

// kokyu's streams in 64 KB chunks, the last through finish(), as fflate's side passes it with push(chunk, true)
function ours<O>(s: { push(c: Uint8Array): O; finish(c?: Uint8Array): O }, data: Uint8Array): O[] {
  const out: O[] = []
  for (let o = 0; o < data.length; o += CHUNK) {
    const c = data.subarray(o, o + CHUNK)
    out.push(o + CHUNK >= data.length ? s.finish(c) : s.push(c))
  }
  return out
}
const oursAsync = <O>(s: { push(c: Uint8Array): Promise<O>; finish(c?: Uint8Array): Promise<O> }, data: Uint8Array): Promise<O[]> =>
  Promise.all(ours(s, data))

// Every file through a zip stream, sync or async, then the archive's end
const zipAll = <T>(z: { file(name: string, opts: k.ZipEntryOptions): { finish(d: Uint8Array): T }; finish(): T }, i: Input, level: k.Level): T[] =>
  [...Object.entries(i.files).map(([name, data]) => z.file(name, { level }).finish(data)), z.finish()]
const ourZip = (level: k.Level) => (i: Input) => zipAll(k.zipStream(), i, level)

const LEVELS = [1, 6, 9] as const

const groups: [string, Op[]][] = [
  ['Sync', [
    ...LEVELS.map((level): Op => ({
      name: `deflateSync (level ${level})`,
      fflate: i => f.deflateSync(i.raw, { level }),
      kokyu: i => k.deflate(i.raw, { level }),
      check: decodes(f.inflateSync),
      size: true,
    })),
    { name: 'inflateSync', fflate: i => f.inflateSync(i.deflated), kokyu: i => k.inflate(i.deflated), check: equalsRaw },
    { name: 'zlibSync', fflate: i => f.zlibSync(i.raw, { level: 6 }), kokyu: i => k.zlib(i.raw, { level: 6 }), check: decodes(f.unzlibSync), size: true },
    { name: 'unzlibSync', fflate: i => f.unzlibSync(i.zlibbed), kokyu: i => k.unzlib(i.zlibbed), check: equalsRaw },
    { name: 'gzipSync', fflate: i => f.gzipSync(i.raw, { level: 6 }), kokyu: i => k.gzip(i.raw, { level: 6 }), check: decodes(f.gunzipSync), size: true },
    { name: 'gunzipSync', fflate: i => f.gunzipSync(i.gzipped), kokyu: i => k.gunzip(i.gzipped), check: equalsRaw },
    { name: 'decompressSync', fflate: i => f.decompressSync(i.gzipped), kokyu: i => k.decompress(i.gzipped), check: equalsRaw },
    { name: 'zipSync', fflate: i => f.zipSync(i.files, { level: 6 }), kokyu: i => k.zip(i.files, { level: 6 }), check: zipHolds, size: true },
    { name: 'unzipSync', fflate: i => f.unzipSync(i.zipped), kokyu: i => k.unzip(i.zipped), check: unzipped },
    { name: 'strToU8', fflate: i => f.strToU8(i.str), kokyu: i => k.encodeText(i.str), check: (o, i) => equal(o as Uint8Array, f.strToU8(i.str)) },
    { name: 'strFromU8', fflate: i => f.strFromU8(i.raw), kokyu: i => k.decodeText(i.raw), check: (o, i) => o === f.strFromU8(i.raw) },
  ]],
  ['Async (workers)', [
    { name: 'deflate', fflate: call(i => i.raw, (d, cb) => f.deflate(d, { level: 6 }, cb)), kokyu: i => k.deflateAsync(i.raw, { level: 6 }), check: decodes(f.inflateSync) },
    { name: 'inflate', fflate: call(i => i.deflated, (d, cb) => f.inflate(d, cb)), kokyu: i => k.inflateAsync(i.deflated), check: equalsRaw },
    { name: 'zlib', fflate: call(i => i.raw, (d, cb) => f.zlib(d, { level: 6 }, cb)), kokyu: i => k.zlibAsync(i.raw, { level: 6 }), check: decodes(f.unzlibSync) },
    { name: 'unzlib', fflate: call(i => i.zlibbed, (d, cb) => f.unzlib(d, cb)), kokyu: i => k.unzlibAsync(i.zlibbed), check: equalsRaw },
    { name: 'gzip', fflate: call(i => i.raw, (d, cb) => f.gzip(d, { level: 6 }, cb)), kokyu: i => k.gzipAsync(i.raw, { level: 6 }), check: decodes(f.gunzipSync) },
    { name: 'gunzip', fflate: call(i => i.gzipped, (d, cb) => f.gunzip(d, cb)), kokyu: i => k.gunzipAsync(i.gzipped), check: equalsRaw },
    { name: 'decompress', fflate: call(i => i.gzipped, (d, cb) => f.decompress(d, cb)), kokyu: i => k.decompressAsync(i.gzipped), check: equalsRaw },
    { name: 'zip', fflate: call(i => i.files, (d, cb) => f.zip(d, { level: 6 }, cb)), kokyu: i => k.zipAsync(i.files, { level: 6 }), check: zipHolds },
    { name: 'unzip', fflate: call(i => i.zipped, (d, cb) => f.unzip(d, cb)), kokyu: i => k.unzipAsync(i.zipped), check: unzipped },
  ]],
  ['Streams (64 KB chunks)', [
    { name: 'Deflate', fflate: stream(cb => new f.Deflate({ level: 6 }, cb), i => i.raw), kokyu: i => ours(k.deflateStream({ level: 6 }), i.raw), check: joined(f.inflateSync) },
    { name: 'Inflate', fflate: stream(cb => new f.Inflate(cb), i => i.deflated), kokyu: i => ours(k.inflateStream(), i.deflated), check: joinedRaw },
    { name: 'Zlib', fflate: stream(cb => new f.Zlib({ level: 6 }, cb), i => i.raw), kokyu: i => ours(k.zlibStream({ level: 6 }), i.raw), check: joined(f.unzlibSync) },
    { name: 'Unzlib', fflate: stream(cb => new f.Unzlib(cb), i => i.zlibbed), kokyu: i => ours(k.unzlibStream(), i.zlibbed), check: joinedRaw },
    { name: 'Gzip', fflate: stream(cb => new f.Gzip({ level: 6 }, cb), i => i.raw), kokyu: i => ours(k.gzipStream({ level: 6 }), i.raw), check: joined(f.gunzipSync) },
    { name: 'Gunzip', fflate: stream(cb => new f.Gunzip(cb), i => i.gzipped), kokyu: i => ours(k.gunzipStream(), i.gzipped), check: joinedRaw },
    { name: 'Decompress', fflate: stream(cb => new f.Decompress(cb), i => i.gzipped), kokyu: i => ours(k.decompressStream(), i.gzipped), check: joinedRaw },
    { name: 'Zip + ZipDeflate', fflate: zipStream(name => new f.ZipDeflate(name, { level: 6 })), kokyu: ourZip(6), check: zipPartsHold },
    { name: 'Zip + ZipPassThrough', fflate: zipStream(name => new f.ZipPassThrough(name)), kokyu: ourZip(0), check: zipPartsHold },
    { name: 'Unzip + UnzipInflate', fflate: unzipStream(f.UnzipInflate, i => i.zipped), kokyu: i => ours(k.unzipStream(), i.zipped).flat(), check: unzipChunks },
    { name: 'Unzip + UnzipPassThrough', fflate: unzipStream(f.UnzipPassThrough, i => i.stored), kokyu: i => ours(k.unzipStream(), i.stored).flat(), check: unzipChunks },
    {
      name: 'EncodeUTF8',
      fflate: i => {
        const out: Uint8Array[] = []
        const s = new f.EncodeUTF8(c => { out.push(c) })
        for (let o = 0; o < i.str.length; o += CHUNK) s.push(i.str.slice(o, o + CHUNK), o + CHUNK >= i.str.length)
        return out
      },
      kokyu: i => {
        const s = k.encodeTextStream(), out: Uint8Array[] = []
        for (let o = 0; o < i.str.length; o += CHUNK) {
          const c = i.str.slice(o, o + CHUNK)
          out.push(o + CHUNK >= i.str.length ? s.finish(c) : s.push(c))
        }
        return out
      },
      check: (o, i) => equal(cat(o as Uint8Array[]), f.strToU8(i.str)),
    },
    {
      name: 'DecodeUTF8',
      fflate: i => {
        let out = ''
        pushAll(new f.DecodeUTF8(c => { out += c }), i.raw)
        return out
      },
      // Built with += like the fflate side; join('') would add a full copy fflate's rope never pays
      kokyu: i => {
        const s = k.decodeTextStream()
        let out = ''
        for (let o = 0; o < i.raw.length; o += CHUNK) {
          const c = i.raw.subarray(o, o + CHUNK)
          out += o + CHUNK >= i.raw.length ? s.finish(c) : s.push(c)
        }
        return out
      },
      check: (o, i) => o === f.strFromU8(i.raw),
    },
  ]],
  ['Async streams (workers, 64 KB chunks)', [
    { name: 'AsyncDeflate', fflate: asyncStream(cb => new f.AsyncDeflate({ level: 6 }, cb), i => i.raw), kokyu: i => oursAsync(k.deflateStreamAsync({ level: 6 }), i.raw), check: joined(f.inflateSync) },
    { name: 'AsyncInflate', fflate: asyncStream(cb => new f.AsyncInflate(cb), i => i.deflated), kokyu: i => oursAsync(k.inflateStreamAsync(), i.deflated), check: joinedRaw },
    { name: 'AsyncZlib', fflate: asyncStream(cb => new f.AsyncZlib({ level: 6 }, cb), i => i.raw), kokyu: i => oursAsync(k.zlibStreamAsync({ level: 6 }), i.raw), check: joined(f.unzlibSync) },
    { name: 'AsyncUnzlib', fflate: asyncStream(cb => new f.AsyncUnzlib(cb), i => i.zlibbed), kokyu: i => oursAsync(k.unzlibStreamAsync(), i.zlibbed), check: joinedRaw },
    { name: 'AsyncGzip', fflate: asyncStream(cb => new f.AsyncGzip({ level: 6 }, cb), i => i.raw), kokyu: i => oursAsync(k.gzipStreamAsync({ level: 6 }), i.raw), check: joined(f.gunzipSync) },
    { name: 'AsyncGunzip', fflate: asyncStream(cb => new f.AsyncGunzip(cb), i => i.gzipped), kokyu: i => oursAsync(k.gunzipStreamAsync(), i.gzipped), check: joinedRaw },
    { name: 'AsyncDecompress', fflate: asyncStream(cb => new f.AsyncDecompress(cb), i => i.gzipped), kokyu: i => oursAsync(k.decompressStreamAsync(), i.gzipped), check: joinedRaw },
    {
      name: 'Zip + AsyncZipDeflate',
      fflate: asyncZipStream(name => new f.AsyncZipDeflate(name, { level: 6 })),
      kokyu: i => Promise.all(zipAll(k.zipStreamAsync(), i, 6)),
      check: zipPartsHold,
    },
    { name: 'Unzip + AsyncUnzipInflate', fflate: asyncUnzipStream, kokyu: i => oursAsync(k.unzipStreamAsync(), i.zipped).then(o => o.flat()), check: unzipChunks },
  ]],
]

const bytes = (n: number) => n.toLocaleString('en-US')

// How kokyu's time compares with fflate's, in words
const speed = (ours: number, ff: number): string => {
  const r = ff / ours
  if (r.toFixed(2) === '1.00') return 'same'
  return r > 1 ? `${r.toFixed(2)}× faster` : `${(1 / r).toFixed(2)}× slower`
}

async function row(op: Op, i: Input): Promise<string[]> {
  try {
    if (!op.check(await op.kokyu(i), i)) return ['FAIL', '–', '–']
  } catch (e) {
    return [`FAIL (${(e as Error).message})`, '–', '–']
  }
  // fflate's DecodeUTF8 throws on input that ends mid-character, which binary data can
  try {
    await op.fflate(i)
  } catch (e) {
    return [time(await measure(() => op.kokyu(i))), `throws (${(e as Error).message})`, '–']
  }
  const [b, a] = await race(() => op.kokyu(i), () => op.fflate(i))
  return [time(b), time(a), speed(b, a)]
}

// `npm run bench -- <regex>` runs only the matching rows
const only = process.argv[2] ? new RegExp(process.argv[2], 'i') : null
const picked: [string, Op[]][] = groups.map(([g, ops]) => [g, ops.filter(op => !only || only.test(`${g} ${op.name}`))])
const inputs = columns.map(([label, raw]) => [label, prepare(raw)] as const)
const HEADER = '| Operation | Input | kokyu | fflate | Difference |\n|---|---|--:|--:|---|'

console.log('Median time per call.')
for (const [group, ops] of picked) {
  if (!ops.length) continue
  console.log(`\n### ${group}\n\n${HEADER}`)
  for (const op of ops) {
    for (const [label, i] of inputs) console.log(`| ${op.name} | ${label} | ${(await row(op, i)).join(' | ')} |`)
  }
}

const sized = picked.flatMap(([, ops]) => ops.filter(op => op.size))
if (sized.length) {
  console.log(`\n### Compressed size\n\n${HEADER}`)
  for (const op of sized) {
    for (const [label, i] of inputs) {
      const ff = (op.fflate(i) as Uint8Array).length
      const ours = (op.kokyu(i) as Uint8Array).length
      const pct = Math.abs((ours / ff - 1) * 100).toFixed(1)
      const diff = ours === ff ? 'same' : `${pct}% ${ours < ff ? 'smaller' : 'larger'}`
      console.log(`| ${op.name} | ${label} | ${bytes(ours)} B | ${bytes(ff)} B | ${diff} |`)
    }
  }
}
