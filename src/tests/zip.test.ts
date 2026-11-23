import { test } from 'node:test'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import * as ff from 'fflate'
import * as k from '../engine/index.js'
import {
  cat, centralRecords, failureCost, ffZip, has, joinChunks, pieces, pushChunked, python, randomBytes, sameFiles, sample, throwsCode, u16, u32,
} from './helpers.js'

const files: Record<string, Uint8Array> = {
  'a.txt': sample,
  'dir/b.bin': randomBytes(20_000, 5),
  'ünïcødé/文件.txt': sample.subarray(1000, 9000),
  'empty.txt': new Uint8Array(0),
}

const readStream = (archive: Uint8Array, max: number, opts: k.UnzipStreamOptions = {}): Record<string, Uint8Array> =>
  joinChunks(pushChunked(k.unzipStream(opts), archive, max, 11).flat())

function writeStream(level: 0 | 6): Uint8Array {
  const z = k.zipStream({ comment: 'streamed' }), out: Uint8Array[] = []
  for (const [name, data] of Object.entries(files)) {
    out.push(...pieces(z.file(name, { level: name.endsWith('.bin') ? 0 : level }), data, 777))
  }
  out.push(z.finish())
  return cat(out)
}

test('zip round-trips with fflate in both directions', () => {
  for (const level of [0, 1, 6, 9] as const) {
    const archive = k.zip(files, { level, comment: 'archive ✓', mtime: new Date(2020, 5, 15, 12, 30, 10) })
    sameFiles(ff.unzipSync(archive), files)
    sameFiles(k.unzip(archive), files)
  }
  sameFiles(k.unzip(ff.zipSync(files, { level: 6 })), files)
})

test('unzipStream reads ours and fflate\'s streamed zips at any chunk size', () => {
  const ours = k.zip(files)
  const theirs = ffZip(Object.entries(files), n => n.endsWith('.bin'))
  for (const max of [1, 13, 5000, 1e6]) {
    sameFiles(readStream(ours, max), files)
    sameFiles(readStream(theirs, max), files)
  }
})

test('zipStream output reads back everywhere, including stored entries with descriptors', () => {
  for (const level of [0, 6] as const) {
    const archive = writeStream(level)
    sameFiles(ff.unzipSync(archive), files)
    sameFiles(k.unzip(archive), files)
    for (const max of [1, 64, 70_000]) sameFiles(readStream(archive, max), files)
  }
})

test('the system unzip tool accepts both writers', (t) => {
  if (!has('unzip', '-v')) { t.skip('no unzip on this machine'); return }
  const dir = mkdtempSync(join(tmpdir(), 'kokyu-'))
  try {
    for (const archive of [k.zip(files), writeStream(6)]) {
      const p = join(dir, 'test.zip')
      writeFileSync(p, archive)
      // unzip exits nonzero on any error or warning, which throws
      execFileSync('unzip', ['-tq', p])
    }
  } finally {
    rmSync(dir, { recursive: true })
  }
})

test('unzipStream reads a ZIP64 stored entry that ends in a 64-bit data descriptor', () => {
  const data = sample.subarray(0, 3000), b: number[] = []
  const u64 = (v: number): number[] => [...u32(v), 0, 0, 0, 0]
  // The local header's sizes defer to a ZIP64 extra field of zeros, then to the descriptor after the data
  b.push(...u32(0x04034b50), ...u16(45), ...u16(8), ...u16(0), ...u32(0), ...u32(0), ...u32(0xFFFFFFFF), ...u32(0xFFFFFFFF), ...u16(5), ...u16(20))
  b.push(...new TextEncoder().encode('a.txt'), ...u16(1), ...u16(16), ...u64(0), ...u64(0), ...data)
  b.push(...u32(0x08074b50), ...u32(k.crc32(data)), ...u64(data.length), ...u64(data.length), ...u32(0x06054b50), ...new Array<number>(18).fill(0))
  for (const max of [1, 7, 1e6]) sameFiles(readStream(Uint8Array.from(b), max), { 'a.txt': data })
})

test('filters, nested input, directories and entry listing', () => {
  const archive = k.zip(files)
  assert.deepEqual(Object.keys(k.unzip(archive, { filter: e => e.name.endsWith('.bin') })), ['dir/b.bin'])
  assert.deepEqual(Object.keys(readStream(archive, 999, { filter: e => e.name === 'a.txt' })), ['a.txt'])
  const nested = k.unzip(k.zip({ top: { inner: { 'x.txt': new Uint8Array([1]) }, emptyDir: {} } }))
  assert.deepEqual(nested['top/inner/x.txt'], new Uint8Array([1]))
  assert.ok('top/emptyDir/' in nested)
  assert.deepEqual(k.zipEntries(archive).map(e => e.name).sort(), Object.keys(files).sort())
  const mixed = k.unzip(k.zip({ 'small.txt': [sample, { level: 0 }], 'big.txt': [sample, { level: 9, comment: 'hi' }] }))
  assert.deepEqual(mixed, { 'small.txt': sample, 'big.txt': sample })
})

test('an entry named __proto__ stays an own key and never touches the prototype', () => {
  const tricky: Record<string, Uint8Array> = {}
  Object.defineProperty(tricky, '__proto__', { value: new Uint8Array([1, 2, 3]), enumerable: true })
  const r = k.unzip(k.zip(tricky))
  assert.equal(Object.getPrototypeOf(r), Object.prototype)
  assert.ok(Object.prototype.hasOwnProperty.call(r, '__proto__'))
  assert.deepEqual(r['__proto__'], new Uint8Array([1, 2, 3]))
})

test('bad archives and misuse fail with the right code', () => {
  const archive = k.zip({ 'a.txt': sample.subarray(0, 5000) })
  const broken = archive.slice(); broken[40]! ^= 0xFF
  assert.throws(() => k.unzip(broken), k.KokyuError)
  assert.ok(throwsCode(() => k.unzip(sample.subarray(0, 100)), 'invalid-data'))
  assert.ok(throwsCode(() => readStream(archive.subarray(0, 60), 10), 'truncated'))
  const z = k.zipStream()
  z.file('a')
  assert.ok(throwsCode(() => z.file('b'), 'invalid-state'))
  assert.ok(throwsCode(() => z.finish(), 'invalid-state'))
})

test('an entry that decodes past its stated size fails early, without allocating the real size', () => {
  const archive = k.zip({ 'big.bin': new Uint8Array(4 << 20) })
  // Claim 100 bytes in the central directory, where unzip reads sizes from
  new DataView(archive.buffer, archive.byteOffset).setUint32(centralRecords(archive).at[0]! + 24, 100, true)
  const { code, largest } = failureCost(() => k.unzip(archive))
  assert.equal(code, 'too-large')
  assert.ok(largest < 1 << 16, `allocated ${largest} bytes`)
})

test('every DOS stamp reads as the instant new Date(year, month, ...) gives, in zones with odd offsets and changes', () => {
  // Hours either side of the usual change times, and impossible fields, against every date field
  const times = [[0, 0, 0], [1, 30, 0], [1, 45, 0], [2, 0, 0], [2, 15, 0], [2, 30, 0], [3, 0, 0], [12, 0, 0], [23, 59, 58], [31, 63, 62]]
    .map(([h, m, s]) => h! << 11 | m! << 5 | s! >> 1)
  const script = `const { dosDate } = await import(${JSON.stringify(new URL('../engine/zip.js', import.meta.url).href)})
    let wrong = 0, first = ''
    const days = new Map()
    for (let date = 0; date < 65536; date++) for (const time of ${JSON.stringify(times)}) {
      const want = new Date(1980 + (date >> 9), ((date >> 5) & 15) - 1, date & 31, time >> 11, (time >> 5) & 63, (time & 31) * 2)
      if (dosDate(time, date, days).getTime() !== want.getTime()) { wrong++; first ||= date + ' ' + time }
    }
    console.log(wrong ? wrong + ' wrong, first ' + first : 'ok')`
  for (const tz of ['UTC', 'Europe/Stockholm', 'America/New_York', 'America/St_Johns', 'Australia/Lord_Howe', 'Asia/Kathmandu',
    'Africa/Casablanca', 'Pacific/Apia', 'Asia/Gaza']) {
    const out = String(execFileSync(process.execPath, ['--input-type=module', '-e', script], { env: { ...process.env, TZ: tz } })).trim()
    assert.equal(out, 'ok', tz)
  }
})

test('zip() of the most entries kokyu writes needs no more stack than zipStream', () => {
  const url = new URL('../engine/index.js', import.meta.url).href
  const script = `import('${url}').then(k => { const f = {}; for (let i = 0; i < 65534; i++) f[i] = new Uint8Array(0); k.zip(f) })`
  // A child on a small stack throws, and so fails execFileSync, if each record became a call argument
  execFileSync(process.execPath, ['--stack-size=500', '--input-type=module', '-e', script], { timeout: 60_000 })
})

test('a name or comment without the UTF-8 flag that isn\'t UTF-8 reads as code page 437, as the spec and Python read it', () => {
  const z = k.zip({ 'cafX.txt': [Uint8Array.of(1), { comment: 'costs 5Y' }] }, { mtime: new Date(2020, 0, 1) })
  const v = new DataView(z.buffer), cd = v.getUint32(z.length - 6, true)
  const find = (t: string): number[] => {
    const b = k.encodeText(t), found: number[] = []
    for (let i = 0; i + b.length <= z.length; i++) if (b.every((x, j) => z[i + j] === x)) found.push(i)
    return found
  }
  // As DOS wrote them: 0x82 is é and 0x9C is £ in code page 437, and the UTF-8 flag is clear
  for (const i of find('cafX.txt')) z[i + 3] = 0x82
  for (const i of find('costs 5Y')) z[i + 7] = 0x9C
  for (const at of [6, cd + 8]) v.setUint16(at, v.getUint16(at, true) & ~0x800, true)
  assert.deepEqual(k.zipEntries(z).map(e => [e.name, e.comment]), [['café.txt', 'costs 5£']])
  assert.deepEqual(k.unzipStream().finish(z).filter(c => c.final).map(c => c.entry.name), ['café.txt'])
})

test('an archive whose 16-bit entry count wrapped past 65535, from a writer without ZIP64, reads every entry', () => {
  // Python writes 65537 entries with ZIP64, then the end record is rewritten as an older writer leaves it: count 1
  const script = `import io, struct, sys, zipfile
b = io.BytesIO()
with zipfile.ZipFile(b, 'w') as z:
    for i in range(65537): z.writestr(str(i), b'')
d = b.getvalue()
e = d.rfind(b'PK\\x05\\x06')
z64 = struct.unpack('<Q', d[e - 12:e - 4])[0]
n, size, off = struct.unpack('<QQQ', d[z64 + 32:z64 + 56])
end = bytearray(d[e:e + 22])
struct.pack_into('<HHII', end, 8, n & 0xFFFF, n & 0xFFFF, size, off)
sys.stdout.buffer.write(d[:z64] + bytes(end))`
  const wrapped = new Uint8Array(python(script))
  assert.equal(k.zipEntries(wrapped).length, 65537)
  assert.equal(Object.keys(k.unzip(wrapped)).length, 65537)
  // A count that falls short of the records some other way is broken, not a reason to drop entries
  const two = k.zip({ a: Uint8Array.of(1), b: Uint8Array.of(2) }), v = new DataView(two.buffer)
  for (const at of [two.length - 14, two.length - 12]) v.setUint16(at, 1, true)
  assert.throws(() => k.zipEntries(two), { code: 'invalid-data' })
})

// plain's records under a ZIP64 end record, its locator, and an end record whose fields are all saturated. `set` can
// change that 98-byte tail: the ZIP64 record is at 0, its locator at 56.
function asZip64(plain: Uint8Array, count: number, set: (tail: DataView) => void = () => {}): Uint8Array {
  const pv = new DataView(plain.buffer, plain.byteOffset), e = plain.length - 22, size = pv.getUint32(e + 12, true), at = pv.getUint32(e + 16, true)
  const tail = new Uint8Array(98), v = new DataView(tail.buffer)
  v.setUint32(0, 0x06064B50, true); v.setBigUint64(4, 44n, true); v.setUint16(12, 45, true); v.setUint16(14, 45, true)
  v.setBigUint64(24, BigInt(count), true); v.setBigUint64(32, BigInt(count), true)
  v.setBigUint64(40, BigInt(size), true); v.setBigUint64(48, BigInt(at), true)
  v.setUint32(56, 0x07064B50, true); v.setBigUint64(64, BigInt(at + size), true); v.setUint32(72, 1, true)
  v.setUint32(76, 0x06054B50, true); v.setUint16(84, 0xFFFF, true); v.setUint16(86, 0xFFFF, true)
  v.setUint32(88, 0xFFFFFFFF, true); v.setUint32(92, 0xFFFFFFFF, true)
  set(v)
  return cat([plain.subarray(0, at + size), tail])
}

test('a ZIP64 entry count short of its directory\'s records fails, as a plain one does, rather than hiding entries', () => {
  const plain = k.zip({ 'a.txt': k.encodeText('a'), 'b.txt': k.encodeText('b'), 'c.txt': k.encodeText('c') }, { mtime: new Date(2020, 0, 1) })
  assert.deepEqual(k.zipEntries(asZip64(plain, 3)).map(x => x.name), ['a.txt', 'b.txt', 'c.txt'])
  assert.throws(() => k.zipEntries(asZip64(plain, 2)), { code: 'invalid-data' })
  assert.throws(() => k.unzip(asZip64(plain, 2)), { code: 'invalid-data' })
})

test('one part of a split archive fails as unsupported, rather than reading other parts\' offsets in this one', () => {
  const plain = k.zip({ 'a.txt': k.encodeText('a'), 'b.txt': k.encodeText('b') }, { mtime: new Date(2020, 0, 1) })
  // This part's number, and the part the central directory starts in
  for (const at of [4, 6]) {
    const part = plain.slice()
    new DataView(part.buffer).setUint16(part.length - 22 + at, 1, true)
    for (const read of [k.zipEntries, k.unzip]) assert.throws(() => read(part), { code: 'unsupported' })
  }
  // In ZIP64: the record's two part numbers, the locator's part, and its count of parts
  for (const [at, value] of [[16, 1], [20, 1], [60, 1], [72, 2]] as const) {
    assert.throws(() => k.zipEntries(asZip64(plain, 2, v => v.setUint32(at, value, true))), { code: 'unsupported' }, `at ${at}`)
  }
  assert.deepEqual(k.zipEntries(asZip64(plain, 2)).map(e => e.name), ['a.txt', 'b.txt'])
})

test('an entry flagged as strong encryption or patch data fails as unsupported, rather than reading as the file', () => {
  const z = k.zip({ 'a.txt': [k.encodeText('hello'), { level: 0 }] }, { mtime: new Date(2020, 0, 1) })
  for (const bit of [0x20, 0x40]) {
    const flagged = z.slice(), v = new DataView(flagged.buffer), at = v.getUint32(flagged.length - 22 + 16, true)
    v.setUint16(6, v.getUint16(6, true) | bit, true)
    v.setUint16(at + 8, v.getUint16(at + 8, true) | bit, true)
    assert.throws(() => k.unzip(flagged), { code: 'unsupported' })
    assert.throws(() => k.unzipStream().finish(flagged), { code: 'unsupported' })
  }
})

test('an archive that opens with a split marker, as zip -s writes one that fits in a single part, reads in every reader', () => {
  const plain = k.zip({ 'a.txt': sample.subarray(0, 3000), 'b.txt': [k.encodeText('b'), { level: 0 }] }, { mtime: new Date(2020, 0, 1) })
  for (const marker of [0x08074B50, 0x30304B50]) {
    // The marker comes first, and every offset moves past it
    const z = new Uint8Array(plain.length + 4), v = new DataView(z.buffer), e = z.length - 22
    v.setUint32(0, marker, true)
    z.set(plain, 4)
    const at = v.getUint32(e + 16, true) + 4
    v.setUint32(e + 16, at, true)
    for (let i = 0, p = at; i < v.getUint16(e + 10, true); i++) {
      v.setUint32(p + 42, v.getUint32(p + 42, true) + 4, true)
      p += 46 + v.getUint16(p + 28, true) + v.getUint16(p + 30, true) + v.getUint16(p + 32, true)
    }
    const whole = k.unzip(z)
    assert.deepEqual(Object.keys(whole), ['a.txt', 'b.txt'])
    for (const max of [1, 7, z.length]) assert.deepEqual(joinChunks(pushChunked(k.unzipStream(), z, max, 3).flat()), whole)
  }
})

test('entries default to Unix mode 0o644, or 0o755 for a folder, unless an MS-DOS host is asked for', () => {
  const d = k.encodeText('x'), modes = (z: Uint8Array): unknown => k.zipEntries(z).map(e => [e.name, e.mode])
  assert.deepEqual(modes(k.zip({ 'a.txt': d, dir: {} })), [['a.txt', 0o644], ['dir/', 0o755]])
  // os 3 without attrs gets the same, not the mode 000 the MS-DOS default's attrs would read as
  const z = k.zipStream(), parts = [z.file('a.txt', { os: 3 }).finish(d), z.file('dir/', { os: 3 }).finish(), z.finish()]
  assert.deepEqual(modes(cat(parts)), [['a.txt', 0o644], ['dir/', 0o755]])
  assert.deepEqual(modes(k.zip({ 'a.txt': [d, { os: 0 }], 'b.txt': [d, { os: 3, attrs: 0o100600 << 16 }] })), [['a.txt', undefined], ['b.txt', 0o600]])
})
