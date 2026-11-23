// ZIP archives (PKWARE APPNOTE), stored and deflated entries. Reading understands ZIP64,
// data descriptors and non-UTF-8 names; writing covers everything below 4 GB and 65535 entries.

import { deflater, type Deflater } from './deflate.js'
import { inflater, inflateRawEnd, type Inflater } from './inflate.js'
import { crc32 } from './checksum.js'
import { encodeText, decodeName } from './text.js'
import {
  BUSY, EMPTY, FINISHED, bytes, checkLevel, checkMaxSize, concat, copy, fail, grow, isBytes, readU16LE, readU32LE, readU64LE, shown,
  writeU16LE, writeU32LE, type Level, type Stream,
} from './shared.js'

/** Options for one zip entry, overriding the archive's */
export interface ZipEntryOptions {
  /** 0 stores, 1 is fastest, 9 is smallest. Default: the archive's level, or 6. */
  level?: Level | undefined
  /** Modification time, from 1980 to 2107. Default: the archive's mtime, or the current time. */
  mtime?: Date | string | number | undefined
  /** A comment stored with the entry */
  comment?: string | undefined
  /**
   * Raw external attributes, read by `os`'s rules. For Unix permissions, `mode` is simpler. Default: Unix mode 0o644,
   * or 0o755 for a folder, or with `os` 0 a plain MS-DOS file or folder.
   */
  attrs?: number | undefined
  /** The system that made the entry, which tells readers how to read `attrs`: 3 is Unix (the default), 0 is MS-DOS */
  os?: number | undefined
  /** Extra fields by header ID, each as its data bytes */
  extra?: Record<number, Uint8Array> | undefined
  /** Unix permission bits, such as 0o644 or 0o755. It sets `os` and `attrs` for you, so it can't be combined with them. */
  mode?: number | undefined
}

/** Options for a whole zip archive */
export interface ZipOptions {
  /** 0 stores, 1 is fastest, 9 is smallest. Default 6. */
  level?: Level | undefined
  /** Modification time for every entry, from 1980 to 2107. Default: the current time. */
  mtime?: Date | string | number | undefined
  /** The archive's comment, up to 16,361 bytes: bsdtar finds an archive's end only in its last 16 KB */
  comment?: string | undefined
}

/**
 * The files to zip: each path maps to its bytes, to `[bytes, options]`, or to a nested folder of the same shape.
 * An empty folder becomes a directory entry. Folders are separated by `/`. A name that extractors would put on another
 * path fails with `invalid-option`: an empty one, one holding `\` or a control character, one starting with `/` or a
 * drive letter or holding an empty, `.` or `..` folder, and a file name ending in `;` and digits. So do two names for
 * one path, such as a file and a folder of the same name.
 */
export interface ZipInput {
  [path: string]: Uint8Array | readonly [Uint8Array, ZipEntryOptions] | ZipInput
}

/** What a zip stream knows about an entry when it reaches it */
export interface ZipEntryInfo {
  /** The full path, with `/` between folders; a directory's ends in `/` */
  name: string
  /** Compressed size. Missing in a stream when the entry's sizes follow its data: treat that as any size. */
  size?: number | undefined
  /** Original size. Missing in a stream when the entry's sizes follow its data: treat that as any size. */
  originalSize?: number | undefined
  /** 0 is stored, 8 is deflate. Other methods can be listed but not read. */
  compression: number
  /** Modification time, in local time to the 2 seconds zip stores */
  mtime: Date
}

/** An entry as the archive's central directory lists it, which always has both sizes */
export interface ZipEntry extends ZipEntryInfo {
  size: number
  originalSize: number
  /** The entry's comment, or '' */
  comment: string
  /** Unix permission bits, such as 0o755, when the entry was made on Unix */
  mode?: number | undefined
}

/** Options for reading a whole zip */
export interface UnzipOptions {
  /** Which entries to read: return false to skip one, which is then never decompressed */
  filter?: ((entry: ZipEntry) => boolean) | undefined
  /** Fail with `too-large` once the entries read would add up to more than this many bytes. No cap by default. */
  maxSize?: number | undefined
}

/** Options for reading a zip as a stream */
export interface UnzipStreamOptions {
  /**
   * Which entries to read: return false to skip one. Sizes may be missing here, when they follow the entry's data;
   * a deflated entry like that is still decompressed when skipped, since only that finds where it ends.
   */
  filter?: ((entry: ZipEntryInfo) => boolean) | undefined
  /** Fail with `too-large` once the entries read would add up to more than this many bytes. No cap by default. */
  maxSize?: number | undefined
}

/** A piece of one entry's data from a zip stream */
export interface UnzipChunk {
  /** The entry this data belongs to: the same object for every chunk of it, so it can key a Map. Entries come one at a time. */
  entry: ZipEntryInfo
  /** The next piece of the entry, possibly empty */
  data: Uint8Array<ArrayBuffer>
  /** True on the entry's last chunk */
  final: boolean
}

// Internal: in ask mode, the header of an entry waiting for decide()
export interface AskChunk extends UnzipChunk { ask?: true }

const LOCAL = 0x04034b50
const CENTRAL = 0x02014b50
const END = 0x06054b50
const END64 = 0x06064b50
const LOCATOR64 = 0x07064b50
const DESCRIPTOR = 0x08074b50
// A split archive's first part opens with the descriptor signature, or with this when it fit in one (APPNOTE 8.5.4)
const SPLIT_ONE = 0x30304b50
const MAX32 = 0xFFFFFFFF
const UTF8 = 0x800
const HAS_DESCRIPTOR = 8
// Messages more than one place fails with; the first three are zipStreamAsync's too
export const ENTRY_OPEN = 'finish the open zip entry before starting another'
export const ENTRY_OPEN_AT_END = 'finish the open zip entry before the archive'
export const ENTRY_DONE = 'this zip entry is already finished'
export const LOST = 'a canceled pipe dropped bytes of this zip archive, so it can\'t be finished'
// Keys a zip entry stream's hook for toTransformStream. A failed pipe leaves the entry open, since finishing it writes
// into an archive the caller goes on with, and a cancel that drops bytes the entry returned calls the hook.
export const ENTRY: unique symbol = Symbol('kokyu zip entry')
const TOO_MANY = 'the entries add up to more than maxSize'
const BROKEN_ZIP64 = 'broken ZIP64 extra field'
const BROKEN_CENTRAL = 'broken central directory'
const wrongSize = (name: string): never => fail('invalid-data', `${quote(name)} has the wrong size`)
const endsEarly = (name: string): never => fail('truncated', `${quote(name)} ends early`)
// Deflate can't expand data by more than this factor, so claimed sizes above it are rejected before allocating
const MAX_RATIO = 1032

// Names from archives are quoted in messages: no line break, escape code or bidi control can forge a log line.
// JSON.stringify escapes controls only up to U+001F; the rest are escaped here, so the quote parses back to the name.
const quote = (name: string): string =>
  JSON.stringify(name).replace(/[\u007F-\u009F\u061C\u200E\u200F\u2028\u2029\u202A-\u202E\u2066-\u2069]/g, c => `\\u${c.charCodeAt(0).toString(16).padStart(4, '0')}`)

interface Entry {
  name: Uint8Array
  comment: Uint8Array
  extra: Uint8Array
  flags: number
  method: number
  time: number
  date: number
  crc: number
  csize: number
  usize: number
  offset: number
  attrs: number
  os: number
  level: number
}

const DAY = 86_400_000
// Days from 1970 to the first of each DOS month, by year * 12 + month field (0 to 15, rolling over as Date does)
let months: Int32Array | undefined

// A local day's midnight in one archive read, once a second entry falls on it (SEEN after the first), or NaN when
// the day isn't DAY long, as an offset change makes it. new Date(year, month, ...) costs 4x copying a Date.
const SEEN = Infinity
function dayStart(day: number, days: Map<number, number>): number {
  const at = days.get(day)
  if (at === undefined) days.set(day, SEEN)
  if (at !== SEEN) return at ?? NaN
  const start = new Date(1970, 0, 1 + day).getTime()
  const uniform = new Date(1970, 0, 2 + day).getTime() - start === DAY ? start : NaN
  days.set(day, uniform)
  return uniform
}

// The inverse of dosTime, giving what new Date(year, month, ...) does for the stamp's fields, impossible ones included
export function dosDate(time: number, date: number, days: Map<number, number>): Date {
  months ??= Int32Array.from({ length: 127 * 12 + 16 }, (_, i) => Date.UTC(1980, i - 1, 1) / DAY)
  const day = months[(date >> 9) * 12 + ((date >> 5) & 15)]! + (date & 31) - 1
  const t = (time >> 11) * 3_600_000 + ((time >> 5) & 63) * 60_000 + (time & 31) * 2000, start = dayStart(day, days)
  return t < DAY && !Number.isNaN(start) ? new Date(start + t)
    : new Date(1980 + (date >> 9), ((date >> 5) & 15) - 1, date & 31, time >> 11, (time >> 5) & 63, (time & 31) * 2)
}

function dosTime(mtime: Date | string | number | undefined): [number, number] {
  const d = new Date(mtime ?? Date.now())
  const year = d.getFullYear()
  if (!(year >= 1980 && year <= 2107)) fail('invalid-option', 'zip dates must fall between 1980 and 2107')
  return [
    (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
    ((year - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  ]
}

function extraField(extra: Record<number, Uint8Array> | undefined): Uint8Array<ArrayBuffer> {
  if (!extra) return EMPTY
  if (typeof extra !== 'object') fail('invalid-option', 'zip extra fields must map 16-bit ids to bytes')
  const parts: Uint8Array<ArrayBuffer>[] = []
  for (const [key, data] of Object.entries(extra)) {
    const id = Number(key)
    if (!Number.isInteger(id) || id < 0 || id > 0xFFFF || !isBytes(data) || data.length > 0xFFFF) {
      fail('invalid-option', 'zip extra fields need a 16-bit id and at most 65535 bytes')
    }
    // Readers size a header's data descriptor by this field, and kokyu doesn't write ZIP64 (APPNOTE 4.3.9.2)
    if (id === 1) fail('invalid-option', 'zip extra field 1 is ZIP64\'s, which kokyu doesn\'t write')
    // Python takes the entry's name from a Unicode Path field whose CRC matches the header name's, kokyu never does
    if (id === 0x7075) fail('invalid-option', 'zip extra field 0x7075 would rename the entry for some readers')
    const field = new Uint8Array(4 + data.length)
    writeU16LE(field, 0, id)
    writeU16LE(field, 2, data.length)
    field.set(bytes(data, `zip extra field 0x${id.toString(16)}`), 4)
    parts.push(field)
  }
  const all = parts.length ? concat(parts) : EMPTY
  if (all.length > 0xFFFF) fail('invalid-option', 'zip extra fields add up to more than 65535 bytes')
  return all
}

// Zip text fields hold at most 65535 bytes
function zipText(text: string, what: string): Uint8Array<ArrayBuffer> {
  const b = encodeText(text)
  if (b.length > 0xFFFF) fail('invalid-option', `zip ${what} are limited to 65535 bytes`)
  return b
}

// An entry's own options, then the archive's, then the defaults. `now` is an async call's own time: a worker may run it
// much later.
function makeEntry(name: string, opts: ZipEntryOptions, archive: ZipOptions, now?: number): Entry {
  const nameBytes = zipText(name, 'names'), comment = zipText(opts.comment ?? '', 'comments')
  // Python and Info-ZIP end a name at its first NUL, and Info-ZIP drops the other C0 controls and DEL, so they would
  // extract another name
  if (/[\0-\x1F\x7F]/.test(name)) fail('invalid-option', 'zip entry names can\'t hold a NUL or another control character')
  // Info-ZIP lists and extracts an empty name as the name of the entry before it
  if (!nameBytes.length) fail('invalid-option', 'zip entry names can\'t be empty')
  // Info-ZIP (for MS-DOS hosts) and bsdtar (for any) extract \ as /, so a\b would land on a/b
  if (name.includes('\\')) fail('invalid-option', 'zip entry names separate folders with /, never \\')
  // Extractors drop a leading / or drive and every empty, . or .. folder: ./a and /a land on a, x//a and x/../a on x/a
  if (/^[A-Za-z]:|(^|\/)\.{0,2}(\/|$)/.test(name.replace(/\/$/, ''))) fail('invalid-option', 'zip entry names can\'t start with / or a drive, or hold an empty, . or .. folder')
  // Info-ZIP takes a file name's last ; and the digits after it for a VMS version and drops them: a;1 lands on a
  if (/;\d*$/.test(name)) fail('invalid-option', 'zip entry names can\'t end in ; and digits')
  const [time, date] = dosTime(opts.mtime ?? archive.mtime ?? now)
  const dir = name.endsWith('/'), mode = opts.mode
  if (mode != null) {
    if (!Number.isInteger(mode) || mode < 0 || mode > 0o7777) fail('invalid-option', `zip mode must be 0 to 0o7777, got ${shown(mode)}`)
    if (opts.os != null || opts.attrs != null) fail('invalid-option', 'zip mode sets os and attrs, so it can\'t be combined with them')
  }
  // A Unix mode goes in the high half of attrs with the file type, file or directory. Unix (os 3) is the default, as
  // Python and Info-ZIP write on Unix: Apple's unzip can't extract a non-ASCII name an MS-DOS host wrote.
  const os = mode != null ? 3 : opts.os ?? 3
  // Unix readers take the high half as the mode, so an entry without attrs gets one rather than 0
  const unix = mode ?? (os === 3 && opts.attrs == null ? (dir ? 0o755 : 0o644) : null)
  const attrs = unix != null ? ((dir ? 0o040000 : 0o100000) | unix) << 16 | (dir ? 0x10 : 0) : opts.attrs ?? (dir ? 0x10 : 0)
  // Any 32-bit pattern goes for attrs: a Unix mode shifted up, like 0o100644 << 16, comes out negative
  if (!Number.isInteger(os) || os < 0 || os > 0xFF) fail('invalid-option', `zip os must be 0 to 255, got ${shown(os)}`)
  if (!Number.isInteger(attrs) || attrs < -0x80000000 || attrs > 0xFFFFFFFF) fail('invalid-option', `zip attrs must fit in 32 bits, got ${shown(attrs)}`)
  const nonAscii = nameBytes.some(b => b > 0x7F) || comment.some(b => b > 0x7F)
  const extra = extraField(opts.extra), level = opts.level ?? archive.level ?? 6
  checkLevel(level)
  return { name: nameBytes, comment, extra, flags: nonAscii ? UTF8 : 0, method: 0, time, date, crc: 0, csize: 0, usize: 0, offset: 0, os, attrs, level }
}

function localHeader(e: Entry): Uint8Array<ArrayBuffer> {
  const h = new Uint8Array(30 + e.name.length + e.extra.length)
  writeU32LE(h, 0, LOCAL)
  writeU16LE(h, 4, 20)
  writeU16LE(h, 6, e.flags)
  writeU16LE(h, 8, e.method)
  writeU16LE(h, 10, e.time)
  writeU16LE(h, 12, e.date)
  // With a data descriptor these stay zero and the real values follow the data
  if (!(e.flags & HAS_DESCRIPTOR)) {
    writeU32LE(h, 14, e.crc)
    writeU32LE(h, 18, e.csize)
    writeU32LE(h, 22, e.usize)
  }
  writeU16LE(h, 26, e.name.length)
  writeU16LE(h, 28, e.extra.length)
  h.set(e.name, 30)
  h.set(e.extra, 30 + e.name.length)
  return h
}

function centralRecord(e: Entry): Uint8Array<ArrayBuffer> {
  // 0xFFFFFFFF itself means "see the ZIP64 field", so it's out of range too
  if (e.usize >= MAX32 || e.csize >= MAX32 || e.offset >= MAX32) fail('too-large', 'entries past 4 GB need ZIP64, which kokyu reads but doesn\'t write')
  const h = new Uint8Array(46 + e.name.length + e.extra.length + e.comment.length)
  writeU32LE(h, 0, CENTRAL)
  writeU16LE(h, 4, (e.os << 8) | 20)
  writeU16LE(h, 6, 20)
  writeU16LE(h, 8, e.flags)
  writeU16LE(h, 10, e.method)
  writeU16LE(h, 12, e.time)
  writeU16LE(h, 14, e.date)
  writeU32LE(h, 16, e.crc)
  writeU32LE(h, 20, e.csize)
  writeU32LE(h, 24, e.usize)
  writeU16LE(h, 28, e.name.length)
  writeU16LE(h, 30, e.extra.length)
  writeU16LE(h, 32, e.comment.length)
  writeU32LE(h, 38, e.attrs)
  writeU32LE(h, 42, e.offset)
  h.set(e.name, 46)
  h.set(e.extra, 46 + e.name.length)
  h.set(e.comment, 46 + e.name.length + e.extra.length)
  return h
}

function endRecord(records: Uint8Array[], cdOffset: number, comment: Uint8Array): Uint8Array<ArrayBuffer> {
  let cdSize = 0
  for (const r of records) cdSize += r.length
  if (records.length >= 0xFFFF || cdOffset >= MAX32 || cdOffset + cdSize > MAX32) fail('too-large', 'this archive needs ZIP64, which kokyu reads but doesn\'t write')
  // Readers that know ZIP64 look for its locator in the 20 bytes before the end record. Put there by a name or comment,
  // one would point them at a central directory of its own.
  const last = records[records.length - 1]
  if (last && readU32LE(last, last.length - 20) === LOCATOR64) fail('invalid-option', 'the last entry ends in what readers take for a ZIP64 locator')
  const h = new Uint8Array(22 + comment.length)
  writeU32LE(h, 0, END)
  writeU16LE(h, 8, records.length)
  writeU16LE(h, 10, records.length)
  writeU32LE(h, 12, cdSize)
  writeU32LE(h, 16, cdOffset)
  writeU16LE(h, 20, comment.length)
  h.set(comment, 22)
  // With a comment, readers search back for the signature, so one spelled by a size or offset field is found first.
  // From byte 10: before it, the signature, the zero disk numbers and the count written twice can't spell another.
  if (comment.length) for (let i = 10; i < 20; i++) if (readU32LE(h, i) === END) fail('invalid-option', 'this archive can\'t have a comment: its sizes spell an end record signature')
  return h
}

// Readers find the end record by searching back from the end, so one inside the comment would be taken for it. One
// starting in the length field before it would need a comment longer than this.
const MAX_COMMENT = (1 << 14) - 23
function archiveComment(comment: string | undefined): Uint8Array<ArrayBuffer> {
  const b = zipText(comment ?? '', 'comments')
  // bsdtar looks for the end record only in an archive's last 16 KB; past that it reads the local headers, which hold
  // no modes, so executables and symlinks extract as plain files
  if (b.length > MAX_COMMENT) fail('invalid-option', `zip archive comments are limited to ${MAX_COMMENT} bytes`)
  for (let i = 0; i + 4 <= b.length; i++) if (readU32LE(b, i) === END) fail('invalid-option', 'a zip comment can\'t hold an end record signature')
  return b
}

type ZipFile = [path: string, data: Uint8Array, opts: ZipEntryOptions]

// The path a name extracts to, which is what makes two names one: UTF-8 turns every lone surrogate into U+FFFD, and
// a folder's / names the path a file of that name would take (bsdtar silently swaps one for the other)
const storedName = (name: string): string => (/[\uD800-\uDFFF]/.test(name) ? decodeName(encodeText(name), true) : name).replace(/\/$/, '')

// A plain object from any realm: its prototype is null, or a realm's Object.prototype (the one with no prototype)
const isFolder = (v: unknown): v is ZipInput => {
  if (typeof v !== 'object' || v === null) return false
  const proto = Object.getPrototypeOf(v) as object | null
  return proto === null || Object.getPrototypeOf(proto) === null
}

// A view's length now and for good: one over shared memory that another thread grows would otherwise grow midway
const pinned = (v: Uint8Array, path: string): Uint8Array => bytes(v, `zip input ${quote(path)}`).subarray(0, v.length)

// The input as a flat list of entries, each path once. Folders end in '/'.
export function zipFiles(input: ZipInput, prefix = '', out: ZipFile[] = [], seen: Map<string, string> = new Map()): ZipFile[] {
  if (!isFolder(input)) fail('invalid-option', 'zip input must be an object mapping paths to files')
  for (const [key, value] of Object.entries(input)) {
    const path = prefix + key
    let file: ZipFile
    if (isBytes(value)) {
      file = [path, pinned(value, path), {}]
    } else if (Array.isArray(value) && isBytes(value[0]) && (value[1] == null || typeof value[1] === 'object')) {
      file = [path, pinned(value[0], path), (value[1] ?? {}) as ZipEntryOptions]
    } else if (isFolder(value)) {
      if (Object.keys(value).length) { zipFiles(value, path + '/', out, seen); continue }
      file = [path.endsWith('/') ? path : path + '/', EMPTY, {}]
    } else {
      return fail('invalid-option', `zip input ${quote(path)} must be bytes, [bytes, options] or a folder`)
    }
    const stored = storedName(file[0])
    const first = seen.get(stored)
    if (first !== undefined) fail('invalid-option', `zip input names ${first === file[0] ? `${quote(first)} twice` : `${quote(first)} and ${quote(file[0])}, which extract to one path`}`)
    seen.set(stored, file[0])
    out.push(file)
  }
  return out
}

/**
 * Builds a zip archive. Data that compression can't shrink is stored instead.
 * @example const archive = zip({ 'hello.txt': encodeText('hi'), images: { 'a.png': [png, { level: 0 }] } })
 */
export function zip(input: ZipInput, opts: ZipOptions = {}): Uint8Array<ArrayBuffer> { return zipList(zipFiles(input), opts) }

// An archive of entries zipFiles() has listed and checked
export function zipList(files: ZipFile[], opts: ZipOptions | null, now?: number): Uint8Array<ArrayBuffer> {
  opts ??= {}
  // A stored entry's bytes stay the caller's until the final concat, so every option is read first: a getter run
  // later could detach or shrink them, leaving headers that claim bytes the archive doesn't hold
  const comment = archiveComment(opts.comment), entries = files.map(([name, , own]) => makeEntry(name, own, opts, now))
  const parts: Uint8Array[] = []
  const records: Uint8Array<ArrayBuffer>[] = []
  let offset = 0
  for (let i = 0; i < files.length; i++) {
    const e = entries[i]!, data = files[i]![1]
    let body = data
    // Data that deflate can't shrink is stored instead
    if (e.level > 0 && data.length) {
      const packed = deflater(e.level).push(data, true)
      if (packed.length < data.length) { body = packed; e.method = 8 }
    }
    e.crc = crc32(data)
    e.csize = body.length
    e.usize = data.length
    e.offset = offset
    const head = localHeader(e)
    records.push(centralRecord(e))
    parts.push(head, body)
    offset += head.length + body.length
  }
  // Stored entries are the caller's bytes, but the end record always follows them, so concat copies everything.
  // Spread into an array, not into push(): 65534 records as arguments need half the stack.
  return concat([...parts, ...records, endRecord(records, offset, comment)]) as Uint8Array<ArrayBuffer>
}

// Reading

interface CentralEntry {
  info: ZipEntry
  flags: number
  offset: number
}

// A reader of the ZIP64 extra field's 8-byte values in order (APPNOTE 4.5.3), or null without the field.
// Every value must lie inside the field, and the field inside the extra area.
function zip64Values(d: Uint8Array, start: number, len: number): (() => number) | null {
  for (let q = start, end = start + len; q + 4 <= end; q += 4 + readU16LE(d, q + 2)) {
    if (readU16LE(d, q) !== 1) continue
    let at = q + 4
    const to = at + readU16LE(d, q + 2)
    if (to > end) fail('invalid-data', BROKEN_ZIP64)
    return () => {
      if (at + 8 > to) fail('invalid-data', BROKEN_ZIP64)
      at += 8
      return readU64LE(d, at - 8)
    }
  }
  return null
}

function findEnd(d: Uint8Array): number {
  for (let p = d.length - 22, stop = Math.max(0, d.length - 22 - 0xFFFF); p >= stop; p--) {
    if (readU32LE(d, p) === END) return p
  }
  return fail('invalid-data', 'not a zip archive (no end of central directory record)')
}

function readCentral(d: Uint8Array): CentralEntry[] {
  const e = findEnd(bytes(d))
  let count = readU16LE(d, e + 10)
  let p = readU32LE(d, e + 16)
  // Writers without ZIP64 let the 16-bit count wrap past 65535 entries, so records go on to the directory's stated end
  let end = p + readU32LE(d, e + 12)
  // Only a field too small for its value calls for ZIP64. Otherwise the 20 bytes before the end record belong to the
  // last entry, whose name or comment could fake a locator.
  const saturated = count === 0xFFFF || readU32LE(d, e + 12) === MAX32 || p === MAX32
  // The last part of a split archive lists every part's entries at offsets within their own part, so read alone it
  // would hand out whatever sits at those offsets here
  let split = readU16LE(d, e + 4) !== 0 || readU16LE(d, e + 6) !== 0
  if (saturated && e >= 20 && readU32LE(d, e - 20) === LOCATOR64) {
    const z = readU64LE(d, e - 12)
    if (z + 56 > d.length || readU32LE(d, z) !== END64) fail('invalid-data', 'broken ZIP64 end record')
    split = readU32LE(d, e - 16) !== 0 || readU32LE(d, e - 4) > 1 || readU32LE(d, z + 16) !== 0 || readU32LE(d, z + 20) !== 0
    count = readU64LE(d, z + 32)
    p = readU64LE(d, z + 48)
    end = p + readU64LE(d, z + 40)
  }
  if (split) fail('unsupported', 'zip archives split into several files can\'t be read')
  const out: CentralEntry[] = [], days = new Map<number, number>()
  for (let i = 0; i < count || (p < end && readU32LE(d, p) === CENTRAL); i++) {
    if (p + 46 > d.length || readU32LE(d, p) !== CENTRAL) fail('invalid-data', BROKEN_CENTRAL)
    const flags = readU16LE(d, p + 8)
    const nl = readU16LE(d, p + 28), el = readU16LE(d, p + 30), cl = readU16LE(d, p + 32)
    if (p + 46 + nl + el + cl > d.length) fail('invalid-data', BROKEN_CENTRAL)
    let csize = readU32LE(d, p + 20), usize = readU32LE(d, p + 24), offset = readU32LE(d, p + 42)
    // ZIP64 extra field: whichever of these overflowed 32 bits, in this order
    if (usize === MAX32 || csize === MAX32 || offset === MAX32) {
      const next = zip64Values(d, p + 46 + nl, el)
      if (next) {
        if (usize === MAX32) usize = next()
        if (csize === MAX32) csize = next()
        if (offset === MAX32) offset = next()
      }
    }
    const utf8 = (flags & UTF8) !== 0, at = p + 46 + nl + el, attrs = readU32LE(d, p + 38)
    const info: ZipEntry = {
      name: decodeName(d.subarray(p + 46, p + 46 + nl), utf8), size: csize, originalSize: usize, compression: readU16LE(d, p + 10),
      mtime: dosDate(readU16LE(d, p + 12), readU16LE(d, p + 14), days), comment: cl ? decodeName(d.subarray(at, at + cl), utf8) : '',
    }
    // Made on Unix (os 3), with a mode in the high half of attrs
    if (d[p + 5] === 3 && attrs >>> 16) info.mode = (attrs >>> 16) & 0o7777
    out.push({ info, flags, offset })
    p += 46 + nl + el + cl
  }
  if ((out.length - count) % 0x10000) fail('invalid-data', BROKEN_CENTRAL)
  return out
}

// Fails unless kokyu can decode the entry
function checkSupported({ name, compression }: ZipEntryInfo, flags: number): void {
  // Bit 6 is strong encryption, and bit 5 data that patches another file rather than the file itself
  if (flags & 0x41) fail('unsupported', `${quote(name)} is encrypted`)
  if (flags & 0x20) fail('unsupported', `${quote(name)} holds patch data`)
  if (compression !== 0 && compression !== 8) fail('unsupported', `compression method ${compression} in ${quote(name)}`)
}

// The span of an entry's data, once its method and local header check out
function dataSpan(d: Uint8Array, c: CentralEntry): [number, number] {
  const { info } = c
  checkSupported(info, c.flags)
  const o = c.offset
  if (o + 30 > d.length || readU32LE(d, o) !== LOCAL) fail('invalid-data', `broken local header for ${quote(info.name)}`)
  const start = o + 30 + readU16LE(d, o + 26) + readU16LE(d, o + 28)
  if (start + info.size > d.length) endsEarly(info.name)
  return [start, start + info.size]
}

function entryData(raw: Uint8Array, info: ZipEntry): Uint8Array<ArrayBuffer> {
  let bytes: Uint8Array<ArrayBuffer>
  if (info.compression === 0) {
    bytes = copy(raw)
  } else {
    if (info.originalSize > info.size * MAX_RATIO + 1024) fail('invalid-data', `${quote(info.name)} claims an impossible size`)
    bytes = inflateRawEnd(raw, { sizeHint: info.originalSize, limit: info.originalSize }).bytes
  }
  if (bytes.length !== info.originalSize) wrongSize(info.name)
  return bytes
}

// Defined rather than assigned, so an entry named __proto__ can't touch the prototype
const define = (o: Record<string, Uint8Array<ArrayBuffer>>, name: string, value: Uint8Array<ArrayBuffer>): void => {
  Object.defineProperty(o, name, { value, enumerable: true, writable: true, configurable: true })
}

/**
 * Extracts a zip archive into an object of paths and their bytes. Directories appear with empty data.
 * @example const files = unzip(archive, { filter: e => e.name.endsWith('.txt'), maxSize: 100_000_000 })
 */
export function unzip(data: Uint8Array, opts: UnzipOptions = {}): Record<string, Uint8Array<ArrayBuffer>> {
  opts ??= {}
  const max = checkMaxSize(opts.maxSize), filter = opts.filter
  // The filter gets a copy, as in a worker: an edit to the entry it's shown can't change what's decoded or returned
  const kept = readCentral(data).filter(c => !filter || filter({ ...c.info })).map(c => ({ c, span: dataSpan(data, c) }))
  // Each entry decodes to exactly its stated size, so the total is known before anything is decoded
  if (kept.reduce((n, e) => n + e.c.info.originalSize, 0) > max) fail('too-large', TOO_MANY)
  // Entries sharing bytes would let a small archive decode the same data again and again (an overlapping zip bomb)
  const order = kept.map(e => [e.c.offset, e.span[1]] as const).sort((a, b) => a[0] - b[0])
  for (let i = 1; i < order.length; i++) if (order[i]![0] < order[i - 1]![1]) fail('invalid-data', 'zip entries overlap')
  const files: Record<string, Uint8Array<ArrayBuffer>> = {}
  for (const { c, span } of kept) define(files, c.info.name, entryData(data.subarray(span[0], span[1]), c.info))
  return files
}

/**
 * Lists a zip's entries, with names, sizes, dates, comments and modes, without decompressing anything.
 * @example const total = zipEntries(archive).reduce((n, e) => n + e.originalSize, 0)
 */
export const zipEntries = (data: Uint8Array): ZipEntry[] => readCentral(data).map(c => c.info)

// Streaming writer

/** A zip archive written as a stream, one entry at a time */
export interface ZipStream {
  /**
   * Starts an entry and returns its stream: push the entry's data, then finish it before the next file() or finish().
   * The name follows the same rules as a {@link ZipInput} path.
   */
  file(name: string, opts?: ZipEntryOptions): Stream
  /** Ends the archive and returns its last bytes */
  finish(): Uint8Array<ArrayBuffer>
}

interface OpenFile {
  e: Entry
  d: Deflater | null
  head: Uint8Array<ArrayBuffer> | null
}

// Entries are written one at a time, each with a data descriptor since sizes aren't known
// up front. pushFile, finishFile and finish return the archive bytes they produce.
export class ZipWriter implements ZipStream {
  private offset = 0
  private busy = false
  private lost = false
  private readonly records: Uint8Array<ArrayBuffer>[] = []
  private current: OpenFile | null = null
  private done = false
  private readonly names = new Map<string, string>()
  private readonly defaults: { level: Level | undefined; mtime: Date | undefined }
  private readonly comment: Uint8Array<ArrayBuffer>

  // Taken now, as the async stream takes them: the caller may change the object or its Date before the next entry
  constructor(opts: ZipOptions = {}) {
    opts ??= {}
    this.defaults = { level: opts.level, mtime: opts.mtime == null ? undefined : new Date(opts.mtime) }
    this.comment = archiveComment(opts.comment)
  }

  file(name: string, opts: ZipEntryOptions = {}): Stream {
    this.openFile(name, opts)
    // Each writer belongs to its own entry; once that's finished, it must not reach the next one
    const entry = this.current
    const check = (): void => { if (this.current !== entry) fail('invalid-state', ENTRY_DONE) }
    return { push: c => { check(); return this.pushFile(c) }, finish: c => { check(); return this.finishFile(c) }, [ENTRY]: () => { this.lost = true } } as Stream
  }

  openFile(name: string, opts: ZipEntryOptions = {}, now?: number): void {
    this.run(() => {
      opts ??= {}
      if (this.done) fail('invalid-state', FINISHED)
      if (this.current) fail('invalid-state', ENTRY_OPEN)
      // Names are the entries' identities, so they aren't coerced: 123 and '123' would be the same name twice
      if (typeof name !== 'string') fail('invalid-option', 'zip entry names must be strings')
      const stored = storedName(name)
      const first = this.names.get(stored)
      if (first !== undefined) fail('invalid-option', `the zip archive already has ${quote(first)}${first === name ? '' : `, which extracts where ${quote(name)} would`}`)
      const e = makeEntry(name, opts, this.defaults, now)
      this.names.set(stored, name)
      // Even level 0 is deflate, in stored blocks: a stream reader ends stored data at the first descriptor that
      // matches it, so stored data could hold a forged one and end early
      e.method = 8
      e.flags |= HAS_DESCRIPTOR
      e.offset = this.offset
      this.current = { e, d: null, head: localHeader(e) }
    })
  }

  pushFile(chunk: Uint8Array): Uint8Array<ArrayBuffer> { return this.run(() => this.write(chunk, false)) }

  finishFile(chunk: Uint8Array = EMPTY): Uint8Array<ArrayBuffer> { return this.run(() => this.write(chunk, true)) }

  finish(): Uint8Array<ArrayBuffer> {
    return this.run(() => {
      if (this.current) fail('invalid-state', ENTRY_OPEN_AT_END)
      if (this.done) fail('invalid-state', FINISHED)
      this.done = true
      return concat([...this.records, endRecord(this.records, this.offset, this.comment)])
    })
  }

  // Reading the caller's options and chunks can run its code, a getter say, and a call it makes from there is refused
  private run<T>(fn: () => T): T {
    if (this.lost) fail('invalid-state', LOST)
    if (this.busy) fail('invalid-state', BUSY)
    this.busy = true
    try {
      return fn()
    } finally {
      this.busy = false
    }
  }

  private write(chunk: Uint8Array, last: boolean): Uint8Array<ArrayBuffer> {
    const cur = this.current ?? fail('invalid-state', 'no zip entry is open')
    const { e } = cur
    e.crc = crc32(chunk, e.crc)
    e.usize += chunk.length
    // The first call decides: an entry that ends empty there is stored, as nothing sits between header and descriptor
    if (cur.head) {
      if (last && !e.usize) { e.method = 0; cur.head = localHeader(e) } else cur.d = deflater(e.level)
    }
    const body = cur.d ? cur.d.push(chunk, last) : chunk
    e.csize += body.length
    const parts = cur.head ? [cur.head, body] : [body]
    cur.head = null
    if (last) {
      const desc = new Uint8Array(16)
      writeU32LE(desc, 0, DESCRIPTOR)
      writeU32LE(desc, 4, e.crc)
      writeU32LE(desc, 8, e.csize)
      writeU32LE(desc, 12, e.usize)
      parts.push(desc)
      this.records.push(centralRecord(e))
      this.current = null
    }
    // Only an empty entry is stored here, and it goes out with its header and descriptor: no chunk goes out uncopied
    const out = concat(parts) as Uint8Array<ArrayBuffer>
    this.offset += out.length
    return out
  }
}

/**
 * Writes a zip archive as a stream, one entry at a time: every push and finish returns the bytes it completes.
 * Level 0 stores data in deflate's stored blocks, which mark their own end, so entries read safely as a stream.
 */
export const zipStream = (opts: ZipOptions = {}): ZipStream => new ZipWriter(opts)

// Streaming reader

const SIG = 0, HEAD = 1, DATA = 2, DESC = 3, DONE = 4

/** A zip read as a stream: push the archive's bytes and get back chunks of its entries as they complete */
export type UnzipStream = Stream<Uint8Array, UnzipChunk[]>

// Reads entries front to back from local headers, never the central directory, though the stream must reach it.
// Stored entries with a data descriptor end where a descriptor matching the bytes so far appears.
export class UnzipReader implements UnzipStream {
  // Bytes not yet consumed, from `start` in a buffer that doubles: a long header in tiny pushes joins in linear time
  private hold = EMPTY
  private held = 0
  // Where the held bytes start in hold: a call that stops early at an ask moves nothing, or each decide() would
  // move the rest of a long chunk again
  private start = 0
  private out: UnzipChunk[] = []
  private state = SIG
  private begun = false
  private info: ZipEntryInfo = { name: '', compression: 0, mtime: new Date(0) }
  private readonly days = new Map<number, number>()
  // The size the entry is held to and its name for messages, kept apart from `info`: that goes out with every chunk,
  // and the caller may edit it
  private originalSize = 0
  private name = ''
  private keep = false
  private descriptor = false
  private zip64 = false
  // Like fflate, entries aren't checked against their CRC-32. A stored entry ended by a data descriptor keeps
  // a running one only because matching it is how the reader finds where that entry ends.
  private sum = 0
  private got = 0
  private left = 0
  // Bytes of kept entries returned so far, against maxSize
  private total = 0
  private inf: Inflater | null = null
  private padding = false
  private ended = false
  private broken = false
  private busy = false
  private failure: unknown
  private answer: boolean | undefined
  private waiting = false
  private readonly filter: ((entry: ZipEntryInfo) => boolean) | undefined
  private readonly max: number
  private readonly ask: boolean

  // `ask` is for the async API, whose filter can't run in a worker: each entry's header ends the call's output as
  // an AskChunk, and nothing of that entry is decoded until decide() brings the filter's answer
  constructor(opts: UnzipStreamOptions = {}, ask = false) {
    opts ??= {}
    this.filter = opts.filter
    this.max = checkMaxSize(opts.maxSize)
    this.ask = ask
  }

  push(chunk: Uint8Array): UnzipChunk[] { return this.run(chunk, false) }

  finish(chunk: Uint8Array = EMPTY): UnzipChunk[] { return this.run(chunk, true) }

  // Internal: the filter's answer for the entry the stream stopped at; reading goes on from there
  decide(keep: boolean): UnzipChunk[] {
    if (!this.waiting) fail('invalid-state', 'no zip entry is waiting for a decision')
    this.answer = keep
    return this.run(EMPTY, this.ended, true)
  }

  private run(chunk: Uint8Array, last: boolean, resume = false): UnzipChunk[] {
    // A call that threw left the stream mid-entry, so every later call rethrows that error
    if (this.broken) throw this.failure
    if (this.ended && !resume) fail('invalid-state', FINISHED)
    // A filter, or a getter on the chunk, calling the stream would parse over the call still under way
    if (this.busy) fail('invalid-state', BUSY)
    this.busy = true
    let started = false
    try {
      // A chunk refused here leaves the stream as it was
      bytes(chunk)
      started = true
      this.ended = last
      this.out = []
      this.parse(chunk, last)
    } catch (e) {
      if (started) {
        this.broken = true
        this.failure = e
      }
      throw e
    } finally {
      this.busy = false
    }
    return this.out
  }

  private parse(chunk: Uint8Array, last: boolean): void {
    let d = chunk
    if (this.held) {
      if (chunk.length) {
        if (this.start) this.hold.copyWithin(0, this.start, this.start + this.held)
        this.start = 0
        this.hold = grow(this.hold, this.held, chunk.length)
        this.hold.set(chunk, this.held)
      }
      d = this.hold.subarray(this.start, this.start + this.held + chunk.length)
    }
    let p = 0
    for (;;) {
      if (this.state === SIG) {
        if (d.length - p < 4) break
        const sig = readU32LE(d, p)
        if (!this.begun && (sig === DESCRIPTOR || sig === SPLIT_ONE)) { this.begun = true; p += 4; continue }
        this.begun = true
        if (sig === LOCAL) this.state = HEAD
        else if (sig === CENTRAL || sig === END || sig === END64) { this.state = DONE; break }
        else fail('invalid-data', 'unexpected bytes between zip entries')
      } else if (this.state === HEAD) {
        if (d.length - p < 30) break
        const nl = readU16LE(d, p + 26), el = readU16LE(d, p + 28)
        if (d.length - p < 30 + nl + el || !this.open(d, p, nl, el)) break
        p += 30 + nl + el
      } else if (this.state === DATA) {
        p = this.data(d, p, last)
        if (this.state === DATA) break
      } else if (this.state === DESC) {
        if (d.length - p < 4) break
        const q = p + (readU32LE(d, p) === DESCRIPTOR ? 4 : 0)
        if (d.length - q < (this.zip64 ? 20 : 12)) break
        const usize = this.zip64 ? readU64LE(d, q + 12) : readU32LE(d, q + 8)
        if (usize !== this.got) wrongSize(this.name)
        p = q + (this.zip64 ? 20 : 12)
        this.close()
      } else {
        break
      }
    }
    const rest = this.state === DONE ? 0 : d.length - p
    if (rest && d === chunk) { this.hold = copy(d, p); this.start = 0 }
    else if (rest) this.start += p
    this.held = rest
    // Every zip ends in a central directory; without one, entries after the cut would vanish silently
    if (last && !this.waiting && this.state !== DONE) fail('truncated', 'zip data ends before its central directory')
  }

  // Starts the entry whose local header is at `p`, or in ask mode without an answer yet, stops there (false)
  private open(d: Uint8Array, p: number, nl: number, el: number): boolean {
    const flags = readU16LE(d, p + 6)
    let csize = readU32LE(d, p + 18), usize = readU32LE(d, p + 22)
    // With a ZIP64 field the descriptor's sizes are 8 bytes, whatever this header holds (APPNOTE 4.3.9.2)
    const next = zip64Values(d, p + 30 + nl, el)
    if (next) {
      if (usize === MAX32) usize = next()
      if (csize === MAX32) csize = next()
    }
    const name = decodeName(d.subarray(p + 30, p + 30 + nl), (flags & UTF8) !== 0)
    const descriptor = (flags & HAS_DESCRIPTOR) !== 0, compression = readU16LE(d, p + 8)
    const mtime = dosDate(readU16LE(d, p + 10), readU16LE(d, p + 12), this.days)
    // With a descriptor the header's sizes are placeholders, so they're left out rather than passed on as zero
    const info: ZipEntryInfo = descriptor ? { name, compression, mtime } : { name, size: csize, originalSize: usize, compression, mtime }
    let keep: boolean
    if (this.ask) {
      if (this.answer === undefined) {
        this.waiting = true
        this.out.push({ entry: info, data: new Uint8Array(0), final: false, ask: true } as AskChunk)
        return false
      }
      keep = this.answer
      this.answer = undefined
      this.waiting = false
    } else {
      // Called on its own, as unzip() calls it, with a copy down to its Date: the filter can't edit what goes out
      const filter = this.filter, n = d.length
      keep = !filter || filter({ ...info, mtime: new Date(mtime) })
      // The filter is the caller's code: a chunk it detached or resized can't be read on from where it was
      if (d.length !== n) fail('invalid-option', 'the zip data was detached or resized while its filter ran')
    }
    this.info = info
    this.name = name
    this.originalSize = usize
    this.keep = keep
    this.zip64 = next !== null
    this.descriptor = descriptor
    this.padding = false
    // A skipped entry is never decoded, unless only decoding finds its end, so only then must kokyu support it
    if (keep || descriptor) checkSupported(info, flags)
    this.sum = 0
    this.got = 0
    this.left = csize
    // A skipped deflate entry is still decoded when that's the only way to find where it ends, keeping nothing
    this.inf = compression === 8 && (keep || descriptor) ? inflater({ discard: !keep, limit: keep ? this.max - this.total : undefined }) : null
    this.state = DATA
    return true
  }

  private emit(bytes: Uint8Array<ArrayBuffer>): void {
    this.got += bytes.length
    if (!this.keep) return
    // Checked as data arrives, so an entry never returns more than its header claims, the size a filter judged
    if (!this.descriptor && this.got > this.originalSize) wrongSize(this.name)
    if ((this.total += bytes.length) > this.max) fail('too-large', TOO_MANY)
    if (bytes.length) this.out.push({ entry: this.info, data: bytes, final: false })
  }

  // Consumes entry data from `p` and returns the new position. The state stays DATA when more input is needed.
  private data(d: Uint8Array, p: number, last: boolean): number {
    if (this.inf) {
      const avail = d.length - p
      const n = this.descriptor ? avail : Math.min(this.left, avail)
      const bytes = this.inf.push(d.subarray(p, p + n), !this.descriptor && n === this.left)
      if (this.keep) this.emit(bytes)
      else this.got = this.inf.skipped
      if (!this.inf.finished) {
        this.left -= n
        return p + n
      }
      const used = n - this.inf.rest().length
      this.inf = null
      if (this.descriptor) {
        this.state = DESC
        return p + used
      }
      // Bytes after the deflate data but inside the stated size are skipped below, as one-shot readers skip them
      this.left -= used
      this.padding = true
      p += used
    }
    if (!this.descriptor) {
      const n = Math.min(this.left, d.length - p)
      if (this.keep && !this.padding) this.emit(copy(d, p, p + n))
      this.left -= n
      if (this.left === 0) this.close()
      return p + n
    }
    // Stored with a descriptor: the data ends at a descriptor whose CRC and size match everything before it.
    // The CRC runs forward with the scan, so each byte is summed once however many false descriptors there are.
    const z = this.zip64, span = z ? 24 : 16
    let sum = this.sum, at = p
    for (let k = p; k + span <= d.length; k++) {
      if (readU32LE(d, k) !== DESCRIPTOR || (z ? readU64LE(d, k + 16) : readU32LE(d, k + 12)) !== this.got + (k - p)) continue
      sum = crc32(d.subarray(at, k), sum)
      at = k
      if (sum !== readU32LE(d, k + 4)) continue
      this.emit(copy(d, p, k))
      this.state = DESC
      return k
    }
    if (last) endsEarly(this.name)
    // Hold back enough bytes for a descriptor that may be cut off
    const safe = Math.max(p, d.length - span + 1)
    this.sum = crc32(d.subarray(at, safe), sum)
    if (safe > p) this.emit(copy(d, p, safe))
    return safe
  }

  // Entry complete: check its size and mark the end of it
  private close(): void {
    if (this.keep && !this.descriptor && this.got !== this.originalSize) wrongSize(this.name)
    if (this.keep) this.out.push({ entry: this.info, data: new Uint8Array(0), final: true })
    this.state = SIG
  }
}

/**
 * Reads a zip as a stream, front to back, without needing the whole archive.
 * @example const s = unzipStream(); for (const { entry, data, final } of s.push(chunk)) write(entry.name, data, final)
 */
export const unzipStream = (opts: UnzipStreamOptions = {}): UnzipStream => new UnzipReader(opts)
