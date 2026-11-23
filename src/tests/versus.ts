// kokyu against fflate on ten kinds of data, deflate and inflate only. Every level must give back the exact input
// through kokyu, fflate and node:zlib in both directions; one level is timed. Any mismatch fails the run.

import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { deflateRawSync, inflateRawSync } from 'node:zlib'
import * as f from 'fflate'
import * as k from '../engine/index.js'
import { equal, fullText, machineCode, now, race, randomBytes, rng, time } from './helpers.js'

const KB = 1024, MB = 1024 * KB
const SIZES: [string, number][] = [['1 KB', KB], ['64 KB', 64 * KB], ['1 MB', MB]]
const LEVELS: k.Level[] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
const at = process.argv.indexOf('--level')
const LEVEL = (at < 0 ? 6 : Number(process.argv[at + 1])) as k.Level
if (!LEVELS.includes(LEVEL)) throw new Error('--level must be 0 to 9')

// Generated data comes from one seed, built in a fixed order, so every run gets the same bytes
const r = rng(2026)
const int = (n: number): number => (r() * n) | 0
const pick = <T>(xs: readonly T[]): T => xs[int(xs.length)]!
const enc = new TextEncoder()
const text = (next: () => string): Uint8Array => {
  let s = ''
  while (s.length < MB) s += next()
  return enc.encode(s).subarray(0, MB)
}
const words = ['data', 'stream', 'block', 'window', 'match', 'user', 'request', 'value', 'server', 'cache', 'error', 'event']
const first = ['Ada', 'Grace', 'Linus', 'Ken', 'Barbara', 'Alan', 'Margaret', 'Dennis', 'Radia', 'Frances']
const last = ['Lovelace', 'Hopper', 'Torvalds', 'Thompson', 'Liskov', 'Turing', 'Hamilton', 'Ritchie', 'Perlman', 'Allen']
const intl = ['데이터', '압축', '서버', 'データ', '圧縮', '速度', '数据', 'данные', 'сжатие', 'بيانات', 'données', 'naïve', 'stream', '🚀', '✅', '📦']
const epoch = 1_700_000_000_000
let clock = epoch

function source(): Uint8Array {
  const root = fileURLToPath(new URL('../../node_modules/', import.meta.url))
  const files = ['typescript', 'fflate/esm'].flatMap(dir =>
    readdirSync(join(root, dir), { recursive: true, encoding: 'utf8' }).filter(p => /\.m?js$/.test(p)).sort()
      .map(p => readFileSync(join(root, dir, p))))
  return new Uint8Array(Buffer.concat(files).subarray(0, MB))
}

function sensor(): Uint8Array {
  const s = new Float32Array(MB / 4)
  for (let i = 0; i < s.length; i++) s[i] = Math.sin(i / 50) * 100 + Math.sin(i / 7) * 10 + r() - 0.5
  return new Uint8Array(s.buffer)
}

const noise = (n: number): string => Array.from({ length: n }, () => String.fromCharCode(32 + int(95))).join('')

const kinds: [string, Uint8Array][] = [
  ['TS declarations', fullText.subarray(0, MB)],
  ['JS source', source()],
  ['JSON records', text(() => JSON.stringify({
    id: int(1e6), name: `${pick(first)} ${pick(last)}`, email: `${pick(first).toLowerCase()}${int(1000)}@example.com`,
    active: r() < 0.8, score: int(10000) / 100, tags: [pick(words), pick(words)], createdAt: new Date(epoch + int(3e10)).toISOString(),
  }) + ',')],
  ['Log lines', text(() => `${new Date(clock += int(2000)).toISOString()} ${pick(['INFO', 'INFO', 'INFO', 'WARN', 'ERROR'])} `
    + `[${pick(['api', 'auth', 'billing', 'search'])}] ${pick(['GET', 'GET', 'POST', 'PUT', 'DELETE'])} /v1/${pick(words)}s/${int(1e5)} `
    + `${pick([200, 200, 200, 201, 204, 304, 400, 404, 500])} ${1 + int(900)}ms ip=10.${int(256)}.${int(256)}.${int(256)}\n`)],
  ['CSV numbers', text(() => `${(clock += 1000) / 1000},s${int(40)},${(20 + r() * 5).toFixed(2)},`
    + `${(40 + r() * 20).toFixed(1)},${(1000 + r() * 30).toFixed(1)}\n`)],
  ['UTF-8 multilingual', text(() => Array.from({ length: 4 + int(8) }, () => pick(intl)).join(' ') + pick(['. ', '! ', '? ', '\n']))],
  ['Program binary', machineCode(MB)],
  ['Float32 samples', sensor()],
  ['Random bytes', randomBytes(MB, 2026)],
  ['Runs and repeats', text(() => [
    () => '\0'.repeat(100 + int(4000)),
    () => pick(words).repeat(10 + int(200)),
    () => noise(100).repeat(2 + int(20)),
    () => noise(50),
  ][int(4)]!())],
]
for (const [name, d] of kinds) if (d.length < MB) throw new Error(`${name}: only ${d.length} bytes`)

const tty = process.stdout.isTTY === true, color = tty && !process.env['NO_COLOR']
const paint = (code: number, s: string): string => color ? `\x1b[${code}m${s}\x1b[0m` : s
const status = (s: string): void => { if (tty) process.stdout.write(`\r\x1b[2K${paint(2, s)}`) }
const size = (n: number): string => n < KB ? `${n} B` : n < MB ? `${(n / KB).toFixed(1)} KB` : `${(n / MB).toFixed(2)} MB`
// How kokyu's time compares with fflate's; within 5% counts as a tie
const verdict = (ours: number, theirs: number): [string, number] => {
  const x = theirs / ours, by = `${Math.max(x, 1 / x).toFixed(2)}×`
  return x >= 1.05 ? [`${by} ${paint(32, '▲')}`, 1] : x <= 1 / 1.05 ? [`${by} ${paint(31, '▼')}`, -1] : [`${by} ${paint(2, '=')}`, 0]
}
const pad = (s: string, n: number): string => s.padStart(n + s.length - s.replace(/\x1b\[\d+m/g, '').length)
const cell = (ours: string, theirs: string, third: string): string => `${pad(ours, 9)} ${pad(theirs, 9)} ${pad(third, 8)}`

const failures: string[] = []
let trips = 0
function identical(name: string, d: Uint8Array): number {
  let ok = 0
  for (const level of LEVELS) {
    const kc = k.deflate(d, { level }), fc = f.deflateSync(d, { level }), zc = deflateRawSync(d, { level })
    const ways: [string, () => Uint8Array][] = [
      ['kokyu → kokyu', () => k.inflate(kc)],
      ['kokyu → fflate', () => f.inflateSync(kc)],
      ['kokyu → zlib', () => inflateRawSync(kc)],
      ['fflate → kokyu', () => k.inflate(fc)],
      ['zlib → kokyu', () => k.inflate(zc)],
    ]
    for (const [way, run] of ways) {
      let good = false
      try { good = equal(run(), d) } catch { /* a throw counts as a mismatch */ }
      trips++
      if (good) ok++
      else failures.push(`${name}, level ${level}: ${way}`)
    }
  }
  return ok
}

const started = now()
const tally = { compress: [0, 0, 0], decompress: [0, 0, 0], smaller: 0, larger: 0 }
const totals = { ck: 0, cf: 0, dk: 0, df: 0 }
const rule = '─'.repeat(25) + ['┼', '┼', '┼', '┼'].join('─'.repeat(30)) + '─'.repeat(10)
console.log(`kokyu vs fflate: deflate + inflate · timed at level ${LEVEL} · median per call, alternating over 3 rounds`)
console.log(`Decompress: both decode the same input, node:zlib's level ${LEVEL} output`)
console.log(`Identical: at every level 0–9, kokyu's output decoded by kokyu, fflate and zlib, and fflate's and zlib's by kokyu`)
console.log(`${paint(32, '▲')} kokyu faster   ${paint(31, '▼')} kokyu slower   ${paint(2, '=')} within 5%\n`)
console.log(`${''.padEnd(24)} │ ${'Compress'.padEnd(28)} │ ${'Decompress'.padEnd(28)} │ ${'Compressed size'.padEnd(28)} │`)
console.log(`${'Data'.padEnd(18)} ${'Size'.padStart(5)} │ ${cell('kokyu', 'fflate', '')} │ ${cell('kokyu', 'fflate', '')} │ ${cell('kokyu', 'fflate', '')} │ Identical`)
console.log(rule)

for (const [name, all] of kinds) {
  for (const [label, n] of SIZES) {
    const d = new Uint8Array(all.subarray(0, n)), what = `${name} ${label}`
    status(`  … ${what}: checking every level`)
    const ok = identical(what, d)
    status(`  … ${what}: timing compression`)
    const [ck, cf] = await race(() => k.deflate(d, { level: LEVEL }), () => f.deflateSync(d, { level: LEVEL }))
    const zc = deflateRawSync(d, { level: LEVEL })
    status(`  … ${what}: timing decompression`)
    const [dk, df] = await race(() => k.inflate(zc), () => f.inflateSync(zc))
    const ks = k.deflate(d, { level: LEVEL }).length, fs = f.deflateSync(d, { level: LEVEL }).length
    const [ct, cv] = verdict(ck, cf), [dt, dv] = verdict(dk, df)
    tally.compress[1 - cv]!++
    tally.decompress[1 - dv]!++
    if (ks < fs) tally.smaller++
    else if (ks > fs) tally.larger++
    totals.ck += ck; totals.cf += cf; totals.dk += dk; totals.df += df
    const diff = (ks - fs) / fs * 100, pct = `${diff > 0 ? '+' : diff < 0 ? '−' : ''}${Math.abs(diff).toFixed(1)}%`
    const mark = ok === 50 ? paint(32, `✓ ${ok}/50`) : paint(31, `✗ ${ok}/50`)
    if (tty) process.stdout.write('\r\x1b[2K')
    console.log(`${name.padEnd(18)} ${label.padStart(5)} │ ${cell(time(ck), time(cf), ct)} │ ${cell(time(dk), time(df), dt)} │ ${cell(size(ks), size(fs), pct)} │ ${mark}`)
  }
}

const rows = kinds.length * SIZES.length
const count = ([faster, tied, slower]: number[]): string => `kokyu faster in ${faster} of ${rows}, tied ${tied}, slower ${slower}`
const sum = (ours: number, theirs: number): string => `${time(ours)} vs ${time(theirs)} (${verdict(ours, theirs)[0]})`
const took = Math.round((now() - started) / 1000)
console.log(rule.replace(/┼/g, '┴'))
console.log(failures.length
  ? paint(31, `${rows} rows · ${trips.toLocaleString('en-US')} round trips · ${failures.length} NOT identical:\n  `
    + failures.slice(0, 20).join('\n  ') + (failures.length > 20 ? `\n  …and ${failures.length - 20} more` : ''))
  : `${rows} rows · ${trips.toLocaleString('en-US')} round trips, all byte-identical ${paint(32, '✓')}`)
console.log(`Compress     ${count(tally.compress)}`)
console.log(`Decompress   ${count(tally.decompress)}`)
console.log(`Size         kokyu smaller in ${tally.smaller} of ${rows}, larger in ${tally.larger}, equal in ${rows - tally.smaller - tally.larger}`)
console.log(`\nTotal time, one call of each row added up (kokyu vs fflate):`)
console.log(`  Compress     ${sum(totals.ck, totals.cf)}`)
console.log(`  Decompress   ${sum(totals.dk, totals.df)}`)
console.log(`  Both         ${sum(totals.ck + totals.dk, totals.cf + totals.df)}`)
console.log(`\nFinished in ${took < 60 ? `${took} s` : `${Math.floor(took / 60)} min ${took % 60} s`}`)
if (failures.length) process.exitCode = 1
