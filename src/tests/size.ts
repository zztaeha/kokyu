// Minified bundle sizes, measured the way fflate states its 8 kB core, beside fflate's own. Needs Bun.
// Fails when the core (deflate + inflate) passes its budget, the size reached plus some room, so it can't creep back.

import { execFileSync } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { gzipSync } from 'node:zlib'

const BUDGET = 12_950
const kokyu = new URL('../engine/index.js', import.meta.url).pathname
const fflate = new URL('../../node_modules/fflate/esm/browser.js', import.meta.url).pathname

const bundles: [string, string, string][] = [
  ['core: deflate + inflate', '{ deflate, inflate }', '{ deflateSync, inflateSync }'],
  ['inflate only', '{ inflate }', '{ inflateSync }'],
  ['deflate only', '{ deflate }', '{ deflateSync }'],
  ['whole library', '*', '*'],
]

const dir = mkdtempSync(join(tmpdir(), 'kokyu-size-'))
const measure = (names: string, from: string): [number, number] => {
  const entry = join(dir, 'entry.mjs')
  writeFileSync(entry, `export ${names} from ${JSON.stringify(from)}`)
  const out = execFileSync('bun', ['build', entry, '--minify', '--target=browser'])
  return [out.length, gzipSync(out, { level: 9 }).length]
}
const kb = (n: number): string => `${(n / 1024).toFixed(1)} KB`

let core = 0
try {
  console.log('| Bundle | kokyu | gzipped | fflate | gzipped |\n|---|--:|--:|--:|--:|')
  for (const [label, ours, theirs] of bundles) {
    const [k, kg] = measure(ours, kokyu), [f, fg] = measure(theirs, fflate)
    core ||= k
    console.log(`| ${label} | ${kb(k)} | ${kb(kg)} | ${kb(f)} | ${kb(fg)} |`)
  }
} finally {
  rmSync(dir, { recursive: true })
}
if (core > BUDGET) {
  console.error(`\nThe core is ${core} B, over its ${BUDGET} B budget`)
  process.exitCode = 1
}
