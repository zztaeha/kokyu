// Runs portable.ts in every runtime on this machine: Node, Bun and Deno as CLIs (each must also
// exit on its own once the pool is idle), Chrome and Firefox as headless pages, and Safari in an automation window.

import { spawn, type ChildProcess } from 'node:child_process'
import { createServer } from 'node:http'
import { existsSync } from 'node:fs'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { extname, join } from 'node:path'

type Status = 'pass' | 'fail' | 'skip'
interface Result { runtime: string; status: Status; detail: string }

const root = new URL('../../', import.meta.url)
const cli = new URL('portable-cli.js', import.meta.url).pathname
const TIMEOUT_MS = 60_000
const EXIT_GRACE_MS = 5_000

const summary = (failures: string[]): [Status, string] =>
  failures.length ? ['fail', failures.join('; ')] : ['pass', 'all checks passed']

function runCli(runtime: string, cmd: string, args: string[]): Promise<Result> {
  return new Promise(resolve => {
    let done = false, out = '', err = ''
    let failures: string[] | null = null
    const timers: ReturnType<typeof setTimeout>[] = []
    const finish = (status: Status, detail: string): void => {
      if (done) return
      done = true
      timers.forEach(clearTimeout)
      p.kill()
      resolve({ runtime, status, detail })
    }
    const p = spawn(cmd, args, { cwd: root.pathname })
    timers.push(setTimeout(() => finish('fail', 'timed out without a result'), TIMEOUT_MS))
    p.on('error', e => finish('skip', e.message))
    p.stderr.on('data', d => { err += d })
    p.stdout.on('data', d => {
      out += d
      const m = /KOKYU_RESULT (.*)/.exec(out)
      if (!m || failures) return
      failures = JSON.parse(m[1]!) as string[]
      timers.push(setTimeout(() => finish('fail', `${summary(failures!)[1]}, but the process didn't exit (idle workers keep it alive)`), EXIT_GRACE_MS))
    })
    p.on('exit', () => {
      if (failures) finish(...summary(failures))
      else finish('fail', err.trim().split('\n').slice(-3).join(' ') || 'exited without a result')
    })
  })
}

const PAGE = `<!doctype html><meta charset="utf-8"><title>kokyu runtime test</title>
<script type="importmap">{"imports":{"fflate":"/node_modules/fflate/esm/browser.js"}}</script>
<script type="module">
  let failures
  try { failures = await (await import('/build/tests/portable.js')).run() } catch (e) { failures = ['page error: ' + e] }
  await fetch('/result', { method: 'POST', body: JSON.stringify(failures) })
</script>`

const TYPES: Record<string, string> = { '.js': 'text/javascript' }
let waiting: ((failures: string[]) => void) | null = null

// Local-only static server for the project, plus the page and its result endpoint
const server = createServer(async (req, res) => {
  const path = decodeURIComponent((req.url ?? '/').split('?')[0]!)
  if (req.method === 'POST' && path === '/result') {
    let body = ''
    for await (const c of req) body += c
    res.end()
    waiting?.(JSON.parse(body) as string[])
    return
  }
  if (path === '/') { res.setHeader('content-type', 'text/html'); res.end(PAGE); return }
  const file = new URL('.' + path, root)
  if (!file.pathname.startsWith(root.pathname)) { res.statusCode = 403; res.end(); return }
  try {
    const data = await readFile(file)
    res.setHeader('content-type', TYPES[extname(file.pathname)] ?? 'application/octet-stream')
    res.end(data)
  } catch {
    res.statusCode = 404
    res.end()
  }
})
await new Promise<void>(r => server.listen(0, '127.0.0.1', r))
const address = server.address()
const url = `http://127.0.0.1:${typeof address === 'object' && address ? address.port : 0}/`

async function runPage(runtime: string, start: () => Promise<() => Promise<void>>): Promise<Result> {
  const result = new Promise<string[]>(r => { waiting = r })
  let stop: (() => Promise<void>) | null = null
  try {
    stop = await start()
    const timeout = new Promise<null>(r => setTimeout(() => r(null), TIMEOUT_MS))
    const failures = await Promise.race([result, timeout])
    if (!failures) return { runtime, status: 'fail', detail: 'timed out without a result' }
    const [status, detail] = summary(failures)
    return { runtime, status, detail }
  } catch (e) {
    return { runtime, status: 'skip', detail: e instanceof Error ? e.message : String(e) }
  } finally {
    waiting = null
    await stop?.()
  }
}

// A browser binary pointed at the page, with a throwaway profile so the real one is never touched
function browser(bin: string, args: (profile: string) => string[]): () => Promise<() => Promise<void>> {
  return async () => {
    if (!existsSync(bin)) throw new Error('not installed')
    const profile = await mkdtemp(join(tmpdir(), 'kokyu-browser-'))
    const p: ChildProcess = spawn(bin, [...args(profile), url], { stdio: 'ignore' })
    return async () => {
      p.kill()
      await new Promise(r => setTimeout(r, 500))
      await rm(profile, { recursive: true, force: true })
    }
  }
}

// Safari has no headless flag; safaridriver drives an isolated automation window instead
async function safari(): Promise<() => Promise<void>> {
  const port = 4444 + Math.floor(Math.random() * 1000)
  const driver = spawn('safaridriver', ['-p', String(port)], { stdio: 'ignore' })
  const api = `http://127.0.0.1:${port}`
  await new Promise(r => setTimeout(r, 1000))
  const res = await fetch(`${api}/session`, {
    method: 'POST', body: JSON.stringify({ capabilities: { alwaysMatch: { browserName: 'safari' } } }),
  }).then(r => r.json()) as { value: { sessionId?: string; message?: string } }
  const id = res.value.sessionId
  if (!id) {
    driver.kill()
    // A running Safari never answers the session request, which safaridriver reports only as a timeout
    throw new Error(`safaridriver refused (${res.value.message ?? 'no session'}); quit Safari (⌘Q) if it's open, then check Develop > Allow Remote Automation and safaridriver --enable`)
  }
  await fetch(`${api}/session/${id}/url`, { method: 'POST', body: JSON.stringify({ url }) })
  return async () => {
    await fetch(`${api}/session/${id}`, { method: 'DELETE' }).catch(() => {})
    driver.kill()
  }
}

const results: Result[] = []
for (const [runtime, cmd, args] of [
  ['Node', process.execPath, [cli]],
  ['Bun', 'bun', [cli]],
  ['Deno', 'deno', ['run', `--allow-read=${root.pathname}`, cli]],
] as const) {
  results.push(await runCli(runtime, cmd, [...args]))
  console.error(`${runtime}: ${results.at(-1)!.status}`)
}
for (const [runtime, start] of [
  ['Chrome', browser('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', p => ['--headless=new', '--no-first-run', '--no-default-browser-check', '--disable-extensions', `--user-data-dir=${p}`])],
  ['Firefox', browser('/Applications/Firefox.app/Contents/MacOS/firefox', p => ['-headless', '-no-remote', '-profile', p])],
  ['Safari', safari],
] as const) {
  results.push(await runPage(runtime, start))
  console.error(`${runtime}: ${results.at(-1)!.status}`)
}
server.close()

console.log('| Runtime | Result | Detail |\n|---|---|---|')
for (const r of results) console.log(`| ${r.runtime} | ${r.status} | ${r.detail} |`)
if (results.some(r => r.status === 'fail')) process.exitCode = 1
