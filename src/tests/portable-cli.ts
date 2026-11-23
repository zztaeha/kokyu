// Entry for Node, Bun and Deno. The runner reads the result line, then checks the process exits on its own.
import { run } from './portable.js'

const failures = await run()
console.log(`KOKYU_RESULT ${JSON.stringify(failures)}`)
