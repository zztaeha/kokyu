# kokyu

Deflate, zlib, gzip and zip for every JavaScript runtime.

kokyu packs and unpacks raw deflate, zlib, gzip and zip archives, with no dependencies. The same build runs in Node, Deno, Bun and the browser, and every format works the way you need it to: in one call, off the main thread, as a stream, or as an async stream.

**The documentation is at [zztaeha.dev/kokyu](https://zztaeha.dev/kokyu/).**

## Installation

```sh
npm install kokyu
```

That's all there is to it: no dependencies to pull in and nothing to set up. Import what you need by name, and in Deno, import it from `npm:kokyu`.

## Quick start

```ts
import { gzip, gunzip, encodeText, decodeText } from 'kokyu'

const packed = gzip(encodeText('hello, kokyu'))
const text = decodeText(gunzip(packed)) // 'hello, kokyu'
```

Every format comes four ways, so you can pick whatever fits where your data lives:

```ts
import { gzip, gzipAsync, gzipStream, gzipStreamAsync, toTransformStream } from 'kokyu'

// One call, right here
const packed = gzip(bytes)

// The same in a worker, so the page stays smooth while a big file packs
const packedInBackground = await gzipAsync(bytes)

// Piece by piece, as the data arrives
const stream = gzipStream()
const parts = [stream.push(firstChunk), stream.push(secondChunk), stream.finish()]

// Piece by piece in a worker: every stream has an async twin
const background = gzipStreamAsync()
const partsInBackground = await Promise.all([
  background.push(firstChunk),
  background.push(secondChunk),
  background.finish(),
])

// And any stream drops into a web stream pipeline
const response = await fetch('/logs.txt')
const gzipped = response.body!.pipeThrough(toTransformStream(gzipStream()))
```

Zip archives are plain objects of paths:

```ts
import { zip, unzip, encodeText, decodeText } from 'kokyu'

const archive = zip({
  'notes.txt': encodeText('remember the milk'),
  photos: { 'cat.jpg': catPhoto },
})

const files = unzip(archive)
decodeText(files['notes.txt']!) // 'remember the milk'
files['photos/cat.jpg'] // the photo's bytes
```

Unpacking something you didn't make? Give it a cap, so a tiny upload can't unpack into gigabytes:

```ts
import { gunzip } from 'kokyu'

// Fails with a KokyuError whose code is 'too-large' past 10 MB
const body = gunzip(upload, { maxSize: 10 * 1024 * 1024 })
```

## Why kokyu

- **It's just kokyu.** No dependencies, so installing it adds one package and nothing else.
- **Small where it counts.** Import what you use and your bundler drops the rest: decompression alone is 3.3 KB minified and gzipped, both directions 6.2 KB, and everything, zip and the worker pool included, 21.5 KB.
- **Quick.** On an M1 Pro with Node 26, `gzip` packs TypeScript's 2.2 MB `lib.dom.d.ts` down to 349 KB in about 26 ms, and `gunzip` unpacks it in about 5 ms.
- **Every format, four ways.** One-shot calls, async calls that hand big jobs to a pool of workers, streams you feed chunk by chunk, and async streams that do both. Any stream turns into a web `TransformStream` with one call.
- **Your main thread stays free.** Async calls send inputs of 16 KB and up to the workers, and run smaller ones right away, where the trip to a worker would be a big part of the cost.
- **Bring your own buffer.** The `…Into` decoders write straight into memory you already have, shared memory included.
- **Careful with data you didn't make.** Every decoder takes a `maxSize`, archives built to fool a reader are refused, and a zip name that would land outside its folder is refused when you write the archive. Every error kokyu raises is a `KokyuError` with a `code` you can switch on.
- **Typed all the way.** Written in TypeScript, with its types included. The arrays kokyu makes are typed `Uint8Array<ArrayBuffer>`, so they go straight into a `Blob`, a `Response` or `crypto.subtle` without a cast.
- **Runs where your code does.** Node, Deno, Bun and the browser, from one build. Checks of every part of the API run unchanged in Node, Deno, Bun, Chrome, Firefox and Safari, and the tests hold deflate, zlib and gzip up against `node:zlib`.

## Good to know

- kokyu ships as ES modules only, with no CommonJS build.
- TypeScript users need 5.7 or newer, the first version where `Uint8Array` is generic over its buffer. See [TypeScript](https://zztaeha.dev/kokyu/#typescript).
- Zip covers stored and deflated entries. kokyu reads ZIP64 but doesn't write it, and it doesn't read encrypted entries.
- Decoders don't check CRC-32 or Adler-32 checksums. Where corrupt data matters, compare a `crc32` of the output yourself.
- `unzip` gives you each entry's name exactly as the archive stores it, `../` included, so check a name before you write a file with it. See [Errors and untrusted input](https://zztaeha.dev/kokyu/#errors).

## Documentation

| | |
|---|---|
| [Overview](https://zztaeha.dev/kokyu/) | What kokyu is and what it supports |
| [Getting started](https://zztaeha.dev/kokyu/#installing) | [Installing](https://zztaeha.dev/kokyu/#installing), [TypeScript](https://zztaeha.dev/kokyu/#typescript) |
| [Using kokyu](https://zztaeha.dev/kokyu/#compressing) | [Compressing and decompressing](https://zztaeha.dev/kokyu/#compressing), [Streams](https://zztaeha.dev/kokyu/#streams), [Async](https://zztaeha.dev/kokyu/#async), [Zip](https://zztaeha.dev/kokyu/#zip), [Text and checksums](https://zztaeha.dev/kokyu/#text), [Errors and untrusted input](https://zztaeha.dev/kokyu/#errors) |
| [Reference](https://zztaeha.dev/kokyu/reference/functions/) | [Functions](https://zztaeha.dev/kokyu/reference/functions/) and [types](https://zztaeha.dev/kokyu/reference/types/), every one with an example |

Release notes are in the [changelog](https://zztaeha.dev/kokyu/changelog/).

## License

MIT. See [LICENSE](LICENSE).
