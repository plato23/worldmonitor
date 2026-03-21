/**
 * Regression tests for _collectDecompressed() maxBytes guard in ais-relay.cjs.
 *
 * Validates that the streaming size limit aborts decompression mid-flight
 * rather than buffering the full response (memory-pressure protection for
 * the long-running relay process).
 *
 * Run: node --test scripts/ais-relay-gzip.test.cjs
 */
'use strict';

const { strict: assert } = require('node:assert');
const { describe, it } = require('node:test');
const { Readable } = require('node:stream');
const zlib = require('node:zlib');
const { readFileSync } = require('node:fs');
const { resolve } = require('node:path');

const relaySrc = readFileSync(resolve(__dirname, '_relay-decompress.cjs'), 'utf-8');
const relayCjs = readFileSync(resolve(__dirname, 'ais-relay.cjs'), 'utf-8');
const { _collectDecompressed } = require('./_relay-decompress.cjs');

describe('_collectDecompressed source contract', () => {
  it('accepts maxBytes parameter', () => {
    assert.match(relaySrc, /function _collectDecompressed\(response, maxBytes\)/,
      '_collectDecompressed must accept maxBytes as second parameter');
  });

  it('checks totalSize against maxBytes during streaming (not after)', () => {
    const fnStart = relaySrc.indexOf('function _collectDecompressed(');
    const fnEnd = relaySrc.indexOf('\n}\n', fnStart + 10);
    const fnBody = relaySrc.slice(fnStart, fnEnd + 3);

    assert.ok(fnBody.includes('let totalSize = 0'),
      'must track totalSize incrementally');

    const sizeCheckIdx = fnBody.indexOf('totalSize > maxBytes');
    const pushIdx = fnBody.indexOf('chunks.push(chunk)');
    assert.ok(sizeCheckIdx !== -1, 'must compare totalSize against maxBytes');
    assert.ok(pushIdx !== -1, 'must push chunks');
    assert.ok(sizeCheckIdx < pushIdx,
      'size check must happen BEFORE pushing chunk (abort mid-stream, not post-buffer)');
  });

  it('destroys both stream and response on limit exceeded', () => {
    const fnStart = relaySrc.indexOf('function _collectDecompressed(');
    const fnEnd = relaySrc.indexOf('\n}\n', fnStart + 10);
    const fnBody = relaySrc.slice(fnStart, fnEnd + 3);

    assert.ok(fnBody.includes('stream.destroy()'),
      'must destroy decompression stream on limit');
    assert.ok(fnBody.includes('response.destroy()'),
      'must destroy HTTP response on limit to stop network I/O');
  });

  it('rejects with descriptive error including byte counts', () => {
    const fnStart = relaySrc.indexOf('function _collectDecompressed(');
    const fnEnd = relaySrc.indexOf('\n}\n', fnStart + 10);
    const fnBody = relaySrc.slice(fnStart, fnEnd + 3);

    assert.ok(fnBody.includes('payload exceeds'),
      'error message must indicate payload exceeded limit');
    assert.ok(fnBody.includes('bytes decompressed'),
      'error message must include decompressed byte count');
  });

  it('CelesTrak fetch uses maxBytes=2MB', () => {
    assert.ok(relayCjs.includes('_collectDecompressed(resp, 2 * 1024 * 1024)'),
      'CelesTrak TLE fetch must pass 2MB limit to _collectDecompressed');
  });
});

// ─── Behavioral tests using real implementation ───

function makeGzipStream(data) {
  const compressed = zlib.gzipSync(data);
  const stream = Readable.from(compressed);
  stream.headers = { 'content-encoding': 'gzip' };
  stream.pipe = function (decompressor) {
    return Readable.from(compressed).pipe(decompressor);
  };
  stream.destroy = function () {};
  return stream;
}

describe('_collectDecompressed maxBytes behavior', () => {
  it('resolves when payload is under maxBytes', async () => {
    const payload = JSON.stringify({ data: 'small' });
    const stream = makeGzipStream(payload);
    const result = await _collectDecompressed(stream, 1024);
    assert.equal(result, payload);
  });

  it('resolves when no maxBytes is set (unlimited)', async () => {
    const payload = 'x'.repeat(5000);
    const stream = makeGzipStream(payload);
    const result = await _collectDecompressed(stream);
    assert.equal(result, payload);
  });

  it('rejects when decompressed payload exceeds maxBytes', async () => {
    const payload = 'x'.repeat(5000);
    const stream = makeGzipStream(payload);
    await assert.rejects(
      () => _collectDecompressed(stream, 100),
      (err) => {
        assert.ok(err.message.includes('payload exceeds 100 byte limit'),
          `Expected limit error, got: ${err.message}`);
        assert.ok(err.message.includes('bytes decompressed'),
          'Error must include decompressed byte count');
        return true;
      }
    );
  });

  it('works without compression (identity)', async () => {
    const payload = JSON.stringify({ ok: true });
    const stream = Readable.from(Buffer.from(payload));
    stream.headers = {};
    stream.destroy = function () {};
    const result = await _collectDecompressed(stream, 10000);
    assert.equal(result, payload);
  });

  it('rejects on corrupt gzip data', async () => {
    const corrupt = Buffer.from([0x1f, 0x8b, 0x08, 0x00, 0xff, 0xff, 0xff]);
    const stream = Readable.from(corrupt);
    stream.headers = { 'content-encoding': 'gzip' };
    stream.pipe = function (decompressor) {
      return Readable.from(corrupt).pipe(decompressor);
    };
    stream.destroy = function () {};
    await assert.rejects(
      () => _collectDecompressed(stream, 10000),
      (err) => {
        assert.ok(err.message.includes('decompression failed'),
          `Expected decompression error, got: ${err.message}`);
        return true;
      }
    );
  });
});
