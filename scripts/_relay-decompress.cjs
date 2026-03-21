'use strict';

const zlib = require('zlib');

function _collectDecompressed(response, maxBytes) {
  return new Promise((resolve, reject) => {
    const enc = (response.headers['content-encoding'] || '').trim().toLowerCase();
    let stream = response;
    if (enc === 'gzip' || enc === 'x-gzip') stream = response.pipe(zlib.createGunzip());
    else if (enc === 'deflate') stream = response.pipe(zlib.createInflate());
    else if (enc === 'br') stream = response.pipe(zlib.createBrotliDecompress());
    const chunks = [];
    let totalSize = 0;
    stream.on('data', chunk => {
      totalSize += chunk.length;
      if (maxBytes && totalSize > maxBytes) {
        stream.destroy();
        response.destroy();
        return reject(new Error(`payload exceeds ${maxBytes} byte limit (${totalSize} bytes decompressed)`));
      }
      chunks.push(chunk);
    });
    stream.on('end', () => resolve(Buffer.concat(chunks).toString()));
    stream.on('error', (err) => reject(new Error(`decompression failed (${enc}): ${err.message}`)));
  });
}

module.exports = { _collectDecompressed };
