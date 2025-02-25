package com.purbon.kafka.topology.backend.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Tool to split a byte array into smaller chunks, given a <code>chunkSize</code>. Every chunk
 * except potentially the last will be exactly <code>chunkSize</code> bytes long. The first chunk
 * will have a 5 byte "header": A single byte to represent the chunking version (currently 1), and 4
 * bytes in network byte order (big endian) giving an <code>int</code> representing the total number
 * of bytes in the original array (i.e. excluding the header). Except for the first chunk, the other
 * chunks are headless.
 */
public final class ByteArrayChunker {

  private static final int VERSION = 1;
  private static final int MIN_CHUNK_SIZE = 16;
  private final int chunkSize;

  public ByteArrayChunker(final int chunkSize) {
    if (chunkSize < MIN_CHUNK_SIZE) {
      throw new IllegalArgumentException("Chunk size must be at least " + MIN_CHUNK_SIZE);
    }
    this.chunkSize = chunkSize;
  }

  public List<byte[]> chunk(final byte[] bytes) {
    Objects.requireNonNull(bytes);
    final List<byte[]> chunks = new ArrayList<>();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(VERSION);
    writeInt(baos, bytes.length);
    int bytesWritten = 0;
    do {
      final int toWrite = Math.min(bytes.length - bytesWritten, chunkSize - baos.size());
      baos.write(bytes, bytesWritten, toWrite);
      bytesWritten += toWrite;
      chunks.add(baos.toByteArray());
      baos.reset();
    } while (bytesWritten < bytes.length);
    return chunks;
  }

  public byte[] dechunk(final List<byte[]> chunks) {
    Objects.requireNonNull(chunks);
    if (chunks.isEmpty()) {
      throw new IllegalArgumentException("List of chunks must not be empty");
    }
    boolean firstRound = true;
    int numBytes = 0;
    byte[] bytes = null;
    int bytesWritten = 0;
    for (final byte[] chunk : chunks) {
      final ByteArrayInputStream bais = new ByteArrayInputStream(chunk);
      if (firstRound) {
        final int version = bais.read();
        if (version > VERSION) {
          throw new RuntimeException("Don't know how to handle chunking version " + version);
        }
        numBytes = readInt(bais);
        bytes = new byte[numBytes];
        firstRound = false;
      }
      bytesWritten += Math.max(bais.read(bytes, bytesWritten, bais.available()), 0);
      if (bytesWritten == numBytes) {
        break;
      }
    }
    if (bytesWritten != numBytes) {
      throw new RuntimeException(
          "Not enough bytes in chunks. Got " + bytesWritten + ", expected " + numBytes);
    }
    return bytes;
  }

  private static void writeInt(final ByteArrayOutputStream baos, final int i) {
    baos.write((i >> 24) & 0xff);
    baos.write((i >> 16) & 0xff);
    baos.write((i >> 8) & 0xff);
    baos.write(i & 0xff);
  }

  private static int readInt(final ByteArrayInputStream bais) {
    int i = bais.read() << 24;
    i |= bais.read() << 16;
    i |= bais.read() << 8;
    i |= bais.read();
    return i;
  }
}
