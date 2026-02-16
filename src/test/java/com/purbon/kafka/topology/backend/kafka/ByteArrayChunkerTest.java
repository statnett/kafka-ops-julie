package com.purbon.kafka.topology.backend.kafka;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public final class ByteArrayChunkerTest {

  public static final int CHUNK_SIZE = 16;
  public static final int HEADER_SIZE = 5;

  @Test
  public void assertCorrectHeaderSize() {
    final ByteArrayChunker chunker = new ByteArrayChunker(CHUNK_SIZE);
    final byte[] bytes = new byte[0];
    final List<byte[]> chunks = chunker.chunk(bytes);
    Assert.assertEquals(1, chunks.size());
    Assert.assertEquals(HEADER_SIZE, chunks.getFirst().length);
  }

  @Test
  public void testVariousSizesWithMethodCall() {
    for (int numBytes = 0; numBytes < 512; numBytes++) {
      final int expectedChunks = 1 + (numBytes + HEADER_SIZE - 1) / CHUNK_SIZE;
      testChunkDechunkWithMethodCall(numBytes, expectedChunks);
    }
  }

  private void testChunkDechunkWithMethodCall(final int numBytes, final int expectedChunks) {
    final ByteArrayChunker chunker = new ByteArrayChunker(CHUNK_SIZE);
    final byte[] bytes = getRandomBytes(numBytes);
    final List<byte[]> chunks = chunker.chunk(bytes);
    for (final byte[] chunk : chunks) {
      Assert.assertTrue(
          "Too long chunk " + chunk.length + " for " + numBytes + " bytes",
          chunk.length <= CHUNK_SIZE);
    }
    Assert.assertArrayEquals("Error for " + numBytes + " bytes", bytes, chunker.dechunk(chunks));
    Assert.assertEquals("Error for " + numBytes + " bytes", expectedChunks, chunks.size());
  }

  private byte[] getRandomBytes(final int length) {
    final byte[] bytes = new byte[length];
    for (int q = 0; q < bytes.length; q++) {
      bytes[q] = (byte) (Math.random() * 256);
    }
    return bytes;
  }
}
