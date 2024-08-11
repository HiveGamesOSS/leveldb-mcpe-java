/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.iq80.leveldb.util.PureJavaCrc32C;
import org.iq80.leveldb.env.SequentialFile;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.env.WritableFile;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class LogReaderTest
{
    private StringDest dest;
    private StringSource source;
    private ReportCollector report;

    private boolean reading;
    private LogWriter writer;
    private LogReader reader;
    private int[] initialOffsetRecordSizes;
    private long[] initialOffsetLastRecordOffsets;
    private int numInitialOffsetRecords;

    @BeforeMethod
    public void setUp()
    {
        dest = new StringDest();
        source = new StringSource();
        report = new ReportCollector();

        reading = false;
        writer = LogWriter.createWriter(0, dest);
        reader = new LogReader(source, report, true/*checksum*/, 0/*initialOffset*/);
        initialOffsetRecordSizes = new int[] {
                10000,  // Two sizable records in first block
                10000,
                2 * BLOCK_SIZE - 1000,  // Span three blocks
                1,
                13716,  // Consume all but two bytes of block 3.
                BLOCK_SIZE - HEADER_SIZE, // Consume the entirety of block 4.
        };
        initialOffsetLastRecordOffsets = new long[] {
                0,
                HEADER_SIZE + 10000,
                2 * (HEADER_SIZE + 10000),
                2 * (HEADER_SIZE + 10000) +
                        (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
                2 * (HEADER_SIZE + 10000) +
                        (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE
                        + HEADER_SIZE + 1,
                3 * BLOCK_SIZE,
        };
        numInitialOffsetRecords = initialOffsetLastRecordOffsets.length;
    }

    @Test
    public void testEmpty() throws Exception
    {
        assertEquals(read(), "EOF");
    }

    @Test
    public void testReadWrite() throws Exception
    {
        write("foo");
        write("bar");
        write("");
        write("xxxx");
        assertEquals(read(), "foo");
        assertEquals(read(), "bar");
        assertEquals(read(), "");
        assertEquals(read(), "xxxx");
        assertEquals(read(), "EOF");
        assertEquals(read(), "EOF");  // Make sure reads at eof work
    }

    @Test
    public void testManyBlocks() throws Exception
    {
        for (int i = 0; i < 100000; i++) {
            write(numberString(i));
        }
        for (int i = 0; i < 100000; i++) {
            assertEquals(read(), numberString(i));
        }
        assertEquals(read(), "EOF");
    }

    @Test
    public void testFragmentation() throws Exception
    {
        write("small");
        write(bigString("medium", 50000));
        write(bigString("large", 100000));
        assertEquals(read(), "small");
        assertEquals(read(), bigString("medium", 50000));
        assertEquals(read(), bigString("large", 100000));
        assertEquals(read(), "EOF");
    }

    @Test
    public void testMarginalTrailer() throws Exception
    {
        // Make a trailer that is exactly the same length as an empty record.
        int n = BLOCK_SIZE - 2 * HEADER_SIZE;
        write(bigString("foo", n));
        assertEquals(writtenBytes(), BLOCK_SIZE - HEADER_SIZE);
        write("");
        write("bar");
        assertEquals(read(), bigString("foo", n));
        assertEquals(read(), "");
        assertEquals(read(), "bar");
        assertEquals(read(), "EOF");
    }

    @Test
    public void testMarginalTrailer2() throws Exception
    {
        // Make a trailer that is exactly the same length as an empty record.
        int n = BLOCK_SIZE - 2 * HEADER_SIZE;
        write(bigString("foo", n));
        assertEquals(writtenBytes(), BLOCK_SIZE - HEADER_SIZE);
        write("bar");
        assertEquals(read(), bigString("foo", n));
        assertEquals(read(), "bar");
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 0);
        assertEquals(reportMessage(), "");
    }

    @Test
    public void testShortTrailer() throws Exception
    {
        int n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        write(bigString("foo", n));
        assertEquals(writtenBytes(), BLOCK_SIZE - HEADER_SIZE + 4);
        write("");
        write("bar");
        assertEquals(read(), bigString("foo", n));
        assertEquals(read(), "");
        assertEquals(read(), "bar");
        assertEquals(read(), "EOF");
    }

    @Test
    public void testAlignedEof() throws Exception
    {
        int n = BLOCK_SIZE - 2 * HEADER_SIZE + 4;
        write(bigString("foo", n));
        assertEquals(writtenBytes(), BLOCK_SIZE - HEADER_SIZE + 4);
        assertEquals(read(), bigString("foo", n));
        assertEquals(read(), "EOF");
    }

    @Test
    public void testOpenForAppend() throws Exception
    {
        write("hello");
        reopenForAppend();
        write("world");
        assertEquals(read(), "hello");
        assertEquals(read(), "world");
        assertEquals(read(), "EOF");
    }

    @Test
    public void testRandomRead() throws Exception
    {
        int n = 500;
        Random writeRnd = new Random(301);
        for (int i = 0; i < n; i++) {
            write(randomSkewedString(i, writeRnd));
        }
        Random readRnd = new Random(301);
        for (int i = 0; i < n; i++) {
            assertEquals(read(), randomSkewedString(i, readRnd));
        }
        assertEquals(read(), "EOF");
    }

    // Tests of all the error paths in LogLeader follow:
    @Test
    public void testReadError() throws Exception
    {
        write("foo");
        forceError();
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), BLOCK_SIZE);
        assertEquals(matchError("read error"), "OK");
    }

    @Test
    public void testBadRecordType() throws Exception
    {
        write("foo");
        // Type is stored in header[6]
        incrementByte(6, 100);
        fixChecksum(0, 3);
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 3);
        assertEquals(matchError("unknown record type"), "OK");
    }

    @Test
    public void testTruncatedTrailingRecordIsIgnored() throws Exception
    {
        write("foo");
        shrinkSize(4);   // Drop all payload as well as a header byte
        assertEquals(read(), "EOF");
        // Truncated last record is ignored, not treated as an error.
        assertEquals(droppedBytes(), 0);
        assertEquals(reportMessage(), "");
    }

    @Test
    public void testBadLength() throws Exception
    {
        int kPayloadSize = BLOCK_SIZE - HEADER_SIZE;
        write(bigString("bar", kPayloadSize));
        write("foo");
        // Least significant size byte is stored in header[4].
        incrementByte(4, 1);
        assertEquals(read(), "foo");
        assertEquals(droppedBytes(), BLOCK_SIZE);
        assertEquals(matchError("bad record length"), "OK");
    }

    @Test
    public void testBadLengthAtEndIsIgnored() throws Exception
    {
        write("foo");
        shrinkSize(1);
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 0);
        assertEquals(reportMessage(), "");
    }

    @Test
    public void testChecksumMismatch() throws Exception
    {
        write("foo");
        incrementByte(0, 10);
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 10);
        assertEquals(matchError("checksum mismatch"), "OK");
    }

    @Test
    public void testUnexpectedMiddleType() throws Exception
    {
        write("foo");
        setByte(6, LogChunkType.MIDDLE.getPersistentId());
        fixChecksum(0, 3);
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 3);
        assertEquals(matchError("missing start"), "OK");
    }

    @Test
    public void testUnexpectedLastType() throws Exception
    {
        write("foo");
        setByte(6, LogChunkType.LAST.getPersistentId());
        fixChecksum(0, 3);
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 3);
        assertEquals(matchError("missing start"), "OK");
    }

    @Test
    public void testUnexpectedFullType() throws Exception
    {
        write("foo");
        write("bar");
        setByte(6, LogChunkType.FIRST.getPersistentId());
        fixChecksum(0, 3);
        assertEquals(read(), "bar");
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 3);
        assertEquals(matchError("partial record without end"), "OK");
    }

    @Test
    public void testUnexpectedFirstType() throws Exception
    {
        write("foo");
        write(bigString("bar", 100000));
        setByte(6, LogChunkType.FIRST.getPersistentId());
        fixChecksum(0, 3);
        assertEquals(read(), bigString("bar", 100000));
        assertEquals(read(), "EOF");
        assertEquals(droppedBytes(), 3);
        assertEquals(matchError("partial record without end"), "OK");
    }

    @Test
    public void testMissingLastIsIgnored() throws Exception
    {
        write(bigString("bar", BLOCK_SIZE));
        // Remove the LAST block, including header.
        shrinkSize(14);
        assertEquals(read(), "EOF");
        assertEquals(reportMessage(), "");
        assertEquals(droppedBytes(), 0);
    }

    @Test
    public void testPartialLastIsIgnored() throws Exception
    {
        write(bigString("bar", BLOCK_SIZE));
        // Cause a bad record length in the LAST block.
        shrinkSize(1);
        assertEquals(read(), "EOF");
        assertEquals(reportMessage(), "");
        assertEquals(droppedBytes(), 0);
    }

    @Test
    public void testSkipIntoMultiRecord() throws Exception
    {
        // Consider a fragmented record:
        //    first(R1), middle(R1), last(R1), first(R2)
        // If initialOffset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        write(bigString("foo", 3 * BLOCK_SIZE));
        write("correct");
        startReadingAt(BLOCK_SIZE);

        assertEquals(read(), "correct");
        assertEquals(reportMessage(), "");
        assertEquals(droppedBytes(), 0);
        assertEquals(read(), "EOF");
    }

    @Test
    public void testErrorJoinsRecords() throws Exception
    {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        // write records that span two blocks
        write(bigString("foo", BLOCK_SIZE));
        write(bigString("bar", BLOCK_SIZE));
        write("correct");

        // Wipe the middle block
        for (int offset = BLOCK_SIZE; offset < 2 * BLOCK_SIZE; offset++) {
            setByte(offset, 'x');
        }

        assertEquals(read(), "correct");
        assertEquals(read(), "EOF");
        int dropped = droppedBytes();
        assertTrue((long) dropped <= (long) (2 * BLOCK_SIZE + 100));
        assertTrue((long) dropped >= (long) (2 * BLOCK_SIZE));
    }

    @Test
    public void testReadStart() throws Exception
    {
        checkInitialOffsetRecord(0, 0);
    }

    @Test
    public void testReadSecondOneOff() throws Exception
    {
        checkInitialOffsetRecord(1, 1);
    }

    @Test
    public void testReadSecondTenThousand() throws Exception
    {
        checkInitialOffsetRecord(10000, 1);
    }

    @Test
    public void testReadSecondStart() throws Exception
    {
        checkInitialOffsetRecord(10007, 1);
    }

    @Test
    public void testReadThirdOneOff() throws Exception
    {
        checkInitialOffsetRecord(10008, 2);
    }

    @Test
    public void testReadThirdStart() throws Exception
    {
        checkInitialOffsetRecord(20014, 2);
    }

    @Test
    public void testReadFourthOneOff() throws Exception
    {
        checkInitialOffsetRecord(20015, 3);
    }

    @Test
    public void testReadFourthFirstBlockTrailer() throws Exception
    {
        checkInitialOffsetRecord(BLOCK_SIZE - 4, 3);
    }

    @Test
    public void testReadFourthMiddleBlock() throws Exception
    {
        checkInitialOffsetRecord(BLOCK_SIZE + 1, 3);
    }

    @Test
    public void testReadFourthLastBlock() throws Exception
    {
        checkInitialOffsetRecord(2 * BLOCK_SIZE + 1, 3);
    }

    @Test
    public void testReadFourthStart() throws Exception
    {
        checkInitialOffsetRecord(
                2 * (HEADER_SIZE + 1000) + (2 * BLOCK_SIZE - 1000) + 3 * HEADER_SIZE,
                3);
    }

    @Test
    public void testReadInitialOffsetIntoBlockPadding() throws Exception
    {
        checkInitialOffsetRecord(3 * BLOCK_SIZE - 3, 5);
    }

    @Test
    public void testReadEnd() throws Exception
    {
        checkOffsetPastEndReturnsNoRecords(0);
    }

    @Test
    public void testReadPastEnd() throws Exception
    {
        checkOffsetPastEndReturnsNoRecords(5);
    }

    private String read()
    {
        if (!reading) {
            reading = true;
            source.contents = new Slice(dest.contents.toByteArray());
        }

        Slice slice = reader.readRecord();
        if (slice != null) {
            return new String(slice.getBytes());
        }
        else {
            return "EOF";
        }
    }

    private static String bigString(String partialString, int n)
    {
        return Strings.repeat(partialString, (n / partialString.length()) + 1).substring(0, n);
    }

    // Construct a string from a number
    private static String numberString(int n)
    {
        return n + ".";
    }

    // Return a skewed potentially long string
    static String randomSkewedString(int i, Random rnd)
    {
        return bigString(numberString(i), skewed(rnd, 17));
    }

    private static int skewed(Random rnd, int i1)
    {
        return rnd.nextInt(Integer.MAX_VALUE) % (1 << rnd.nextInt(Integer.MAX_VALUE) % (i1 + 1));
    }

    void reopenForAppend() throws IOException
    {
        writer.close();

        writer = LogWriter.createWriter(0, dest, dest.contents.size());
    }

    void incrementByte(int offset, int delta)
    {
        dest.contents.getBuf()[offset] += (byte) delta;
    }

    void setByte(int offset, int newByte)
    {
        dest.contents.getBuf()[offset] = (byte) newByte;
    }

    void shrinkSize(int bytes)
    {
        dest.contents.shrink(dest.contents.size() - bytes);
    }

    void fixChecksum(int headerOffset, int len)
    {
        // Compute crc of type/len/data
        PureJavaCrc32C jCrc = new PureJavaCrc32C();
        jCrc.update(dest.contents.getBuf(), headerOffset + 6, 1 + len);
        int crc = jCrc.getMaskedValue();
        new Slice(dest.contents.getBuf()).setInt(headerOffset, crc);
    }

    void forceError()
    {
        source.forceError = true;
    }

    int droppedBytes()
    {
        return report.doppedBytes;
    }

    String reportMessage()
    {
        return report.message;
    }

    // Returns OK iff recorded error message contains "msg"
    String matchError(String msg)
    {
        if (!report.message.contains(msg)) {
            return report.message;
        }
        else {
            return "OK";
        }
    }

    void write(String msg) throws IOException
    {
        assertTrue(!reading, "write() after starting to read");
        writer.addRecord(new Slice(msg.getBytes()), false);
    }

    long writtenBytes()
    {
        return dest.contents.size();
    }

    void writeInitialOffsetLog() throws IOException
    {
        for (int i = 0; i < numInitialOffsetRecords; i++) {
            write(Strings.repeat(String.valueOf((char) ((byte) 'a' + i)), initialOffsetRecordSizes[i]));
        }
    }

    void startReadingAt(long initialOffset)
    {
        reader = new LogReader(source, report, true/*checksum*/, initialOffset);
    }

    void checkOffsetPastEndReturnsNoRecords(long offsetPastEnd) throws IOException
    {
        writeInitialOffsetLog();
        reading = true;
        source.contents = new Slice(dest.contents.toByteArray());
        LogReader offsetReader = new LogReader(source, report, true/*checksum*/,
                writtenBytes() + offsetPastEnd);

        assertNull(offsetReader.readRecord());
    }

    void checkInitialOffsetRecord(long initialOffset,
                                  int expectedRecordOffset) throws IOException
    {
        writeInitialOffsetLog();
        reading = true;
        source.contents = new Slice(dest.contents.toByteArray());
        LogReader offsetReader = new LogReader(source, report, true/*checksum*/,
                initialOffset);

        // read all records from expectedRecordOffset through the last one.
        assertLt(expectedRecordOffset, numInitialOffsetRecords);
        for (; expectedRecordOffset < numInitialOffsetRecords;
             ++expectedRecordOffset) {
            Slice record = offsetReader.readRecord();
            assertEquals(record.length(), initialOffsetRecordSizes[expectedRecordOffset]);
            assertEquals(offsetReader.getLastRecordOffset(), initialOffsetLastRecordOffsets[expectedRecordOffset]);
            assertEquals(record.getByte(0), (char) ('a' + expectedRecordOffset));
        }
    }

    private void assertLt(long a, long b)
    {
        assertTrue(a < b, "Expect that " + a + "<" + b + " is false");
    }

    private static class StringSource
            implements SequentialFile
    {
        Slice contents = new Slice(0);
        boolean forceError = false;
        boolean returnedPartial = false;

        @Override
        public void skip(long n) throws IOException
        {
            if (n > contents.length()) {
                contents = new Slice(0);
                throw new IOException("in-memory file skipped past end");
            }

            contents = contents.slice((int) n, contents.length() - (int) n);
        }

        @Override
        public int read(int atMost, SliceOutput destination) throws IOException
        {
            assertTrue(!returnedPartial, "must not read() after eof/error");
            if (forceError) {
                forceError = false;
                returnedPartial = true;
                throw new IOException("read error");
            }
            int read = atMost;
            int available = contents.length();
            if (available == 0) {
                returnedPartial = true;
                return -1; //eof
            }
            else if (available < read) {
                read = contents.length();
            }
            destination.writeBytes(contents, 0, read);
            contents = contents.slice(read, contents.length() - read);
            return read;
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    private static class StringDest
            implements WritableFile
    {
        SettableByteArrayOutputStream contents = new SettableByteArrayOutputStream();

        @Override
        public void append(Slice data) throws IOException
        {
            contents.write(data.getBytes());
        }

        @Override
        public void force() throws IOException
        {
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    private static class SettableByteArrayOutputStream
            extends ByteArrayOutputStream
    {
        //expose buffer
        public byte[] getBuf()
        {
            return buf;
        }

        //expose buffer
        public void shrink(int size)
        {
            Preconditions.checkElementIndex(size, count);
            count = size;
        }
    }

    private static class ReportCollector
            implements LogMonitor
    {
        int doppedBytes;
        String message = "";

        @Override
        public void corruption(long bytes, String reason)
        {
            doppedBytes += bytes;
            message += reason;
        }

        @Override
        public void corruption(long bytes, Throwable reason)
        {
            doppedBytes += bytes;
            message += reason.getMessage();
        }
    }
}
