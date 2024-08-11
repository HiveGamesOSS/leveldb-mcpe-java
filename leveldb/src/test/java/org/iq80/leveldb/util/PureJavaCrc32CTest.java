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
package org.iq80.leveldb.util;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.iq80.leveldb.util.PureJavaCrc32C.mask;
import static org.iq80.leveldb.util.PureJavaCrc32C.unmask;
import static org.testng.Assert.assertFalse;

public class PureJavaCrc32CTest
{
    private static final IntFunction<ByteBuffer> DIRECT_LE = cap -> ByteBuffer.allocateDirect(cap).order(ByteOrder.LITTLE_ENDIAN);
    private static final IntFunction<ByteBuffer> DIRECT_BE = cap -> ByteBuffer.allocateDirect(cap).order(ByteOrder.BIG_ENDIAN);
    private static final IntFunction<ByteBuffer> HEAP = cap -> ByteBuffer.allocate(cap);

    @Test(dataProvider = "crcs")
    public void testStandardResults(int expectedCrc, byte[] data)
    {
        assertEquals(computeCrc(data), expectedCrc);
    }

    @Test(dataProvider = "crcs")
    public void testBufferStandardResults(int expectedCrc, byte[] b)
    {
        //ensure correct handling of offset/positions/limits in DIRECT_LE/DIRECT_BE/array buffers
        assertCrcWithBuffers(expectedCrc, b, DIRECT_LE);
        assertCrcWithBuffers(expectedCrc, b, DIRECT_BE);
        assertCrcWithBuffers(expectedCrc, b, HEAP);
        //with array offset
        final byte[] dest = new byte[b.length + 2];
        System.arraycopy(b, 0, dest, 2, b.length);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(dest, 2, b.length);
        assertEquals(expectedCrc, computeCrc(byteBuffer));
    }

    private void assertCrcWithBuffers(int expectedCrc, byte[] b, IntFunction<ByteBuffer> factory)
    {
        assertEquals(expectedCrc, computeCrc(fillBuffer(b, factory.apply(b.length), 0))); //position = 0 & limit = b.length
        assertEquals(expectedCrc, computeCrc(fillBuffer(b, factory.apply(b.length + 5), 0))); //limit < than accessible size
        assertEquals(expectedCrc, computeCrc(fillBuffer(b, factory.apply(b.length + 5), 2)));  //position > 0 & limit < than accessible size
        assertEquals(expectedCrc, computeCrc(fillBuffer(b, factory.apply(b.length + 7), 2)));  //position > 0 & limit < than accessible size
    }

    @DataProvider(name = "crcs")
    public Object[][] data()
    {
        return new Object[][] {
                // Standard results from rfc3720 section B.4.
                new Object[] {0x8a9136aa, arrayOf(32, (byte) 0)},
                new Object[] {0x62a8ab43, arrayOf(32, (byte) 0xff)},
                new Object[] {0x46dd794e, arrayOf(32, position -> (byte) position.intValue())},
                new Object[] {0x113fdb5c, arrayOf(32, position -> (byte) (31 - position))},
                new Object[] {0xd9963a56, arrayOf(new int[] {
                        0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})}
        };
    }

    @Test
    public void testProducesDifferentCrcs()
            throws UnsupportedEncodingException
    {
        assertFalse(computeCrc("a".getBytes(US_ASCII)) == computeCrc("foo".getBytes(US_ASCII)));
    }

    @Test
    public void testProducesDifferentCrcs2()
            throws UnsupportedEncodingException
    {
        assertFalse(computeCrc(fillBuffer("a".getBytes(US_ASCII), ByteBuffer.allocateDirect(10), 0)) == computeCrc(fillBuffer("foo".getBytes(US_ASCII), ByteBuffer.allocateDirect(10), 0)));
    }

    @Test
    public void testLoopUnroll() throws Exception
    {
        assertCrcWithBuffers(0xb219db69, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, DIRECT_BE);
        assertCrcWithBuffers(0xb219db69, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, DIRECT_LE);
        assertCrcWithBuffers(0xb219db69, new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, HEAP);

        assertCrcWithBuffers(0xbd3a64dc, new byte[] {1, 2, 3, 4, 5, 6, 7}, DIRECT_BE);
        assertCrcWithBuffers(0xbd3a64dc, new byte[] {1, 2, 3, 4, 5, 6, 7}, DIRECT_LE);
        assertCrcWithBuffers(0xbd3a64dc, new byte[] {1, 2, 3, 4, 5, 6, 7}, HEAP);
    }

    @Test
    public void testComposes()
            throws UnsupportedEncodingException
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update("hello ".getBytes(US_ASCII), 0, 6);
        crc.update("world".getBytes(US_ASCII), 0, 5);

        assertEquals(crc.getIntValue(), computeCrc("hello world".getBytes(US_ASCII)));
    }

    @Test
    public void testComposesDirectBuffers()
            throws UnsupportedEncodingException
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(fillBuffer("hello ".getBytes(US_ASCII), ByteBuffer.allocateDirect(6), 0));
        crc.update(fillBuffer("world".getBytes(US_ASCII), ByteBuffer.allocateDirect(5), 0));

        assertEquals(crc.getIntValue(), computeCrc("hello world".getBytes(US_ASCII)));
    }

    @Test
    public void testMask()
            throws UnsupportedEncodingException
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update("foo".getBytes(US_ASCII), 0, 3);

        assertEquals(crc.getMaskedValue(), mask(crc.getIntValue()));
        assertFalse(crc.getIntValue() == crc.getMaskedValue(), "crc should not match masked crc");
        assertFalse(crc.getIntValue() == mask(crc.getMaskedValue()), "crc should not match double masked crc");
        assertEquals(crc.getIntValue(), unmask(crc.getMaskedValue()));
        assertEquals(crc.getIntValue(), unmask(unmask(mask(crc.getMaskedValue()))));
    }

    private static int computeCrc(byte[] data)
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(data, 0, data.length);
        return crc.getIntValue();
    }

    private static int computeCrc(ByteBuffer buffer)
    {
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(buffer);
        return crc.getIntValue();
    }

    private static ByteBuffer fillBuffer(byte[] data, ByteBuffer byteBuffer, int initialPos)
    {
        byteBuffer.position(initialPos);
        byteBuffer.put(data);
        byteBuffer.position(initialPos);
        byteBuffer.limit(initialPos + data.length);
        return byteBuffer;
    }

    private static byte[] arrayOf(int size, byte value)
    {
        byte[] result = new byte[size];
        Arrays.fill(result, value);
        return result;
    }

    @SuppressWarnings("ConstantConditions")
    private static byte[] arrayOf(int size, Function<Integer, Byte> generator)
    {
        byte[] result = new byte[size];
        for (int i = 0; i < result.length; ++i) {
            result[i] = generator.apply(i);
        }

        return result;
    }

    private static byte[] arrayOf(int[] bytes)
    {
        byte[] result = new byte[bytes.length];
        for (int i = 0; i < result.length; ++i) {
            result[i] = (byte) bytes[i];
        }

        return result;
    }

    private void assertEquals(int actual, int required)
    {
        Assert.assertEquals(actual, required, String.format("Required 0x%s but actual is 0x%s", Integer.toHexString(required), Integer.toHexString(actual)));
    }
}
