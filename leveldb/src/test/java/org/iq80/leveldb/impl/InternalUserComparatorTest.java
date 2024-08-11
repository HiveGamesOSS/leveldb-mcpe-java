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

import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class InternalUserComparatorTest
{
    final ValueType valueTypeForSeek = ValueType.VALUE;

    @Test
    public void testInternalKeyShortSeparator()
    {
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("foo", 99, ValueType.VALUE)));
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("foo", 101, ValueType.VALUE)));
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("foo", 100, ValueType.VALUE)));
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("foo", 100, ValueType.DELETION)));

        // When user keys are misordered
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("bar", 99, ValueType.VALUE)));

        // When user keys are different, but correctly ordered
        assertEquals(key("g", SequenceNumber.MAX_SEQUENCE_NUMBER, valueTypeForSeek),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("hello", 200, ValueType.VALUE)));

        // When start user key is prefix of limit user key
        assertEquals(key("foo", 100, ValueType.VALUE),
                shorten(key("foo", 100, ValueType.VALUE),
                        key("foobar", 200, ValueType.VALUE)));

        // When limit user key is prefix of start user key
        assertEquals(key("foobar", 100, ValueType.VALUE),
                shorten(key("foobar", 100, ValueType.VALUE),
                        key("foo", 200, ValueType.VALUE)));
    }

    @Test
    public void testInternalKeyShortestSuccessor()
    {
        assertEquals(key("g", SequenceNumber.MAX_SEQUENCE_NUMBER, valueTypeForSeek),
                shortSuccessor(key("foo", 100, ValueType.VALUE)));
        assertEquals(key(new byte[] {(byte) 0xff, (byte) 0xff}, 100, ValueType.VALUE),
                shortSuccessor(key(new byte[] {(byte) 0xff, (byte) 0xff}, 100, ValueType.VALUE)));
    }

    private InternalKey key(String foo, long sequenceNumber, ValueType value)
    {
        return new InternalKey(Slices.wrappedBuffer(foo.getBytes(UTF_8)), sequenceNumber, value);
    }

    private InternalKey key(byte[] key, long sequenceNumber, ValueType value)
    {
        return new InternalKey(Slices.wrappedBuffer(key), sequenceNumber, value);
    }

    private InternalKey shorten(InternalKey s, InternalKey l)
    {
        Slice shortestSeparator = new InternalUserComparator(new InternalKeyComparator(new BytewiseComparator()))
                .findShortestSeparator(s.encode(), l.encode());
        return new InternalKey(shortestSeparator);
    }

    private InternalKey shortSuccessor(InternalKey s)
    {
        Slice shortestSeparator = new InternalUserComparator(new InternalKeyComparator(new BytewiseComparator()))
                .findShortSuccessor(s.encode());
        return new InternalKey(shortestSeparator);
    }
}
