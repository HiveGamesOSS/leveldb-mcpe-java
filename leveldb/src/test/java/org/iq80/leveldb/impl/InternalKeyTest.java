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

import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class InternalKeyTest
{
    @Test
    public void testEncodeDecode() throws Exception
    {
        String[] keys = {"", "k", "hello", "longggggggggggggggggggggg"};
        long[] seq = {
                1, 2, 3,
                (1L << 8) - 1, 1L << 8, (1L << 8) + 1,
                (1L << 16) - 1, 1L << 16, (1L << 16) + 1,
                (1L << 32) - 1, 1L << 32, (1L << 32) + 1
        };
        for (String key : keys) {
            for (long s : seq) {
                testKey(key, s, ValueType.VALUE);
                testKey("hello", 1, ValueType.DELETION);
            }
        }
        try {
            InternalKey internalKey = new InternalKey(new Slice("bar".getBytes(UTF_8)));
            Assert.fail("value " + internalKey + " ot expected");
        }
        catch (Exception e) {
            //expected
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDecodeEmpty()
    {
        new InternalKey(Slices.wrappedBuffer(new byte[0]));
    }

    private void testKey(String key, long seq, ValueType valueType)
    {
        InternalKey k = new InternalKey(Slices.wrappedBuffer(key.getBytes(UTF_8)), seq, valueType);
        InternalKey decoded = new InternalKey(k.encode());

        assertEquals(key, decoded.getUserKey().toString(UTF_8));
        assertEquals(seq, decoded.getSequenceNumber());
        assertEquals(valueType, decoded.getValueType());
    }
}
