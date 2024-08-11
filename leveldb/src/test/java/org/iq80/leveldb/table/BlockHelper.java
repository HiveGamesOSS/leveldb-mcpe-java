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
package org.iq80.leveldb.table;

import com.google.common.collect.Iterables;
import org.iq80.leveldb.iterator.SeekingIterator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.iterator.IteratorTestUtils.entry;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public final class BlockHelper
{
    private BlockHelper()
    {
    }

    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        if (entries.isEmpty()) {
            return SIZE_OF_INT * 2; //restart[0] + restart size
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZE_OF_INT) +
                SIZE_OF_INT;
    }

    @SafeVarargs
    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries)
    {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    @SafeVarargs
    public static <K, V> void assertReverseSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries)
    {
        assertReverseSequence(seekingIterator, Arrays.asList(entries));
    }

    private static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries, Function<SeekingIterator<K, V>, Boolean> next)
    {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");
        boolean valid = true;
        for (Entry<K, V> entry : entries) {
            assertTrue(valid, "Last method should have return true");
            assertTrue(seekingIterator.valid(), "Expecting next element to be " + entry);
            assertEntryEquals(entry(seekingIterator), entry);
            valid = next.apply(seekingIterator);
        }
        assertFalse(valid && !Iterables.isEmpty(entries), "Last method should have return false");
        assertFalse(seekingIterator.valid());

        assertFalse(next.apply(seekingIterator), "expected no more elements");
        assertThrows(NoSuchElementException.class, seekingIterator::key);
        assertThrows(NoSuchElementException.class, seekingIterator::value);
    }

    public static <K, V> void assertReverseSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries)
    {
        assertSequence(seekingIterator, entries, SeekingIterator::prev);
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries)
    {
        assertSequence(seekingIterator, entries, SeekingIterator::next);
    }

    public static <K, V> void assertEntryEquals(Entry<K, V> actual, Entry<K, V> expected)
    {
        if (actual.getKey() instanceof Slice) {
            assertSliceEquals((Slice) actual.getKey(), (Slice) expected.getKey());
            assertSliceEquals((Slice) actual.getValue(), (Slice) expected.getValue());
        }
        else {
            assertEquals(actual.getKey(), expected.getKey());
            assertEquals(actual.getValue(), expected.getValue());
        }
    }

    public static void assertSliceEquals(Slice actual, Slice expected)
    {
        assertEquals(actual.toString(UTF_8), expected.toString(UTF_8));
    }

    public static String beforeString(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String afterString(Entry<String, ?> expectedEntry)
    {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static Slice before(Entry<Slice, ?> expectedEntry)
    {
        Slice slice = expectedEntry.getKey().copySlice(0, expectedEntry.getKey().length());
        int lastByte = slice.length() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) - 1);
        return slice;
    }

    public static Slice after(Entry<Slice, ?> expectedEntry)
    {
        Slice slice = expectedEntry.getKey().copySlice(0, expectedEntry.getKey().length());
        int lastByte = slice.length() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) + 1);
        return slice;
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
    {
        int size = 0;
        Slice previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = entry.getKey().length() - BlockBuilder.calculateSharedBytes(entry.getKey(), previousKey);
            }
            else {
                nonSharedBytes = entry.getKey().length();
                restartBlockCount = 0;
            }
            size += nonSharedBytes +
                    entry.getValue().length() +
                    (SIZE_OF_BYTE * 3); // 3 bytes for sizes

            previousKey = entry.getKey();
            restartBlockCount++;

        }
        return size;
    }

    static BlockEntry createBlockEntry(String key, String value)
    {
        return new BlockEntry(Slices.copiedBuffer(key, UTF_8), Slices.copiedBuffer(value, UTF_8));
    }
}
