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

import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BlockTest
{
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyBuffer()
            throws Exception
    {
        new Block(Slices.EMPTY_SLICE, new BytewiseComparator());
    }

    @Test
    public void testEmptyBlock()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE);
    }

    @Test
    public void testSingleEntry()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("name", "dain sundstrom"));
    }

    @Test
    public void testMultipleEntriesWithNonSharedKey()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("beer", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("scotch", "Highland Park"));
    }

    @Test
    public void testMultipleEntriesWithSharedKey()
            throws Exception
    {
        blockTest(Integer.MAX_VALUE,
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("scotch", "Highland Park"));
    }

    @Test
    public void testMultipleEntriesWithNonSharedKeyAndRestartPositions()
            throws Exception
    {
        List<BlockEntry> entries = asList(
                BlockHelper.createBlockEntry("ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            blockTest(i, entries);
        }
    }

    @Test
    public void testMultipleEntriesWithSharedKeyAndRestartPositions()
            throws Exception
    {
        List<BlockEntry> entries = asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            blockTest(i, entries);
        }
    }

    @Test
    public void testNextPrev()
    {
        List<BlockEntry> entries = asList(
                BlockHelper.createBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.createBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.createBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.createBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.createBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.createBlockEntry("scotch/strong", "Lagavulin"));
        Block block = buildBLock(2, entries);
        try (BlockIterator it = block.iterator()) {
            assertTrue(it.next());
            assertEquals(entry(it), entries.get(0));
            assertFalse(it.prev());
            assertTrue(it.next());
            assertEquals(entry(it), entries.get(0));
            for (int i = 1; i < entries.size(); i++) {
                assertTrue(it.next());
                assertEquals(entry(it), entries.get(i), "Entry #" + i + " should match its pair");
                assertTrue(it.prev());
                assertEquals(entry(it), entries.get(i - 1));
                assertTrue(it.next());
                assertEquals(entry(it), entries.get(i));
            }
            assertFalse(it.next());
            assertTrue(it.prev());
            assertEquals(entry(it), entries.get(entries.size() - 1));
        }
    }

    private static BlockEntry entry(BlockIterator it)
    {
        return new BlockEntry(it.key(), it.value());
    }

    private static void blockTest(int blockRestartInterval, BlockEntry... entries)
    {
        blockTest(blockRestartInterval, asList(entries));
    }

    private static Block buildBLock(int blockRestartInterval, List<BlockEntry> entries)
    {
        BlockBuilder builder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator());

        for (BlockEntry entry : entries) {
            builder.add(entry);
        }

        assertEquals(builder.currentSizeEstimate(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));
        Slice blockSlice = builder.finish();
        assertEquals(builder.currentSizeEstimate(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));

        Block block = new Block(blockSlice, new BytewiseComparator());
        assertEquals(block.size(), BlockHelper.estimateBlockSize(blockRestartInterval, entries));

        return block;
    }

    private static void blockTest(int blockRestartInterval, List<BlockEntry> entries)
    {
        Block block = buildBLock(blockRestartInterval, entries);
        try (BlockIterator it = block.iterator()) {
            assertTrue(it.next() || entries.isEmpty(), "Next should return validity of iterator");
            BlockHelper.assertSequence(it, entries);

            assertTrue(it.seekToFirst() || entries.isEmpty());
            BlockHelper.assertSequence(it, entries);

            for (BlockEntry entry : entries) {
                List<BlockEntry> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
                assertEquals(it.seek(entry.getKey()), !nextEntries.isEmpty());
                BlockHelper.assertSequence(it, nextEntries);

                assertEquals(it.seek(BlockHelper.before(entry)), !nextEntries.isEmpty());
                BlockHelper.assertSequence(it, nextEntries);

                List<BlockEntry> entries1 = nextEntries.subList(1, nextEntries.size());
                assertEquals(it.seek(BlockHelper.after(entry)), !entries1.isEmpty());
                BlockHelper.assertSequence(it, entries1);
            }

            it.seek(Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}));
            BlockHelper.assertSequence(it, Collections.<BlockEntry>emptyList());
        }
        BlockIterator iterator = block.iterator();
        iterator.seekToFirst();
        iterator.close();
        assertFalse(iterator.valid());
        //Should not be possible to use iterator after close
        Assert.assertThrows(iterator::next);
        Assert.assertThrows(iterator::key);
        Assert.assertThrows(iterator::value);
        Assert.assertThrows(iterator::seekToFirst);
        Assert.assertThrows(iterator::seekToLast);
        Assert.assertThrows(iterator::close);
    }
}
