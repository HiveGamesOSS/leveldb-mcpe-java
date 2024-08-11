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

import com.google.common.collect.Lists;
import org.iq80.leveldb.iterator.DBIteratorAdapter;
import org.iq80.leveldb.iterator.DbIterator;
import org.iq80.leveldb.iterator.MemTableIterator;
import org.iq80.leveldb.iterator.MergingIterator;
import org.iq80.leveldb.iterator.SeekingDBIteratorAdapter;
import org.iq80.leveldb.iterator.SeekingIterator;
import org.iq80.leveldb.iterator.SeekingIterators;
import org.iq80.leveldb.iterator.SnapshotSeekingIterator;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.iq80.leveldb.util.TestUtils.asciiToBytes;
import static org.iq80.leveldb.util.TestUtils.asciiToSlice;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MemTableTest
{
    /**
     * skipFirst + iter to last ok
     */
    @Test
    public void testTestSimple() throws Exception
    {
        final MemTableIterator iter = getMemTableIterator(new InternalKeyComparator(new BytewiseComparator()));
        assertTrue(iter.seekToFirst());
        assertEntry(iter, "k1", "v1", 101);
        assertTrue(iter.next());
        assertEntry(iter, "k1", "v1p", 100);
        assertTrue(iter.next());
        assertEntry(iter, "k2", "v2", 102);
        assertTrue(iter.next());
        assertEntry(iter, "k3", "v3", 103);
        assertTrue(iter.next());
        assertEntry(iter, "largekey", "vlarge", 104);
        assertFalse(iter.next());
    }

    @Test
    public void testMemIterator()
    {
        BytewiseComparator userComparator = new BytewiseComparator();
        InternalKeyComparator cmp = new InternalKeyComparator(userComparator);
        final MemTableIterator iter = getMemTableIterator(cmp);
        test(iter);
    }

    @Test
    public void testDbIterator()
    {
        BytewiseComparator userComparator = new BytewiseComparator();
        InternalKeyComparator cmp = new InternalKeyComparator(userComparator);
        final MemTableIterator iter = getMemTableIterator(cmp);
        MergingIterator mIter = new MergingIterator(Lists.newArrayList(iter), cmp);
        testUserKey(new SnapshotSeekingIterator(new DbIterator(mIter, () -> {
        }), Integer.MAX_VALUE, userComparator, (internalKey, bytes) -> {
        }));
    }

    @Test
    public void testMergingIterator()
    {
        BytewiseComparator userComparator = new BytewiseComparator();
        InternalKeyComparator cmp = new InternalKeyComparator(userComparator);
        final MemTableIterator iter = getMemTableIterator(cmp);
        MergingIterator mIter = new MergingIterator(Lists.newArrayList(iter), cmp);
        test(mIter);
    }

    @Test
    public void testSeekingIteratorAdapter()
    {
        BytewiseComparator userComparator = new BytewiseComparator();
        InternalKeyComparator cmp = new InternalKeyComparator(userComparator);
        final MemTableIterator iter = getMemTableIterator(cmp);
        MergingIterator mIter = new MergingIterator(Lists.newArrayList(iter), cmp);
        DBIteratorAdapter adapter = new DBIteratorAdapter(new SnapshotSeekingIterator(new DbIterator(mIter, () -> {
        }), Integer.MAX_VALUE, userComparator, (internalKey, bytes) -> {
        }));
        SeekingIterator<Slice, Slice> sliceSliceSeekingIterator = SeekingDBIteratorAdapter.toSeekingIterator(adapter, Slice::getBytes, Slice::new, Slice::new);
        testUserKey(sliceSliceSeekingIterator);
    }

    @Test
    public void testCollectionIterator()
    {
        BytewiseComparator userComparator = new BytewiseComparator();
        InternalKeyComparator cmp = new InternalKeyComparator(userComparator);
        final MemTableIterator iter = getMemTableIterator(cmp);
        final List<Map.Entry<InternalKey, Slice>> objects = new ArrayList<>();
        while (iter.next()) {
            objects.add(new InternalEntry(iter.key(), iter.value()));
        }
        SeekingIterator<InternalKey, Slice> internalKeySliceSeekingIterator = SeekingIterators.fromSortedList(objects, Map.Entry::getKey, Map.Entry::getValue, cmp);
        test(internalKeySliceSeekingIterator);
    }

    private void test(SeekingIterator<InternalKey, Slice> iter)
    {
        assertTrue(iter.next());
        assertEntry(iter, "k1", "v1", 101);
        assertTrue(iter.seekToFirst());
        assertEntry(iter, "k1", "v1", 101);
        iter.seekToLast();
        assertEntry(iter, "largekey", "vlarge", 104);
        assertTrue(iter.prev());
        assertTrue(iter.valid());
        assertEntry(iter, "k3", "v3", 103);
        assertTrue(iter.seek(new InternalKey(asciiToSlice("k2"), 102, ValueType.VALUE)));
        assertEntry(iter, "k2", "v2", 102);
        iter.seekToFirst();
        assertTrue(iter.seek(new InternalKey(asciiToSlice("k2"), 102, ValueType.VALUE)));
        assertEntry(iter, "k2", "v2", 102);
        assertTrue(iter.prev());
        assertEntry(iter, "k1", "v1p", 100);
        assertTrue(iter.next());
        assertEntry(iter, "k2", "v2", 102);
        assertTrue(iter.seek(new InternalKey(asciiToSlice("k1"), 100, ValueType.VALUE)));
        assertEntry(iter, "k1", "v1p", 100);
        assertTrue(iter.seek(new InternalKey(asciiToSlice("largekey"), 190, ValueType.VALUE)));
        assertEntry(iter, "largekey", "vlarge", 104);
        assertFalse(iter.seek(new InternalKey(asciiToSlice("largekey"), 100, ValueType.VALUE)));
        assertFalse(iter.valid());

    }

    private void testUserKey(SeekingIterator<Slice, Slice> iter)
    {
        assertTrue(iter.next());
        assertEntry(iter, "k1", "v1");
        assertTrue(iter.seekToFirst());
        assertEntry(iter, "k1", "v1");
        iter.seekToLast();
        assertEntry(iter, "largekey", "vlarge");
        assertTrue(iter.prev());
        assertTrue(iter.valid());
        assertEntry(iter, "k3", "v3");
        assertTrue(iter.seek(asciiToSlice("k2")));
        assertEntry(iter, "k2", "v2");
        iter.seekToFirst();
        assertTrue(iter.seek(asciiToSlice("k2")));
        assertEntry(iter, "k2", "v2");
        assertTrue(iter.prev());
        assertEntry(iter, "k1", "v1");
        assertTrue(iter.next());
        assertEntry(iter, "k2", "v2");
        assertTrue(iter.seek(asciiToSlice("k1")));
        assertEntry(iter, "k1", "v1");
        assertTrue(iter.seek(asciiToSlice("largekey")));
        assertEntry(iter, "largekey", "vlarge");
        assertFalse(iter.seek(asciiToSlice("largekez")));
        assertFalse(iter.valid());
    }

    private MemTableIterator getMemTableIterator(InternalKeyComparator cmp)
    {
        final MemTable memtable = new MemTable(cmp);
        WriteBatchImpl batch = new WriteBatchImpl();
        batch.put(asciiToBytes("k1"), asciiToBytes("v1p"));
        batch.put(asciiToBytes("k1"), asciiToBytes("v1"));
        batch.put(asciiToBytes("k2"), asciiToBytes("v2"));
        batch.put(asciiToBytes("k3"), asciiToBytes("v3"));
        batch.put(asciiToBytes("largekey"), asciiToBytes("vlarge"));
        batch.forEach(new InsertIntoHandler(memtable, 100));
        return memtable.iterator();
    }

    private void assertEntry(SeekingIterator<InternalKey, Slice> iter, String key, String value, int sequenceNumber)
    {
        assertEquals(new InternalKey(asciiToSlice(key), sequenceNumber, ValueType.VALUE), iter.key());
        assertEquals(asciiToSlice(value), iter.value());
    }

    private void assertEntry(SeekingIterator<Slice, Slice> iter, String key, String value)
    {
        assertEquals(asciiToSlice(key), iter.key());
        assertEquals(asciiToSlice(value), iter.value());
    }
}
