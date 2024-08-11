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
package org.iq80.leveldb.iterator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MemTableIteratorTest
{
    List<Map.Entry<InternalKey, Slice>> sampleEntries1 = Lists.newArrayList(entry("k1", 102, ValueType.VALUE, "v2"), entry("k1", 100, ValueType.VALUE, "v1"), entry("k2", 100, ValueType.VALUE, "v1"), entry("k3", 300, ValueType.VALUE, "v1"), entry("k4", 100, ValueType.VALUE, "v1")
    );

    protected InternalIterator iterFactory(List<Map.Entry<InternalKey, Slice>> data)
    {
        return new MemTableIterator(getMapOf(data));
    }

    @Test
    public void testSeekAfterLast() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.seek(entry("k4", 90, ValueType.VALUE, "").getKey()));
        assertTrue(iter.seek(entry("k4", 200, ValueType.VALUE, "").getKey()));
        assertEntryEquals(iter, sampleEntries1.get(4));
        assertTrue(iter.seek(entry("k1", 101, ValueType.VALUE, "").getKey()));
        assertEntryEquals(iter, sampleEntries1.get(1));
        assertTrue(iter.seek(entry("k1", 100, ValueType.VALUE, "").getKey()));
        assertEntryEquals(iter, sampleEntries1.get(1));
        assertTrue(iter.seek(entry("k1", 102, ValueType.VALUE, "").getKey()));
        assertEntryEquals(iter, sampleEntries1.get(0));
    }

    @Test
    public void testForwardScanAfterSeek() throws Exception
    {
        int count = 0;
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        Iterator<Map.Entry<InternalKey, Slice>> iterator = sampleEntries1.iterator();
        Iterators.advance(iterator, 2);
        for (boolean valid = iter.seek(new InternalKey(sampleEntries1.get(2).getKey().encode())); valid; valid = iter.next()) {
            assertEntryEquals(iter, iterator.next());
            count++;
        }
        assertFalse(iter.next());
        assertEndAndClose(iter);
        assertEquals(count, sampleEntries1.size() - 2);
    }

    @Test
    public void testForwardScanAfterSeekFirst() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testForwardScan() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        validateAll(iter, iter.next(), sampleEntries1);
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testReverseScan() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertFalse(iter.prev());
        assertEndAndClose(iter);
    }

    @Test
    public void testReverseScanAfterSeekLast() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        validateAllReverse(iter, iter.seekToLast(), sampleEntries1);
        validateAllReverse(iter, iter.seekToLast(), sampleEntries1);
        assertFalse(iter.prev());
        assertTrue(iter.next());
        assertFalse(iter.prev());
        assertEndAndClose(iter);
    }

    @Test
    public void testReverseScanAfterSeekFirst() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertTrue(iter.seekToFirst());
        assertFalse(iter.prev());
        assertEndAndClose(iter);
    }

    @Test
    public void testForwardScanAfterSeekLast() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertTrue(iter.seekToLast());
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testEmpty() throws Exception
    {
        ConcurrentSkipListMap<InternalKey, Slice> table = getMapOf(Collections.emptyList());
        {
            MemTableIterator iter = new MemTableIterator(table);
            assertFalse(iter.valid());
            assertFalse(iter.seekToLast());
            assertFalse(iter.next());
            assertFalse(iter.prev());
            assertEndAndClose(iter);
        }
        {
            MemTableIterator iter = new MemTableIterator(table);
            assertFalse(iter.valid());
            assertFalse(iter.seekToLast());
            assertFalse(iter.prev());
            assertFalse(iter.next());
            assertEndAndClose(iter);
        }
        {
            MemTableIterator iter = new MemTableIterator(table);
            assertFalse(iter.next());
            assertFalse(iter.valid());
            assertFalse(iter.prev());
            assertFalse(iter.seekToLast());
            assertFalse(iter.seekToFirst());
            assertEndAndClose(iter);
        }
    }

    @Test
    public void testReverseAfterLastNext() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        validateAll(iter, iter.next(), sampleEntries1);
        validateAllReverse(iter, iter.prev(), sampleEntries1);
        assertEndAndClose(iter);
    }

    @Test
    public void testNextAfterLastPrev() throws Exception
    {
        InternalIterator iter = iterFactory(sampleEntries1);
        validateAllReverse(iter, iter.seekToLast(), sampleEntries1);
        validateAll(iter, iter.next(), sampleEntries1);
        assertEndAndClose(iter);
    }

    private void validateAll(InternalIterator iter, boolean valid1, List<Map.Entry<InternalKey, Slice>> data)
    {
        int count = 0;
        Iterator<Map.Entry<InternalKey, Slice>> iterator = data.iterator();
        for (boolean valid = valid1; valid; valid = iter.next()) {
            assertEntryEquals(iter, iterator.next());
            count++;
        }
        assertEquals(count, data.size());
    }

    private void validateAllReverse(InternalIterator iter, boolean valid1, List<Map.Entry<InternalKey, Slice>> data)
    {
        int count = 0;
        Iterator<Map.Entry<InternalKey, Slice>> iterator = Lists.reverse(data).iterator();
        for (boolean valid = valid1; valid; valid = iter.prev()) {
            assertEntryEquals(iter, iterator.next());
            count++;
        }
        assertEquals(count, data.size());
    }

    private void assertEndAndClose(InternalIterator iter) throws IOException
    {
        assertFalse(iter.valid());
        Assert.assertThrows(NoSuchElementException.class, iter::key);
        Assert.assertThrows(NoSuchElementException.class, iter::value);
        iter.close();
        //after close, call should not succeed
        Assert.assertThrows(iter::next);
        Assert.assertThrows(iter::prev);
        Assert.assertThrows(iter::seekToFirst);
        Assert.assertThrows(iter::seekToLast);
        Assert.assertThrows(() -> iter.seek(new InternalKey(new Slice("k1".getBytes()), 100, ValueType.DELETION)));
    }

    protected final ConcurrentSkipListMap<InternalKey, Slice> getMapOf(List<Map.Entry<InternalKey, Slice>> entries)
    {
        ConcurrentSkipListMap<InternalKey, Slice> table = new ConcurrentSkipListMap<>(new InternalKeyComparator(new BytewiseComparator()));
        ArrayList<Map.Entry<InternalKey, Slice>> entries1 = Lists.newArrayList(entries);
        Collections.shuffle(entries1);
        entries1.forEach(e -> table.put(e.getKey(), e.getValue()));
        return table;
    }

    private <K, V> void assertEntryEquals(SeekingIterator<K, V> iter, Map.Entry<K, V> next)
    {
        assertTrue(iter.valid());
        assertEquals(iter.key(), next.getKey());
        assertEquals(iter.key(), next.getKey());
        assertEquals(iter.value(), next.getValue());
        assertEquals(iter.value(), next.getValue());
    }

    private Map.Entry<InternalKey, Slice> entry(String k, int sec, ValueType vt, String v)
    {
        return new AbstractMap.SimpleEntry<>(new InternalKey(new Slice(k.getBytes()), sec, vt), new Slice(v.getBytes()));
    }
}
