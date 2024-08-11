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
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SortedCollectionIteratorTest
{
    List<Map.Entry<String, String>> sampleEntries1 = Lists.newArrayList(
            entry("k1", "v1"), entry("k2", "v2"), entry("k3", "v3"), entry("k3", "v3"), entry("k4", "v4"), entry("k5", "v5"), entry("k7", "v5")
    );

    protected SeekingIterator<String, String> iterFactory(List<Map.Entry<String, String>> data)
    {
        return new SortedCollectionIterator<>(data, Map.Entry::getKey, Map.Entry::getValue, String::compareTo);
    }

    @Test
    public void testSeekAfterLast() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.seek("k8"));
        assertTrue(iter.seek("k5"));
        assertEntryEquals(iter, sampleEntries1.get(5));
        assertTrue(iter.seek("k1"));
        assertEntryEquals(iter, sampleEntries1.get(0));
        assertTrue(iter.seek("k6"));
        assertEntryEquals(iter, sampleEntries1.get(6));
        assertFalse(iter.next());
    }

    @Test
    public void testForwardScanAfterSeek() throws Exception
    {
        int count = 0;
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        Iterator<Map.Entry<String, String>> iterator = sampleEntries1.iterator();
        Iterators.advance(iterator, 2);
        for (boolean valid = iter.seek(sampleEntries1.get(2).getKey()); valid; valid = iter.next()) {
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
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testForwardScan() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        validateAll(iter, iter.next(), sampleEntries1);
        validateAll(iter, iter.seekToFirst(), sampleEntries1);
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testReverseScan() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertFalse(iter.prev());
        assertEndAndClose(iter);
    }

    @Test
    public void testReverseScanAfterSeekLast() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
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
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertTrue(iter.seekToFirst());
        assertFalse(iter.prev());
        assertEndAndClose(iter);
    }

    @Test
    public void testForwardScanAfterSeekLast() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        assertFalse(iter.valid());
        assertTrue(iter.seekToLast());
        assertFalse(iter.next());
        assertEndAndClose(iter);
    }

    @Test
    public void testEmpty() throws Exception
    {
        {
            SeekingIterator<String, String> iter = iterFactory(Collections.emptyList());
            assertFalse(iter.valid());
            assertFalse(iter.seekToLast());
            assertFalse(iter.next());
            assertFalse(iter.prev());
            assertEndAndClose(iter);
        }
        {
            SeekingIterator<String, String> iter = iterFactory(Collections.emptyList());
            assertFalse(iter.valid());
            assertFalse(iter.seekToLast());
            assertFalse(iter.prev());
            assertFalse(iter.next());
            assertEndAndClose(iter);
        }
        {
            SeekingIterator<String, String> iter = iterFactory(Collections.emptyList());
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
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        validateAll(iter, iter.next(), sampleEntries1);
        validateAllReverse(iter, iter.prev(), sampleEntries1);
        assertEndAndClose(iter);
    }

    @Test
    public void testNextAfterLastPrev() throws Exception
    {
        SeekingIterator<String, String> iter = iterFactory(sampleEntries1);
        validateAllReverse(iter, iter.seekToLast(), sampleEntries1);
        validateAll(iter, iter.next(), sampleEntries1);
        assertEndAndClose(iter);
    }

    private void validateAll(SeekingIterator<String, String> iter, boolean valid1, List<Map.Entry<String, String>> data)
    {
        int count = 0;
        Iterator<Map.Entry<String, String>> iterator = data.iterator();
        for (boolean valid = valid1; valid; valid = iter.next()) {
            assertEntryEquals(iter, iterator.next());
            count++;
        }
        assertEquals(count, data.size());
    }

    private void validateAllReverse(SeekingIterator<String, String> iter, boolean valid1, List<Map.Entry<String, String>> data)
    {
        int count = 0;
        Iterator<Map.Entry<String, String>> iterator = Lists.reverse(data).iterator();
        for (boolean valid = valid1; valid; valid = iter.prev()) {
            assertEntryEquals(iter, iterator.next());
            count++;
        }
        assertEquals(count, data.size());
    }

    private void assertEndAndClose(SeekingIterator<String, String> iter) throws IOException
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
        Assert.assertThrows(() -> iter.seek("k2"));
    }

    private <K, V> void assertEntryEquals(SeekingIterator<K, V> iter, Map.Entry<K, V> next)
    {
        assertTrue(iter.valid());
        assertEquals(iter.key(), next.getKey());
        assertEquals(iter.key(), next.getKey());
        assertEquals(iter.value(), next.getValue());
        assertEquals(iter.value(), next.getValue());
    }

    private Map.Entry<String, String> entry(String k, String v)
    {
        return new AbstractMap.SimpleEntry<>(k, v);
    }
}
