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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.InternalKeyComparator;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.iq80.leveldb.iterator.IteratorTestUtils.assertValidKV;
import static org.iq80.leveldb.iterator.IteratorTestUtils.key;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MergingIteratorTest
{
    @Test
    public void testCantReuseAfterClose()
    {
        InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());
        InternalIterator b = toIter(comparator, Arrays.asList(Maps.immutableEntry(key("B", 4, ValueType.VALUE), Slices.EMPTY_SLICE)));
        MergingIterator mergingIterator1 = new MergingIterator(Arrays.asList(b), comparator);
        mergingIterator1.close();
        Assert.assertThrows(mergingIterator1::close);
        Assert.assertThrows(b::close);
    }

    @Test
    public void testRandomizeGroups()
    {
        InternalKeyComparator comparator = new InternalKeyComparator(new BytewiseComparator());
        List<InternalKey> internalKeys = Arrays.asList(
                key("A", 1, ValueType.DELETION),
                key("B", 4, ValueType.VALUE),
                key("B", 3, ValueType.VALUE),
                key("B", 2, ValueType.VALUE),
                key("C", 5, ValueType.VALUE),
                key("E", 6, ValueType.VALUE),
                key("H", 7, ValueType.DELETION),
                key("I", 8, ValueType.VALUE),
                key("K", 9, ValueType.VALUE),
                key("L", 11, ValueType.DELETION),
                key("L", 10, ValueType.DELETION),
                key("M", 12, ValueType.DELETION),
                key("O", 13, ValueType.VALUE)
        );
        List<Slice> values = IntStream.range(0, internalKeys.size()).mapToObj(i -> new Slice(String.valueOf(i).getBytes())).collect(Collectors.toList());

        for (int i = 0; i < 100; i++) {
            for (Supplier<List<InternalIterator>> randomGroup : randomGroups(internalKeys, values, comparator)) {
                try (MergingIterator mergingIterator = new MergingIterator(randomGroup.get(), comparator)) {
                    //can iterate without calling seek first
                    int idx = 0;
                    while (mergingIterator.next()) {
                        assertValidKV(mergingIterator, internalKeys.get(idx), values.get(idx));
                        idx++;
                    }
                    assertEquals(idx, internalKeys.size());

                    //ensure we can restart safely
                    assertTrue(mergingIterator.seekToFirst());
                    assertValidKV(mergingIterator, internalKeys.get(0), values.get(0));

                    //seeking unknown key after last
                    assertFalse(mergingIterator.seek(key("Z", 2, ValueType.VALUE)));
                    assertFalse(mergingIterator.valid());

                    //seeking

                    //can use iterator in revers order after reaching the end
                    idx = internalKeys.size();
                    for (boolean ok = mergingIterator.seekToLast(); ok; ok = mergingIterator.prev()) {
                        idx--;
                        assertValidKV(mergingIterator, internalKeys.get(idx), values.get(idx));
                    }
                    assertEquals(idx, 0);
                }
                //can iterate with seekFirst
                try (MergingIterator mergingIterator = new MergingIterator(randomGroup.get(), comparator)) {
                    //can iterate without calling seek first
                    int idx = 0;
                    for (boolean ok = mergingIterator.seekToFirst(); ok; ok = mergingIterator.next()) {
                        assertValidKV(mergingIterator, internalKeys.get(idx), values.get(idx));
                        idx++;
                    }
                    assertFalse(mergingIterator.valid());
                }
                try (MergingIterator mergingIterator = new MergingIterator(randomGroup.get(), comparator)) {
                    //start invalid
                    assertFalse(mergingIterator.valid());

                    //find key with distinct sequence
                    assertTrue(mergingIterator.seek(key("B", 200, ValueType.VALUE)));
                    assertValidKV(mergingIterator, internalKeys.get(1), values.get(1));

                    assertTrue(mergingIterator.prev());
                    assertValidKV(mergingIterator, internalKeys.get(0), values.get(0));
                    assertTrue(mergingIterator.next());
                    assertValidKV(mergingIterator, internalKeys.get(1), values.get(1));
                    mergingIterator.prev();
                    mergingIterator.prev();
                    mergingIterator.next();
                    assertValidKV(mergingIterator, internalKeys.get(0), values.get(0));

                    assertTrue(mergingIterator.seek(key("B", 2, ValueType.VALUE)));
                    assertValidKV(mergingIterator, internalKeys.get(3), values.get(3));
                    assertTrue(mergingIterator.seek(key("B", 3, ValueType.VALUE)));
                    assertValidKV(mergingIterator, internalKeys.get(2), values.get(2));
                    assertTrue(mergingIterator.seek(key("B", 4, ValueType.VALUE)));
                    assertValidKV(mergingIterator, internalKeys.get(1), values.get(1));

                    //find key that don't exist due to previous sequence
                    assertTrue(mergingIterator.seek(key("B", 1, ValueType.VALUE)));
                    assertValidKV(mergingIterator, internalKeys.get(4), values.get(4));

                }
                try (MergingIterator mergingIterator = new MergingIterator(randomGroup.get(), comparator)) {
                    assertFalse(mergingIterator.seek(key("Z", 2, ValueType.VALUE)));
                    assertFalse(mergingIterator.valid());
                }
            }
        }
    }

    /*
    Create some random groups of keys to ensure merge is ordering correctly the output
     */
    private List<Supplier<List<InternalIterator>>> randomGroups(List<InternalKey> internalKeys, List<Slice> values, Comparator<InternalKey> comparator)
    {
        List<Supplier<List<InternalIterator>>> multipleSets = new ArrayList<>();
        Random random = new Random();
        for (int groups = 1; groups < internalKeys.size(); groups++) {
            Iterator<InternalKey> iterator = new ArrayList<>(internalKeys).iterator();
            Iterator<Slice> valIter = values.iterator();
            List<List<Map.Entry<InternalKey, Slice>>> splitGroups = new ArrayList<>();
            for (int i = 0; i < groups; i++) {
                splitGroups.add(Lists.newArrayList(Maps.immutableEntry(iterator.next(), valIter.next())));
            }
            while (iterator.hasNext()) {
                int i = random.nextInt(splitGroups.size());
                splitGroups.get(i).add(Maps.immutableEntry(iterator.next(), valIter.next()));
            }
            multipleSets.add(() -> splitGroups.stream().map(e -> toIter(comparator, e)).collect(Collectors.toList()));
        }
        Collections.shuffle(multipleSets);
        return multipleSets;
    }

    private InternalIterator toIter(Comparator<InternalKey> comparator, List<Map.Entry<InternalKey, Slice>> of1)
    {
        return IteratorTestUtils.asInternalIterator(SeekingIterators.fromSortedList(of1, Map.Entry::getKey, Map.Entry::getValue, comparator));
    }
}
