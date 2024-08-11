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

import com.google.common.cache.Weigher;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutionException;

import static org.testng.Assert.assertEquals;

/**
 * @author Honore Vasconcelos
 */
public class LRUCacheTest
{
    @Test
    public void testMultipleClientWithSameKey() throws Exception
    {
        final ILRUCache<Long, Integer> cache = LRUCache.createCache(2 * 5, new CountWeigher());
        final CacheWithStatistics[] caches = CacheWithStatistics.withStatistics(cache, 2);

        for (int x = 0; x < 3; ++x) {
            for (int i = 0; i < caches.length; ++i) {
                for (int j = 0; j < 5; ++j) {
                    assertEquals(((int) caches[i].load(j)), j * (i + 1) * 3);
                }
            }
            //only first run should load data into cache, as such, only 5 load should be executed instead of 30
            for (CacheWithStatistics cache1 : caches) {
                assertEquals(cache1.count, 5);
            }
        }
    }

    @Test
    public void testLimitIsRespected() throws Exception
    {
        // size is respected by guava but we could have some type of bug :)
        final ILRUCache<Long, Integer> cache = LRUCache.createCache(2, new CountWeigher());
        final CacheWithStatistics[] caches = CacheWithStatistics.withStatistics(cache, 2);
        caches[0].load(0);
        caches[0].load(1);
        caches[0].load(2);
        caches[0].load(1);
        caches[0].load(0);

        assertEquals(caches[0].count, 4);
        assertEquals(caches[1].count, 0);

        caches[1].load(0);
        caches[0].load(0);
        assertEquals(caches[0].count, 4);
        assertEquals(caches[1].count, 1);

        caches[0].load(2);
        caches[1].load(1);
        assertEquals(caches[0].count, 5);
        assertEquals(caches[1].count, 2);
    }

    private static class CacheWithStatistics
    {
        private final ILRUCache<Long, Integer> cache;
        private final int i;
        private int count;

        private CacheWithStatistics(ILRUCache<Long, Integer> cache, final int i)
        {
            this.cache = cache;
            this.i = i;
        }

        static CacheWithStatistics[] withStatistics(ILRUCache<Long, Integer> cache, int clients)
        {
            final CacheWithStatistics[] caches = new CacheWithStatistics[clients];
            for (int i = 0; i < clients; ++i) {
                caches[i] = new CacheWithStatistics(cache, i);
            }
            return caches;
        }

        public Integer load(final Integer key) throws ExecutionException
        {
            return cache.load(((long) i) << 32 | key, () -> {
                count++;
                return key * (i + 1) * 3;

            });
        }
    }

    private static class CountWeigher implements Weigher<Long, Integer>
    {
        @Override
        public int weigh(Long key, Integer value)
        {
            return 1;
        }
    }
}
