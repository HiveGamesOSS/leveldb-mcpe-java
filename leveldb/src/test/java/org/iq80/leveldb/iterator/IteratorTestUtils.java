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

import org.iq80.leveldb.impl.InternalKey;
import org.iq80.leveldb.impl.ValueType;
import org.iq80.leveldb.util.Slice;
import org.testng.Assert;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;

public final class IteratorTestUtils
{
    private IteratorTestUtils()
    {
        //util
    }

    public static InternalIterator asInternalIterator(SeekingIterator<InternalKey, Slice> iterator)
    {
        return new InternalForwardingIterator(iterator);
    }

    public static SliceIterator asSliceIterator(SeekingIterator<Slice, Slice> iterator)
    {
        return new SliceForwardingIterator(iterator);
    }

    public static InternalKey key(String userK, int sequence, ValueType valueType)
    {
        return new InternalKey(new Slice(userK.getBytes()), sequence, valueType);
    }

    public static <K, V> void assertValidKV(SeekingIterator<K, V> mergingIterator, K internalKey, V value)
    {
        Assert.assertTrue(mergingIterator.valid());
        Assert.assertEquals(mergingIterator.key(), internalKey);
        Assert.assertEquals(mergingIterator.value(), value);
    }

    public static void assertInvalid(boolean op, SeekingIterator it)
    {
        Assert.assertFalse(op);
        Assert.assertFalse(it.valid());
        Assert.assertThrows(it::key);
        Assert.assertThrows(it::value);
    }

    public static <K, V> String toString(SeekingIterator<K, V> it)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        while (it.valid()) {
            if (stringBuilder.length() > 1) {
                stringBuilder.append(", ");
            }
            stringBuilder.append(it.key()).append("=").append(it.value());
            it.next();
        }
        return stringBuilder.append("]").toString();
    }

    public static <K, V> Map.Entry<K, V> entry(SeekingIterator<K, V> it)
    {
        return new AbstractMap.SimpleEntry<>(it.key(), it.value());
    }

    private static class InternalForwardingIterator extends ForwardingIterator<InternalKey, Slice>
            implements InternalIterator
    {
        private final SeekingIterator<InternalKey, Slice> iterator;

        InternalForwardingIterator(SeekingIterator<InternalKey, Slice> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        protected SeekingIterator<InternalKey, Slice> delegate()
        {
            return iterator;
        }
    }

    private static class SliceForwardingIterator extends ForwardingIterator<Slice, Slice>
            implements SliceIterator
    {
        private final SeekingIterator<Slice, Slice> iterator;

        SliceForwardingIterator(SeekingIterator<Slice, Slice> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        protected SeekingIterator<Slice, Slice> delegate()
        {
            return iterator;
        }
    }

    public abstract static class ForwardingIterator<K, V> implements SeekingIterator<K, V>
    {
        protected abstract SeekingIterator<K, V> delegate();

        @Override
        public boolean valid()
        {
            return delegate().valid();
        }

        @Override
        public boolean seekToFirst()
        {
            return delegate().seekToFirst();
        }

        @Override
        public boolean seekToLast()
        {
            return delegate().seekToLast();
        }

        @Override
        public boolean seek(K key)
        {
            return delegate().seek(key);
        }

        @Override
        public boolean next()
        {
            return delegate().next();
        }

        @Override
        public boolean prev()
        {
            return delegate().prev();
        }

        @Override
        public K key()
        {
            return delegate().key();
        }

        @Override
        public V value()
        {
            return delegate().value();
        }

        @Override
        public void close() throws IOException
        {
            delegate().close();
        }
    }
}
