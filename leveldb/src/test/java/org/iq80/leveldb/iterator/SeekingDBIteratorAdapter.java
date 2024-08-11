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

import org.iq80.leveldb.DBIterator;

import java.util.Map;
import java.util.function.Function;

public class SeekingDBIteratorAdapter<K, V> extends ASeekingIterator<K, V>
{
    private final DBIterator iterator;
    private final Function<K, byte[]> toKey;
    private final Function<byte[], K> key;
    private final Function<byte[], V> value;
    private Map.Entry<byte[], byte[]> entry;

    private SeekingDBIteratorAdapter(DBIterator iterator, Function<K, byte[]> toKey, Function<byte[], K> key, Function<byte[], V> value)
    {
        this.iterator = iterator;
        this.toKey = toKey;
        this.key = key;
        this.value = value;
    }

    public static <K, V> SeekingIterator<K, V> toSeekingIterator(DBIterator iterator, Function<K, byte[]> toKey, Function<byte[], K> key, Function<byte[], V> value)
    {
        return new SeekingDBIteratorAdapter<>(iterator, toKey, key, value);
    }

    @Override
    protected boolean internalSeekToFirst()
    {
        iterator.seekToFirst();
        entry = iterator.hasNext() ? iterator.next() : null;
        return entry != null;
    }

    @Override
    protected boolean internalSeekToLast()
    {
        iterator.seekToLast();
        entry = iterator.hasPrev() ? iterator.prev() : null;
        return entry != null;
    }

    @Override
    protected boolean internalSeek(K key)
    {
        iterator.seek(this.toKey.apply(key));
        entry = iterator.hasNext() ? iterator.next() : null;
        return entry != null;
    }

    @Override
    protected boolean internalNext(boolean switchDirection)
    {
        entry = iterator.hasNext() ? iterator.next() : null;
        return entry != null;
    }

    @Override
    protected boolean internalPrev(boolean switchDirection)
    {
        entry = iterator.hasPrev() ? iterator.prev() : null;
        return entry != null;
    }

    @Override
    protected K internalKey()
    {
        return key.apply(entry.getKey());
    }

    @Override
    protected V internalValue()
    {
        return value.apply(entry.getValue());
    }

    @Override
    protected void internalClose()
    {
        entry = null;
        iterator.close();
    }
}
