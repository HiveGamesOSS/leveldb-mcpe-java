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
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class WriteBatchImplTest
{
    @Test
    public void testApproximateSize()
    {
        WriteBatchImpl batch = new WriteBatchImpl();
        int emptySize = batch.getApproximateSize();

        batch.put(slice("foo"), slice("bar"));
        int oneKeySize = batch.getApproximateSize();
        assertTrue(emptySize < oneKeySize);

        batch.put(slice("baz"), slice("boo"));
        int twoKeysSize = batch.getApproximateSize();
        assertTrue(oneKeySize < twoKeysSize);

        batch.delete(slice("box"));
        int postDeleteSize = batch.getApproximateSize();
        assertTrue(twoKeysSize < postDeleteSize);
    }

    private static Slice slice(String txt)
    {
        return new Slice(txt.getBytes());
    }
}
