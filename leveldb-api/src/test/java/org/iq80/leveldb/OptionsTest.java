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
package org.iq80.leveldb;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class OptionsTest
{
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testDefaults() throws Exception
    {
        Options.fromOptions(null);
    }

    @Test
    public void testCopy() throws Exception
    {
        MyDBComparator comparator = new MyDBComparator();
        Logger logger = msg -> {
        };
        XFilterPolicy filterPolicy = new XFilterPolicy()
        {
        };
        Options op = new Options();
        op.createIfMissing(false);
        op.errorIfExists(true);
        op.writeBufferSize(1234);
        op.maxFileSize(56790);
        op.maxOpenFiles(2);
        op.blockRestartInterval(789);
        op.blockSize(345);
        op.compressionType(CompressionType.NONE);
        op.paranoidChecks(true);
        op.comparator(comparator);
        op.logger(logger);
        op.cacheSize(678);
        op.filterPolicy(filterPolicy);
        op.reuseLogs(true);
        Options op2 = Options.fromOptions(op);

        assertEquals(op2.createIfMissing(), false);
        assertEquals(op2.errorIfExists(), true);
        assertEquals(op2.writeBufferSize(), 1234);
        assertEquals(op2.maxFileSize(), 56790);
        assertEquals(op2.maxOpenFiles(), 2);
        assertEquals(op2.blockRestartInterval(), 789);
        assertEquals(op2.blockSize(), 345);
        assertEquals(op2.compressionType(), CompressionType.NONE);
        assertEquals(op2.paranoidChecks(), true);
        assertEquals(op2.comparator(), comparator);
        assertEquals(op2.logger(), logger);
        assertEquals(op2.cacheSize(), 678);
        assertEquals(op2.filterPolicy(), filterPolicy);
        assertEquals(op2.reuseLogs(), true);
    }

    private static class MyDBComparator implements DBComparator
    {
        @Override
        public String name()
        {
            return null;
        }

        @Override
        public byte[] findShortestSeparator(byte[] start, byte[] limit)
        {
            return new byte[0];
        }

        @Override
        public byte[] findShortSuccessor(byte[] key)
        {
            return new byte[0];
        }

        @Override
        public int compare(byte[] o1, byte[] o2)
        {
            return 0;
        }
    }
}
