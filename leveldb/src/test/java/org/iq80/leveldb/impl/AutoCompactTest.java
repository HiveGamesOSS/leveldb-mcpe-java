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

import com.google.common.base.Strings;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.fileenv.FileUtils;
import org.iq80.leveldb.iterator.DBIteratorAdapter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AutoCompactTest
{
    private static final int K_VALUE_SIZE = 200 * 1024;
    private static final int K_TOTAL_SIZE = 100 * 1024 * 1024;
    private static final int K_COUNT = K_TOTAL_SIZE / K_VALUE_SIZE;
    private File databaseDir;
    private DbImpl db;

    private byte[] key(int i)
    {
        return String.format("key%06d", i).getBytes();
    }

    @Test
    public void testReadAll() throws Exception
    {
        doReads(K_COUNT);
    }

    @Test
    public void testReadHalf() throws Exception
    {
        doReads(K_COUNT / 2);
    }

    public void doReads(int n) throws Exception
    {
        final byte[] value = Strings.repeat("x", K_VALUE_SIZE).getBytes();
        // Fill database
        for (int i = 0; i < K_COUNT; i++) {
            db.put(key(i), value);
        }
        db.testCompactMemTable();
        // Delete everything
        for (int i = 0; i < K_COUNT; i++) {
            db.delete(key(i));
        }
        db.testCompactMemTable();
        // Get initial measurement of the space we will be reading.
        final long initialSize = size(key(0), key(n));
        final long initialOtherSize = size(key(n), key(K_COUNT));
        // Read until size drops significantly.
        byte[] limitKey = key(n);
        for (int read = 0; true; read++) {
            assertTrue(read < 100, "Taking too long to compact");
            try (DBIteratorAdapter iterator = db.iterator()) {
                iterator.seekToFirst();
                while (iterator.hasNext()) {
                    final DBIteratorAdapter.DbEntry next = iterator.next();
                    if (new String(next.getKey()).compareTo(new String(limitKey)) >= 0) {
                        break;
                    }
                }
            }
            Thread.sleep(1000L);
            final long size = size(key(0), key(n));
            System.out.printf("iter %3d => %7.3f MB [other %7.3f MB]\n",
                    read + 1, size / 1048576.0, size(key(0), key(K_COUNT)) / 1048576.0);
            if (size <= initialSize / 10) {
                break;
            }
        }
        // Verify that the size of the key space not touched by the reads
        // is pretty much unchanged.
        long finalOtherSize = size(key(n), key(K_COUNT));
        assertTrue(finalOtherSize <= initialOtherSize + 1048576, finalOtherSize + "<=" + (initialOtherSize + 1048576));
        assertTrue(finalOtherSize >= initialOtherSize / 5 - 1048576, finalOtherSize + "<=" + (initialOtherSize / 5 - 1048576));
    }

    private long size(byte[] key, byte[] key1)
    {
        final Range range = new Range(key, key1);
        return db.getApproximateSizes(range);
    }

    //https://github.com/google/leveldb/commit/748539c183453bdeaff1eb0da8ccf5adacb796e7#diff-0465a3d0601c0cd6f05a6d0e9bfabd36
    @BeforeMethod
    public void setUp() throws IOException
    {
        databaseDir = FileUtils.createTempDir("leveldb_autocompact_test");
        final Options options = new Options()
                .paranoidChecks(true)
                .createIfMissing(true)
                .errorIfExists(true)
                .compressionType(CompressionType.NONE)
                .cacheSize(100); //tiny cache
        db = new DbImpl(options, databaseDir.getAbsolutePath(), EnvImpl.createEnv());
    }

    @AfterMethod
    public void tearDown()
    {
        db.close();
        boolean b = FileUtils.deleteRecursively(databaseDir);
        assertFalse(!b && databaseDir.exists(), "Dir should be possible to delete! All files should have been released.");
    }
}
