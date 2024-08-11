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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.UnsignedBytes;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.env.DbLock;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.env.File;
import org.iq80.leveldb.env.RandomInputFile;
import org.iq80.leveldb.env.SequentialFile;
import org.iq80.leveldb.env.WritableFile;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.fileenv.FileUtils;
import org.iq80.leveldb.iterator.InternalIterator;
import org.iq80.leveldb.iterator.IteratorTestUtils;
import org.iq80.leveldb.iterator.SeekingDBIteratorAdapter;
import org.iq80.leveldb.iterator.SeekingIterator;
import org.iq80.leveldb.table.BloomFilterPolicy;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.iq80.leveldb.CompressionType.NONE;
import static org.iq80.leveldb.impl.DbConstants.NUM_LEVELS;
import static org.iq80.leveldb.iterator.IteratorTestUtils.assertInvalid;
import static org.iq80.leveldb.iterator.IteratorTestUtils.assertValidKV;
import static org.iq80.leveldb.iterator.IteratorTestUtils.entry;
import static org.iq80.leveldb.table.BlockHelper.afterString;
import static org.iq80.leveldb.table.BlockHelper.assertReverseSequence;
import static org.iq80.leveldb.table.BlockHelper.assertSequence;
import static org.iq80.leveldb.table.BlockHelper.beforeString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DbImplTest
{
    // You can set the STRESS_FACTOR system property to make the tests run more iterations.
    private static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1"));

    private static final String DOES_NOT_EXIST_FILENAME = "/foo/bar/doowop/idontexist";
    private static final String DOES_NOT_EXIST_FILENAME_PATTERN = ".foo.bar.doowop.idontexist";
    private Env defaultEnv;

    private File databaseDir;

    @DataProvider(name = "options")
    public Object[][] optionsProvider()
    {
        return new Object[][] {
                {new OptionsDesc("Default")},
                //new OptionsDesc("Reuse").reuseLog(true)},
                {new OptionsDesc("Bloom Filter").filterPolicy(new BloomFilterPolicy(10))},
                {new OptionsDesc("No Compression").compressionType(CompressionType.NONE)},
                {new OptionsDesc("Snappy").compressionType(CompressionType.SNAPPY)},
                {new OptionsDesc("ZLib").compressionType(CompressionType.ZLIB)},
                {new OptionsDesc("ZLib Raw").compressionType(CompressionType.ZLIB_RAW)}
        };
    }

    @Test(dataProvider = "options")
    public void testBackgroundCompaction(final Options options)
            throws Exception
    {
        options.maxOpenFiles(100);
        options.createIfMissing(true);
        DbStringWrapper db = new DbStringWrapper(options, this.databaseDir, defaultEnv);
        Random random = new Random(301);
        for (int i = 0; i < 200000 * STRESS_FACTOR; i++) {
            db.put(randomString(random, 64), new String(new byte[] {0x01}, UTF_8), new WriteOptions().sync(false));
            db.get(randomString(random, 64));
            if ((i % 50000) == 0 && i != 0) {
                System.out.println(i + " rows written");
            }
        }
    }

    @Test
    public void testConcurrentWrite() throws Exception
    {
        Options options = new Options();
        options.maxOpenFiles(50);
        options.createIfMissing(true);
        ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
        try {
            DbStringWrapper db = new DbStringWrapper(options, this.databaseDir, defaultEnv);
            final int numEntries = 1000000;
            final int growValueBy = 10;
            final CountDownLatch segmentsToPutEnd = new CountDownLatch(numEntries / 100);
            final Random random = new Random(Thread.currentThread().getId());
            final int segmentSize = 100;
            //dispatch writes
            for (int i = 0; i < numEntries; i += segmentSize) {
                final int finalI = i;
                ex.submit(() -> {
                    final int i2 = finalI + segmentSize;
                    for (int j = finalI; j < i2; j++) {
                        final BigInteger bigInteger = BigInteger.valueOf(j);
                        final byte[] value = bigInteger.toByteArray();
                        final byte[] bytes = new byte[growValueBy + value.length];
                        for (int k = 0; k < growValueBy; k += value.length) {
                            System.arraycopy(value, 0, bytes, k, value.length);
                        }
                        db.db.put(value, bytes);
                        if (random.nextInt(100) < 2) {
                            Thread.yield();
                        }
                    }
                    segmentsToPutEnd.countDown();
                });
            }
            segmentsToPutEnd.await();
            //check all writes have
            for (int i = 0; i < numEntries; i++) {
                final BigInteger bigInteger = BigInteger.valueOf(i);
                final byte[] value = bigInteger.toByteArray();
                final byte[] bytes = new byte[growValueBy + value.length];
                for (int k = 0; k < growValueBy; k += value.length) {
                    System.arraycopy(value, 0, bytes, k, value.length);
                }
                assertEquals(db.db.get(value), bytes);
            }
        }
        finally {
            ex.shutdownNow();
        }
    }

    @Test(dataProvider = "options")
    public void testCompactionsOnBigDataSet(final Options options)
            throws Exception
    {
        options.createIfMissing(true);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir, defaultEnv);
        for (int index = 0; index < 5000000; index++) {
            String key = "Key LOOOOOOOOOOOOOOOOOONG KEY " + index;
            String value = "This is element " + index + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDF";
            db.put(key, value);
        }
    }

    @Test(dataProvider = "options")
    public void testEmpty(final Options options)
            throws Exception
    {
        File databaseDir = this.databaseDir;
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testEmptyKey(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("", "v1");
        assertEquals(db.get(""), "v1");
        db.put("", "v2");
        assertEquals(db.get(""), "v2");
    }

    @Test(dataProvider = "options")
    public void testEmptyValue(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("key", "v1");
        assertEquals(db.get("key"), "v1");
        db.put("key", "");
        assertEquals(db.get("key"), "");
        db.put("key", "v2");
        assertEquals(db.get("key"), "v2");
    }

    @Test(dataProvider = "options")
    public void testEmptyBatch(final Options options)
            throws Exception
    {
        // open new db
        options.createIfMissing(true);

        DB db = new DbImpl(options, databaseDir.getPath(), defaultEnv);

        // write an empty batch
        WriteBatch batch = db.createWriteBatch();
        batch.close();
        db.write(batch);

        // close the db
        db.close();

        // reopen db
        new DbImpl(options, databaseDir.getPath(), defaultEnv).close();
    }

    @Test(dataProvider = "options")
    public void testReadWrite(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");
        db.put("bar", "v2");
        db.put("foo", "v3");
        assertEquals(db.get("foo"), "v3");
        assertEquals(db.get("bar"), "v2");
    }

    @Test(dataProvider = "options")
    public void testPutDeleteGet(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");
        db.put("foo", "v2");
        assertEquals(db.get("foo"), "v2");
        db.delete("foo");
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testGetFromImmutableLayer(final Options options)
            throws Exception
    {
        // create db with small write buffer
        SpecialEnv env = new SpecialEnv(defaultEnv);
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(100000), databaseDir, env);
        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");

        env.delayDataSync.set(true);

        // Fill memtable
        db.put("k1", longString(100000, 'x'));
        // Trigger compaction
        db.put("k2", longString(100000, 'y'));
        assertEquals(db.get("foo"), "v1");

        env.delayDataSync.set(false);
    }

    @Test(dataProvider = "options")
    public void testGetFromVersions(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.testCompactMemTable();
        assertEquals(db.get("foo"), "v1");
    }

    @Test(dataProvider = "options")
    public void testGetSnapshot(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // Try with both a short key and a long key
        for (int i = 0; i < 2; i++) {
            String key = (i == 0) ? "foo" : longString(200, 'x');
            db.put(key, "v1");
            Snapshot s1 = db.getSnapshot();
            db.put(key, "v2");
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s1), "v1");

            db.testCompactMemTable();
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s1), "v1");
            s1.close();
        }
    }

    @Test(dataProvider = "options")
    public void testGetIdenticalSnapshots(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        // Try with both a short key and a long key
        for (int i = 0; i < 2; i++) {
            String key = (i == 0) ? "foo" : "X" + Strings.repeat(" ", 199);
            db.put(key, "v1");
            Snapshot s1 = db.getSnapshot();
            Snapshot s2 = db.getSnapshot();
            Snapshot s3 = db.getSnapshot();
            db.put(key, "v2");
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s1), "v1");
            assertEquals(db.get(key, s2), "v1");
            assertEquals(db.get(key, s3), "v1");
            s1.close();
            db.testCompactMemTable();
            assertEquals(db.get(key), "v2");
            assertEquals(db.get(key, s2), "v1");
            s2.close();
            assertEquals(db.get(key, s3), "v1");
            s3.close();
        }
    }

    @Test(dataProvider = "options")
    public void testIterateOverEmptySnapshot(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        Snapshot snapshot = db.getSnapshot();
        ReadOptions readOptions = new ReadOptions();
        readOptions.snapshot(snapshot);
        db.put("foo", "v1");
        db.put("foo", "v2");

        SeekingIterator<String, String> iterator = db.iterator(readOptions);
        iterator.seekToFirst();
        assertFalse(iterator.valid());
        iterator.close();

        db.testCompactMemTable();

        SeekingIterator<String, String> iterator2 = db.iterator(readOptions);
        iterator2.seekToFirst();
        assertFalse(iterator2.valid());
        iterator2.close();

        snapshot.close();
    }

    @Test(dataProvider = "options")
    public void testGetLevel0Ordering(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // Check that we process level-0 files in correct order.  The code
        // below generates two level-0 files where the earlier one comes
        // before the later one in the level-0 file list since the earlier
        // one has a smaller "smallest" key.
        db.put("bar", "b");
        db.put("foo", "v1");
        db.testCompactMemTable();
        db.put("foo", "v2");
        db.testCompactMemTable();
        assertEquals(db.get("foo"), "v2");
    }

    @Test(dataProvider = "options")
    public void testGetOrderedByLevels(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.compactRange("a", "z");
        assertEquals(db.get("foo"), "v1");
        db.put("foo", "v2");
        assertEquals(db.get("foo"), "v2");
        db.testCompactMemTable();
        assertEquals(db.get("foo"), "v2");
    }

    @Test(dataProvider = "options")
    public void testGetPicksCorrectFile(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.compactRange("a", "b");
        db.put("x", "vx");
        db.compactRange("x", "y");
        db.put("f", "vf");
        db.compactRange("f", "g");

        assertEquals(db.get("a"), "va");
        assertEquals(db.get("f"), "vf");
        assertEquals(db.get("x"), "vx");
    }

    @Test(dataProvider = "options")
    public void testGetEncountersEmptyLevel(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        // Arrange for the following to happen:
        //   * sstable A in level 0
        //   * nothing in level 1
        //   * sstable B in level 2
        // Then do enough Get() calls to arrange for an automatic compaction
        // of sstable A.  A bug would cause the compaction to be marked as
        // occurring at level 1 (instead of the correct level 0).

        // Step 1: First place sstables in levels 0 and 2
        int compactionCount = 0;
        while (db.numberOfFilesInLevel(0) == 0 || db.numberOfFilesInLevel(2) == 0) {
            assertTrue(compactionCount <= 100, "could not fill levels 0 and 2");
            compactionCount++;
            db.put("a", "begin");
            db.put("z", "end");
            db.testCompactMemTable();
        }

        // Step 2: clear level 1 if necessary.
        db.testCompactRange(1, null, null);
        assertEquals(db.numberOfFilesInLevel(0), 1);
        assertEquals(db.numberOfFilesInLevel(1), 0);
        assertEquals(db.numberOfFilesInLevel(2), 1);

        // Step 3: read a bunch of times
        for (int i = 0; i < 1000; i++) {
            assertNull(db.get("missing"));
        }

        // Step 4: Wait for compaction to finish
        db.waitForBackgroundCompactationToFinish();

        assertEquals(db.numberOfFilesInLevel(0), 0);
    }

    @Test(dataProvider = "options")
    public void testEmptyIterator(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        SeekingIterator<String, String> iterator = db.iterator();

        iterator.seekToFirst();
        assertNoNextElement(iterator);

        iterator.seek("foo");
        assertNoNextElement(iterator);
        iterator.close();
    }

    @Test(dataProvider = "options")
    public void testIteratorSingle(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");

        try (SeekingIterator<String, String> iterator = db.iterator()) {
            iterator.seekToFirst();
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.next(), iterator);
            assertTrue(iterator.seekToFirst());
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.prev(), iterator);

            assertTrue(iterator.seekToLast());
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.next(), iterator);
            assertTrue(iterator.seekToLast());
            assertValidKV(iterator, "a", "va");

            assertInvalid(iterator.prev(), iterator);

            assertTrue(iterator.seek(""));
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.next(), iterator);

            assertTrue(iterator.seek("a"));
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.next(), iterator);

            assertInvalid(iterator.seek("b"), iterator);
        }
    }

    @Test(dataProvider = "options")
    public void testIteratorMultiple(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.put("b", "vb");
        db.put("c", "vc");

        try (SeekingIterator<String, String> iterator = db.iterator()) {
            assertTrue(iterator.seekToFirst());
            assertValidKV(iterator, "a", "va");
            assertTrue(iterator.next());
            assertValidKV(iterator, "b", "vb");
            assertTrue(iterator.next());
            assertValidKV(iterator, "c", "vc");
            assertInvalid(iterator.next(), iterator);
            assertTrue(iterator.seekToFirst());
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.prev(), iterator);

            assertTrue(iterator.seekToLast());
            assertValidKV(iterator, "c", "vc");
            assertTrue(iterator.prev());
            assertValidKV(iterator, "b", "vb");
            assertTrue(iterator.prev());
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.prev(), iterator);
            assertTrue(iterator.seekToLast());
            assertValidKV(iterator, "c", "vc");
            assertInvalid(iterator.next(), iterator);

            assertTrue(iterator.seek(""));
            assertValidKV(iterator, "a", "va");
            assertTrue(iterator.seek("a"));
            assertValidKV(iterator, "a", "va");
            assertTrue(iterator.seek("ax"));
            assertValidKV(iterator, "b", "vb");
            assertTrue(iterator.seek("b"));
            assertValidKV(iterator, "b", "vb");
            assertInvalid(iterator.seek("z"), iterator);

            // Switch from reverse to forward
            assertTrue(iterator.seekToLast());
            assertTrue(iterator.prev());
            assertTrue(iterator.prev());
            assertTrue(iterator.next());
            assertValidKV(iterator, "b", "vb");

            // Switch from forward to reverse
            assertTrue(iterator.seekToFirst());
            assertTrue(iterator.next());
            assertTrue(iterator.next());
            assertTrue(iterator.prev());
            assertValidKV(iterator, "b", "vb");

            // Make sure iter stays at snapshot
            db.put("a", "va2");
            db.put("a2", "va3");
            db.put("b", "vb2");
            db.put("c", "vc2");
            db.delete("b");
            assertTrue(iterator.seekToFirst());
            assertValidKV(iterator, "a", "va");
            assertTrue(iterator.next());
            assertValidKV(iterator, "b", "vb");
            assertTrue(iterator.next());
            assertValidKV(iterator, "c", "vc");
            assertInvalid(iterator.next(), iterator);
            assertTrue(iterator.seekToLast());
            assertValidKV(iterator, "c", "vc");
            assertTrue(iterator.prev());
            assertValidKV(iterator, "b", "vb");
            assertTrue(iterator.prev());
            assertValidKV(iterator, "a", "va");
            assertInvalid(iterator.prev(), iterator);
        }
    }

    @Test(dataProvider = "options")
    public void testIterSmallAndLargeMix(final Options options)
            throws IOException
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("a", "va");
        db.put("b", Strings.repeat("b", 100000));
        db.put("c", "vc");
        db.put("d", Strings.repeat("d", 100000));
        db.put("e", Strings.repeat("e", 100000));
        try (SeekingIterator<String, String> iterator = db.iterator()) {
            assertTrue(iterator.seekToFirst());
            assertSequence(iterator,
                    immutableEntry("a", "va"),
                    immutableEntry("b", Strings.repeat("b", 100000)),
                    immutableEntry("c", "vc"),
                    immutableEntry("d", Strings.repeat("d", 100000)),
                    immutableEntry("e", Strings.repeat("e", 100000)));

            iterator.seekToLast();
            assertReverseSequence(iterator,
                    immutableEntry("e", Strings.repeat("e", 100000)),
                    immutableEntry("d", Strings.repeat("d", 100000)),
                    immutableEntry("c", "vc"),
                    immutableEntry("b", Strings.repeat("b", 100000)),
                    immutableEntry("a", "va")
            );
            assertFalse(iterator.valid());
        }
    }

    @Test(dataProvider = "options")
    public void testIterMultiWithDelete(final Options options)
            throws IOException
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("b", "vb");
        db.put("c", "vc");
        db.put("a", "va");
        db.delete("b");
        assertNull(db.get("b"));
        SeekingIterator<String, String> iterator = db.iterator();
        iterator.seek("c");
        assertValidKV(iterator, "c", "vc");
        assertTrue(iterator.prev());
        assertValidKV(iterator, "a", "va");
        iterator.close();
    }

    @Test(dataProvider = "options")
    public void testRecover(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.put("baz", "v5");

        db.reopen();

        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("baz"), "v5");
        db.put("bar", "v2");
        db.put("foo", "v3");

        db.reopen();

        assertEquals(db.get("foo"), "v3");
        db.put("foo", "v4");
        assertEquals(db.get("foo"), "v4");
        assertEquals(db.get("bar"), "v2");
        assertEquals(db.get("baz"), "v5");
    }

    @Test(dataProvider = "options")
    public void testRecoveryWithEmptyLog(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        db.put("foo", "v2");
        db.reopen();
        db.reopen();
        db.put("foo", "v3");
        db.reopen();
        assertEquals(db.get("foo"), "v3");
    }

    @Test(dataProvider = "options")
    public void testSliceMutationAfterWriteShouldNotAffectInternalState(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        final WriteBatchImpl updates = new WriteBatchImpl();
        final Slice key = new Slice("foo".getBytes());
        final Slice value = new Slice("v1".getBytes());
        updates.put(key, value);
        db.db.write(updates);

        //precondition
        assertEquals(db.get("foo"), "v1");

        //change value should have no effect
        value.setByte(1, '1');
        assertEquals(db.get("foo"), "v1");

        //change key should have no effect
        key.setByte(0, 'x');
        assertEquals(db.get("foo"), "v1");

        //change in delete key should have no effect
        final WriteBatchImpl updates1 = new WriteBatchImpl();
        final Slice key1 = new Slice("foo".getBytes());
        updates1.delete(key1);
        db.db.write(updates1);
        assertNull(db.get("foo"));
        key1.setByte(0, 'x');
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testArrayMutationAfterWriteShouldNotAffectInternalState(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        final byte[] key = {'f', 'o', 'o'};
        final byte[] value = {'v', '1'};
        db.db.put(key, value);

        //precondition
        assertEquals(db.get("foo"), "v1");

        //change value should have no effect
        value[1] = '2';
        assertEquals(db.get("foo"), "v1");

        //change key should have no effect
        key[1] = '1';
        assertEquals(db.get("foo"), "v1");

        //change in delete key should have no effect
        final byte[] key1 = {'f', 'o', 'o'};
        db.db.delete(key1);
        assertNull(db.get("foo"));
        key1[0] = 'x';
        assertNull(db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testRecoverDuringMemtableCompaction(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(1000000), databaseDir);

        // Trigger a long memtable compaction and reopen the database during it
        db.put("foo", "v1");                        // Goes to 1st log file
        db.put("big1", longString(10000000, 'x'));  // Fills memtable
        db.put("big2", longString(1000, 'y'));      // Triggers compaction
        db.put("bar", "v2");                       // Goes to new log file

        db.reopen();
        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("bar"), "v2");
        assertEquals(db.get("big1"), longString(10000000, 'x'));
        assertEquals(db.get("big2"), longString(1000, 'y'));
    }

    @Test(dataProvider = "options")
    public void testMinorCompactionsHappen(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(10000), databaseDir);

        int n = 500;
        int startingNumTables = db.totalTableFiles();
        for (int i = 0; i < n; i++) {
            db.put(key(i), key(i) + longString(1000, 'v'));
        }
        int endingNumTables = db.totalTableFiles();
        assertTrue(endingNumTables > startingNumTables);

        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }
        db.testCompactMemTable();

        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }

        db.reopen();
        for (int i = 0; i < n; i++) {
            assertEquals(db.get(key(i)), key(i) + longString(1000, 'v'));
        }
    }

    @Test(dataProvider = "options")
    public void testRecoverWithLargeLog(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("big1", longString(200000, '1'));
        db.put("big2", longString(200000, '2'));
        db.put("small3", longString(10, '3'));
        db.put("small4", longString(10, '4'));
        assertEquals(db.numberOfFilesInLevel(0), 0);

        db.reopen(options.writeBufferSize(100000));
        assertEquals(db.numberOfFilesInLevel(0), 3);
        assertEquals(db.get("big1"), longString(200000, '1'));
        assertEquals(db.get("big2"), longString(200000, '2'));
        assertEquals(db.get("small3"), longString(10, '3'));
        assertEquals(db.get("small4"), longString(10, '4'));
        assertTrue(db.numberOfFilesInLevel(0) > 1);
    }

    @Test(dataProvider = "options")
    public void testCompactionsGenerateMultipleFiles(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.writeBufferSize(100000000), databaseDir);

        // Write 8MB (80 values, each 100K)
        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertEquals(db.numberOfFilesInLevel(1), 0);
        Random random = new Random(301);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < 80; i++) {
            String value = randomString(random, 100 * 1024);
            db.put(key(i), value);
            values.add(value);
        }

        // Reopening moves updates to level-0
        db.reopen();
        assertTrue(db.numberOfFilesInLevel(0) > 0);
        assertEquals(db.numberOfFilesInLevel(1), 0);
        db.testCompactRange(0, null, null);

        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertTrue(db.numberOfFilesInLevel(1) > 0);
        for (int i = 0; i < 80; i++) {
            assertEquals(db.get(key(i)), values.get(i));
        }
    }

    @Test(dataProvider = "options")
    public void testRepeatedWritesToSameKey(final Options options)
            throws Exception
    {
        options.writeBufferSize(100000);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        // We must have at most one file per level except for level-0,
        // which may have up to kL0_StopWritesTrigger files.
        int maxFiles = NUM_LEVELS + DbConstants.L0_STOP_WRITES_TRIGGER;

        Random random = new Random(301);
        String value = randomString(random, 2 * options.writeBufferSize());
        for (int i = 0; i < 5 * maxFiles; i++) {
            db.put("key", value);
            assertTrue(db.totalTableFiles() < maxFiles);
        }

        db.close();
    }

    @Test(dataProvider = "options")
    public void testSparseMerge(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().compressionType(NONE), databaseDir);

        fillLevels(db, "A", "Z");

        // Suppose there is:
        //    small amount of data with prefix A
        //    large amount of data with prefix B
        //    small amount of data with prefix C
        // and that recent updates have made small changes to all three prefixes.
        // Check that we do not do a compaction that merges all of B in one shot.
        String value = longString(1000, 'x');
        db.put("A", "va");

        // Write approximately 100MB of "B" values
        for (int i = 0; i < 100000; i++) {
            String key = String.format("B%010d", i);
            db.put(key, value);
        }
        db.put("C", "vc");
        db.testCompactMemTable();
        db.testCompactRange(0, null, null);

        // Make sparse update
        db.put("A", "va2");
        db.put("B100", "bvalue2");
        db.put("C", "vc2");
        db.testCompactMemTable();

        // Compactions should not cause us to create a situation where
        // a file overlaps too much data at the next level.
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
        db.testCompactRange(0, null, null);
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
        db.testCompactRange(1, null, null);
        assertTrue(db.getMaxNextLevelOverlappingBytes() <= 20 * 1048576);
    }

    @Test
    public void testApproximateSizes()
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().writeBufferSize(100000000).compressionType(NONE), databaseDir);

        assertBetween(db.size("", "xyz"), 0, 0);
        db.reopen();
        assertBetween(db.size("", "xyz"), 0, 0);

        // Write 8MB (80 values, each 100K)
        assertEquals(db.numberOfFilesInLevel(0), 0);
        int n = 80;
        Random random = new Random(301);
        for (int i = 0; i < n; i++) {
            db.put(key(i), randomString(random, 100000));
        }

        // 0 because GetApproximateSizes() does not account for memtable space
        assertBetween(db.size("", key(50)), 0, 0);

        // Check sizes across recovery by reopening a few times
        for (int run = 0; run < 3; run++) {
            db.reopen();

            for (int compactStart = 0; compactStart < n; compactStart += 10) {
                for (int i = 0; i < n; i += 10) {
                    assertBetween(db.size("", key(i)), 100000 * i, 100000 * i + 10000);
                    assertBetween(db.size("", key(i) + ".suffix"), 100000 * (i + 1), 100000 * (i + 1) + 10000);
                    assertBetween(db.size(key(i), key(i + 10)), 100000 * 10, 100000 * 10 + 10000);
                }
                assertBetween(db.size("", key(50)), 5000000, 5010000);
                assertBetween(db.size("", key(50) + ".suffix"), 5100000, 5110000);

                db.testCompactRange(0, key(compactStart), key(compactStart + 9));
            }

            assertEquals(db.numberOfFilesInLevel(0), 0);
            assertTrue(db.numberOfFilesInLevel(1) > 0);
        }
    }

    @Test
    public void testApproximateSizesMixOfSmallAndLarge()
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().compressionType(NONE), databaseDir);
        Random random = new Random(301);
        String big1 = randomString(random, 100000);
        db.put(key(0), randomString(random, 10000));
        db.put(key(1), randomString(random, 10000));
        db.put(key(2), big1);
        db.put(key(3), randomString(random, 10000));
        db.put(key(4), big1);
        db.put(key(5), randomString(random, 10000));
        db.put(key(6), randomString(random, 300000));
        db.put(key(7), randomString(random, 10000));

        // Check sizes across recovery by reopening a few times
        for (int run = 0; run < 3; run++) {
            db.reopen();

            assertBetween(db.size("", key(0)), 0, 0);
            assertBetween(db.size("", key(1)), 10000, 11000);
            assertBetween(db.size("", key(2)), 20000, 21000);
            assertBetween(db.size("", key(3)), 120000, 121000);
            assertBetween(db.size("", key(4)), 130000, 131000);
            assertBetween(db.size("", key(5)), 230000, 231000);
            assertBetween(db.size("", key(6)), 240000, 241000);
            assertBetween(db.size("", key(7)), 540000, 541000);
            assertBetween(db.size("", key(8)), 550000, 551000);

            assertBetween(db.size(key(3), key(5)), 110000, 111000);

            db.testCompactRange(0, null, null);
        }
    }

    @Test(dataProvider = "options")
    public void testIteratorPinsRef(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "hello");

        try (SeekingIterator<String, String> iterator = db.iterator()) {
            iterator.seekToFirst();

            db.put("foo", "newvalue1");
            for (int i = 0; i < 100; i++) {
                db.put(key(i), key(i) + longString(100000, 'v'));
            }
            db.put("foo", "newvalue1");

            assertSequence(iterator, immutableEntry("foo", "hello"));
        }
    }

    @Test(dataProvider = "options")
    public void testSnapshot(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        db.put("foo", "v1");
        Snapshot s1 = db.getSnapshot();
        db.put("foo", "v2");
        Snapshot s2 = db.getSnapshot();
        db.put("foo", "v3");
        Snapshot s3 = db.getSnapshot();

        db.put("foo", "v4");

        assertEquals("v1", db.get("foo", s1));
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v3", db.get("foo", s3));
        assertEquals("v4", db.get("foo"));

        s3.close();
        assertEquals("v1", db.get("foo", s1));
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v4", db.get("foo"));

        s1.close();
        assertEquals("v2", db.get("foo", s2));
        assertEquals("v4", db.get("foo"));

        s2.close();
        assertEquals("v4", db.get("foo"));
    }

    @Test(dataProvider = "options")
    public void testHiddenValuesAreRemoved(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        Random random = new Random(301);
        fillLevels(db, "a", "z");

        String big = randomString(random, 50000);
        db.put("foo", big);
        db.put("pastFoo", "v");

        Snapshot snapshot = db.getSnapshot();

        db.put("foo", "tiny");
        db.put("pastFoo2", "v2");  // Advance sequence number one more

        db.testCompactMemTable();
        assertTrue(db.numberOfFilesInLevel(0) > 0);

        assertEquals(big, db.get("foo", snapshot));
        assertBetween(db.size("", "pastFoo"), 40000, 60000);
        snapshot.close();
        assertEquals(db.allEntriesFor("foo"), asList("tiny", big));
        db.testCompactRange(0, null, "x");
        assertEquals(db.allEntriesFor("foo"), asList("tiny"));
        assertEquals(db.numberOfFilesInLevel(0), 0);
        assertTrue(db.numberOfFilesInLevel(1) >= 1);
        db.testCompactRange(1, null, "x");
        assertEquals(db.allEntriesFor("foo"), asList("tiny"));

        assertBetween(db.size("", "pastFoo"), 0, 1000);
    }

    @Test
    public void testDeleteEntriesShouldNotAbeamOnIteration() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.put("b", "v");
        db.delete("b");
        db.delete("a");
        assertEquals("[]", toString(db));
    }

    @Test
    public void testL0CompactionGoogleBugIssue44a() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.reopen();
        db.put("b", "v");
        db.reopen();
        db.delete("b");
        db.delete("a");
        db.reopen();
        db.delete("a");
        db.reopen();
        db.put("a", "v");
        db.reopen();
        db.reopen();
        assertEquals("[a=v]", toString(db));
        Thread.sleep(1000);  // Wait for compaction to finish
        assertEquals("[a=v]", toString(db));
    }

    private String toString(DbStringWrapper db)
    {
        String s;
        try (SeekingIterator<String, String> iterator = db.iterator()) {
            iterator.seekToFirst();
            s = IteratorTestUtils.toString(iterator);
            return s;
        }
        catch (IOException e) {
            Assert.fail(e.getMessage());
            return "";
        }
    }

    @Test(invocationCount = 10)
    public void testL0CompactionGoogleBugIssue44b() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options().createIfMissing(true), databaseDir);
        db.reopen();
        db.put("", "");
        db.reopen();
        db.delete("e");
        db.put("", "");
        db.reopen();
        db.put("c", "cv");
        db.reopen();
        assertEquals("[=, c=cv]", toString(db));
        db.put("", "");
        db.reopen();
        db.put("", "");
        Thread.sleep(1000);  // Wait for compaction to finish
        db.reopen();
        db.put("d", "dv");
        db.reopen();
        db.put("", "");
        db.reopen();
        db.delete("d");
        db.delete("b");
        db.reopen();
        assertEquals("[=, c=cv]", toString(db));
        Thread.sleep(1000);  // Wait for compaction to finish
        assertEquals("[=, c=cv]", toString(db));
    }

    @Test(dataProvider = "options")
    public void testDeletionMarkers1(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        db.put("foo", "v1");
        db.testCompactMemTable();

        int last = DbConstants.MAX_MEM_COMPACT_LEVEL;
        assertEquals(db.numberOfFilesInLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        db.put("a", "begin");
        db.put("z", "end");
        db.testCompactMemTable();
        assertEquals(db.numberOfFilesInLevel(last), 1);
        assertEquals(db.numberOfFilesInLevel(last - 1), 1);
        assertEquals(db.get("a"), "begin");
        assertEquals(db.get("foo"), "v1");
        assertEquals(db.get("z"), "end");

        db.delete("foo");
        db.put("foo", "v2");
        final List<String> foo = db.allEntriesFor("foo");
        assertEquals(foo, asList("v2", "DEL", "v1"));
        db.testCompactMemTable();  // Moves to level last-2
        assertEquals(db.get("a"), "begin");
        assertEquals(db.get("foo"), "v2");
        assertEquals(db.get("z"), "end");

        assertEquals(db.allEntriesFor("foo"), asList("v2", "DEL", "v1"));
        db.testCompactRange(last - 2, null, "z");

        // DEL eliminated, but v1 remains because we aren't compacting that level
        // (DEL can be eliminated because v2 hides v1).
        assertEquals(db.allEntriesFor("foo"), asList("v2", "v1"));
        db.testCompactRange(last - 1, null, null);

        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(db.allEntriesFor("foo"), asList("v2"));
    }

    @Test(dataProvider = "options")
    public void testDeletionMarkers2(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        db.put("foo", "v1");
        db.testCompactMemTable();

        int last = DbConstants.MAX_MEM_COMPACT_LEVEL;
        assertEquals(db.numberOfFilesInLevel(last), 1); // foo => v1 is now in last level

        // Place a table at level last-1 to prevent merging with preceding mutation
        db.put("a", "begin");
        db.put("z", "end");
        db.testCompactMemTable();
        assertEquals(db.numberOfFilesInLevel(last), 1);
        assertEquals(db.numberOfFilesInLevel(last - 1), 1);

        db.delete("foo");

        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.testCompactMemTable();  // Moves to level last-2
        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.testCompactRange(last - 2, null, null);

        // DEL kept: "last" file overlaps
        assertEquals(db.allEntriesFor("foo"), asList("DEL", "v1"));
        db.testCompactRange(last - 1, null, null);

        // Merging last-1 w/ last, so we are the base level for "foo", so
        // DEL is removed.  (as is v1).
        assertEquals(db.allEntriesFor("foo"), asList());
    }

    @Test(dataProvider = "options")
    public void testOverlapInLevel0(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        assertEquals(DbConstants.MAX_MEM_COMPACT_LEVEL, 2, "Fix test to match config");

        // Fill levels 1 and 2 to disable the pushing of new memtables to levels >
        // 0.
        db.put("100", "v100");
        db.put("999", "v999");
        db.testCompactMemTable();
        db.delete("100");
        db.delete("999");
        db.testCompactMemTable();
        assertEquals(db.filesPerLevel(), "0,1,1");

        // Make files spanning the following ranges in level-0:
        //  files[0]  200 .. 900
        //  files[1]  300 .. 500
        // Note that files are sorted by smallest key.
        db.put("300", "v300");
        db.put("500", "v500");
        db.testCompactMemTable();
        db.put("200", "v200");
        db.put("600", "v600");
        db.put("900", "v900");
        db.testCompactMemTable();
        assertEquals(db.filesPerLevel(), "2,1,1");

        // Compact away the placeholder files we created initially
        db.testCompactRange(1, null, null);
        db.testCompactRange(2, null, null);
        assertEquals(db.filesPerLevel(), "2");

        // Do a memtable compaction.  Before bug-fix, the compaction would
        // not detect the overlap with level-0 files and would incorrectly place
        // the deletion in a deeper level.
        db.delete("600");
        db.testCompactMemTable();
        assertEquals(db.filesPerLevel(), "3");
        assertNull(db.get("600"));
    }

    @Test(dataProvider = "options")
    public void testEmptyDb(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        testDb(db);
    }

    @Test(dataProvider = "options")
    public void testSingleEntrySingle(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        testDb(db, immutableEntry("name", "dain sundstrom"));
    }

    @Test(dataProvider = "options")
    public void testMultipleEntries(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/strong", "Lagavulin"));

        testDb(db, entries);
    }

    @Test(dataProvider = "options")
    public void testMultiPassMultipleEntries(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/strong", "Lagavulin"));

        for (int i = 1; i < entries.size(); i++) {
            testDb(db, entries);
        }
    }

    //TODO this test may fail in windows. a path that also fails in windows must be found
    @Test(enabled = false, expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Database directory '" + DOES_NOT_EXIST_FILENAME_PATTERN + "'.*")
    public void testCantCreateDirectoryReturnMessage()
            throws Exception
    {
        new DbStringWrapper(new Options(), defaultEnv.toFile(DOES_NOT_EXIST_FILENAME));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Database directory.*is not a directory")
    public void testDBDirectoryIsFileRetrunMessage()
            throws Exception
    {
        File databaseFile = databaseDir.child("imafile");
        defaultEnv.writeStringToFileSync(databaseFile, "");
        new DbStringWrapper(new Options(), databaseFile);
    }

    @Test
    public void testSymbolicLinkForFileWithoutParent()
    {
        assertFalse(FileUtils.isSymbolicLink(new java.io.File("db")));
    }

    @Test
    public void testSymbolicLinkForFileWithParent()
    {
        assertFalse(FileUtils.isSymbolicLink(new java.io.File(DOES_NOT_EXIST_FILENAME, "db")));
    }

    @Test(dataProvider = "options")
    public void testCustomComparator(final Options options)
            throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(options.comparator(new LexicographicalReverseDBComparator()), databaseDir);

        List<Entry<String, String>> entries = asList(
                immutableEntry("scotch/strong", "Lagavulin"),
                immutableEntry("scotch/medium", "Highland Park"),
                immutableEntry("scotch/light", "Oban 14"),
                immutableEntry("beer/stout", "Lagunitas Imperial Stout"),
                immutableEntry("beer/ipa", "Lagunitas IPA"),
                immutableEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’")
        );

        for (Entry<String, String> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        SeekingIterator<String, String> seekingIterator = db.iterator();
        seekingIterator.seekToFirst();
        for (Entry<String, String> entry : entries) {
            assertTrue(seekingIterator.valid());
            assertEquals(entry(seekingIterator), entry);
            seekingIterator.next();
        }

        assertFalse(seekingIterator.valid());
        seekingIterator.close();
    }

    @Test(dataProvider = "options")
    public void testManualCompaction(final Options options) throws Exception
    {
        assertEquals(DbConstants.MAX_MEM_COMPACT_LEVEL, 2);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        makeTables(db, 3, "p", "q");
        assertEquals("1,1,1", db.filesPerLevel());

        // Compaction range falls before files
        db.compactRange("", "c");
        assertEquals("1,1,1", db.filesPerLevel());

        // Compaction range falls after files
        db.compactRange("r", "z");
        assertEquals("1,1,1", db.filesPerLevel());

        // Compaction range overlaps files
        db.compactRange("p1", "p9");
        assertEquals("0,0,1", db.filesPerLevel());

        // Populate a different range
        makeTables(db, 3, "c", "e");
        assertEquals("1,1,2", db.filesPerLevel());

        // Compact just the new range
        db.compactRange("b", "f");
        assertEquals("0,0,2", db.filesPerLevel());

        // Compact all
        makeTables(db, 1, "a", "z");
        assertEquals("0,1,2", db.filesPerLevel());
        db.compactRange(null, null);
        assertEquals("0,0,1", db.filesPerLevel());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*' does not exist.*")
    public void testOpenOptionsCreateIfMissingFalse() throws Exception
    {
        Options options = new Options();
        options.createIfMissing(false);
        new DbImpl(options, databaseDir.child("missing").getPath(), defaultEnv);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*' exists .*")
    public void testOpenOptionsErrorIfExistTrue() throws Exception
    {
        Options options = new Options();
        try {
            for (int i = 0; i < 2; ++i) {
                options.createIfMissing(true);
                options.errorIfExists(false);
                try (DbImpl db = new DbImpl(options, databaseDir.getPath(), defaultEnv)) {
                    //make db and close
                }
            }
        }
        catch (Exception e) {
            Assert.fail("Should not fail exceptions");
        }
        options.createIfMissing(false);
        options.errorIfExists(true);
        new DbImpl(options, databaseDir.getPath(), defaultEnv); //reopen and should fail
    }

    @Test
    public void testDestroyEmptyDir() throws Exception
    {
        DbImpl.destroyDB(databaseDir, defaultEnv);
        assertFalse(databaseDir.exists());
    }

    @Test
    public void testDestroyOpenDB() throws Exception
    {
        databaseDir.delete();
        assertFalse(databaseDir.exists());
        Options options = new Options();
        options.createIfMissing(true);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir);
        assertTrue(databaseDir.exists());

        try {
            //must fail
            DbImpl.destroyDB(databaseDir, defaultEnv);
            Assert.fail("Destroy DB should not complete successfully");
        }
        catch (Exception e) {
            //expected
        }
        db.close();

        // Should succeed destroying a closed db.
        DbImpl.destroyDB(databaseDir, defaultEnv);
        assertFalse(databaseDir.exists());
    }

    //Check that number of files does not grow when we are out of space
    @Test
    public void testNoSpace() throws Exception
    {
        Options options = new Options();
        SpecialEnv env = new SpecialEnv(defaultEnv);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir, env);

        db.put("foo", "v1");
        assertEquals(db.get("foo"), "v1");
        db.compactRange("a", "z");
        int numFiles = databaseDir.listFiles().size();
        env.noSpace.set(true); // Force out-of-space errors
        for (int i = 0; i < 10; i++) {
            for (int level = 0; level < DbConstants.NUM_LEVELS - 1; level++) {
                db.testCompactRange(level, null, null);
            }
        }
        env.noSpace.set(false);
        assertTrue(databaseDir.listFiles().size() < numFiles + 3);
    }

    @Test
    public void testNonWritableFileSystem() throws Exception
    {
        Options options = new Options();
        options.writeBufferSize(1000);
        SpecialEnv env = new SpecialEnv(defaultEnv);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir, env);
        db.put("foo", "v1");
        env.nonWritable.set(true);  // Force errors for new files
        String big = longString(100000, 'x');
        int errors = 0;
        for (int i = 0; i < 20; i++) {
            try {
                db.put("foo", big);
            }
            catch (Exception e) {
                errors++;
                Thread.sleep(100);
            }
        }
        assertTrue(errors > 0);
        env.nonWritable.set(false);
    }

    @Test
    public void testWriteSyncError() throws Exception
    {
        // Check that log sync errors cause the DB to disallow future writes.

        // (a) Cause log sync calls to fail
        SpecialEnv env = new SpecialEnv(defaultEnv);
        DbStringWrapper db = new DbStringWrapper(new Options(), databaseDir, env);
        env.dataSyncError.set(true);

        WriteOptions w = new WriteOptions();
        // (b) Normal write should succeed
        db.put("k1", "v1", w);
        assertEquals(db.get("k1"), "v1");

        // (c) Do a sync write; should fail
        w.sync(true);
        try {
            db.put("k2", "v2", w);
            Assert.fail("Should not reach this");
        }
        catch (Exception ignore) {
        }
        assertEquals(db.get("k1"), "v1");
        assertEquals(db.get("k2"), null);

        // (d) make sync behave normally
        env.dataSyncError.set(false);

        // (e) Do a non-sync write; should fail
        w.sync(false);
        try {
            db.put("k3", "v3", w);
            Assert.fail("Should not reach this");
        }
        catch (Exception e) {
        }
        assertEquals(db.get("k1"), "v1");
        assertEquals(db.get("k2"), null);
        assertEquals(db.get("k3"), null);
    }

    @Test
    public void testManifestWriteError() throws Exception
    {
        // Test for the following problem:
        // (a) Compaction produces file F
        // (b) Log record containing F is written to MANIFEST file, but Sync() fails
        // (c) GC deletes F
        // (d) After reopening DB, reads fail since deleted F is named in log record

        // We iterate twice.  In the second iteration, everything is the
        // same except the log record never makes it to the MANIFEST file.
        SpecialEnv specialEnv = new SpecialEnv(defaultEnv);
        for (int iter = 0; iter < 2; iter++) {
            AtomicBoolean errorType = (iter == 0)
                    ? specialEnv.manifestSyncError
                    : specialEnv.manifestWriteError;

            // Insert foo=>bar mapping
            Options options = new Options();
            options.createIfMissing(true);
            options.errorIfExists(false);
            DbStringWrapper db = new DbStringWrapper(options, databaseDir, specialEnv);
            db.put("foo", "bar");
            assertEquals(db.get("foo"), "bar");

            // Memtable compaction (will succeed)
            db.testCompactMemTable();
            assertEquals(db.get("foo"), "bar");
            int last = DbConstants.MAX_MEM_COMPACT_LEVEL;
            assertEquals(db.numberOfFilesInLevel(last), 1);   // foo=>bar is now in last level

            // Merging compaction (will fail)
            errorType.set(true);
            db.testCompactRange(last, null, null);  // Should fail
            assertEquals(db.get("foo"), "bar");

            // Recovery: should not lose data
            errorType.set(false);
            db.reopen();
            assertEquals(db.get("foo"), "bar");
            db.close();
            databaseDir.deleteRecursively();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".* missing files")
    public void testMissingSSTFile() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options(), databaseDir, defaultEnv);
        db.put("foo", "bar");
        assertEquals(db.get("foo"), "bar");

        // Dump the memtable to disk.
        db.testCompactMemTable();
        assertEquals(db.get("foo"), "bar");

        db.close();
        assertTrue(deleteAnSSTFile());
        db.options.paranoidChecks(true);
        db.reopen();
    }

    private boolean deleteAnSSTFile()
    {
        for (org.iq80.leveldb.env.File f : databaseDir.listFiles()) {
            Filename.FileInfo fileInfo = Filename.parseFileName(f);
            if (fileInfo != null && fileInfo.getFileType() == Filename.FileType.TABLE) {
                assertTrue(f.delete());
                return true;
            }
        }
        return false;
    }

    @Test
    public void testStillReadSST() throws Exception
    {
        DbStringWrapper db = new DbStringWrapper(new Options(), databaseDir, defaultEnv);
        db.put("foo", "bar");
        assertEquals("bar", db.get("foo"));

        // Dump the memtable to disk.
        db.testCompactMemTable();
        assertEquals("bar", db.get("foo"));
        db.close();
        assertTrue(renameLDBToSST() > 0);
        Options options = new Options();
        options.paranoidChecks(true);
        options.errorIfExists(false);
        db.reopen();
        assertEquals("bar", db.get("foo"));
    }

    // Returns number of files renamed.
    private int renameLDBToSST()
    {
        int filesRenamed = 0;
        for (File f : databaseDir.listFiles()) {
            Filename.FileInfo fileInfo = Filename.parseFileName(f);
            if (fileInfo != null && fileInfo.getFileType() == Filename.FileType.TABLE) {
                assertTrue(f.renameTo(f.getParentFile().child(Filename.sstTableFileName(fileInfo.getFileNumber()))));
                filesRenamed++;
            }
        }
        return filesRenamed;
    }

    @Test
    public void testFilesDeletedAfterCompaction() throws Exception
    {
        File counting = databaseDir.child("counting");
        DbStringWrapper db = new DbStringWrapper(new Options(), counting, defaultEnv);
        db.put("foo", "v2");
        db.compactRange("a", "z");
        int files = databaseDir.listFiles().size();
        for (int i = 0; i < 10; i++) {
            db.put("foo", "v2");
            db.compactRange("a", "z");
        }
        assertEquals(databaseDir.listFiles().size(), files);
    }

    @Test
    public void testBloomFilter() throws Exception
    {
        SpecialEnv env = new SpecialEnv(defaultEnv);
        env.countRandomReads = true;
        Options options = new Options()
                .filterPolicy(new BloomFilterPolicy(10))
                .cacheSize(0);
        DbStringWrapper db = new DbStringWrapper(options, databaseDir, env);
        // Populate multiple layers
        int n = 10000;
        for (int i = 0; i < n; i++) {
            db.put(key(i), key(i));
        }
        db.compactRange("a", "z");
        for (int i = 0; i < n; i += 100) {
            db.put(key(i), key(i));
        }
        db.testCompactMemTable();

        // Prevent auto compactions triggered by seeks
        env.delayDataSync.set(true);

        // Lookup present keys.  Should rarely read from small sstable.
        env.randomReadCounter.set(0);
        for (int i = 0; i < n; i++) {
            assertEquals(key(i), db.get(key(i)));
        }
        int reads = env.randomReadCounter.get();
        assertTrue(reads >= n, "no true that (reads>=n) " + reads + ">=" + n);
        assertTrue(reads <= n + 2 * n / 100, "no true that (reads <= n + 2 * n / 100): " + reads + "<= " + n + " + 2 * " + n + " / 100");

        // Lookup present keys.  Should rarely read from either sstable.
        env.randomReadCounter.set(0);
        for (int i = 0; i < n; i++) {
            assertNull(db.get(key(i) + ".missing"));
        }
        reads = env.randomReadCounter.get();
        assertTrue(reads <= 3 * n / 100);

        env.delayDataSync.set(false);
        db.close();
    }

    /**
     * Beside current test, at the end every {@link DbImplTest} test case, close is asserted for opened file handles.
     */
    @Test(dataProvider = "options")
    public void testFileHandlesClosed(final Options options) throws Exception
    {
        assertTrue(options.maxOpenFiles() > 2); //for this test to work
        DbStringWrapper db = new DbStringWrapper(options, databaseDir, defaultEnv);
        fillLevels(db, "A", "C");
        assertNotNull(db.get("A"));
        assertNull(db.get("A.missing"));
        db.db.invalidateAllCaches();
        assertEquals(db.getOpenHandles(), 3, "All files but log and manifest should be closed");
        try (SeekingIterator<String, String> iterator = db.iterator()) {
            iterator.seek("B");
            assertNotNull(iterator.key());
            assertNotNull(iterator.value());
            assertTrue(db.getOpenHandles() > 3);
        }
        db.db.invalidateAllCaches();
        //with no compaction running and no cache, all db files should be closed but log and manifest
        assertEquals(db.getOpenHandles(), 3, "All files but log and manifest should be closed");
        db.close();
        assertEquals(db.getOpenHandles(), 0, "All files should be closed");
    }

    // Do n memtable compactions, each of which produces an sstable
    // covering the range [small,large].
    private void makeTables(DbStringWrapper db, int n, String small, String large)
    {
        for (int i = 0; i < n; i++) {
            db.put(small, "begin");
            db.put(large, "end");
            db.testCompactMemTable();
        }
    }

    @SafeVarargs
    private final void testDb(DbStringWrapper db, Entry<String, String>... entries)
            throws IOException
    {
        testDb(db, asList(entries));
    }

    private void testDb(DbStringWrapper db, List<Entry<String, String>> entries) throws IOException
    {
        for (Entry<String, String> entry : entries) {
            db.put(entry.getKey(), entry.getValue());
        }

        for (Entry<String, String> entry : entries) {
            String actual = db.get(entry.getKey());
            assertEquals(actual, entry.getValue(), "Key: " + entry.getKey());
        }

        SeekingIterator<String, String> seekingIterator = db.iterator();
        seekingIterator.seekToFirst();
        assertSequence(seekingIterator, entries);

        seekingIterator.seekToFirst();
        assertSequence(seekingIterator, entries);

        for (Entry<String, String> entry : entries) {
            List<Entry<String, String>> nextEntries = entries.subList(entries.indexOf(entry), entries.size());
            seekingIterator.seek(entry.getKey());
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(beforeString(entry));
            assertSequence(seekingIterator, nextEntries);

            seekingIterator.seek(afterString(entry));
            assertSequence(seekingIterator, nextEntries.subList(1, nextEntries.size()));
        }

        Slice endKey = Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        seekingIterator.seek(endKey.toString(UTF_8));
        assertSequence(seekingIterator, Collections.emptyList());
        seekingIterator.close();
    }

    @BeforeMethod
    public void setUp()
    {
        defaultEnv = EnvImpl.createEnv();
        databaseDir = defaultEnv.createTempDir("leveldb");
    }

    @AfterMethod
    public void tearDown()
    {
        for (DbStringWrapper db : opened) {
            db.close();
        }
        opened.clear();
        boolean b = databaseDir.deleteRecursively();
        //assertion is specially useful in windows
        assertFalse(!b && databaseDir.exists(), "Dir should be possible to delete! All files should have been released. Existing files: " + databaseDir.listFiles());
    }

    private void assertBetween(long actual, int smallest, int greatest)
    {
        if (!between(actual, smallest, greatest)) {
            fail(String.format("Expected: %s to be between %s and %s", actual, smallest, greatest));
        }
    }

    private void assertNoNextElement(SeekingIterator<String, String> iterator)
    {
        assertFalse(iterator.valid());
        assertFalse(iterator.next());
        assertThrows(NoSuchElementException.class, iterator::key);
        assertThrows(NoSuchElementException.class, iterator::value);
    }

    static byte[] toByteArray(String value)
    {
        return value.getBytes(UTF_8);
    }

    private static String randomString(Random random, int length)
    {
        char[] chars = new char[length];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) ((int) ' ' + random.nextInt(95));
        }
        return new String(chars);
    }

    private static String longString(int length, char character)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, character);
        return new String(chars);
    }

    public static String key(int i)
    {
        return String.format("key%06d", i);
    }

    private boolean between(long size, long left, long right)
    {
        return left <= size && size <= right;
    }

    private void fillLevels(DbStringWrapper db, String smallest, String largest)
    {
        makeTables(db, NUM_LEVELS, smallest, largest);
    }

    private final ArrayList<DbStringWrapper> opened = new ArrayList<>();

    private static class LexicographicalReverseDBComparator
            implements DBComparator
    {
        @Override
        public String name()
        {
            return "test";
        }

        @Override
        public int compare(byte[] sliceA, byte[] sliceB)
        {
            // reverse order
            return -(UnsignedBytes.lexicographicalComparator().compare(sliceA, sliceB));
        }

        @Override
        public byte[] findShortestSeparator(byte[] start, byte[] limit)
        {
            // Find length of common prefix
            int sharedBytes = calculateSharedBytes(start, limit);

            // Do not shorten if one string is a prefix of the other
            if (sharedBytes < Math.min(start.length, limit.length)) {
                // if we can add one to the last shared byte without overflow and the two keys differ by more than
                // one increment at this location.
                int lastSharedByte = start[sharedBytes] & 0xff;
                if (lastSharedByte < 0xff && lastSharedByte + 1 < limit[sharedBytes]) {
                    byte[] result = Arrays.copyOf(start, sharedBytes + 1);
                    result[sharedBytes] = (byte) (lastSharedByte + 1);

                    assert (compare(result, limit) < 0) : "start must be less than last limit";
                    return result;
                }
            }
            return start;
        }

        @Override
        public byte[] findShortSuccessor(byte[] key)
        {
            // Find first character that can be incremented
            for (int i = 0; i < key.length; i++) {
                int b = key[i];
                if (b != 0xff) {
                    byte[] result = Arrays.copyOf(key, i + 1);
                    result[i] = (byte) (b + 1);
                    return result;
                }
            }
            // key is a run of 0xffs.  Leave it alone.
            return key;
        }

        private int calculateSharedBytes(byte[] leftKey, byte[] rightKey)
        {
            int sharedKeyBytes = 0;

            if (leftKey != null && rightKey != null) {
                int minSharedKeyBytes = Math.min(leftKey.length, rightKey.length);
                while (sharedKeyBytes < minSharedKeyBytes && leftKey[sharedKeyBytes] == rightKey[sharedKeyBytes]) {
                    sharedKeyBytes++;
                }
            }

            return sharedKeyBytes;
        }
    }

    private class DbStringWrapper
    {
        private final Options options;
        private final File databaseDir;
        private final CountingHandlesEnv env1;
        private DbImpl db;

        private DbStringWrapper(Options options, File databaseDir)
                throws IOException
        {
            this(options, databaseDir, defaultEnv);
        }

        private DbStringWrapper(Options options, File databaseDir, Env env)
                throws IOException
        {
            this.options = options.paranoidChecks(true).createIfMissing(true).errorIfExists(true);
            this.databaseDir = databaseDir;
            env1 = new CountingHandlesEnv(env);
            this.db = new DbImpl(options, databaseDir.getPath(), env1);
            opened.add(this);
        }

        //get non closed file handles
        public int getOpenHandles()
        {
            return env1.getOpenHandles();
        }

        public String get(String key)
        {
            byte[] slice = db.get(toByteArray(key));
            if (slice == null) {
                return null;
            }
            return new String(slice, UTF_8);
        }

        public String get(String key, Snapshot snapshot)
        {
            byte[] slice = db.get(toByteArray(key), new ReadOptions().snapshot(snapshot));
            if (slice == null) {
                return null;
            }
            return new String(slice, UTF_8);
        }

        public void put(String key, String value)
        {
            db.put(toByteArray(key), toByteArray(value));
        }

        public void put(String key, String value, WriteOptions wo)
        {
            db.put(toByteArray(key), toByteArray(value), wo);
        }

        public void delete(String key)
        {
            db.delete(toByteArray(key));
        }

        public SeekingIterator<String, String> iterator()
        {
            return SeekingDBIteratorAdapter.toSeekingIterator(db.iterator(), k -> k.getBytes(UTF_8), k -> new String(k, UTF_8), v -> new String(v, UTF_8));
        }

        public SeekingIterator<String, String> iterator(ReadOptions readOption)
        {
            return SeekingDBIteratorAdapter.toSeekingIterator(db.iterator(readOption), k -> k.getBytes(UTF_8), k -> new String(k, UTF_8), v -> new String(v, UTF_8));
        }

        public Snapshot getSnapshot()
        {
            return db.getSnapshot();
        }

        public void close()
        {
            db.close();
            assertEquals(env1.getOpenHandles(), 0, "All files should be closed");
        }

        public void testCompactMemTable()
        {
            db.testCompactMemTable();
            db.waitForBackgroundCompactationToFinish();
        }

        public void compactRange(String start, String limit)
        {
            db.compactRange(start == null ? null : Slices.copiedBuffer(start, UTF_8).getBytes(), limit == null ? null : Slices.copiedBuffer(limit, UTF_8).getBytes());
            db.waitForBackgroundCompactationToFinish();
        }

        public void testCompactRange(int level, String start, String limit)
        {
            db.testCompactRange(level, start == null ? null : Slices.copiedBuffer(start, UTF_8), limit == null ? null : Slices.copiedBuffer(limit, UTF_8));
            db.waitForBackgroundCompactationToFinish();
        }

        public void waitForBackgroundCompactationToFinish()
        {
            db.waitForBackgroundCompactationToFinish();
        }

        public int numberOfFilesInLevel(int level)
        {
            return db.numberOfFilesInLevel(level);
        }

        public int totalTableFiles()
        {
            int result = 0;
            for (int level = 0; level < NUM_LEVELS; level++) {
                result += db.numberOfFilesInLevel(level);
            }
            return result;
        }

        // Return spread of files per level
        public String filesPerLevel()
        {
            StringBuilder result = new StringBuilder();
            int lastNonZeroOffset = 0;
            for (int level = 0; level < DbConstants.NUM_LEVELS; level++) {
                int f = db.numberOfFilesInLevel(level);
                if (result.length() > 0) {
                    result.append(",");
                }
                result.append(f);
                if (f > 0) {
                    lastNonZeroOffset = result.length();
                }
            }
            result.setLength(lastNonZeroOffset);
            return result.toString();
        }

        public long size(String start, String limit)
        {
            return db.getApproximateSizes(new Range(toByteArray(start), toByteArray(limit)));
        }

        public long getMaxNextLevelOverlappingBytes()
        {
            return db.getMaxNextLevelOverlappingBytes();
        }

        public void reopen()
                throws IOException
        {
            reopen(options);
        }

        public void reopen(Options options)
                throws IOException
        {
            db.close();
            db = new DbImpl(options.paranoidChecks(true).createIfMissing(false).errorIfExists(false), databaseDir.getPath(), defaultEnv);
        }

        private List<String> allEntriesFor(String userKey) throws IOException
        {
            ImmutableList.Builder<String> result = ImmutableList.builder();
            try (InternalIterator iterator = db.internalIterator(new ReadOptions())) {
                for (boolean valid = iterator.seekToFirst(); valid; valid = iterator.next()) {
                    Entry<InternalKey, Slice> entry = entry(iterator);
                    String entryKey = entry.getKey().getUserKey().toString(UTF_8);
                    if (entryKey.equals(userKey)) {
                        if (entry.getKey().getValueType() == ValueType.VALUE) {
                            result.add(entry.getValue().toString(UTF_8));
                        }
                        else {
                            result.add("DEL");
                        }
                    }
                }
            }
            return result.build();
        }
    }

    private static class SpecialEnv implements Env
    {
        private Env env;
        // sstable/log Sync() calls are blocked while this pointer is non-NULL.
        private AtomicBoolean delayDataSync = new AtomicBoolean();

        // sstable/log Sync() calls return an error.
        private AtomicBoolean dataSyncError = new AtomicBoolean();

        // Simulate no-space errors while this pointer is non-NULL.
        private AtomicBoolean noSpace = new AtomicBoolean();

        // Simulate non-writable file system while this pointer is non-NULL
        protected AtomicBoolean nonWritable = new AtomicBoolean();

        // Force sync of manifest files to fail while this pointer is non-NULL
        private AtomicBoolean manifestSyncError = new AtomicBoolean();

        // Force write to manifest files to fail while this pointer is non-NULL
        private AtomicBoolean manifestWriteError = new AtomicBoolean();
        boolean countRandomReads;

        AtomicInteger randomReadCounter = new AtomicInteger();

        public SpecialEnv(Env env)
        {
            this.env = env;
        }

        @Override
        public long nowMicros()
        {
            return env.nowMicros();
        }

        @Override
        public org.iq80.leveldb.env.File toFile(String filename)
        {
            return env.toFile(filename);
        }

        @Override
        public org.iq80.leveldb.env.File createTempDir(String prefix)
        {
            return env.createTempDir(prefix);
        }

        @Override
        public SequentialFile newSequentialFile(org.iq80.leveldb.env.File file) throws IOException
        {
            return env.newSequentialFile(file);
        }

        @Override
        public RandomInputFile newRandomAccessFile(org.iq80.leveldb.env.File file) throws IOException
        {
            RandomInputFile randomInputFile = env.newRandomAccessFile(file);
            if (countRandomReads) {
                return new CountingFile(randomInputFile);
            }
            return randomInputFile;
        }

        @Override
        public WritableFile newWritableFile(org.iq80.leveldb.env.File file) throws IOException
        {
            if (nonWritable.get()) {
                throw new IOException("simulated write error");
            }
            if (file.getName().endsWith(".ldb") || file.getName().endsWith(".log")) {
                return new DataFile(env.newWritableFile(file));
            }
            else {
                return new ManifestFile(env.newWritableFile(file));
            }
        }

        @Override
        public WritableFile newAppendableFile(org.iq80.leveldb.env.File file) throws IOException
        {
            return env.newAppendableFile(file);
        }

        @Override
        public Logger newLogger(org.iq80.leveldb.env.File loggerFile) throws IOException
        {
            return env.newLogger(loggerFile);
        }

        @Override
        public DbLock tryLock(org.iq80.leveldb.env.File file) throws IOException
        {
            return env.tryLock(file);
        }

        @Override
        public void writeStringToFileSync(File file, String content) throws IOException
        {
            env.writeStringToFileSync(file, content);
        }

        @Override
        public String readFileToString(File file) throws IOException
        {
            return env.readFileToString(file);
        }

        private class CountingFile implements RandomInputFile
        {
            private RandomInputFile randomInputFile;

            public CountingFile(RandomInputFile randomInputFile)
            {
                this.randomInputFile = randomInputFile;
            }

            @Override
            public long size()
            {
                return randomInputFile.size();
            }

            @Override
            public ByteBuffer read(long offset, int length) throws IOException
            {
                randomReadCounter.incrementAndGet();
                return randomInputFile.read(offset, length);
            }

            @Override
            public void close() throws IOException
            {
                randomInputFile.close();
            }
        }

        private class DataFile implements WritableFile
        {
            private final WritableFile writableFile;

            public DataFile(WritableFile writableFile)
            {
                this.writableFile = writableFile;
            }

            @Override
            public void append(Slice data) throws IOException
            {
                if (noSpace.get()) {
                    // Drop writes on the floor
                }
                else {
                    writableFile.append(data);
                }
            }

            @Override
            public void force() throws IOException
            {
                if (dataSyncError.get()) {
                    throw new IOException("simulated data sync error");
                }
                while (delayDataSync.get()) {
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
                writableFile.force();
            }

            @Override
            public void close() throws IOException
            {
                writableFile.close();
            }
        }

        private class ManifestFile implements WritableFile
        {
            private WritableFile writableFile;

            public ManifestFile(WritableFile writableFile)
            {
                this.writableFile = writableFile;
            }

            @Override
            public void append(Slice data) throws IOException
            {
                if (manifestWriteError.get()) {
                    throw new IOException("simulated writer error");
                }
                writableFile.append(data);
            }

            @Override
            public void force() throws IOException
            {
                if (manifestSyncError.get()) {
                    throw new IOException("simulated sync error");
                }
                writableFile.force();
            }

            @Override
            public void close() throws IOException
            {
                writableFile.close();
            }
        }
    }

    static class OptionsDesc extends Options
    {
        private String desc;

        OptionsDesc(String desc)
        {
            this.desc = desc;
        }

        @Override
        public String toString()
        {
            return "Options{" + desc + '}';
        }
    }
}
