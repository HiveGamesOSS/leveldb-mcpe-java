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

import com.google.common.collect.Maps;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.fileenv.FileUtils;
import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Closeables;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class GIssue320Test
{
    private Random rand;
    private DB db;
    private File databaseDir;

    @BeforeMethod
    public void setUp()
    {
        rand = new Random(0);
        databaseDir = FileUtils.createTempDir("leveldbIssues");
    }

    @AfterMethod
    public void tearDown()
    {
        if (db != null) {
            Closeables.closeQuietly(db);
        }
        boolean b = FileUtils.deleteRecursively(databaseDir);
        //assertion is specially useful in windows
        assertFalse(!b && databaseDir.exists(), "Dir should be possible to delete! All files should have been released. Existing files: " + FileUtils.listFiles(databaseDir));
    }

    private byte[] newString(int index)
    {
        int len = 1024;
        byte[] bytes = new byte[len];
        int i = 0;
        while (i < 8) {
            bytes[i] = (byte) ('a' + ((index >> (4 * i)) & 0xf));
            ++i;
        }
        while (i < bytes.length) {
            bytes[i] = (byte) ('a' + rand.nextInt(26));
            ++i;
        }
        return bytes;
    }

    @Test
    public void testRaiseFromTheDead() throws IOException
    {
        //data that after inserted will not be deleted
        final List<byte[]> alwaysPresentData = new ArrayList<>();
        //data that will be entirely deleted
        final List<byte[]> dataToDelete = new ArrayList<>();
        final WriteBatchImpl batch = new WriteBatchImpl();

        final Options rawOptions = new Options()
                .errorIfExists(true)
                .createIfMissing(true)
                .writeBufferSize(1024 * 1024);
        db = new DbImpl(rawOptions, databaseDir.getAbsolutePath(), EnvImpl.createEnv());
        Consumer<WriteBatchImpl> writeAndClear = b -> {
            db.write(b);
            b.clear();
        };
        //to increase the chance to reproduce the issue, increase this number
        for (int i = 0; i < 2000000; i++) {
            if (rand.nextInt(50000) == 0) {
                writeAndClear.accept(batch);
                sortUnique(dataToDelete);
                try (final Snapshot snapshot = db.getSnapshot(); final DBIterator it = db.iterator(new ReadOptions().snapshot(snapshot))) {
                    it.seek(new byte[] {'A'});
                    for (byte[] iterableDatum : dataToDelete) {
                        assertEquals(iterableDatum, it.next().getKey(),
                                "Entry raised from the dead. Key should have been deleted forever");
                        assertEquals(iterableDatum[0], 'A');
                        batch.delete(iterableDatum);
                        if (rand.nextInt(500) == 0) {
                            batch.put(randomPrefixKey(alwaysPresentData, 'M'), newString('m'));
                            writeAndClear.accept(batch);
                        }
                        if (!alwaysPresentData.isEmpty() && rand.nextInt(10) == 0) {
                            final byte[] key = alwaysPresentData.get(rand.nextInt(alwaysPresentData.size()));
                            assertNotNull(db.get(key));
                        }
                    }
                    i += dataToDelete.size();
                    if (it.hasNext()) {
                        //all "A" prefixed keys where deleted so only non A prefixed keys should exist
                        assertNotEquals(it.next().getKey()[0], 'A');
                    }
                    dataToDelete.clear();
                    writeAndClear.accept(batch);
                }
            }
            else {
                if (rand.nextInt(100) == 0) {
                    batch.put(randomPrefixKey(alwaysPresentData, 'Z'), newString('x'));
                    writeAndClear.accept(batch);
                }
                batch.put(randomPrefixKey(dataToDelete, 'A'), new byte[0]);
            }
        }
    }

    private byte[] randomPrefixKey(List<byte[]> cache, char prefix)
    {
        final byte[] key = new byte[197];
        key[0] = (byte) prefix;
        for (int i = 1; i < key.length; i++) {
            key[i] = (byte) (rand.nextInt(26) + 'a');
        }
        cache.add(key);
        return key;
    }

    private void sortUnique(List<byte[]> iterableData)
    {
        final BytewiseComparator bytewiseComparator = new BytewiseComparator();
        iterableData.sort((o1, o2) -> bytewiseComparator.compare(Slices.wrappedBuffer(o1), Slices.wrappedBuffer(o2)));
        byte[] p = null;
        for (Iterator<byte[]> iterator = iterableData.iterator(); iterator.hasNext(); ) {
            byte[] s = iterator.next();
            if (Arrays.equals(s, p)) {
                iterator.remove();
            }
            p = s;
        }
    }

    @Test
    public void testIssue320() throws IOException
    {
        Map.Entry<byte[], byte[]>[] testMap = new Map.Entry[10000];
        Snapshot[] snapshots = new Snapshot[100];

        db = new DbImpl(new Options().createIfMissing(true), databaseDir.getAbsolutePath(), EnvImpl.createEnv());

        int targetSize = 10000;
        int numItems = 0;
        long count = 0;

        WriteOptions writeOptions = new WriteOptions();
        while (count++ < 200000) {
            int index = rand.nextInt(testMap.length);
            WriteBatch batch = new WriteBatchImpl();

            if (testMap[index] == null) {
                numItems++;
                testMap[index] =
                        Maps.immutableEntry(newString(index), newString(index));
                batch.put(testMap[index].getKey(), testMap[index].getValue());
            }
            else {
                byte[] oldValue = db.get(testMap[index].getKey());
                if (!Arrays.equals(oldValue, testMap[index].getValue())) {
                    Assert.fail("ERROR incorrect value returned by get"
                            + " \ncount=" + count
                            + " \nold value=" + new String(oldValue)
                            + " \ntestMap[index].getValue()=" + new String(testMap[index].getValue())
                            + " \ntestMap[index].getKey()=" + new String(testMap[index].getKey())
                            + " \nindex=" + index);
                }

                if (numItems >= targetSize && rand.nextInt(100) > 30) {
                    batch.delete(testMap[index].getKey());
                    testMap[index] = null;
                    --numItems;
                }
                else {
                    testMap[index] = Maps.immutableEntry(testMap[index].getKey(), newString(index));
                    batch.put(testMap[index].getKey(), testMap[index].getValue());
                }
            }

            db.write(batch, writeOptions);

            if (rand.nextInt(10) == 0) {
                final int i = rand.nextInt(snapshots.length);
                if (snapshots[i] != null) {
                    snapshots[i].close();
                }
                snapshots[i] = db.getSnapshot();
            }
        }
        for (Snapshot snapshot : snapshots) {
            if (snapshot != null) {
                snapshot.close();
            }
        }
        db.close();
    }
}
