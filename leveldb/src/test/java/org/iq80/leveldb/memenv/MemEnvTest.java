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
package org.iq80.leveldb.memenv;

import com.google.common.collect.Lists;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.env.DbLock;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.env.File;
import org.iq80.leveldb.env.RandomInputFile;
import org.iq80.leveldb.env.SequentialFile;
import org.iq80.leveldb.env.WritableFile;
import org.iq80.leveldb.impl.DbImpl;
import org.iq80.leveldb.iterator.DBIteratorAdapter;
import org.iq80.leveldb.util.DynamicSliceOutput;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class MemEnvTest
{
    @Test
    public void testNewFineName()
    {
        Env env = MemEnv.createEnv();
        testUnexisting(env);
    }

    private void testUnexisting(Env env)
    {
        File file = env.toFile("/roo/doNotExist");
        assertEquals(file.getName(), "doNotExist");
        assertEquals(file.getPath(), "/roo/doNotExist");
        assertFalse(file.exists());
        assertFalse(file.isDirectory());
        assertFalse(file.isFile());
        assertEquals(file.length(), 0);
    }

    @Test
    public void testCantBeFileAndDir()
    {
        File file = MemEnv.createEnv().toFile("/a/b");
        assertFalse(file.isFile());
        assertFalse(file.isDirectory());
        assertFalse(file.exists());
        assertEquals(file.getName(), "b");
        assertEquals(file.getPath(), "/a/b");
        assertTrue(file.mkdirs());
        assertTrue(file.isDirectory());
        assertFalse(file.isFile());
        assertTrue(file.exists());
    }

    @Test
    public void testCantCreateFileIfDirDoesNotExist()
    {
        Env env = MemEnv.createEnv();
        File abc = env.createTempDir("abc");
        File child = abc.child("a").child("b");
        Assert.assertThrows(() -> env.writeStringToFileSync(child, "newContent"));
    }

    @Test
    public void testTreeIsolated() throws IOException
    {
        Env env = MemEnv.createEnv();
        File abc = env.toFile("/dir");
        assertTrue(abc.mkdirs());
        File child = abc.child("a").child("b");
        File child1 = abc.child("c").child("b");
        child.mkdirs();
        child1.mkdirs();
        //write content on both trees
        // /dir/a/b/c
        // /dir/c/c/c
        env.writeStringToFileSync(child.child("c"), "content2");
        env.writeStringToFileSync(child1.child("c"), "content1");
        assertEquals(abc.listFiles().size(), 2);
        assertEquals(abc.child("a").listFiles().size(), 1);
        assertEquals(abc.child("c").listFiles().size(), 1);
        assertEquals(abc.child("y").listFiles().size(), 0);
        assertEquals(abc.child("a").listFiles().get(0), env.toFile("/dir/a/b"));
        // write content on two distinct sub tree
        assertEquals(env.readFileToString(env.toFile("/dir/a/b/c")), "content2");
        assertEquals(env.readFileToString(env.toFile("/dir/c/b/c")), "content1");

        //delete one of the trees
        env.toFile("/dir/a").deleteRecursively();
        assertEquals(abc.listFiles().size(), 1);

        // unable to read delete file content
        Assert.assertThrows(IOException.class, () -> env.readFileToString(env.toFile("/dir/a/b/c")));

        //can still access non deleted file content
        assertEquals(env.readFileToString(env.toFile("/dir/c/b/c")), "content1");
        assertEquals(abc.child("a").child("b").child("c").getParentFile(), abc.child("a").child("b"));
    }

    @Test
    public void testTempDir() throws IOException
    {
        Env env = MemEnv.createEnv();
        File file = env.createTempDir("prefixme");
        assertTrue(file.isDirectory());
        assertFalse(file.isFile());
        assertTrue(file.exists());
        assertEquals(file.listFiles().size(), 0);

        File current = file.child("current");
        assertEquals(current.getName(), "current");
        assertFalse(current.exists());
        env.writeStringToFileSync(current, "newContent");
        assertTrue(current.exists());
        assertTrue(current.isFile());

        List<File> files = file.listFiles();
        assertEquals(files.size(), 1);
        assertEquals(files.get(0), current);

        testUnexisting(env);
    }

    @Test
    public void testBasic() throws IOException
    {
        Env env = MemEnv.createEnv();
        File file = env.toFile("/dir");
        assertTrue(file.mkdirs());

        // Check that the directory is empty.
        File nonExistingFile = env.toFile("/dir/non_existent");
        assertFalse(nonExistingFile.exists());
        assertEquals(nonExistingFile.length(), 0L);
        assertEquals(file.listFiles().size(), 0);

        // Create a file.
        assertFalse(env.toFile("/dir/f").exists());
        WritableFile f = env.newWritableFile(file.child("f"));
        assertNotNull(f);
        assertEquals(env.toFile("/dir/f").length(), 0);
        f.close();

        // Check that the file exists.
        File file1 = env.toFile("/dir/f");
        assertTrue(file1.exists());
        assertEquals(file1.length(), 0L);
        List<File> files = env.toFile("/dir").listFiles();
        assertEquals(files.size(), 1);
        assertEquals(files.get(0), file1);

        // Write to the file.
        WritableFile writableFile = env.newWritableFile(file1);
        writableFile.append(slice("abc"));
        writableFile.close();

        // Check that append works.
        WritableFile writableFile1 = env.newAppendableFile(env.toFile("/dir/f"));
        assertEquals(env.toFile("/dir/f").length(), 3);
        writableFile1.append(slice("hello"));
        writableFile1.close();

        // Check for expected size.
        assertEquals(env.toFile("/dir/f").length(), 8);

        // Check that renaming works.
        assertFalse(nonExistingFile.renameTo(env.toFile("/dir/g")));
        assertTrue(env.toFile("/dir/f").renameTo(env.toFile("/dir/g")));
        assertFalse(env.toFile("/dir/f").exists());
        assertTrue(env.toFile("/dir/g").exists());
        assertEquals(env.toFile("/dir/g").length(), 8);
        assertEquals(env.readFileToString(env.toFile("/dir/g")), "abchello");

        // Check that opening non-existent file fails.
        Assert.assertThrows(() -> env.newSequentialFile(nonExistingFile));
        Assert.assertThrows(() -> env.newRandomAccessFile(nonExistingFile));
        Assert.assertThrows(() -> env.readFileToString(nonExistingFile));

        // Check that deleting works.
        assertFalse(nonExistingFile.delete());
        assertFalse(nonExistingFile.deleteRecursively());
        assertTrue(env.toFile("/dir/g").delete());
        assertFalse(env.toFile("/dir/g").exists());
        assertEquals(env.toFile("/dir").listFiles().size(), 0);
    }

    @Test
    public void testMkdirs()
    {
        Env env = MemEnv.createEnv();
        File dir = env.toFile("/dir");
        assertFalse(dir.isDirectory());
        assertTrue(dir.mkdirs());
        assertTrue(dir.isDirectory());
    }

    @Test
    public void testReadWrite() throws IOException
    {
        Env env = MemEnv.createEnv();
        File dir = env.toFile("/dir");
        assertTrue(dir.mkdirs());
        File f = dir.child("f");
        WritableFile writableFile = env.newWritableFile(f);
        writableFile.append(slice("hello "));
        writableFile.append(slice("world"));
        writableFile.close();

        // Read sequentially.
        SequentialFile sequentialFile = env.newSequentialFile(f);
        assertEquals(readSeq(sequentialFile, 5), slice("hello"));
        sequentialFile.skip(1);
        assertEquals(readSeq(sequentialFile, 1000), slice("world"));
        assertEquals(readSeq(sequentialFile, 1000), slice(""));
        sequentialFile.skip(100);
        assertEquals(readSeq(sequentialFile, 1000), slice(""));  // Try to skip past end of file.
        sequentialFile.close();

        // Random reads.
        RandomInputFile randomInputFile = env.newRandomAccessFile(env.toFile("/dir/f"));
        assertEquals(slice(randomInputFile.read(6, 5)), slice("world"));
        assertEquals(slice(randomInputFile.read(0, 5)), slice("hello"));
        assertEquals(slice(randomInputFile.read(10, 100)), slice("d"));

        // Too high offset.
        Assert.assertThrows(() -> randomInputFile.read(1000, 5));
        randomInputFile.close();
    }

    private Slice readSeq(SequentialFile sequentialFile, int atMost) throws IOException
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(atMost * 2);
        int read = sequentialFile.read(atMost, dynamicSliceOutput);
        Slice slice = dynamicSliceOutput.slice();
        if (read != -1) {
            assertEquals(read, slice.length());
        }
        else {
            assertEquals(slice.length(), 0);
        }
        return slice;
    }

    @Test(expectedExceptions = IOException.class)
    public void testDbLockOnInvalidPath() throws IOException
    {
        Env env = MemEnv.createEnv();
        env.tryLock(env.toFile("/dir/a"));
    }

    @Test
    public void testDbLock() throws IOException
    {
        Env env = MemEnv.createEnv();
        File lockFile = env.toFile("/dir/a");
        assertTrue(lockFile.getParentFile().mkdirs());
        DbLock dbLock = env.tryLock(lockFile);
        assertTrue(dbLock.isValid());
        dbLock.release();
        assertFalse(dbLock.isValid());
    }

    @Test
    public void testMisc() throws IOException
    {
        Env env = MemEnv.createEnv();
        File test = env.createTempDir("test");
        assertFalse(test.getName().isEmpty());
        WritableFile writableFile = env.newWritableFile(test.child("b"));
        // These are no-ops, but we test they return success.
        // TODO writableFile.sync();
        writableFile.force();
        writableFile.close();
    }

    @Test
    public void testLargeFiles() throws IOException
    {
        int writeSize = 300 * 1024;
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(2 * writeSize);
        for (int i = 0; i < writeSize; i++) {
            dynamicSliceOutput.writeByte((byte) i);
        }
        Slice data = dynamicSliceOutput.slice();
        Env env = MemEnv.createEnv();
        env.toFile("/dir").mkdirs();
        WritableFile writableFile = env.newWritableFile(env.toFile("/dir/f"));
        writableFile.append(slice("foo"));
        writableFile.append(dynamicSliceOutput.slice());
        writableFile.close();

        SequentialFile sequentialFile = env.newSequentialFile(env.toFile("/dir/f"));
        assertEquals(readSeq(sequentialFile, 3), slice("foo"));
        int read = 0;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (read < writeSize) {
            Slice slice = readSeq(sequentialFile, 3);
            read += slice.length();
            byteArrayOutputStream.write(slice.getBytes());
        }
        byte[] bytes = byteArrayOutputStream.toByteArray();
        assertEquals(Slices.wrappedBuffer(bytes), data);
        sequentialFile.close();
    }

    @Test
    public void testOverwriteOpenFile() throws IOException
    {
        Env env = MemEnv.createEnv();
        String writeData = "Write #1 data";
        File prefix = env.createTempDir("prefix");
        File child = prefix.child("leveldb-TestFile.dat");
        env.writeStringToFileSync(child, writeData);
        RandomInputFile randomInputFile = env.newRandomAccessFile(child);
        String write2Data = "Write #2 data";
        env.writeStringToFileSync(child, write2Data);

        // Verify that overwriting an open file will result in the new file data
        // being read from files opened before the write.
        assertEquals(slice(randomInputFile.read(0, writeData.length())), slice(write2Data));
        randomInputFile.close();
    }

    @Test
    public void testDbTest() throws IOException
    {
        Options options1 = new Options();
        options1.createIfMissing(true);
        Env env = MemEnv.createEnv();

        List<Slice> keys = Lists.newArrayList(slice("aaa"), slice("bbb"), slice("ccc"));
        List<Slice> values = Lists.newArrayList(slice("foo"), slice("bar"), slice("baz"));

        try (DbImpl db = new DbImpl(options1, "/dir/db", env)) {
            for (int i = 0; i < keys.size(); i++) {
                db.put(keys.get(i).getBytes(), values.get(i).getBytes());
            }
            for (int i = 0; i < keys.size(); i++) {
                assertEquals(Slices.wrappedBuffer(db.get(keys.get(i).getBytes())), values.get(i));
            }
            try (DBIteratorAdapter iterator = db.iterator()) {
                for (int i = 0; i < keys.size(); i++) {
                    assertTrue(iterator.hasNext());
                    DBIteratorAdapter.DbEntry next = iterator.next();
                    assertArrayEquals(next.getKey(), keys.get(i).getBytes());
                    assertArrayEquals(next.getValue(), values.get(i).getBytes());
                }
                assertFalse(iterator.hasNext());
            }
            db.compactRange(null, null);
            for (int i = 0; i < keys.size(); i++) {
                assertEquals(Slices.wrappedBuffer(db.get(keys.get(i).getBytes())), values.get(i));
            }
        }
    }

    private Slice slice(ByteBuffer read)
    {
        byte[] dst = new byte[read.remaining()];
        read.get(dst);
        return Slices.wrappedBuffer(dst);
    }

    private static Slice slice(String value)
    {
        return Slices.copiedBuffer(value, UTF_8);
    }
}
