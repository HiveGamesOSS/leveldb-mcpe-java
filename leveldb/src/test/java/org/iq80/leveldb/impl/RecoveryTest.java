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

import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.env.File;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.iq80.leveldb.fileenv.MmapLimiter;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.Slices;
import org.iq80.leveldb.env.WritableFile;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class RecoveryTest
{
    private Env env;
    private File dbname;
    private DbImpl db;

    @BeforeMethod
    public void setUp() throws Exception
    {
        env = EnvImpl.createEnv(MmapLimiter.newLimiter(0));
        dbname = env.createTempDir("leveldb").child("recovery_test");
        assertTrue(DbImpl.destroyDB(dbname, env), "unable to close/delete previous db correctly");
        open(null);
    }

    @AfterMethod
    public void tearDown() throws Exception
    {
        close();
        DbImpl.destroyDB(dbname, env);
        dbname.getParentFile().deleteRecursively();
    }

    void close()
    {
        if (db != null) {
            db.close();
        }
        db = null;
    }

    void open(Options options) throws IOException
    {
        close();
        Options opts = new Options();
        if (options != null) {
            opts = options;
        }
        else {
            opts.reuseLogs(true);
            opts.createIfMissing(true);
        }
        db = new DbImpl(opts, dbname.getPath(), env);
        assertEquals(numLogs(), 1);
    }

    void put(String k, String v)
    {
        db.put(k.getBytes(StandardCharsets.UTF_8), v.getBytes(StandardCharsets.UTF_8));
    }

    String get(String k)
    {
        return get(k, null);
    }

    String get(String k, Snapshot snapshot)
    {
        byte[] bytes = db.get(k.getBytes(StandardCharsets.UTF_8));
        return bytes == null ? "NOT_FOUND" : new String(bytes);
    }

    File manifestFileName() throws IOException
    {
        try (BufferedReader in = Files.newReader(new java.io.File(dbname.child(Filename.currentFileName()).getPath()), StandardCharsets.UTF_8)) {
            String current = CharStreams.toString(in);
            int len = current.length();
            if (len > 0 && current.charAt(len - 1) == '\n') {
                current = current.substring(0, len - 1);
            }
            return dbname.child(current);
        }
    }

    File logName(long number)
    {
        return dbname.child(Filename.logFileName(number));
    }

    long deleteLogFiles()
    {
        List<Long> logs = getFiles(Filename.FileType.LOG);
        for (Long log : logs) {
            boolean delete = dbname.child(Filename.logFileName(log)).delete();
            assertTrue(delete);
        }
        return logs.size();
    }

    long firstLogFile()
    {
        return getFiles(Filename.FileType.LOG).get(0);
    }

    List<Long> getFiles(Filename.FileType t)
    {
        ArrayList<Long> result = new ArrayList<>();
        for (File file : dbname.listFiles()) {
            Filename.FileInfo fileInfo = Filename.parseFileName(file);
            if (fileInfo != null && t == fileInfo.getFileType()) {
                result.add(fileInfo.getFileNumber());
            }
        }
        return result;
    }

    int numLogs()
    {
        return getFiles(Filename.FileType.LOG).size();
    }

    int numTables()
    {
        return getFiles(Filename.FileType.TABLE).size();
    }

    long fileSize(File fname)
    {
        return fname.length();
    }

    void compactMemTable()
    {
        db.testCompactMemTable();
    }

    // Directly construct a log file that sets key to val.
    void makeLogFile(long lognum, long seq, Slice key, Slice val) throws IOException
    {
        org.iq80.leveldb.env.File fname = dbname.child(Filename.logFileName(lognum));
        try (LogWriter writer = Logs.createLogWriter(fname, lognum, env)) {
            WriteBatchImpl batch = new WriteBatchImpl();
            batch.put(key, val);
            writer.addRecord(DbImpl.writeWriteBatch(batch, seq), true);
        }
    }

    @Test
    public void testManifestReused() throws Exception
    {
        put("foo", "bar");
        close();
        File oldManifest = manifestFileName();
        open(null);
        assertEquals(manifestFileName(), oldManifest);
        assertEquals(get("foo"), "bar");
        open(null);
        assertEquals(manifestFileName(), oldManifest);
        assertEquals(get("foo"), "bar");
    }

    @Test
    public void testLargeManifestCompacted() throws Exception
    {
        put("foo", "bar");
        close();
        File oldManifest = manifestFileName();

        // Pad with zeroes to make manifest file very big.

        long len = fileSize(oldManifest);
        try (WritableFile file = env.newAppendableFile(oldManifest)) {
            file.append(Slices.allocate(3 * 1048576 - ((int) len)));
            file.force(); //flush
        }

        open(null);
        File newManifest = manifestFileName();
        assertNotEquals(newManifest, oldManifest);
        assertTrue(10000L > fileSize(newManifest));
        assertEquals(get("foo"), "bar");

        open(null);
        assertEquals(manifestFileName(), newManifest);
        assertEquals(get("foo"), "bar");
    }

    @Test
    public void testNoLogFiles() throws Exception
    {
        put("foo", "bar");
        close(); //file are locked in windows, can't delete them fi do not close db.
        assertEquals(deleteLogFiles(), 1);
        open(null);
        assertEquals(get("foo"), "NOT_FOUND");
        open(null);
        assertEquals(get("foo"), "NOT_FOUND");
    }

    @Test
    public void testLogFileReuse() throws Exception
    {
        for (int i = 0; i < 2; i++) {
            put("foo", "bar");
            if (i == 0) {
                // Compact to ensure current log is empty
                compactMemTable();
            }
            close();
            assertEquals(numLogs(), 1);
            long number = firstLogFile();
            if (i == 0) {
                assertEquals(fileSize(logName(number)), 0);
            }
            else {
                assertTrue((long) 0 < fileSize(logName(number)));
            }
            open(null);
            assertEquals(numLogs(), 1);
            assertEquals(firstLogFile(), number, "did not reuse log file");
            assertEquals(get("foo"), "bar");
            open(null);
            assertEquals(numLogs(), 1);
            assertEquals(firstLogFile(), number, "did not reuse log file");
            assertEquals(get("foo"), "bar");
        }
    }

    @Test
    public void testMultipleMemTables() throws Exception
    {
        // Make a large log.
        int kNum = 1000;
        for (int i = 0; i < kNum; i++) {
            String format = String.format("%050d", i);
            put(format, format);
        }
        assertEquals(numTables(), 0);
        close();
        assertEquals(numTables(), 0);
        assertEquals(numLogs(), 1);
        long oldLogFile = firstLogFile();

        // Force creation of multiple memtables by reducing the write buffer size.
        Options opt = new Options();
        opt.reuseLogs(true);
        opt.writeBufferSize((kNum * 100) / 2);
        open(opt);
        assertTrue((long) 2 <= (long) numTables());
        assertEquals(numLogs(), 1);
        assertNotEquals(firstLogFile(), oldLogFile, "must not reuse log");
        for (int i = 0; i < kNum; i++) {
            String format = String.format("%050d", i);
            assertEquals(get(format), format);
        }
    }

    @Test
    public void testMultipleLogFiles() throws Exception
    {
        put("foo", "bar");
        close();
        assertEquals(numLogs(), 1);

        // Make a bunch of uncompacted log files.
        long oldLog = firstLogFile();
        makeLogFile(oldLog + 1, 1000, toSlice("hello"), toSlice("world"));
        makeLogFile(oldLog + 2, 1001, toSlice("hi"), toSlice("there"));
        makeLogFile(oldLog + 3, 1002, toSlice("foo"), toSlice("bar2"));

        // Recover and check that all log files were processed.
        open(null);
        assertTrue((long) 1 <= (long) numTables());
        assertEquals(numLogs(), 1);
        long newLog = firstLogFile();
        assertTrue(oldLog + 3 <= newLog);
        assertEquals(get("foo"), "bar2");
        assertEquals(get("hello"), "world");
        assertEquals(get("hi"), "there");

        // Test that previous recovery produced recoverable state.
        open(null);
        assertTrue((long) 1 <= (long) numTables());
        assertEquals(numLogs(), 1);
        assertEquals(firstLogFile(), newLog);
        assertEquals(get("foo"), "bar2");
        assertEquals(get("hello"), "world");
        assertEquals(get("hi"), "there");

        // Check that introducing an older log file does not cause it to be re-read.
        close();
        makeLogFile(oldLog + 1, 2000, toSlice("hello"), toSlice("stale write"));
        open(null);
        assertTrue((long) 1 <= (long) numTables());
        assertEquals(numLogs(), 1);
        assertEquals(firstLogFile(), newLog);
        assertEquals(get("foo"), "bar2");
        assertEquals(get("hello"), "world");
        assertEquals(get("hi"), "there");
    }

    private Slice toSlice(String hello)
    {
        return Slices.copiedBuffer(hello, StandardCharsets.UTF_8);
    }
}
