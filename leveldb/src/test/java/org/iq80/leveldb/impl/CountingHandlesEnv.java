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

import org.iq80.leveldb.Logger;
import org.iq80.leveldb.env.Env;
import org.iq80.leveldb.env.DbLock;
import org.iq80.leveldb.env.RandomInputFile;
import org.iq80.leveldb.env.SequentialFile;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceOutput;
import org.iq80.leveldb.env.WritableFile;

import org.iq80.leveldb.env.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Environment that count how many handles are currently opened.
 */
public class CountingHandlesEnv implements Env
{
    private final Env env;
    private final AtomicInteger counter = new AtomicInteger();
    private final ConcurrentMap<Object, Object> ob = new ConcurrentHashMap<>();

    public CountingHandlesEnv(Env env)
    {
        this.env = env;
    }

    public int getOpenHandles()
    {
        return counter.get();
    }

    @Override
    public long nowMicros()
    {
        return env.nowMicros();
    }

    @Override
    public File toFile(String filename)
    {
        return env.toFile(filename);
    }

    @Override
    public File createTempDir(String prefix)
    {
        return env.createTempDir(prefix);
    }

    @Override
    public SequentialFile newSequentialFile(File file) throws IOException
    {
        final SequentialFile sequentialFile = env.newSequentialFile(file);
        counter.incrementAndGet();
        return new SequentialFile()
        {
            boolean closed;

            {
                ob.put(this, this);
            }

            public void skip(long n) throws IOException
            {
                sequentialFile.skip(n);
            }

            public int read(int atMost, SliceOutput destination) throws IOException
            {
                return sequentialFile.read(atMost, destination);
            }

            public void close() throws IOException
            {
                if (!closed) {
                    counter.decrementAndGet();
                    closed = true;
                    ob.remove(this);
                }
                sequentialFile.close();
            }
        };
    }

    @Override
    public RandomInputFile newRandomAccessFile(File file) throws IOException
    {
        final RandomInputFile randomInputFile = env.newRandomAccessFile(file);
        counter.incrementAndGet();
        return new RandomInputFile()
        {
            boolean closed;

            {
                ob.put(this, this);
            }

            public long size()
            {
                return randomInputFile.size();
            }

            public ByteBuffer read(long offset, int length) throws IOException
            {
                return randomInputFile.read(offset, length);
            }

            public void close() throws IOException
            {
                if (!closed) {
                    counter.decrementAndGet();
                    closed = true;
                    ob.remove(this);
                }
                randomInputFile.close();
            }
        };
    }

    @Override
    public WritableFile newWritableFile(File file) throws IOException
    {
        return getWritableFile(env.newWritableFile(file));
    }

    @Override
    public WritableFile newAppendableFile(File file) throws IOException
    {
        return getWritableFile(env.newAppendableFile(file));
    }

    @Override
    public Logger newLogger(File loggerFile) throws IOException
    {
        counter.incrementAndGet();
        Logger logger = env.newLogger(loggerFile);
        return new Logger()
        {
            @Override
            public void log(String message)
            {
                logger.log(message);
            }

            @Override
            public void close() throws IOException
            {
                counter.decrementAndGet();
                logger.close();
            }
        };
    }

    @Override
    public DbLock tryLock(File file) throws IOException
    {
        return env.tryLock(file);
    }

    private WritableFile getWritableFile(WritableFile writableFile) throws IOException
    {
        counter.incrementAndGet();
        return new WritableFile()
        {
            boolean closed;

            {
                ob.put(this, this);
            }

            public void append(Slice data) throws IOException
            {
                writableFile.append(data);
            }

            public void force() throws IOException
            {
                writableFile.force();
            }

            public void close() throws IOException
            {
                if (!closed) {
                    counter.decrementAndGet();
                    closed = true;
                    ob.remove(this);
                }
                writableFile.close();
            }
        };
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
}
