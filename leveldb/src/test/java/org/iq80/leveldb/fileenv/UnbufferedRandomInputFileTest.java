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
package org.iq80.leveldb.fileenv;

import org.iq80.leveldb.env.RandomInputFile;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class UnbufferedRandomInputFileTest
{
    @Test
    public void testResilientToThreadInterruptOnReaderThread() throws IOException
    {
        File file = File.createTempFile("table", ".db");
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(new byte[1024]);
        }
        try (final RandomInputFile open = UnbufferedRandomInputFile.open(file)) {
            //mark current thread as interrupted
            Thread.currentThread().interrupt();
            try {
                open.read(200, 200);
                Assert.fail("Should have failed with ClosedByInterruptException");
            }
            catch (ClosedByInterruptException e) {
                //reader that was interrupted is expected fail at this point
                //no other threads
            }
            //clear current thread interrupt
            Thread.interrupted();

            //verify file is still accessible after previous failure
            final ByteBuffer read = open.read(200, 200);
            assertEquals(read.remaining(), 200);
        }
        finally {
            file.delete();
        }
    }

    @Test
    public void testResilientToThreadInterruptOnReaderThreadMultiThread() throws Exception
    {
        File file = File.createTempFile("table", ".db");
        final byte[] bytes = new byte[1024];
        new Random().nextBytes(bytes);
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(bytes);
        }
        try (final RandomInputFile open = UnbufferedRandomInputFile.open(file)) {
            final Reader[] readers = new Reader[Runtime.getRuntime().availableProcessors()];
            for (int i = 0; i < readers.length; i++) {
                readers[i] = new Reader(open, bytes);
                readers[i].start();
            }
            int interrups = 0;
            long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (interrups < 300 && System.nanoTime() < timeout) {
                interrups = 0;
                for (Reader reader : readers) {
                    reader.fireInterrupt();
                    Thread.sleep(5);
                    interrups += reader.getCount();
                }
            }
            for (Reader reader : readers) {
                reader.stop = true;
            }
            for (Reader reader : readers) {
                reader.join();
                assertFalse(reader.failed);
            }
        }
        finally {
            file.delete();
        }
    }

    private static class Reader extends Thread
    {
        private final RandomInputFile reader;
        private final byte[] content;
        private final byte[] result;
        public volatile boolean stop = false;
        private boolean wasInterrupted = false;
        private boolean failed = false;
        private int interruptCount = 0;

        private Object lock = false;

        public Reader(RandomInputFile reader, byte[] content)
        {
            this.reader = reader;
            this.content = content;
            this.result = new byte[content.length];
        }

        public void fireInterrupt()
        {
            synchronized (lock) {
                if (!wasInterrupted) {
                    wasInterrupted = true;
                    this.interrupt();
                }
            }
        }

        public boolean exceptionNoticed()
        {
            synchronized (lock) {
                if (!wasInterrupted || !Thread.interrupted()) {
                    failed = true;
                }
                else {
                    interruptCount++;
                    wasInterrupted = false;
                }
            }
            return failed;
        }

        public int getCount()
        {
            synchronized (lock) {
                return interruptCount;
            }
        }

        @Override
        public void run()
        {
            final Random random = new Random(this.getId());
            while (!stop && !failed) {
                if (random.nextInt(100) > 90) {
                    Thread.yield();
                }
                try {
                    final ByteBuffer read = reader.read(0, content.length);
                    read.get(this.result);
                    assertEquals(this.result, this.content);
                    assertEquals(read.remaining(), 0);
                }
                catch (Exception e) {
                    if (exceptionNoticed()) {
                        return;
                    }
                }
            }
        }
    }
}
