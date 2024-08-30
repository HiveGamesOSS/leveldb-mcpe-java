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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class FileLockTest
{
    @Test
    public void testNoLockAfterUnlock() throws IOException
    {
        File databaseDir = FileUtils.createTempDir("leveldb");
        File lock1 = new File(databaseDir, "LOCK");
        FileLock lock = FileLock.tryLock(lock1);
        lock.release();
        assertFalse(lock1.exists());
        assertTrue(databaseDir.delete());
        assertFalse(databaseDir.exists());
    }

    @Test
    public void testCantDoubleLock() throws IOException
    {
        File databaseDir = FileUtils.createTempDir("leveldb");
        File lock1 = new File(databaseDir, "LOCK");
        FileLock lock = FileLock.tryLock(lock1);
        try {
            FileLock.tryLock(new File(databaseDir, "LOCK"));
            Assert.fail("No expected to aquire more than once the lock");
        }
        catch (Exception e) {
            //expected
        }
        lock.release();
    }

    @Test
    public void testNoLockAfterLockFailure() throws IOException
    {
        File databaseDir = FileUtils.createTempDir("leveldb");
        File lock1 = new File(databaseDir, "LOCK");
        FileLock lock = FileLock.tryLock(lock1);
        try {
            FileLock.tryLock(new File(databaseDir, "LOCK"));
            Assert.fail("Can lock a already locked DB");
        }
        catch (Exception e) {
            //expected
        }
        lock.release();
        assertFalse(lock1.exists());
        assertTrue(databaseDir.delete());
        assertFalse(databaseDir.exists());
    }
}
