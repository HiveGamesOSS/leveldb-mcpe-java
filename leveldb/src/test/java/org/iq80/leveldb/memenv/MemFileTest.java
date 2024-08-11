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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;

public class MemFileTest
{
    @Test
    public void testEquals()
    {
        MemFs fs = new MemFs();
        MemFile ab = MemFile.createMemFile(fs, "/a/b");
        MemFile abParent = ab.getParentFile();
        MemFile a = MemFile.createMemFile(new MemFs(), "/a");
        MemFile ab1 = MemFile.createMemFile(new MemFs(), "/a").child("b");
        MemFile ab1Parent = ab1.getParentFile();

        assertEqualsFile(ab, ab1);
        assertEqualsFile(abParent, ab1Parent);
        assertEqualsFile(abParent, a);
        assertEqualsFile(ab, MemFile.createMemFile(new MemFs(), "/a/b/"));
        assertEqualsFile(ab, MemFile.createMemFile(new MemFs(), "a/b/"));

        assertNotEquals(ab, a);
        assertNotEquals(ab, MemFile.createMemFile(fs, "/a/e"));
        assertNotEquals(ab, MemFile.createMemFile(fs, "/"));
        assertNotEquals(ab.hashCode(), a.hashCode());
        assertNotEquals(ab1.hashCode(), a.hashCode());
        assertNotEquals(ab, a);
    }

    @Test
    public void testParent()
    {
        MemFs fs = new MemFs();
        MemFile ab = MemFile.createMemFile(fs, "/a/b/c");
        MemFile p = ab.getParentFile();
        assertEqualsFile(p, MemFile.createMemFile(fs, "/a/b"));
        p = p.getParentFile();
        assertEqualsFile(p, MemFile.createMemFile(fs, "/a"));
        p = p.getParentFile();
        assertEqualsFile(p, MemFile.createMemFile(fs, "/"));
        p = p.getParentFile();
        assertEqualsFile(p, MemFile.createMemFile(fs, "/"));
    }

    @Test
    public void testDefault()
    {
        MemFile ab = MemFile.createMemFile(new MemFs(), "/a/b");
        assertEquals(ab.getName(), "b");
        assertEquals(ab.getPath(), "/a/b");
        assertFalse(ab.isDirectory());
        assertFalse(ab.isFile());
        assertFalse(ab.exists());
        assertEquals(ab.listFiles(), Collections.emptyList());
        Assert.assertThrows(() -> ab.child("/invalid"));
    }

    private static void assertEqualsFile(MemFile f1, MemFile f2)
    {
        assertEquals(f1, f2);
        assertEquals(f1.getName(), f2.getName());
        assertEquals(f1.getPath(), f2.getPath());
        assertEquals(f1.isFile(), f2.isFile());
        assertEquals(f1.isDirectory(), f2.isDirectory());
        assertEquals(f1.exists(), f2.exists());
        assertEquals(f1.getParentFile(), f2.getParentFile());
        assertEquals(f1.hashCode(), f2.hashCode());
    }
}
