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

import org.iq80.leveldb.table.BytewiseComparator;
import org.iq80.leveldb.util.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AddBoundaryInputsTest
{
    private List<FileMetaData> levelFiles;
    private List<FileMetaData> compactionFiles;
    private InternalKeyComparator icmp;

    @BeforeMethod
    public void setUp()
    {
        levelFiles = new ArrayList<>();
        compactionFiles = new ArrayList<>();
        icmp = new InternalKeyComparator(new BytewiseComparator());
    }

    @Test
    public void testEmptyFileSets() throws Exception
    {
        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertTrue(compactionFiles.isEmpty());
        assertTrue(levelFiles.isEmpty());
    }

    @Test
    public void testEmptyLevelFiles() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 2),
                        internalKey("100", 1));
        compactionFiles.add(f1);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertEquals(compactionFiles.size(), 1);
        assertEquals(compactionFiles.get(0), f1);
        assertTrue(levelFiles.isEmpty());
    }

    @Test
    public void testEmptyCompactionFiles() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 2),
                        internalKey("100", 1));
        levelFiles.add(f1);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertTrue(compactionFiles.isEmpty());
        assertEquals(levelFiles.size(), 1);
        assertEquals(levelFiles.get(0), f1);
    }

    @Test
    public void testNoBoundaryFiles() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 2),
                        internalKey("100", 1));
        FileMetaData f2 =
                createFileMetaData(1, internalKey("200", 2),
                        internalKey("200", 1));
        FileMetaData f3 =
                createFileMetaData(1, internalKey("300", 2),
                        internalKey("300", 1));

        levelFiles.add(f3);
        levelFiles.add(f2);
        levelFiles.add(f1);
        compactionFiles.add(f2);
        compactionFiles.add(f3);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertEquals(compactionFiles.size(), 2);
    }

    @Test
    public void testOneBoundaryFiles() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 3),
                        internalKey("100", 2));
        FileMetaData f2 =
                createFileMetaData(1, internalKey("100", 1),
                        internalKey("200", 3));
        FileMetaData f3 =
                createFileMetaData(1, internalKey("300", 2),
                        internalKey("300", 1));

        levelFiles.add(f3);
        levelFiles.add(f2);
        levelFiles.add(f1);
        compactionFiles.add(f1);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertEquals(compactionFiles.size(), 2);
        assertEquals(compactionFiles.get(0), f1);
        assertEquals(compactionFiles.get(1), f2);
    }

    @Test
    public void testTwoBoundaryFiles() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 6),
                        internalKey("100", 5));
        FileMetaData f2 =
                createFileMetaData(1, internalKey("100", 2),
                        internalKey("300", 1));
        FileMetaData f3 =
                createFileMetaData(1, internalKey("100", 4),
                        internalKey("100", 3));

        levelFiles.add(f2);
        levelFiles.add(f3);
        levelFiles.add(f1);
        compactionFiles.add(f1);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertEquals(compactionFiles.size(), 3);
        assertEquals(compactionFiles.get(0), f1);
        assertEquals(compactionFiles.get(1), f3);
        assertEquals(compactionFiles.get(2), f2);
    }

    @Test
    public void testDisjoinFilePointers() throws Exception
    {
        FileMetaData f1 =
                createFileMetaData(1, internalKey("100", 6),
                        internalKey("100", 5));
        FileMetaData f2 =
                createFileMetaData(1, internalKey("100", 6),
                        internalKey("100", 5));
        FileMetaData f3 =
                createFileMetaData(1, internalKey("100", 2),
                        internalKey("300", 1));
        FileMetaData f4 =
                createFileMetaData(1, internalKey("100", 4),
                        internalKey("100", 3));

        levelFiles.add(f2);
        levelFiles.add(f3);
        levelFiles.add(f4);

        compactionFiles.add(f1);

        VersionSet.addBoundaryInputs(icmp, levelFiles, compactionFiles);
        assertEquals(compactionFiles.size(), 3);
        assertEquals(compactionFiles.get(0), f1);
        assertEquals(compactionFiles.get(1), f4);
        assertEquals(compactionFiles.get(2), f3);
    }

    private FileMetaData createFileMetaData(long number, InternalKey smallest,
                                            InternalKey largest)
    {
        return new FileMetaData(number, 0, smallest, largest);
    }

    private InternalKey internalKey(String s, int sequenceNumber)
    {
        return new InternalKey(new Slice(s.getBytes()), sequenceNumber, ValueType.VALUE);
    }
}
