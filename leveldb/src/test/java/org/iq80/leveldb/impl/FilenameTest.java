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

import com.google.common.collect.Lists;
import org.iq80.leveldb.fileenv.EnvImpl;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class FilenameTest
{
    @Test
    public void testFileNameTest()
    {
        assertFileInfo("100.log", 100L, Filename.FileType.LOG);
        assertFileInfo("0.log", 0L, Filename.FileType.LOG);
        assertFileInfo("0.sst", 0L, Filename.FileType.TABLE);
        assertFileInfo("0.ldb", 0L, Filename.FileType.TABLE);
        assertFileInfo("CURRENT", 0L, Filename.FileType.CURRENT);
        assertFileInfo("LOCK", 0L, Filename.FileType.DB_LOCK);
        assertFileInfo("MANIFEST-2", 2L, Filename.FileType.DESCRIPTOR);
        assertFileInfo("MANIFEST-7", 7L, Filename.FileType.DESCRIPTOR);
        assertFileInfo("LOG", 0L, Filename.FileType.INFO_LOG);
        assertFileInfo("LOG.old", 0L, Filename.FileType.INFO_LOG);
        assertFileInfo("18446744073709551615.log", -1L, Filename.FileType.LOG);
        assertFileInfo("099876.ldb", 99876L, Filename.FileType.TABLE);
    }

    @Test
    public void testShouldNotParse()
    {
        List<String> errors = Lists.newArrayList("",
                "foo",
                "foo-dx-100.log",
                ".log",
                "",
                "manifest",
                "CURREN",
                "CURRENTX",
                "MANIFES",
                "MANIFEST",
                "MANIFEST-",
                "XMANIFEST-3",
                "MANIFEST-3x",
                "LOC",
                "LOCKx",
                "LO",
                "LOGx",
                "18446744073709551616.log",
                "184467440737095516150.log",
                "100",
                "100.",
                "100.lop");
        for (String error : errors) {
            assertNull(Filename.parseFileName(EnvImpl.createEnv().toFile(error)));
        }
    }

    @Test
    public void testGeneratedFileNameAreAsExpected()
    {
        assertEquals(Filename.tableFileName(-1L), "18446744073709551615.ldb");

        assertFileInfo(Filename.currentFileName(), 0, Filename.FileType.CURRENT);
        assertFileInfo(Filename.lockFileName(), 0, Filename.FileType.DB_LOCK);
        assertFileInfo(Filename.logFileName(192), 192, Filename.FileType.LOG);
        assertFileInfo(Filename.tableFileName(200), 200, Filename.FileType.TABLE);
        assertFileInfo(Filename.tableFileName(-1L), -1L, Filename.FileType.TABLE);
        assertFileInfo(Filename.descriptorFileName(100), 100, Filename.FileType.DESCRIPTOR);
        assertFileInfo(Filename.tempFileName(999), 999, Filename.FileType.TEMP);
        assertFileInfo(Filename.infoLogFileName(), 0, Filename.FileType.INFO_LOG);
        assertFileInfo(Filename.oldInfoLogFileName(), 0, Filename.FileType.INFO_LOG);
        assertFileInfo(Filename.sstTableFileName(344), 344, Filename.FileType.TABLE);
    }

    private void assertFileInfo(String file, long expectedNumber, Filename.FileType expectedType)
    {
        Filename.FileInfo fileInfo = Filename.parseFileName(EnvImpl.createEnv().toFile(file));
        assertNotNull(fileInfo);
        assertEquals(fileInfo.getFileNumber(), expectedNumber);
        assertEquals(fileInfo.getFileType(), expectedType);
    }
}
