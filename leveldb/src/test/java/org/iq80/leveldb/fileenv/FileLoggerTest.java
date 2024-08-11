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

import org.iq80.leveldb.Logger;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class FileLoggerTest
{
    @Test
    public void testFormatting() throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        LocalDateTime start = LocalDateTime.now();
        Logger fileLogger = FileLogger.createLogger(outputStream, new LocalDateTimeSupplier(start));
        fileLogger.log("a bc ");
        fileLogger.log("without place", "arg1", "arg2");
        fileLogger.log("- %s -", "abc");
        fileLogger.close();
        LocalDateTimeSupplier d = new LocalDateTimeSupplier(start);
        StringBuilder s = new StringBuilder();
        s.append(d.get()).append(' ').append("a bc ").append(System.lineSeparator());
        s.append(d.get()).append(' ').append("without place [arg1, arg2]").append(System.lineSeparator());
        s.append(d.get()).append(' ').append("- abc -").append(System.lineSeparator());

        assertEquals(new String(outputStream.toByteArray()), s.toString());
    }

    private static class LocalDateTimeSupplier implements Supplier<LocalDateTime>
    {
        LocalDateTime now;

        public LocalDateTimeSupplier(LocalDateTime start)
        {
            now = start;
        }

        @Override
        public LocalDateTime get()
        {
            now = now.plus(1, ChronoUnit.SECONDS);
            return now;
        }
    }
}
