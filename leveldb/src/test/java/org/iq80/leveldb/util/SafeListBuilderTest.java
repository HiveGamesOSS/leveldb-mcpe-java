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
package org.iq80.leveldb.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SafeListBuilderTest
{
    @Test
    public void testAllElementAreClosedEvenOnError() throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        SafeListBuilder<Closeable> builder = SafeListBuilder.builder();
        builder.add(counter::incrementAndGet);
        builder.add(() -> {
            counter.incrementAndGet();
            throw new IOException();
        });
        builder.add(counter::incrementAndGet);
        builder.add(() -> {
            counter.incrementAndGet();
            throw new IOException();
        });
        builder.add(counter::incrementAndGet);
        assertEquals(counter.get(), 0);
        try {
            builder.close();
            Assert.fail("should fail because not all close succeed");
        }
        catch (Exception e) {
            assertTrue(e instanceof IOException);
        }
        assertEquals(counter.get(), 5);
    }

    @Test
    public void testCloseWithoutExceptions() throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        SafeListBuilder<Closeable> builder = SafeListBuilder.builder();
        builder.add(counter::incrementAndGet);
        builder.add(counter::incrementAndGet);
        builder.close();
        assertEquals(counter.get(), 2);
    }

    @Test
    public void testNothingHappenIfBuildWasCalled() throws Exception
    {
        AtomicInteger counter = new AtomicInteger();
        try (SafeListBuilder<Closeable> builder = SafeListBuilder.builder()) {
            builder.add(counter::incrementAndGet);
            builder.add(counter::incrementAndGet);
            builder.add(counter::incrementAndGet);
            final List<Closeable> build = builder.build();
            assertEquals(3, build.size());
        }
        assertEquals(counter.get(), 0);
    }
}
