/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.unit.transport.netty;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.test.integration.ElasticsearchTestCase;
import org.elasticsearch.transport.netty.KeyedLock;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class KeyedLockTests extends ElasticsearchTestCase {

    KeyedLock<String> connectionLock = new KeyedLock<String>();
    String[] names;

    @Test
    public void checkIfMapEmptyAfterLotsOfAcquireAndReleases() throws InterruptedException {

        names = new String[randomIntBetween(10, 20)];
        for (int i = 0; i < names.length; i++) {
            names[i] = randomRealisticUnicodeOfLengthBetween(10, 20);
        }
        CountDownLatch startLatch = new CountDownLatch(1);
        int numThreads = randomIntBetween(10, 50);
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new AcquireAndReleaseThread(startLatch);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        startLatch.countDown();
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
        assertThat(connectionLock.hasLockedKeys(), equalTo(false));

        Set<Entry<String, Integer>> entrySet = counters.entrySet();
        assertThat(counters.size(), equalTo(safeCounter.size()));
        for (Entry<String, Integer> entry : entrySet) {
            AtomicInteger atomicInteger = safeCounter.get(entry.getKey());
            assertThat(atomicInteger, not(Matchers.nullValue()));
            assertThat(atomicInteger.get(), equalTo(entry.getValue()));
        }
    }

    @Test(expected = ElasticSearchIllegalStateException.class)
    public void checkCannotAcquireTwoLocks() throws InterruptedException {
        String name = randomRealisticUnicodeOfLength(atLeast(10));
        connectionLock.acquire(name);
        connectionLock.acquire(name);
    }

    @Test(expected = ElasticSearchIllegalStateException.class)
    public void checkCannotReleaseUnacquiredLock() throws InterruptedException {
        String name = randomRealisticUnicodeOfLength(atLeast(10));
        connectionLock.release(name);
    }

    ConcurrentHashMap<String, Integer> counters = new ConcurrentHashMap<String, Integer>();
    ConcurrentHashMap<String, AtomicInteger> safeCounter = new ConcurrentHashMap<String, AtomicInteger>();

    public class AcquireAndReleaseThread extends Thread {
        private CountDownLatch startLatch;

        public AcquireAndReleaseThread(CountDownLatch startLatch) {
            this.startLatch = startLatch;
        }

        public void run() {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException();
            }
            int numRuns = atLeast(500);
            for (int i = 0; i < numRuns; i++) {
                String curName = names[randomInt(names.length - 1)];
                connectionLock.acquire(curName);
                try {
                    Integer integer = counters.get(curName);
                    if (integer == null) {
                        counters.put(curName, 1);
                    } else {
                        counters.put(curName, integer.intValue() + 1);
                    }
                } finally {
                    connectionLock.release(curName);
                }
                AtomicInteger atomicInteger = new AtomicInteger(0);
                AtomicInteger value = safeCounter.putIfAbsent(curName, atomicInteger);
                if (value == null) {
                    atomicInteger.incrementAndGet();
                } else {
                    value.incrementAndGet();
                }
            }
        }
    }
}
