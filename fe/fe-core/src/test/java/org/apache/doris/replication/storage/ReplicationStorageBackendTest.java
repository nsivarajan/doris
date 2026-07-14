// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.replication.storage;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Provider-agnostic contract test for ReplicationStorageBackend.
 * Runs against LocalReplicationStorage (no network, fast).
 * The same tests can be run against real S3/OSS via integration test subclass.
 */
public class ReplicationStorageBackendTest {

    private LocalReplicationStorage storage;

    @Before
    public void setUp() {
        storage = new LocalReplicationStorage();
    }

    @Test
    public void testPutAndGet() throws Exception {
        byte[] data = "hello-replication".getBytes(StandardCharsets.UTF_8);
        storage.put("group1/fe-editlog/segment_0001.log", data);

        byte[] result = storage.get("group1/fe-editlog/segment_0001.log");
        Assert.assertArrayEquals(data, result);
    }

    @Test
    public void testPutIdempotent() throws Exception {
        // second write overwrites first — no error
        storage.put("key1", "v1".getBytes());
        storage.put("key1", "v2".getBytes());

        Assert.assertArrayEquals("v2".getBytes(), storage.get("key1"));
    }

    @Test
    public void testGetMissingKeyReturnsNull() throws Exception {
        Assert.assertNull(storage.get("does-not-exist"));
    }

    @Test
    public void testListReturnsSortedKeys() throws Exception {
        storage.put("group1/fe-editlog/segment_0003.log", new byte[1]);
        storage.put("group1/fe-editlog/segment_0001.log", new byte[1]);
        storage.put("group1/fe-editlog/segment_0002.log", new byte[1]);

        List<String> keys = storage.list("group1/fe-editlog/segment_");
        Assert.assertEquals(3, keys.size());
        Assert.assertEquals("group1/fe-editlog/segment_0001.log", keys.get(0));
        Assert.assertEquals("group1/fe-editlog/segment_0002.log", keys.get(1));
        Assert.assertEquals("group1/fe-editlog/segment_0003.log", keys.get(2));
    }

    @Test
    public void testListEmptyPrefixReturnsEmpty() throws Exception {
        List<String> keys = storage.list("no-match-prefix/");
        Assert.assertNotNull(keys);
        Assert.assertTrue(keys.isEmpty());
    }

    @Test
    public void testListDoesNotReturnKeysOutsidePrefix() throws Exception {
        storage.put("group1/fe-editlog/segment_0001.log", new byte[1]);
        storage.put("group1/checkpoint/latest.json", new byte[1]);

        List<String> keys = storage.list("group1/fe-editlog/");
        Assert.assertEquals(1, keys.size());
        Assert.assertEquals("group1/fe-editlog/segment_0001.log", keys.get(0));
    }

    @Test
    public void testExistsTrueAfterPut() throws Exception {
        Assert.assertFalse(storage.exists("mykey"));
        storage.put("mykey", new byte[1]);
        Assert.assertTrue(storage.exists("mykey"));
    }

    @Test
    public void testDeleteRemovesKey() throws Exception {
        storage.put("mykey", new byte[1]);
        Assert.assertTrue(storage.exists("mykey"));
        storage.delete("mykey");
        Assert.assertFalse(storage.exists("mykey"));
        Assert.assertNull(storage.get("mykey"));
    }

    @Test
    public void testDeleteMissingKeyIsNoOp() throws Exception {
        // should not throw
        storage.delete("never-existed");
    }

    @Test
    public void testLargePayloadRoundTrip() throws Exception {
        // 10 MB payload
        byte[] big = new byte[10 * 1024 * 1024];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i % 256);
        }
        storage.put("big-key", big);
        byte[] result = storage.get("big-key");
        Assert.assertArrayEquals(big, result);
    }

    @Test
    public void testPutReturnsIsolatedCopy() throws Exception {
        // modifying original array after put must not affect stored value
        byte[] data = "original".getBytes();
        storage.put("key", data);
        data[0] = 'X';
        Assert.assertEquals('o', storage.get("key")[0]);
    }

    @Test
    public void testInjectedFailureRetrySucceeds() throws Exception {
        storage.injectPutFailures(1);
        try {
            storage.put("key", new byte[1]);
            Assert.fail("Expected exception on injected failure");
        } catch (ReplicationStorageException e) {
            Assert.assertEquals(ReplicationStorageException.ErrorCode.NETWORK_ERROR, e.getErrorCode());
        }
        // next put succeeds
        storage.put("key", "ok".getBytes());
        Assert.assertArrayEquals("ok".getBytes(), storage.get("key"));
    }
}
