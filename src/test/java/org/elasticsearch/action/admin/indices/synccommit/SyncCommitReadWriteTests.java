/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.action.admin.indices.synccommit;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.synccommit.WriteSyncCommitRequest;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class SyncCommitReadWriteTests extends ElasticsearchTestCase {

    @Test
    public void streamWriteSyncResponse() throws InterruptedException, IOException {
        ShardId shardId = new ShardId("test", 0);
        ShardRouting shardRouting = new ImmutableShardRouting("test", 0, "test_node",
                "other_test_node", randomBoolean(), ShardRoutingState.STARTED, randomInt());
        Map<ShardRouting, byte[]> commitIds = new HashMap<>();
        commitIds.put(shardRouting, randomRealisticUnicodeOfLength(10).getBytes());
        WriteSyncCommitRequest writeSyncCommitRequest = new WriteSyncCommitRequest(shardId, randomAsciiOfLength(5), commitIds);
        BytesStreamOutput out = new BytesStreamOutput();
        writeSyncCommitRequest.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        WriteSyncCommitRequest request = new WriteSyncCommitRequest();
        request.readFrom(in);
        assertArrayEquals(request.commitIds().get(shardRouting), writeSyncCommitRequest.commitIds().get(shardRouting));
    }

    @Test
    public void streamSyncResponse() throws InterruptedException, IOException {
        ShardRouting shardRouting = new ImmutableShardRouting("test", 0, "test_node",
                "other_test_node", randomBoolean(), ShardRoutingState.STARTED, randomInt());
        AtomicReferenceArray atomicReferenceArray = new AtomicReferenceArray(1);
        atomicReferenceArray.set(0, new ShardSyncCommitResponse(randomRealisticUnicodeOfLength(10).getBytes(), shardRouting));
        SyncCommitResponse syncCommitResponse = new SyncCommitResponse(randomInt(), randomInt(), randomInt(), new ArrayList<ShardOperationFailedException>(), atomicReferenceArray);
        BytesStreamOutput out = new BytesStreamOutput();
        syncCommitResponse.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        SyncCommitResponse request = new SyncCommitResponse();
        request.readFrom(in);
        assertArrayEquals(request.commitIds().get(shardRouting), syncCommitResponse.commitIds().get(shardRouting));
    }

    @Test
    public void streamShardSyncResponse() throws InterruptedException, IOException {
        ShardRouting shardRouting = new ImmutableShardRouting("test", 0, "test_node",
                "other_test_node", randomBoolean(), ShardRoutingState.STARTED, randomInt());
        ShardSyncCommitResponse shardSyncCommitResponse = new ShardSyncCommitResponse(randomRealisticUnicodeOfLength(10).getBytes(), shardRouting);
        BytesStreamOutput out = new BytesStreamOutput();
        shardSyncCommitResponse.writeTo(out);
        out.close();
        StreamInput in = new BytesStreamInput(out.bytes());
        ShardSyncCommitResponse request = new ShardSyncCommitResponse();
        request.readFrom(in);
        assertArrayEquals(request.id(), shardSyncCommitResponse.id());
    }
}
