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

package org.elasticsearch.action.synccommit;

import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class WriteSyncCommitRequest extends ShardReplicationOperationRequest<WriteSyncCommitRequest> {

    private String syncId;
    private Map<ShardRouting, byte[]> commitIds;
    private ShardId shardId;

    public WriteSyncCommitRequest() {
    }

    public WriteSyncCommitRequest(ShardId shardId, String syncId, Map<ShardRouting, byte[]> commitIds) {
        this.commitIds = commitIds;
        this.shardId = shardId;
        this.syncId = syncId;
        this.index(shardId.index().getName());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        commitIds = new HashMap<>();
        int numCommitIds = in.readInt();
        for (int i = 0; i < numCommitIds; i++) {
            ShardRouting shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);
            byte[] id = in.readByteArray();
            commitIds.put(shardRouting, id);
        }
        syncId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeInt(commitIds.size());
        for (Map.Entry<ShardRouting, byte[]> entry : commitIds.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeByteArray(entry.getValue());
        }
        out.writeString(syncId);
    }

    @Override
    public String toString() {
        return "write sync commit {" + shardId + "}";
    }

    public ShardId shardId() {
        return shardId;
    }

    public String syncId() {
        return syncId;
    }

    public Map<ShardRouting, byte[]> commitIds() {
        return commitIds;
    }
}
