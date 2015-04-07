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

package org.elasticsearch.action.seal;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 */
public class SealShardRequest extends ShardReplicationOperationRequest<SealShardRequest> {

    ShardId shardId;

    public SealShardRequest() {
    }

    public SealShardRequest(ShardId shardId) {
        this.shardId = shardId;
        this.index(shardId.index().getName());
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        return validationException;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = ShardId.readShardId(in);
        this.index(shardId.index().getName());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        this.index(shardId.index().getName());
    }

    @Override
    public String toString() {
        return "index {[" + index + "]]}";
    }

    public ShardId shardId() {
        return shardId;
    }
}
