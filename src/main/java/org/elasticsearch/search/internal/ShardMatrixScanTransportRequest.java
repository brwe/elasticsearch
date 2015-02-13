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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Shard level search request that represents an actual search sent from the coordinating node to the nodes holding
 * the shards where the query needs to be executed. Holds the same info as {@link ShardSearchLocalRequest}
 * but gets sent over the transport and holds also the indices coming from the original request that generated it, plus its headers and context.
 */
public class ShardMatrixScanTransportRequest extends ShardSearchTransportRequest {

    public ShardMatrixScanTransportRequest() {

    }

    public String[] getDictionary() {
        return dictionary;
    }

    private String[] dictionary;

    public ShardMatrixScanTransportRequest(SearchRequest searchRequest, ShardRouting shardRouting, int numberOfShards,
                                           boolean useSlowScroll, String[] filteringAliases, long nowInMillis, String[] dictionary) {
        super(searchRequest, shardRouting, numberOfShards, useSlowScroll, filteringAliases, nowInMillis);
        this.dictionary=dictionary;
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        dictionary= in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(dictionary);
    }
}
