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

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.search.internal.InternalSearchHits.StreamContext;

/**
 *
 */
public class MatrixScanResult extends TransportResponse {

    private long id;
    private SearchShardTarget shardTarget;
    private InternalSearchHits hits;
    // client side counter
    private transient int counter;

    List<Tuple<String, long[]>> postingLists = new ArrayList<>();

    public MatrixScanResult() {

    }

    public MatrixScanResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    public MatrixScanResult matrixScanResult() {
        return this;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return this.shardTarget;
    }

    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public void hits(InternalSearchHits hits) {
        this.hits = hits;
    }

    public InternalSearchHits hits() {
        return hits;
    }

    public MatrixScanResult initCounter() {
        counter = 0;
        return this;
    }

    public int counterGetAndIncrement() {
        return counter++;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
        long numberOfRows = in.readLong();

        postingLists = new ArrayList<>();
        for (int i = 0; i < numberOfRows; i++) {
            postingLists.add(new Tuple<>(in.readString(), in.readLongArray()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        if (postingLists != null) {
            out.writeLong(postingLists.size());
            for (Tuple<String, long[]> entry : postingLists) {
                out.writeString(entry.v1());
                out.writeLongArray(entry.v2());
            }
        } else {
            out.writeLong(0);
        }
    }

    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("not implemented", "yet");
    }

    public void addRow(String term, long[] list) {
        postingLists.add(new Tuple<>(term, list));
    }

    public List<Tuple<String, long[]>> getPostingLists() {
        return postingLists;
    }
}
