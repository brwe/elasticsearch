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

package org.elasticsearch.search.reducers.bucket.correlation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InternalCorrelationReorder extends InternalBucketReducerAggregation implements CorrelationReorder {

    public static final Type TYPE = new Type("correlation");

    private List<Double> correlations;

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalCorrelationReorder readResult(StreamInput in) throws IOException {
            InternalCorrelationReorder correlatedCurves = new InternalCorrelationReorder();
            correlatedCurves.readFrom(in);
            return correlatedCurves;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new InternalSelection();
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            return context;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    public InternalCorrelationReorder() {
        super();
    }

    public InternalCorrelationReorder(String name, List<InternalSelection> selections, List<Double> correlations) {
        super(name, selections, null);
        this.correlations = correlations;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    public void doReadFrom(StreamInput in) throws IOException {

        correlations = new ArrayList<>();
        this.name = in.readString();
        int size = in.readVInt();
        selections = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalSelection selection = new InternalSelection();
            selection.readFrom(in);
            selections.add(selection);
            correlations.add(in.readDouble());

        }
        this.selectionMap = null;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(selections.size());
        int i = 0;
        for (Selection selection : selections) {
            selection.writeTo(out);
            out.writeDouble(correlations.get(i));
            i++;
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        int i = 0;
        builder.startArray("selections");
        for (Selection selection : selections) {
            builder.startObject();
            builder.field("correlation", correlations.get(i));
            builder.field("selection");
            selection.toXContent(builder, params);
            builder.endObject();
            i++;
        }
        builder.endArray();
        return builder;
    }

    public Double[] getCorrelations() {
        return correlations.toArray(new Double[correlations.size()]);
    }
}
