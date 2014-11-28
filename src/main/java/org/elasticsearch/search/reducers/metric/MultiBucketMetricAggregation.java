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

package org.elasticsearch.search.reducers.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiBucketMetricAggregation extends InternalMultiBucketAggregation {

    public static final Type TYPE = new Type("multi_buckets_metric");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public MultiBucketMetricAggregation readResult(StreamInput in) throws IOException {
            MultiBucketMetricAggregation buckets = new MultiBucketMetricAggregation();
            buckets.readFrom(in);
            return buckets;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket buckets = new InternalBucket();
            buckets.readFrom(in);
            return buckets;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            return context;
        }
    };

    List<InternalBucket> buckets = new ArrayList<>();
    public MultiBucketMetricAggregation() {

    }
    public MultiBucketMetricAggregation(String name, List<InternalBucket> buckets) {
        this.name = name;
        this.buckets = buckets;
    }

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, TYPE.stream());
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        return null;
    }


    @Override
    protected void doReadFrom(StreamInput in) throws IOException {

        int size = in.readVInt();
        List<InternalBucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalBucket bucket = new InternalBucket();
            bucket.readFrom(in);
            buckets.add(bucket);
        }
        this.buckets = buckets;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {

        builder.startArray(CommonFields.BUCKETS);
        for (InternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public List<? extends Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public <B extends Bucket> B getBucketByKey(String key) {
        return null;
    }

    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket {
        InternalAggregations aggregations = null;

        String key;

        public InternalBucket() {
        }

        public InternalBucket(String key, InternalAggregations aggregations) {
            this.key = key;
            this.aggregations = aggregations;
        }

        @Override
        public String getKey() {
            return null;
        }

        @Override
        public Text getKeyAsText() {
            return null;
        }

        @Override
        public long getDocCount() {
            return 0;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            key = in.readString();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

}
