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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.reducers.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetricReducer extends Reducer {

    private final MetricOp op;
    protected String fieldName;
    protected String bucketsPath;
    public final static InternalAggregation.Type TYPE = new InternalAggregation.Type("simple_metric");

    @Override
    public InternalAggregation reduce(Aggregations aggregationsTree, Aggregation currentAggregation) {
        AggregationPath path = AggregationPath.parse(bucketsPath);
        InternalAggregation reducedAgg = traverseAggTree(path, aggregationsTree.get(path.getPathElements().get(0).name));
        List<InternalAggregation> reducedAggs = new ArrayList<>();
        reducedAggs.add(reducedAgg);
        return new SingleBucketMetricAggregation(name(), 0, new InternalAggregations(reducedAggs), null);

    }

    private InternalAggregation traverseAggTree(AggregationPath path, Aggregation aggregation) {
        if (path.getPathElements().size() == 1) {
            if (!(aggregation instanceof MultiBucketsAggregation)) {
                throw new ReductionExecutionException("aggregation must be a muti-bucket aggregation for reducer [" + name() + "]");
            }
            InternalAggregation reducedAgg = doReduce(aggregation);
            List<InternalAggregation> reducedAggs = new ArrayList<>();
            reducedAggs.add(reducedAgg);
            return new SingleBucketMetricAggregation(aggregation.getName(), 0, new InternalAggregations(reducedAggs), null);
        } else {
            if (aggregation instanceof SingleBucketAggregation) {
                Aggregation nestedAgg = ((SingleBucketMetricAggregation) aggregation).getAggregations().get(path.getPathElements().get(0).name);
                AggregationPath subPath = path.subPath(1, path.getPathElements().size() - 1);
                InternalAggregation reducedAgg = traverseAggTree(subPath, nestedAgg);
                List<InternalAggregation> reducedAggs = new ArrayList<>();
                reducedAggs.add(reducedAgg);
                return new SingleBucketMetricAggregation(aggregation.getName(), 0, new InternalAggregations(reducedAggs), null);
            } else if (aggregation instanceof MultiBucketsAggregation) {
                List<MultiBucketMetricAggregation.InternalBucket> buckets = new ArrayList<>();
                for (MultiBucketsAggregation.Bucket bucket : ((MultiBucketsAggregation)aggregation).getBuckets()) {
                   InternalAggregation bucketAgg = traverseAggTree(path.subPath(1, path.getPathElements().size() - 1), bucket.getAggregations().asMap().get(path.getPathElements().get(1).name));
                    List<InternalAggregation> reducedAggs = new ArrayList<>();
                    reducedAggs.add(bucketAgg);
                    MultiBucketMetricAggregation.InternalBucket metricBucket = new MultiBucketMetricAggregation.InternalBucket(bucket.getKeyAsText().string(), new InternalAggregations(reducedAggs));
                    buckets.add(metricBucket);
                }
                return new MultiBucketMetricAggregation(aggregation.getName(), buckets);
            } else {
                throw new ReductionExecutionException("wrong agg type");
            }
        }
    }

    public InternalAggregation doReduce(Aggregation aggregation) throws ReductionExecutionException {
        Object[] bucketProperties = getProperties(aggregation);
        return new InternalMetric(name(), op.evaluate(bucketProperties));

    }

    protected Object[] getProperties(Aggregation aggregation) {
        return (Object[]) aggregation.getProperty(fieldName);
    }

    public MetricReducer(String name, String bucketsPath, String fieldName, MetricOp op, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, factories, context, parent, null);
        this.bucketsPath = bucketsPath;
        this.fieldName = fieldName;
        this.op = op;
    }

    public static class Factory extends ReducerFactory {

        protected String bucketsPath;
        protected String fieldName;
        protected MetricOp operation;

        public Factory() {
            super(TYPE);
        }

        public Factory(String name, String bucketsPath, String fieldName, MetricOp operation) {
            super(name, TYPE);
            this.fieldName = fieldName;
            this.bucketsPath = bucketsPath;
            this.operation = operation;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            fieldName = in.readString();

            operation = OperationFactory.get(in.readString());
            operation.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeString(fieldName);
            operation.writeTo(out);
        }

        @Override
        public Reducer doCreate(ReducerContext context, Reducer parent, Map<String, Object> metaData) {
            return new MetricReducer(name, bucketsPath, fieldName, operation, factories, context, parent);
        }
    }

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, TYPE.stream());
    }

}
