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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.reducers.*;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

public class CorrelationReorderReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalCorrelationReorder.TYPE.stream());
    }

    private List<String> curves;
    private String referenceAgg;
    private int numCorrelatingCurves;

    public CorrelationReorderReducer(String name, String referenceAgg, List<String> curves, int numCorrelatingCurves, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, topLevelAggregationName(referenceAgg, curves), factories, context, parent);
        this.referenceAgg = referenceAgg;
        this.curves = new ArrayList<>();
        // remove the common prefix from all the paths
        this.referenceAgg = referenceAgg.substring(bucketsPath().length() + 1);
        for (String curve : curves) {
            this.curves.add(curve.substring(bucketsPath().length() + 1));
        }
        this.numCorrelatingCurves = numCorrelatingCurves;
    }

    private static String topLevelAggregationName(String referenceAgg, List<String> curves) {
        String aggPath = null;
        for (String curveName : curves) {
            aggPath = findMinPrefix(referenceAgg, curveName);
        }
        return aggPath;
    }

    public static String findMinPrefix(String aggPath, String curveName) {
        String prefix = "";
        int i = 0;
        String[] test = aggPath.split("\\.");
        List<String> aggPathElements = Arrays.asList(aggPath.split("\\."));
        List<String> curvePathElements = Arrays.asList(curveName.split("\\."));
        while (aggPathElements.get(i).equals(curvePathElements.get(i))) {
            prefix += aggPathElements.get(i) + ".";
            i++;
        }
        return prefix.substring(0, prefix.length() - 1);
    }

    public InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) {
        List<InternalSelection> selections = new ArrayList<>();

        List<BucketsValuesTuple> referenceBuckets = gatherBucketLists(referenceAgg, aggregation);
        List<BucketsValuesTuple> curveBuckets = gatherBucketLists(curves, aggregation);
        PriorityQueue<SortableInternalSelection> queue = new PriorityQueue<>();
        for (BucketsValuesTuple tuple : curveBuckets) {
            InternalSelection selection = new InternalSelection(tuple.key, tuple.bucketType, bucketStreamContext, tuple.bucketList, InternalAggregations.EMPTY);
            InternalAggregations subReducersResults = runSubReducers(selection);
            selection.setAggregations(subReducersResults);
            queue.offer(new SortableInternalSelection(selection, NormalizedCorrelation.normalizedCorrelation(tuple.values, referenceBuckets.get(0).values)));
        }
        List<Double> correlations = new ArrayList<>();
        for (int i = 0; i < numCorrelatingCurves; i++) {
            SortableInternalSelection selection = queue.poll();
            if (selection == null) {
                break;
            }
            selections.add(selection.selection);
            correlations.add(selection.sortValue);
        }
        return new InternalCorrelationReorder(name(), selections, correlations);
    }

    private List<BucketsValuesTuple> gatherBucketLists(String referenceAgg, MultiBucketsAggregation aggregation) {
        List<BucketsValuesTuple> bucketsValues = new ArrayList<>();
        List<String> pathElements = Arrays.asList(referenceAgg.split("\\."));
        Object properties = aggregation.getProperty(pathElements);
        addValuesAndBuckets(properties, aggregation, pathElements, bucketsValues, pathElements.get(0));
        return bucketsValues;
    }

    private List<BucketsValuesTuple> gatherBucketLists(List<String> curves, MultiBucketsAggregation aggregation) {
        List<BucketsValuesTuple> bucketsValues = new ArrayList<>();
        for (String curve : curves) {
            List<String> pathElements = Arrays.asList(curve.split("\\."));
            Object properties = aggregation.getProperty(pathElements);
            addValuesAndBuckets(properties, aggregation, pathElements, bucketsValues, pathElements.get(0));
        }
        return bucketsValues;
    }

    private void addValuesAndBuckets(Object properties, MultiBucketsAggregation aggregation, List<String> curve, List<BucketsValuesTuple> bucketsValues, String label) {
        //path length is the dimension of the agg so we stop if we reach the bottom
        if (curve.size() > 1) {
            Object[] propertiesArray = (Object[]) properties;
            int bucketCounter = 0;
            List<String> innerCurves = curve.subList(1, curve.size());
            for (Bucket bucket : aggregation.getBuckets()) {
                addValuesAndBuckets(propertiesArray[bucketCounter], (MultiBucketsAggregation) bucket.getAggregations().asMap().get(curve.get(0)), innerCurves, bucketsValues, label + "." + bucket.getKey());
                bucketCounter++;
            }
        } else {
            bucketsValues.add(new BucketsValuesTuple((Object[]) properties, aggregation.getBuckets(), label, ((InternalAggregation) aggregation).type().stream()));
        }
    }

    public static class Factory extends ReducerFactory {

        private String referenceAgg;
        private List<String> curves;
        private int numCorrelatingCurves;

        public Factory() {
            super(InternalCorrelationReorder.TYPE);
        }

        public Factory(String name, String referenceAgg, List<String> curves, int numCorrelatingCurves) {
            super(name, InternalCorrelationReorder.TYPE);
            this.referenceAgg = referenceAgg;
            this.curves = curves;
            this.numCorrelatingCurves = numCorrelatingCurves;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new CorrelationReorderReducer(name, referenceAgg, curves, numCorrelatingCurves, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            referenceAgg = in.readString();
            curves = Arrays.asList(in.readStringArray());
            numCorrelatingCurves = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(referenceAgg);
            out.writeStringArray(curves.toArray(new String[curves.size()]));
            out.writeInt(numCorrelatingCurves);
        }

    }

    private class BucketsValuesTuple {
        List<Bucket> bucketList;
        Object[] values;
        String key;
        BytesReference bucketType;

        public BucketsValuesTuple(Object[] properties, List<? extends Bucket> buckets, String label, BytesReference bucketType) {
            this.values = properties;
            this.bucketList = (List<Bucket>) buckets;
            this.key = label;
            this.bucketType = bucketType;
        }
    }

    private class SortableInternalSelection implements Comparable {
        InternalSelection selection;
        double sortValue;

        public SortableInternalSelection(InternalSelection selection, double sortValue) {
            this.selection = selection;
            this.sortValue = sortValue;
        }

        @Override
        public int compareTo(Object o) {
            SortableInternalSelection other = (SortableInternalSelection) o;
            if (other.sortValue > this.sortValue) {
                return 1;
            } else if (other.sortValue < this.sortValue) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
