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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
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

    private List<CorrelationReorderParser.CurveXY> curves;
    private CorrelationReorderParser.CurveXY referenceAgg;
    private int numCorrelatingCurves;

    public CorrelationReorderReducer(String name, CorrelationReorderParser.CurveXY referenceAgg, List<CorrelationReorderParser.CurveXY> curves, int numCorrelatingCurves, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, topLevelAggregationName(referenceAgg, curves), factories, context, parent);
        this.referenceAgg = referenceAgg;
        this.curves = new ArrayList<>();
        // remove the common prefix from all the paths
        this.referenceAgg = new CorrelationReorderParser.CurveXY(referenceAgg.getXPath().substring(bucketsPath().length() + 1), referenceAgg.getYPath().substring(bucketsPath().length() + 1));
        for (CorrelationReorderParser.CurveXY curve : curves) {
            this.curves.add(new CorrelationReorderParser.CurveXY(curve.getXPath().substring(bucketsPath().length() + 1), curve.getYPath().substring(bucketsPath().length() + 1)));
        }
        this.numCorrelatingCurves = numCorrelatingCurves;
    }

    private static String topLevelAggregationName(CorrelationReorderParser.CurveXY referenceAgg, List<CorrelationReorderParser.CurveXY> curves) {
        String aggPath = findMinPrefix(referenceAgg.getXPath(), referenceAgg.getYPath());
        for (CorrelationReorderParser.CurveXY curveName : curves) {
            aggPath = findMinPrefix(aggPath, curveName.getXPath());
            aggPath = findMinPrefix(aggPath, curveName.getYPath());
        }
        return aggPath;
    }

    public static String findMinPrefix(String aggPath, String curveName) {
        String prefix = "";
        int i = 0;
        List<String> aggPathElements = Arrays.asList(aggPath.split("\\."));
        List<String> curvePathElements = Arrays.asList(curveName.split("\\."));
        while ((i < aggPathElements.size() && i < curvePathElements.size())
                && aggPathElements.get(i).equals(curvePathElements.get(i))) {
            prefix += aggPathElements.get(i) + ".";
            i++;
        }
        return prefix.substring(0, prefix.length() - 1);
    }

    public InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) {
        List<InternalSelection> selections = new ArrayList<>();

        List<BucketsAndValues> referenceBuckets = gatherBucketLists(referenceAgg, aggregation);
        List<BucketsAndValues> curveBuckets = gatherBucketLists(curves, aggregation);
        PriorityQueue<SortableInternalSelection> queue = new PriorityQueue<>();
        for (BucketsAndValues tuple : curveBuckets) {
            InternalSelection selection = new InternalSelection(tuple.key, tuple.bucketType, bucketStreamContext, tuple.bucketList, InternalAggregations.EMPTY);
            InternalAggregations subReducersResults = runSubReducers(selection);
            selection.setAggregations(subReducersResults);
            queue.offer(new SortableInternalSelection(selection, NormalizedCorrelation.normalizedCorrelation(tuple.ys, referenceBuckets.get(0).ys)));
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

    private List<BucketsAndValues> gatherBucketLists(CorrelationReorderParser.CurveXY curve, MultiBucketsAggregation aggregation) {
        List<BucketsAndValues> bucketsValues = new ArrayList<>();
        addXYCurve(aggregation, bucketsValues, curve);
        return bucketsValues;
    }

    private List<BucketsAndValues> gatherBucketLists(List<CorrelationReorderParser.CurveXY> curves, MultiBucketsAggregation aggregation) {
        List<BucketsAndValues> bucketsValues = new ArrayList<>();
        for (CorrelationReorderParser.CurveXY curve : curves) {
            addXYCurve(aggregation, bucketsValues, curve);
        }
        return bucketsValues;
    }

    private void addXYCurve(MultiBucketsAggregation aggregation, List<BucketsAndValues> bucketsValues, CorrelationReorderParser.CurveXY curve) {
        List<String> yPath = Arrays.asList(curve.getYPath().split("\\."));
        Object xs = aggregation.getProperty(curve.getXPath().replace('.', '>'));
        Object ys = aggregation.getProperty(curve.getYPath().replace('.', '>'));
        addValuesAndBuckets(ys, xs, aggregation, yPath, bucketsValues, yPath.get(0));
    }

    private void addValuesAndBuckets(Object ys, Object xs, Aggregation aggregation, List<String> curve, List<BucketsAndValues> bucketsValues, String label) {
        //path length is the dimension of the agg so we stop if we reach the bottom
        Object[] xArray = (Object[]) xs;
        Object[] yArray = (Object[]) ys;
        if (curve.size() > 2) {

            int bucketCounter = 0;
            List<String> innerCurves = curve.subList(1, curve.size());
            if (aggregation instanceof MultiBucketsAggregation) {
                for (Bucket bucket : ((MultiBucketsAggregation) aggregation).getBuckets()) {
                    addValuesAndBuckets(yArray[bucketCounter],xArray[bucketCounter], bucket.getAggregations().asMap().get(curve.get(0)), innerCurves, bucketsValues, label + "." + bucket.getKey());
                    bucketCounter++;
                }
            } else {
                addValuesAndBuckets(yArray,xArray, ((SingleBucketAggregation)aggregation).getAggregations().asMap().get(curve.get(0)), innerCurves, bucketsValues, label + "." + aggregation.getName());
            }

        } else {
            bucketsValues.add(new BucketsAndValues(yArray,xArray, ((MultiBucketsAggregation)aggregation).getBuckets(), label, ((InternalAggregation) aggregation).type().stream()));
        }
    }

    public static class Factory extends ReducerFactory {

        private CorrelationReorderParser.CurveXY referenceAgg;
        private List<CorrelationReorderParser.CurveXY> curves;
        private int numCorrelatingCurves;

        public Factory() {
            super(InternalCorrelationReorder.TYPE);
        }

        public Factory(String name, CorrelationReorderParser.CurveXY referenceAgg, List<CorrelationReorderParser.CurveXY> curves, int numCorrelatingCurves) {
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
            referenceAgg = new CorrelationReorderParser.CurveXY(in.readString(), in.readString());
            int numCurves = in.readInt();
            curves = new ArrayList<>();
            for (int i = 0; i< numCurves; i++) {
                curves.add(new CorrelationReorderParser.CurveXY(in.readString(), in.readString()));
            }
            numCorrelatingCurves = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(referenceAgg.getXPath());
            out.writeString(referenceAgg.getYPath());
            out.writeInt(curves.size());
            for (CorrelationReorderParser.CurveXY curve : curves) {
                out.writeString(curve.getXPath());
                out.writeString(curve.getYPath());
            }
            out.writeInt(numCorrelatingCurves);
        }

    }

    private class BucketsAndValues {
        List<Bucket> bucketList;
        Object[] xs;
        Object[] ys;
        String key;
        BytesReference bucketType;

        public BucketsAndValues(Object[] ys, Object[]xs , List<? extends Bucket> buckets, String label, BytesReference bucketType) {
            this.xs = xs;
            this.ys = ys;
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
