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

package org.elasticsearch.search.reducers.bucket.linearregression.slidingwindow;


import org.apache.commons.math.linear.MatrixUtils;
import org.apache.commons.math.linear.RealMatrix;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.reducers.*;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.io.IOException;
import java.util.*;

import Jama.Matrix;

public class LinearRegressionReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    private final static BucketStreams.Stream<Bucket> BUCKET_STREAM = new BucketStreams.Stream<Bucket>() {
        @Override
        public Bucket readResult(StreamInput in, BucketStreamContext context) throws IOException {
            Bucket filters = new LRBucket();
            filters.readFrom(in);
            return filters;
        }

        @Override
        public BucketStreamContext getBucketStreamContext(Bucket bucket) {
            BucketStreamContext context = new BucketStreamContext();
            context.keyed(true);
            return context;
        }
    };
    private final List<String> xMetrics;
    String yMetric;
    List<Double> predictXs;

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalLinearRegression.TYPE.stream());
        BucketStreams.registerStream(BUCKET_STREAM, InternalLinearRegression.TYPE.stream());
    }


    public LinearRegressionReducer(String name, String bucketsPath, List<String> xMetrics, String yMetric, List<Double> predictXs, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
        this.xMetrics = xMetrics;
        this.yMetric = yMetric;
        this.predictXs = predictXs;
    }

    public InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) {
        List<InternalSelection> selections = new ArrayList<>();
        List<? extends Bucket> aggBuckets = aggregation.getBuckets();
        List<Bucket> resultBuckets = new ArrayList<>();
        int xDim = (xMetrics == null || xMetrics.size() == 0) ? 1 : xMetrics.size();
        int yDim = aggBuckets.size();
        RealMatrix Theta = MatrixUtils.createRealMatrix(xDim + 1, yDim); // holds the xes, metric values or count of the buckets
        Theta = Theta.scalarAdd(1.0);
        RealMatrix t = MatrixUtils.createRealMatrix(yDim, 1); // holds the measured values (metric value that is to be predicted)
        Queue<String> queue = new LinkedList<>();

        for (int j = 0; j < xDim; j++) {
            queue.clear();
            queue.add(xMetrics.get(0));
            Object[] values = (Object[]) aggregation.getProperty(queue);
            for (int i = 0; i < values.length; i++) {
                Theta.setEntry(j, i, ((Number) values[i]).doubleValue());
            }
        }
        queue.clear();
        queue.add(yMetric);
        Object[] values = (Object[]) aggregation.getProperty(queue);
        for (int i = 0; i < values.length; i++) {
            t.setEntry(i, 0, ((Number) values[i]).doubleValue());
        }
        //here do the regression. for now, just try and fit a line
        double alpha = 1;
        double beta = 1;
        RealMatrix S_N = MatrixUtils.createRealIdentityMatrix(xDim + 1);
        S_N = S_N.scalarMultiply(alpha);
        S_N = S_N.add(Theta.multiply(Theta.transpose()).scalarMultiply(beta));
        S_N = S_N.inverse();

        RealMatrix mN = S_N.multiply(Theta).multiply(t).scalarMultiply(beta);

        double predictedY = 0.0;
        for (int i = 0; i < xDim; i++) {
            predictedY += mN.getEntry(i, 0) * predictXs.get(i);
        }
        predictedY += mN.getEntry(xDim, 0);
        //create result buckets
        for (int i = 0; i < aggBuckets.size(); i++) {
            double[] xs = new double[xDim];
            for (int j = 0; j < xDim; j++) {
                xs[j] = Theta.getEntry(j, i);
            }
            double y = t.getEntry(i, 0);
            double bucketPredictedY = 0;
            for (int j = 0; j < xDim; j++) {
                bucketPredictedY += mN.getEntry(j, 0) * xs[j];
            }
            bucketPredictedY += mN.getEntry(xDim, 0);
            resultBuckets.add(new LRBucket(Integer.toString(i), xs, y, bucketPredictedY));
        }
        double[] pxs = new double[predictXs.size()];
        for (int i = 0; i < pxs.length; i++) {
            pxs[i] = predictXs.get(i);
        }
        resultBuckets.add(new LRBucket("prediction", pxs, predictedY, predictedY));
        InternalSelection selection = new InternalSelection(name(), InternalLinearRegression.TYPE.stream(), bucketStreamContext, resultBuckets, null);
        selections.add(selection);
        double[] finalParameters = new double[xDim + 1];
        for (int i = 0; i <= xDim; i++) {
            finalParameters[i] = mN.getEntry(i, 0);
        }
        return new InternalLinearRegression(name(), selections, finalParameters);
    }

    public static class LRBucket implements Bucket {
        double[] Xs;
        double y;
        double predictedY;
        String key;

        public LRBucket() {

        }

        public LRBucket(String key, double[] Xs, double y, double predictedY) {
            this.Xs = Xs;
            this.y = y;
            this.predictedY = predictedY;
            this.key = key;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Text getKeyAsText() {
            return new StringText(getKey());
        }

        @Override
        public long getDocCount() {
            return 0;
        }

        @Override
        public Aggregations getAggregations() {
            return null;
        }

        @Override
        public Object getProperty(String containingAggName, Queue<String> path) {
            throw new UnsupportedOperationException("getProperty not implemented yet by LinReg");

        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            key = in.readString();
            Xs = in.readDoubleArray();
            y = in.readDouble();
            predictedY = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(key);
            out.writeDoubleArray(Xs);
            out.writeDouble(y);
            out.writeDouble(predictedY);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("key", key);
            builder.field("x_s", Xs);
            builder.field("y", y);
            builder.field("predicted_y", predictedY);
            builder.endObject();
            return builder;
        }
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        List<String> xMetrics;
        String yMetric;
        List<Double> predictXs;

        public Factory() {
            super(InternalLinearRegression.TYPE);
        }

        public Factory(String name, String bucketsPath, List<String> xMetrics, String yMetric, List<Double> predictXs) {
            super(name, InternalLinearRegression.TYPE);
            this.bucketsPath = bucketsPath;
            this.xMetrics = xMetrics;
            this.yMetric = yMetric;
            this.predictXs = predictXs;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new LinearRegressionReducer(name, bucketsPath, xMetrics, yMetric, predictXs, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            xMetrics = Arrays.asList(in.readStringArray());
            yMetric = in.readOptionalString();
            double[] pxs = in.readDoubleArray();
            predictXs = new ArrayList<>();
            for (double val : pxs) {
                predictXs.add(val);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeStringArray(xMetrics.toArray(new String[xMetrics.size()]));
            out.writeOptionalString(yMetric);
            double[] pxs = new double[predictXs.size()];
            int counter = 0;
            for (Double val : predictXs) {
                pxs[counter] = val;
                counter++;
            }
            out.writeDoubleArray(pxs);
        }

    }

}
