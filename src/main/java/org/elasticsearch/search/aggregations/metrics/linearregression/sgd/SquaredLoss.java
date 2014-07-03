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
package org.elasticsearch.search.aggregations.metrics.linearregression.sgd;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.util.Map;


public class SquaredLoss extends SgdRegressor {

    public SquaredLoss(long estimatedBucketsCount, AggregationContext context, double alpha) {
        super(context, estimatedBucketsCount, alpha);
    }

    protected double loss(double[] thetas, double[] xs, double y) {
        double y_hat = 0;
        for (int i = 0; i < thetas.length; i++) {
            y_hat += thetas[i] * xs[i];
        }
        return y - y_hat;
    }

    @Override
    public void close() throws ElasticsearchException {
        release();
    }


    public static class Factory implements SgdRegressor.Factory {

        private double alpha = 0.5;

        public Factory(double alpha) {
            this.alpha = alpha;
        }

        public SquaredLoss create(long estimatedBucketCount, AggregationContext context) {
            return new SquaredLoss(estimatedBucketCount, context, alpha);
        }
    }

}