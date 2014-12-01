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
package org.elasticsearch.search.reducers;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.reducers.bucket.range.InternalRange;
import org.elasticsearch.search.reducers.bucket.range.RangeReducer;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.search.reducers.bucket.slidingwindow.SlidingWindowReducer;
import org.elasticsearch.search.reducers.bucket.union.InternalUnion;
import org.elasticsearch.search.reducers.bucket.union.UnionReducer;
import org.elasticsearch.search.reducers.bucket.unpacking.InternalUnpacking;
import org.elasticsearch.search.reducers.bucket.unpacking.UnpackingReducer;
import org.elasticsearch.search.reducers.metric.InternalMetric;
import org.elasticsearch.search.reducers.metric.MetricReducer;
import org.elasticsearch.search.reducers.metric.MultiBucketMetricAggregation;
import org.elasticsearch.search.reducers.metric.SingleBucketMetricAggregation;

/**
 * A module that registers all the transport streams for the addAggregation
 */
public class TransportReductionModule extends AbstractModule {

    @Override
    protected void configure() {

        InternalSlidingWindow.registerStreams();
        InternalUnion.registerStreams();
        InternalUnpacking.registerStreams();
        InternalRange.registerStreams();
        InternalMetric.registerStreams();
        SingleBucketMetricAggregation.registerStreams();
        MultiBucketMetricAggregation.registerStreams();

        SlidingWindowReducer.registerStreams();
        UnionReducer.registerStreams();
        UnpackingReducer.registerStreams();
        MetricReducer.registerStreams();
        RangeReducer.registerStreams();
    }
}
