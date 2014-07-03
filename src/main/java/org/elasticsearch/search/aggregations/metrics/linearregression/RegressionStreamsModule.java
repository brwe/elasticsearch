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


package org.elasticsearch.search.aggregations.metrics.linearregression;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.search.aggregations.metrics.linearregression.bayes.BayesRegressor;
import org.elasticsearch.search.aggregations.metrics.linearregression.sgd.SgdRegressor;

import java.util.List;


public class RegressionStreamsModule extends AbstractModule {

    private List<RegressionMethodStreams.Stream> streams = Lists.newArrayList();

    public RegressionStreamsModule() {
        stream(SgdRegressor.STREAM);
        stream(BayesRegressor.STREAM);
    }

    public void stream(RegressionMethodStreams.Stream stream) {
        streams.add(stream);
    }

    @Override
    protected void configure() {
        for (RegressionMethodStreams.Stream stream : streams) {
            RegressionMethodStreams.registerStream(stream, stream.getName());
        }
    }
}
