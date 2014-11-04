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

package org.elasticsearch.search.aggregations.reducers;


import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.reducers.bucket.correlation.CorrelationReorderBuilder;
import org.elasticsearch.search.reducers.bucket.correlation.CorrelationReorderReducer;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.*;
import static org.hamcrest.CoreMatchers.equalTo;

public class CorrelationUnitTests extends ElasticsearchSingleNodeTest {

    @Test
    public void testMinCommonPrefix() throws IOException, ExecutionException, InterruptedException {
        String path1 = "global.reference.avg";
        String path2 = "global.label.curve.avg";
        assertThat(CorrelationReorderReducer.findMinPrefix(path1, path2).toString(),equalTo("global"));
    }



}
