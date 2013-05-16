/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.search.recencyscore;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.recencyScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

public class RecencyScoreTest extends AbstractSharedClusterTest {

    @Test
    public void testRecencyScoreSimple() throws Exception {
        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();
        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(recencyScoreQuery(termQuery("test", "value")).field("num1").function("Gaussian"))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));
        assertThat(sh.getAt(0).getScore(), Matchers.greaterThan(sh.getAt(1).getScore()));
    }

}
