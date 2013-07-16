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

package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.distancescoring.multiplydistancescores.ExponentialDecayFunctionBuilder;
import org.elasticsearch.index.query.distancescoring.multiplydistancescores.GaussDecayFunctionBuilder;
import org.elasticsearch.index.query.distancescoring.multiplydistancescores.LinearDecayFunctionBuilder;
import org.elasticsearch.index.query.distancescoring.multiplydistancescores.MultiplyingFunctionBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.distanceScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class DistanceScoreTest extends AbstractSharedClusterTest {

    @Test
    public void testDistanceScoreTime() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();

        MultiplyingFunctionBuilder gfb = new GaussDecayFunctionBuilder();
        gfb.addVariable("num1", "2013-05-28", "+1d");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }

    @Test
    public void testLinearScoring() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())).actionGet();

        refresh();

        MultiplyingFunctionBuilder gfb = new LinearDecayFunctionBuilder();
        gfb.addVariable("num1", "2013-05-28", "+3d");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(3));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));
        assertThat(sh.getAt(2).getId(), equalTo("3"));

    }

    @Test
    public void testDistanceScoreGeoGauss() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "loc", "geo_point");
        ensureYellow();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("1")
                        .source(jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 10).field("lon", 20)
                                .endObject().endObject())).actionGet();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("2")
                        .source(jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 22)
                                .endObject().endObject())).actionGet();
        refresh();

        // Test Gauss
        MultiplyingFunctionBuilder fb = new GaussDecayFunctionBuilder();
        fb.addGeoVariable("loc", 11, 20, "1000km");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        SearchResponse sr = response.actionGet();
        SearchHits sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));
        assertThat(sh.hits().length, equalTo(2));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Gauss
        fb = new ExponentialDecayFunctionBuilder();
        fb.addGeoVariable("loc", 11, 20, "1000km");

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));
        assertThat(sh.hits().length, equalTo(2));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
        // Test Gauss
        fb = new LinearDecayFunctionBuilder();
        fb.addGeoVariable("loc", 11, 20, "1000km");

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(false).query(termQuery("test", "value"))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));

        response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), fb))));
        sr = response.actionGet();
        sh = sr.getHits();
        assertThat(sh.getTotalHits(), equalTo(2l));
        assertThat(sh.hits().length, equalTo(2));

        assertThat(sh.getAt(0).getId(), equalTo("1"));
        assertThat(sh.getAt(1).getId(), equalTo("2"));
    }

    @Test(expectedExceptions = SearchPhaseExecutionException.class)
    public void testExceptionThrownIfScaleLE0() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();

        MultiplyingFunctionBuilder gfb = new GaussDecayFunctionBuilder();
        gfb.addVariable("num1", "2013-05-28", "-1d");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }
    
    @Test(expectedExceptions = SearchPhaseExecutionException.class)
    public void testExceptionThrownIfScaleRefNotBetween0And1() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date");
        ensureYellow();
        client().index(
                indexRequest("test").type("type1").id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-28").endObject())).actionGet();
        refresh();

        MultiplyingFunctionBuilder gfb = new GaussDecayFunctionBuilder();
        gfb.addVariable("num1", "2013-05-28", "1d", "-1");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(2));
        assertThat(sh.getAt(0).getId(), equalTo("2"));
        assertThat(sh.getAt(1).getId(), equalTo("1"));

    }

    @Test
    public void testValueMissing() throws Exception {

        createIndexMapped("test", "type1", "test", "string", "num1", "date", "num2", "double");
        ensureYellow();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("1")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-27").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("2")
                        .source(jsonBuilder().startObject().field("test", "value").field("num2", "1.0").endObject())).actionGet();
        client().index(
                indexRequest("test")
                        .type("type1")
                        .id("3")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").field("num2", "1.0")
                                .endObject())).actionGet();
        client().index(
                indexRequest("test").type("type1").id("4")
                        .source(jsonBuilder().startObject().field("test", "value").field("num1", "2013-05-30").endObject())).actionGet();

        refresh();

        MultiplyingFunctionBuilder gfb = new LinearDecayFunctionBuilder();
        gfb.addVariable("num1", "2013-05-28", "+3d");
        gfb.addVariable("num2", "0.0", "1");

        ActionFuture<SearchResponse> response = client().search(
                searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                        searchSource().explain(true).query(distanceScoreQuery(termQuery("test", "value"), gfb))));

        SearchResponse sr = response.actionGet();
        ElasticsearchAssertions.assertNoFailures(sr);
        SearchHits sh = sr.getHits();
        assertThat(sh.hits().length, equalTo(4));
        double[] scores = new double[4];
        for (int i = 0; i < sh.hits().length; i++) {
            scores[Integer.parseInt(sh.getAt(i).getId()) - 1] = sh.getAt(i).getScore();
        }
        assertThat(scores[0], lessThan(scores[1]));
        assertThat(scores[2], lessThan(scores[3]));

    }

}
