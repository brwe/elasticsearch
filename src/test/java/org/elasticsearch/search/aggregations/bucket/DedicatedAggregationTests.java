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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;


public class DedicatedAggregationTests extends ElasticsearchIntegrationTest {

    // https://github.com/elasticsearch/elasticsearch/issues/7240
    @Test
    public void testEmptyBoolIsMatchAll() throws IOException {
        String query = copyToStringFromClasspath("/org/elasticsearch/search/aggregations/bucket/agg-filter-with-empty-bool.json");
        createIndex("testidx");
        index("testidx", "apache", "1", "field", "text");
        index("testidx", "nginx", "2", "field", "text");
        refresh();
        ensureGreen("testidx");
        SearchResponse searchResponse = client().prepareSearch("testidx").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        searchResponse = client().prepareSearch("testidx").setSource(query).get();
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getAggregations().getAsMap().get("issue7240"), instanceOf(Filter.class));
        Filter filterAgg = (Filter) searchResponse.getAggregations().getAsMap().get("issue7240");
        assertThat(filterAgg.getAggregations().getAsMap().get("terms"), instanceOf(StringTerms.class));
        assertThat(((StringTerms) filterAgg.getAggregations().getAsMap().get("terms")).getBuckets().get(0).getDocCount(), equalTo(1l));
    }

    @Test
    public void testParentIdAggregation() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("childtype")
                .startObject("_parent")
                .field("type", "parenttype")
                .field("store", true)
                .endObject()
                .endObject();
        assertAcked(prepareCreate("testidx").addMapping("childtype", mapping));
        client().prepareIndex("testidx", "childtype").setSource(jsonBuilder().startObject().field("num", 1).endObject()).setParent("p1").get();
        client().prepareIndex("testidx", "childtype").setSource(jsonBuilder().startObject().field("num", 2).endObject()).setParent("p1").get();

        refresh();
        ensureGreen("testidx");
        SearchResponse searchResponse = client().prepareSearch("testidx").setTypes("childtype").setQuery(matchAllQuery()).addAggregation(AggregationBuilders.terms("children").field("_parent")).get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getAggregations().getAsMap().get("children"), instanceOf(Terms.class));
        Terms terms = (Terms) searchResponse.getAggregations().getAsMap().get("children");
    }
}
