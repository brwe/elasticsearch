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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBounds;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.geoBounds;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
@TestLogging("indices.recovery:TRACE,action.search.type:TRACE")
public class SimpleSetupTests extends ElasticsearchIntegrationTest {

    private static final String SINGLE_VALUED_FIELD_NAME = "geo_value";
    private static final String MULTI_VALUED_FIELD_NAME = "geo_values";
    private static final String NUMBER_FIELD_NAME = "l_values";

    static int numDocs;
    static int numUniqueGeoPoints;
    static GeoPoint[] singleValues, multiValues;
    static GeoPoint singleTopLeft, singleBottomRight, multiTopLeft, multiBottomRight, unmappedTopLeft, unmappedBottomRight;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point", MULTI_VALUED_FIELD_NAME, "type=geo_point", NUMBER_FIELD_NAME, "type=long", "tag", "type=string,index=not_analyzed"));
        createIndex("idx_unmapped");

        unmappedTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        unmappedBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        singleTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        singleBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        multiTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        multiBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        numDocs = randomIntBetween(6, 20);
        numUniqueGeoPoints = randomIntBetween(1, numDocs);

        singleValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0 ; i < singleValues.length; i++)
        {
            singleValues[i] = randomGeoPoint();
            updateBoundsTopLeft(singleValues[i], singleTopLeft);
            updateBoundsBottomRight(singleValues[i], singleBottomRight);
        }

        multiValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0 ; i < multiValues.length; i++)
        {
            multiValues[i] = randomGeoPoint();
            updateBoundsTopLeft(multiValues[i], multiTopLeft);
            updateBoundsBottomRight(multiValues[i], multiBottomRight);
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();


        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .array(SINGLE_VALUED_FIELD_NAME, singleValues[i % numUniqueGeoPoints].lon(), singleValues[i % numUniqueGeoPoints].lat())
                    .startArray(MULTI_VALUED_FIELD_NAME)
                        .startArray().value(multiValues[i % numUniqueGeoPoints].lon()).value(multiValues[i % numUniqueGeoPoints].lat()).endArray()
                        .startArray().value(multiValues[(i+1) % numUniqueGeoPoints].lon()).value(multiValues[(i+1) % numUniqueGeoPoints].lat()).endArray()
                     .endArray()
                    .field(NUMBER_FIELD_NAME, i)
                    .field("tag", "tag" + i)
                    .endObject()));
        }

        assertAcked(prepareCreate("empty_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point"));

        assertAcked(prepareCreate("idx_dateline")
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point", MULTI_VALUED_FIELD_NAME, "type=geo_point", NUMBER_FIELD_NAME, "type=long", "tag", "type=string,index=not_analyzed"));

        GeoPoint[] geoValues = new GeoPoint[5];
        geoValues[0] = new GeoPoint(38, 178);
        geoValues[1] = new GeoPoint(12, -179);
        geoValues[2] = new GeoPoint(-24, 170);
        geoValues[3] = new GeoPoint(32, -175);
        geoValues[4] = new GeoPoint(-11, 178);

        for (int i = 0; i < 5; i++) {
            builders.add(client().prepareIndex("idx_dateline", "type").setSource(jsonBuilder()
                    .startObject()
                    .array(SINGLE_VALUED_FIELD_NAME, geoValues[i].lon(), geoValues[i].lat())
                    .field(NUMBER_FIELD_NAME, i)
                    .field("tag", "tag" + i)
                    .endObject()));
        }
        assertAcked(prepareCreate("high_card_idx").setSettings(ImmutableSettings.builder().put("number_of_shards", 2))
                .addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=geo_point", MULTI_VALUED_FIELD_NAME, "type=geo_point", NUMBER_FIELD_NAME, "type=long", "tag", "type=string,index=not_analyzed"));


        for (int i = 0; i < 2000; i++) {
            builders.add(client().prepareIndex("high_card_idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .array(SINGLE_VALUED_FIELD_NAME, singleValues[i % numUniqueGeoPoints].lon(), singleValues[i % numUniqueGeoPoints].lat())
                    .startArray(MULTI_VALUED_FIELD_NAME)
                        .startArray().value(multiValues[i % numUniqueGeoPoints].lon()).value(multiValues[i % numUniqueGeoPoints].lat()).endArray()
                        .startArray().value(multiValues[(i+1) % numUniqueGeoPoints].lon()).value(multiValues[(i+1) % numUniqueGeoPoints].lat()).endArray()
                     .endArray()
                    .field(NUMBER_FIELD_NAME, i)
                    .field("tag", "tag" + i)
                    .endObject()));
        }

        indexRandom(true, builders);
        ensureSearchable();

        // Added to debug a test failure where the terms aggregation seems to be reporting two documents with the same value for NUMBER_FIELD_NAME.  This will check that after
        // random indexing each document only has 1 value for NUMBER_FIELD_NAME and it is the correct value. Following this initial change its seems that this call was getting
        // more that 2000 hits (actual value was 2059) so now it will also check to ensure all hits have the correct index and type
        SearchResponse response = client().prepareSearch("high_card_idx").addField(NUMBER_FIELD_NAME).addSort(SortBuilders.fieldSort(NUMBER_FIELD_NAME).order(SortOrder.ASC)).setSize(5000).get();
        assertSearchResponse(response);
        long totalHits = response.getHits().totalHits();
        XContentBuilder builder = response.toXContent(XContentBuilder.builder(JsonXContent.jsonXContent), ToXContent.EMPTY_PARAMS);
        logger.info("Full high_card_idx Response Content:\n{ {} }", builder.string());
        for (int i = 0; i < totalHits; i++) {
            SearchHit searchHit = response.getHits().getAt(i);
            assertThat("Hit " + i + " with id: " + searchHit.getId(), searchHit.getIndex(), equalTo("high_card_idx"));
            assertThat("Hit " + i + " with id: " + searchHit.getId(), searchHit.getType(), equalTo("type"));
            SearchHitField hitField = searchHit.field(NUMBER_FIELD_NAME);

            assertThat("Hit " + i + " has wrong number of values", hitField.getValues().size(), equalTo(1));
            Integer value = hitField.getValue();
            assertThat("Hit " + i + " has wrong value", value, equalTo(i));
        }
        assertThat(totalHits, equalTo(2000l));
    }

    private void updateBoundsBottomRight(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() < currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() > currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    private void updateBoundsTopLeft(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() > currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() < currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    private GeoPoint randomGeoPoint() {
        return new GeoPoint((randomDouble() * 180) - 90, (randomDouble() * 360) - 180);
    }

    @Test
    public void heya() throws Exception {
        cluster().wipeIndices("*");
        setupSuiteScopeCluster();
    }
}
