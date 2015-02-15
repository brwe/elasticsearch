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

package org.elasticsearch.search.matrix;

import com.google.common.collect.Sets;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import javax.naming.CompositeName;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchMatrixScanScrollingTests extends ElasticsearchIntegrationTest {

    public static final String[] letters = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};

    @Test
    public void testRandomized() throws Exception {
        testScroll(scaledRandomIntBetween(100, 200), between(1, 300), getRandom().nextBoolean(), getRandom().nextBoolean());
    }

    private void testScroll(long numberOfDocs, int size, boolean unbalanced, boolean trackScores) throws Exception {
        createIndex("test");
        ensureGreen();
        Set<String> indexedWords = new HashSet<>();
        Set<String> ids = Sets.newHashSet();
        Set<String> expectedIds = Sets.newHashSet();
        for (int i = 0; i < numberOfDocs; i++) {
            String id = Integer.toString(i);
            expectedIds.add(id);
            String routing = null;
            if (unbalanced) {
                if (i < (numberOfDocs * 0.6)) {
                    routing = "0";
                } else if (i < (numberOfDocs * 0.9)) {
                    routing = "1";
                } else {
                    routing = "2";
                }
            }

            String text = letters[randomInt(letters.length -1)];
            indexedWords.add(text);
            client().prepareIndex("test", "type1", id).setRouting(routing).setSource("field", text).execute().actionGet();
            // make some segments
            if (i % 10 == 0) {
                client().admin().indices().prepareFlush().execute().actionGet();
            }
        }

        refresh();
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type1").setSearchType(SearchType.MATRIX)
                .setSize(size)
                .setScroll(TimeValue.timeValueMinutes(2))
                .setTrackScores(trackScores).setSource(new BytesArray(new BytesRef("{\"query\":{\"match_all\":{}},\"analyzed_text\": [{\"field\":\"test_field\",\"idf_threshold\": 0, \"df_threshold\": 0}]}"))).get();
        assertHitCount(searchResponse, numberOfDocs);
        int numWords = randomInt(10);
        try {
            Set<String> words = new HashSet<>();
            String from = null;
            while(true) {
                Set<String> curSet = new HashSet<String>();
                searchResponse = client().prepareMatrixSearchScroll(searchResponse.getScrollId(), from, numWords).setScroll(TimeValue.timeValueMinutes(2)).execute().actionGet();
                assertHitCount(searchResponse, 0);
                assertNotNull(searchResponse.getMatrixRows());
                assertThat(searchResponse.getMatrixRows().getPostingLists().size(), lessThanOrEqualTo(numWords));

                if (searchResponse.getMatrixRows().getPostingLists().size() == 0) {
                    break;
                }

                for (Tuple<String, long[]> postingList : searchResponse.getMatrixRows().getPostingLists()) {
                    curSet.add(postingList.v1());
                }
                assertFalse(curSet.contains(from));
                words.addAll(curSet);
                from = searchResponse.getMatrixRows().getPostingLists().get(searchResponse.getMatrixRows().getPostingLists().size()-1).v1();

            }
            assertThat(words.size(), equalTo(indexedWords.size()));
            for (String word : words) {
                assertTrue(indexedWords.contains(word));
            }
        } finally {
            clearScroll(searchResponse.getScrollId());
        }
    }
}