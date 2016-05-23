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

package org.elasticsearch.index;

import org.apache.lucene.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

@TestLogging("indices.recovery:TRACE,action.support.replication:TRACE")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexAndRelocateConcurrentlyIT extends ESIntegTestCase {

    @Test
    public void testRecoverFromPreviousVersion() throws ExecutionException, InterruptedException {
        Settings blueSetting = Settings.builder().put("node.attr.color", "blue").build();
        InternalTestCluster.Async<List<String>> blueFuture = internalCluster().startNodesAsync(blueSetting, blueSetting);
        Settings redSetting = Settings.builder().put("node.attr.color", "red").build();
        InternalTestCluster.Async<java.util.List<String>> redFuture = internalCluster().startNodesAsync(redSetting, redSetting);
        blueFuture.get();
        redFuture.get();
        logger.info("blue nodes: {}", blueFuture.get());
        logger.info("red nodes: {}", redFuture.get());
        ensureStableCluster(4);

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.routing.allocation.exclude.color", "blue")
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(indexSettings())));
        ensureYellow();
        assertAllShardsOnNodes("test", redFuture.get().toArray(new String[2]));
        int numDocs = randomIntBetween(100, 150);
        ArrayList<String> ids = new ArrayList<>();
        logger.info(" --> indexing [{}] docs", numDocs);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(i);
            ids.add(id);
            docs[i] = client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        SearchResponse countResponse = client().prepareSearch("test").get();
        assertHitCount(countResponse, numDocs);

        logger.info(" --> moving index to new nodes");
        Settings build = Settings.builder().put("index.routing.allocation.exclude.color", "red")
            .put("index.routing.allocation.include.color", "blue").build();
        client().admin().indices().prepareUpdateSettings("test").setSettings(build).execute().actionGet();

        // index while relocating

        logger.info(" --> indexing [{}] more docs", numDocs);
        for (int i = 0; i < numDocs; i++) {
            String id = randomRealisticUnicodeOfLength(10) + String.valueOf(numDocs + i);
            ids.add(id);
            docs[i] = client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(numDocs + i));
        }
        indexRandom(true, docs);
        numDocs *= 2;

        logger.info(" --> waiting for relocation to complete", numDocs);
        ensureGreen("test");// move all shards to the new node (it waits on relocation)
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            SearchResponse afterRelocation = client().prepareSearch().setSize(ids.size()).get();
            assertNoFailures(afterRelocation);
            assertSearchHits(afterRelocation, ids.toArray(new String[ids.size()]));
        }
    }
}
