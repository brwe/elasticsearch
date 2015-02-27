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

package org.elasticsearch.indices.recovery;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.gateway.local.state.meta.LocalGatewayMetaState;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class MetaDataWriteDataNodesTests extends ElasticsearchIntegrationTest {

    @Test
    public void testMetaWrittenAlsoOnDataNode() throws Exception {
        // this test checks that index state is written on data only nodes
        startMasterNode("master_node", false);
        startDataNode("red_node", "red", false);
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0)));
        index("test", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        waitForConcreteMappingsOnAll("test", "doc", "text");
        ensureGreen("test");
        assertIndexInMetaState("red_node", "test");
        assertIndexInMetaState("master_node", "test");
        //stop master node and start again with an empty data folder
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        startMasterNode("new_master_node", true);
        client().admin().cluster().prepareHealth()
                .setWaitForRelocatingShards(0)
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setWaitForGreenStatus().get();
        ensureGreen("test");
        // wait for mapping also on master becasue then we can be sure the state was written
        waitForConcreteMappingsOnAll("test", "doc", "text");
        // check for meta data
        assertIndexInMetaState("red_node", "test");
        assertIndexInMetaState("new_master_node", "test");
        // check if index and doc is still there
        ensureGreen("test");
        assertTrue(client().prepareGet("test", "doc", "1").get().isExists());
    }

    @Test
    public void testMetaWrittenOnlyForIndicesOnNodesThatHaveAShard() throws Exception {
        // this test checks that the index state is only written to a data only node if they have a shard of that index allocated on the node
        startMasterNode("master_node", false);
        startDataNode("blue_node", "blue", false);
        startDataNode("red_node", "red", false);

        assertAcked(prepareCreate("blue_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        ensureGreen();
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");
        assertIndexNotInMetaState("blue_node", "red_index");
        assertIndexNotInMetaState("red_node", "blue_index");
        assertIndexInMetaState("blue_node", "blue_index");
        assertIndexInMetaState("red_node", "red_index");
        assertIndexInMetaState("master_node", "red_index");
        assertIndexInMetaState("master_node", "blue_index");

        // not the index state for blue_index should only be written on blue_node and the for red_index only on red_node
        // we restart red node and master but with empty data folders
        stopNode("red_node");
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        startMasterNode("new_master_node", true);
        startDataNode("new_red_node", "red", true);

        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).get();
        assertIndexNotInMetaState("blue_node", "red_index");
        assertIndexInMetaState("blue_node", "blue_index");
        assertIndexNotInMetaState("new_red_node", "red_index");
        assertIndexNotInMetaState("new_red_node", "blue_index");
        assertIndexNotInMetaState("new_master_node", "red_index");
        assertIndexInMetaState("new_master_node", "blue_index");
        // check that blue index is still there
        assertFalse(client().admin().indices().prepareExists("red_index").get().isExists());
        assertTrue(client().prepareGet("blue_index", "doc", "1").get().isExists());
        // red index should be gone
        // if the blue node had stored the index state then cluster health would be red and red_index would exist
        assertFalse(client().admin().indices().prepareExists("red_index").get().isExists());
        ensureGreen();

    }

    @Test
    public void testMetaIsRemovedIfAllShardsFromIndexRemoved() throws Exception {
        // this test checks that the index state is removed from a data only node once all shards have been allocated away from it
        startMasterNode("master_node", false);
        startDataNode("blue_node", "blue", false);
        startDataNode("red_node", "red", false);

        // create blue_index on blue_node and same for red
        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("3")).get();
        assertAcked(prepareCreate("blue_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());

        ensureGreen();
        assertIndexNotInMetaState("red_node", "blue_index");
        assertIndexNotInMetaState("blue_node", "red_index");
        assertIndexInMetaState("red_node", "red_index");
        assertIndexInMetaState("blue_node", "blue_index");
        assertIndexInMetaState("master_node", "red_index");
        assertIndexInMetaState("master_node", "blue_index");

        // now relocate blue_index to red_node and red_index to blue_node
        logger.debug("relocating indices...");
        client().admin().indices().prepareUpdateSettings("blue_index").setSettings(ImmutableSettings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")).get();
        client().admin().indices().prepareUpdateSettings("red_index").setSettings(ImmutableSettings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")).get();
        client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get();
        ensureGreen();
        assertIndexNotInMetaState("red_node", "red_index");
        assertIndexNotInMetaState("blue_node", "blue_index");
        assertIndexInMetaState("red_node", "blue_index");
        assertIndexInMetaState("blue_node", "red_index");
        assertIndexInMetaState("master_node", "red_index");
        assertIndexInMetaState("master_node", "blue_index");
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");

        //at this point the blue_index is on red node and the red_index on blue node
        // now, when we start red and master node again but without data folder, the red index should be gone but the blue index should initialize fine
        stopNode("red_node");
        ((InternalTestCluster) cluster()).stopCurrentMasterNode();
        startMasterNode("new_master_node", true);
        startDataNode("new_red_node", "red", true);
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).get();
        assertIndexNotInMetaState("new_red_node", "blue_index");
        assertIndexNotInMetaState("blue_node", "blue_index");
        assertIndexNotInMetaState("new_red_node", "red_index");
        assertIndexInMetaState("blue_node", "red_index");
        assertIndexInMetaState("new_master_node", "red_index");
        assertIndexNotInMetaState("new_master_node", "blue_index");
        assertTrue(client().prepareGet("red_index", "doc", "1").get().isExists());
        // if the red_node had stored the index state then cluster health would be red and blue_index would exist
        assertFalse(client().admin().indices().prepareExists("blue_index").get().isExists());
    }

    @Test
    public void randomStartStopReloacteWithAndWithoutData() throws Exception {
        // relocate shards to and fro and start master nodes randomly with or without data folder and see what happens
        // not sure if this test is any good, it did not uncover anything

        Map<String, Boolean> runningMasterNodes = new HashMap<>();
        String[] masterNodeNames = {"master_node_1", "master_node_2"};
        for (String name : masterNodeNames) {
            runningMasterNodes.put(name, true);
            startMasterNode(name, false);
        }
        logger.debug("start data node");
        startDataNode("blue_node", "blue", false);
        startDataNode("red_node", "red", false);

        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("3")).get();
        assertAcked(prepareCreate("blue_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());

        // green index can be anywhere
        assertAcked(prepareCreate("green_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0)));
        index("green_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");
        waitForConcreteMappingsOnAll("green_index", "doc", "text");

        String[] indexNames = {"blue_index", "red_index"};
        String[] colors = {"blue", "red"};
        ensureGreen();
        int numMasterNodesUp = masterNodeNames.length;
        for (int i = 0; i < 10; i++) {
            // 1. start stop node or relocate?
            if (randomBoolean()) {
                // we start or stop a master node
                if (numMasterNodesUp > 0 && numMasterNodesUp < masterNodeNames.length) {
                    //we have a working cluster, we can either stop or start a master node
                    if (randomBoolean()) {
                        numMasterNodesUp = stopMasterNode(runningMasterNodes, numMasterNodesUp);
                    } else {
                        numMasterNodesUp = startMasterNode(runningMasterNodes, numMasterNodesUp);
                    }
                } else if (numMasterNodesUp == masterNodeNames.length) {
                    // all masters are running, can only stop one
                    numMasterNodesUp = stopMasterNode(runningMasterNodes, numMasterNodesUp);
                } else if (numMasterNodesUp == 0) {
                    // all masters are stopped, must start one
                    numMasterNodesUp = startMasterNode(runningMasterNodes, numMasterNodesUp);
                }

            } else {
                if (numMasterNodesUp > 0) {
                    // relocate a random index to red or blue
                    String index = indexNames[randomInt(1)];
                    String color = colors[randomInt(1)];
                    logger.info("move index {} to node with color {}", index, color);
                    ensureGreen(index);
                    client().admin().indices().prepareUpdateSettings(index).setSettings(ImmutableSettings.builder().put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", color)).get();
                    client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).get();
                    ensureGreen();
                }
            }
        }
        if (numMasterNodesUp == 0) {
            numMasterNodesUp = startMasterNode(runningMasterNodes, numMasterNodesUp);
        }
        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).get();
        ensureGreen("blue_index");
        assertTrue(client().admin().indices().prepareExists("blue_index").get().isExists());
        ensureGreen("red_index");
        assertTrue(client().admin().indices().prepareExists("red_index").get().isExists());
        ensureGreen("green_index");
        assertTrue(client().admin().indices().prepareExists("green_index").get().isExists());
        assertTrue(client().prepareGet("red_index", "doc", "1").get().isExists());
        assertTrue(client().prepareGet("blue_index", "doc", "1").get().isExists());
        assertTrue(client().prepareGet("green_index", "doc", "1").get().isExists());
    }

    @Test
    @TestLogging("gateway:TRACE")
    public void testDanglingOnDataMasterNodes() throws Exception {
        // this test checks that regular dangling indices still works
        startMasterNode("master_node", false);
        String dataPathBlue = newTempDir().toString();
        String dataPathRed = newTempDir().toString();
        String dataPathGreen = newTempDir().toString();
        startDataMasterNode("blue_node", "blue", dataPathBlue);
        startDataMasterNode("red_node", "red", dataPathRed);
        startDataMasterNode("green_node", "green", dataPathGreen);

        // create blue_index on blue_node and same for red and green
        client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForNodes("3")).get();
        assertAcked(prepareCreate("blue_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "blue")));
        index("blue_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("red_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "red")));
        index("red_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        assertAcked(prepareCreate("green_index").setSettings(ImmutableSettings.builder().put("index.number_of_replicas", 0).put(FilterAllocationDecider.INDEX_ROUTING_INCLUDE_GROUP + "color", "green")));
        index("green_index", "doc", "1", jsonBuilder().startObject().field("text", "some text").endObject());
        waitForConcreteMappingsOnAll("green_index", "doc", "text");
        waitForConcreteMappingsOnAll("red_index", "doc", "text");
        waitForConcreteMappingsOnAll("blue_index", "doc", "text");
        ensureGreen("blue_index", "red_index", "green_index");

        stopNode("green_node");
        stopNode("red_node");
        stopNode("blue_node");
        stopNode("master_node");

        // bring master node up with empty data folder and data nodes with same folder as before
        startMasterNode("new_master_node", true);
        startDataMasterNode("green_node", "green", dataPathGreen);
        startDataMasterNode("blue_node", "blue", dataPathBlue);
        startDataMasterNode("red_node", "red", dataPathRed);

        client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).get();
        ensureGreen("blue_index");
        assertTrue(client().admin().indices().prepareExists("blue_index").get().isExists());
        ensureGreen("red_index");
        assertTrue(client().admin().indices().prepareExists("red_index").get().isExists());
        ensureGreen("green_index");
        assertTrue(client().admin().indices().prepareExists("green_index").get().isExists());
        assertTrue(client().prepareGet("red_index", "doc", "1").get().isExists());
        assertTrue(client().prepareGet("blue_index", "doc", "1").get().isExists());
        assertTrue(client().prepareGet("green_index", "doc", "1").get().isExists());
    }

    protected int stopMasterNode(Map<String, Boolean> runningMasterNodes, int numMasterNodesUp) throws IOException {
        String runningMasterNode = getRunningMasterNode(runningMasterNodes);
        logger.info("Stopping master eligible node {}", runningMasterNode);
        stopNode(runningMasterNode);
        runningMasterNodes.put(runningMasterNode, false);
        numMasterNodesUp--;
        return numMasterNodesUp;
    }

    protected int startMasterNode(Map<String, Boolean> runningMasterNodes, int numMasterNodesUp) {
        String downMasterNode = getDownMasterNode(runningMasterNodes);
        boolean withNewDataPath = randomBoolean();
        logger.info("Starting master eligible node {} with {} data path", downMasterNode, (withNewDataPath ? "random empty " : "default"));
        startMasterNode(downMasterNode, withNewDataPath);
        runningMasterNodes.put(downMasterNode, true);
        numMasterNodesUp++;
        return numMasterNodesUp;
    }

    private String getRunningMasterNode(Map<String, Boolean> masterNodes) {
        for (Map.Entry<String, Boolean> masterNode : masterNodes.entrySet()) {
            if (masterNode.getValue() == true) {
                return masterNode.getKey();
            }
        }
        return null;
    }

    private String getDownMasterNode(Map<String, Boolean> masterNodes) {
        for (Map.Entry<String, Boolean> masterNode : masterNodes.entrySet()) {
            if (masterNode.getValue() == false) {
                return masterNode.getKey();
            }
        }
        return null;
    }

    private void startDataNode(String name, String color, boolean newDataPath) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder()
                .put("gateway.type", "local")
                .put("node.data", true)
                .put("node.master", false)
                .put("node.color", color)
                .put("node.name", name);
        if (newDataPath) {
            settingsBuilder.put("path.data", newTempDir().toString());
        }
        internalCluster().startNode(settingsBuilder.build());
    }

    private void startDataMasterNode(String name, String color, String explicitDataPath) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder()
                .put("gateway.type", "local")
                .put("node.data", true)
                .put("node.master", true)
                .put("node.color", color)
                .put("node.name", name);
        if (explicitDataPath != null) {
            settingsBuilder.put("path.data", explicitDataPath);
        }
        internalCluster().startNode(settingsBuilder.build());
    }

    private void startMasterNode(String name, boolean newDataPath) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder()
                .put("gateway.type", "local")
                .put("node.data", false)
                .put("node.master", true)
                .put("node.name", name);
        if (newDataPath) {
            settingsBuilder.put("path.data", newTempDir().toString());
        }
        internalCluster().startNode(settingsBuilder.build());
    }

    private void stopNode(String name) throws IOException {
        ((InternalTestCluster) cluster()).stopNode(name);
    }

    protected void assertIndexNotInMetaState(String nodeName, String indexName) throws Exception {
        assertMetaState(nodeName, indexName, false);
    }

    protected void assertIndexInMetaState(String nodeName, String indexName) throws Exception {
        assertMetaState(nodeName, indexName, true);
    }

    private void assertMetaState(String nodeName, String indexName, boolean shouldBe) throws Exception {
        LocalGatewayMetaState redNodeMetaState = ((InternalTestCluster) cluster()).getInstance(LocalGatewayMetaState.class, nodeName);
        MetaData redNodeMetaData = redNodeMetaState.loadMetaState();
        ImmutableOpenMap<String, IndexMetaData> indices = redNodeMetaData.getIndices();
        boolean inMetaSate = false;
        for (ObjectObjectCursor<String, IndexMetaData> index : indices) {
            inMetaSate = inMetaSate || index.key.equals(indexName);
        }
        if (shouldBe) {
            assertTrue("expected " + indexName + " in meta state of node " + nodeName, inMetaSate);
        } else {
            assertFalse("expected " + indexName + " to not be in meta state of node " + nodeName, inMetaSate);
        }
    }
}
