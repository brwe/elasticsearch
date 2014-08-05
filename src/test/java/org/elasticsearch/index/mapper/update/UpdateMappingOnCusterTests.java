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

package org.elasticsearch.index.mapper.update;

import com.google.common.base.Predicate;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class UpdateMappingOnCusterTests extends ElasticsearchIntegrationTest {

    private static final String INDEX = "index";
    private static final String TYPE = "type";


    @Test
    public void test_field_type_change() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        XContentBuilder mappingUpdate = jsonBuilder().startObject().startObject("properties").startObject("text").field("type", "float").endObject().startObject("num").field("type", "float").endObject().endObject().endObject();
        String errorMessage = "[mapper [text] of different type, current_type [string], merged_type [float]";
        testConflict(mapping, mappingUpdate, errorMessage);
    }

    @Test
    public void test_all_enabled() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_all").field("enabled", "false").endObject().endObject();
        XContentBuilder mappingUpdate = jsonBuilder().startObject().startObject("_all").field("enabled", "true").endObject().startObject("properties").startObject("text").field("type", "string").endObject().endObject().endObject();
        String errorMessage = "cannot merge _all: enabled is false now encountering true";
        testConflict(mapping, mappingUpdate, errorMessage);
    }

    @Test
    public void test_dynamic() throws Exception {
        Map<String, Object> fields = getFieldAsMap();
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("dynamic", "false");
        Map<String, Object> mappingUpdate = new HashMap<>();
        mapping.put("dynamic", "true");
        mappingUpdate.put("properties", fields);
        testNoConflict(mapping, mappingUpdate);
    }

    protected Map<String, Object> getFieldAsMap() {
        Map<String, Object> fields = new HashMap<>();
        Map<String, Object> type = new HashMap<>();
        type.put("type", "string");
        fields.put("text", type);
        return fields;
    }

    private void testNoConflict(Map<String, Object> mapping, Map<String, Object> mappingUpdate) throws InterruptedException, IOException {
        assertAcked(prepareCreate(INDEX).addMapping(TYPE, mapping).get());
        ensureGreen(INDEX);
        client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mappingUpdate).get();

        // make sure all nodes have same cluster state
        // TODO: is this actually needed?
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                List<Long> states = new ArrayList<>();
                for (Client client : cluster()) {
                    ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
                    states.add(clusterStateResponse.getState().getVersion());
                }
                boolean clusterStateHasPropagated = true;
                for (Long state : states) {
                    clusterStateHasPropagated = clusterStateHasPropagated && state.equals(states.get(0));
                }
                return clusterStateHasPropagated;
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));

        for (Client client : cluster()) {
            GetMappingsResponse mappingsAfterUpdateResponse = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            for (Map.Entry<String, Object> entry : mappingUpdate.entrySet()) {
                assertThat(mappingUpdate.get(entry.getKey()), equalTo(mappingsAfterUpdateResponse.getMappings().get(INDEX).get(TYPE).getSourceAsMap().get(entry.getKey())));
            }
        }
    }

    @Test
    public void randomMappingConflictTest() throws IOException, InterruptedException {
        Map<String, Object> rootTypes = getRootTypes();
        Map<String, Object> conflictingRootTypes = getConflictingRootTypes();
        List<String> list = Arrays.asList(conflictingRootTypes.keySet().toArray(new String[conflictingRootTypes.size() - 1]));
        String conflictingRootType = list.get(10);//randomInt(conflictingRootTypes.size() - 1));
        XContentBuilder mapping = jsonBuilder();
        mapping.map(rootTypes);
        rootTypes.put(conflictingRootType, conflictingRootTypes.get(conflictingRootType));
        logger.info("Testing conflict for: " + conflictingRootType);
        XContentBuilder mappingUpdate = jsonBuilder();
        mappingUpdate.map(rootTypes);
        testConflict(mapping, mappingUpdate, "onflict");
    }


    protected Map<String, Object> getRootTypes() {
        Map<String, Object> rootTypes = new HashMap<>();
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("enabled", true);
        rootTypes.put(SizeFieldMapper.NAME, valueMap);
        rootTypes.put(IndexFieldMapper.NAME, valueMap);
        rootTypes.put(SourceFieldMapper.NAME, valueMap);
        valueMap.clear();
        valueMap.put("store", true);
        rootTypes.put(TypeFieldMapper.NAME, valueMap);
        rootTypes.put(FieldNamesFieldMapper.NAME, valueMap);
        rootTypes.put("include_in_all", "true");
        rootTypes.put("index_analyzer", "standard");
        rootTypes.put("search_analyzer", "standard");
        rootTypes.put("analyzer", "standard");
        String[] dateTypes = {"dd-MM-yyyy"};
        rootTypes.put("dynamic_date_formats", dateTypes);
        rootTypes.put("numeric_detection", "true");
        valueMap.clear();
        valueMap.put("match", "*");
        HashMap<String, Object> typeValueMap = new HashMap<>();
        typeValueMap.put("type", "float");
        valueMap.put("mapping", typeValueMap);
        HashMap<String, Object> templateMap = new HashMap<>();
        templateMap.put("template_name", valueMap);
        Object[] dynamic_template = {templateMap};
        rootTypes.put("dynamic_templates", dynamic_template);
        return rootTypes;
    }

    protected Map<String, Boolean> getConflictOrNot() {
        Map<String, Boolean> conflictOrNot = new HashMap<>();

        /*conflictOrNot.put(SizeFieldMapper.NAME, valueMap);
        rootTypes.put(IndexFieldMapper.NAME, valueMap);
        rootTypes.put(SourceFieldMapper.NAME, valueMap);
        valueMap.clear();
        valueMap.put("store", true);
        rootTypes.put(TypeFieldMapper.NAME, valueMap);
        rootTypes.put(FieldNamesFieldMapper.NAME, valueMap);
        rootTypes.put("include_in_all", "true");
        rootTypes.put("index_analyzer", "standard");
        rootTypes.put("search_analyzer", "standard");
        rootTypes.put("analyzer", "standard");
        String[] dateTypes = {"dd-MM-yyyy"};
        rootTypes.put("dynamic_date_formats", dateTypes);
        rootTypes.put("numeric_detection", "true");
        valueMap.clear();
        valueMap.put("match", "*");
        HashMap<String, Object> typeValueMap = new HashMap<>();
        typeValueMap.put("type", "float");
        valueMap.put("mapping", typeValueMap);
        HashMap<String, Object> templateMap = new HashMap<>();
        templateMap.put("template_name", valueMap);
        Object[] dynamic_template = {templateMap};
        rootTypes.put("dynamic_templates", dynamic_template);
        return rootTypes;*/
        return conflictOrNot;
    }

    protected Map<String, Object> getConflictingRootTypes() {
        Map<String, Object> rootTypes = new HashMap<>();
        HashMap<String, Object> valueMap = new HashMap<>();
        valueMap.put("enabled", false);
        rootTypes.put(SizeFieldMapper.NAME, valueMap);
        rootTypes.put(IndexFieldMapper.NAME, valueMap);
        rootTypes.put(SourceFieldMapper.NAME, valueMap);
        valueMap.clear();
        valueMap.put("store", false);
        rootTypes.put(TypeFieldMapper.NAME, valueMap);
        rootTypes.put(FieldNamesFieldMapper.NAME, valueMap);
        rootTypes.put("include_in_all", "false");
        rootTypes.put("index_analyzer", "keyword");
        rootTypes.put("search_analyzer", "keyword");
        rootTypes.put("analyzer", "keyword");
        String[] dateTypes = {"yyyy-MM-dd"};
        rootTypes.put("dynamic_date_formats", dateTypes);
        rootTypes.put("numeric_detection", "false");
        valueMap.clear();
        valueMap.put("match", "*");
        HashMap<String, Object> typeValueMap = new HashMap<>();
        typeValueMap.put("type", "string");
        valueMap.put("mapping", typeValueMap);
        HashMap<String, Object> templateMap = new HashMap<>();
        templateMap.put("template_name", valueMap);
        Object[] dynamic_template = {templateMap};
        rootTypes.put("dynamic_templates", dynamic_template);
        return rootTypes;
    }

    protected void testConflict(XContentBuilder mapping, XContentBuilder mappingUpdate, String errorMessage) throws InterruptedException {
        assertAcked(prepareCreate(INDEX).addMapping(TYPE, mapping).get());
        ensureGreen(INDEX);
        GetMappingsResponse mappingsBeforeUpdateResponse = client().admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).get();
        try {
            client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mappingUpdate).get();
            fail();
        } catch (MergeMappingException e) {
            assertThat(e.getDetailedMessage(), containsString(errorMessage));
        }

        // make sure all nodes have same cluster state
        // TODO: is this actually needed?
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                List<Long> states = new ArrayList<>();
                for (Client client : cluster()) {
                    ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().setLocal(true).get();
                    states.add(clusterStateResponse.getState().getVersion());
                }
                boolean clusterStateHasPropagated = true;
                for (Long state : states) {
                    clusterStateHasPropagated = clusterStateHasPropagated && state.equals(states.get(0));
                }
                return clusterStateHasPropagated;
            }
        }, 10, TimeUnit.SECONDS), equalTo(true));

        for (Client client : cluster()) {
            GetMappingsResponse mappingsAfterUpdateResponse = client.admin().indices().prepareGetMappings(INDEX).addTypes(TYPE).setLocal(true).get();
            assertThat(mappingsBeforeUpdateResponse.getMappings().get(INDEX).get(TYPE).source(), equalTo(mappingsAfterUpdateResponse.getMappings().get(INDEX).get(TYPE).source()));
        }
    }

}
