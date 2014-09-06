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

package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;


public class DeleteMappingTests extends ElasticsearchIntegrationTest {

    private void createIndices() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("test_type1").endObject().endObject();
        assertAcked(prepareCreate("test_index1").addMapping("test_type1", mapping));
        mapping = jsonBuilder().startObject().startObject("test_type2").endObject().endObject();
        assertAcked(prepareCreate("test_index2").addMapping("test_type2", mapping));
        assertAcked(prepareCreate("foo").addMapping("test_type2", mapping));
        ensureYellow();

    }

    @Test
    public void testDeleteTypeWith_all() throws Exception {
        createIndices();
        client().admin().indices().prepareDeleteMapping("_all").setType("test_type2").get();

        TypesExistsResponse response = client().admin().indices().prepareTypesExists("test_index1").setTypes("test_type1").get();
        assertTrue(response.isExists());

        response = client().admin().indices().prepareTypesExists("test_index2").setTypes("test_type2").get();
        assertFalse(response.isExists());

        response = client().admin().indices().prepareTypesExists("foo").setTypes("test_type2").get();
        assertFalse(response.isExists());

    }

    @Test
    public void testDeleteTypeWithIndicesWildcard() throws Exception {
        createIndices();
        client().admin().indices().prepareDeleteMapping("*").setType("test_type2").get();

        TypesExistsResponse response = client().admin().indices().prepareTypesExists("test_index1").setTypes("test_type1").get();
        assertTrue(response.isExists());

        response = client().admin().indices().prepareTypesExists("test_index2").setTypes("test_type2").get();
        assertFalse(response.isExists());

        response = client().admin().indices().prepareTypesExists("foo").setTypes("test_type2").get();
        assertFalse(response.isExists());
    }

    @Test
    public void testDeleteIndexListAnd_allType() throws Exception {
        createIndices();
        client().admin().indices().prepareDeleteMapping("test_index1", "test_index2").setType("_all").get();

        TypesExistsResponse response = client().admin().indices().prepareTypesExists("test_index1").setTypes("test_type1").get();
        assertFalse(response.isExists());

        response = client().admin().indices().prepareTypesExists("test_index2").setTypes("test_type2").get();
        assertFalse(response.isExists());

        response = client().admin().indices().prepareTypesExists("foo").setTypes("test_type2").get();
        assertTrue(response.isExists());
    }

    @Test
    public void testDeleteTypeWithListOfIndices() throws Exception {
        createIndices();
        client().admin().indices().prepareDeleteMapping("test_index2", "foo").setType("test_type2").get();

        TypesExistsResponse response = client().admin().indices().prepareTypesExists("test_index1").setTypes("test_type1").get();
        assertTrue(response.isExists());

        response = client().admin().indices().prepareTypesExists("test_index2").setTypes("test_type2").get();
        assertFalse(response.isExists());

        response = client().admin().indices().prepareTypesExists("foo").setTypes("test_type2").get();
        assertFalse(response.isExists());
    }
}
