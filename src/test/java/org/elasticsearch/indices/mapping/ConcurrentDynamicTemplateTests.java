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


import com.google.common.collect.Sets;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;

public class ConcurrentDynamicTemplateTests extends ElasticsearchIntegrationTest {

    private final String mappingType = "test-mapping";

    @Test // see #3544
    public void testConcurrentDynamicMapping() throws Exception {
        final String fieldName = "field";
        final String mapping = "{ \"" + mappingType + "\": {" +
                "\"dynamic_templates\": ["
                + "{ \"" + fieldName + "\": {" + "\"path_match\": \"*\"," + "\"mapping\": {" + "\"type\": \"string\"," + "\"store\": \"yes\","
                + "\"index\": \"analyzed\", \"analyzer\": \"whitespace\" } } } ] } }";
        // The 'fieldNames' array is used to help with retrieval of index terms
        // after testing

        int iters = scaledRandomIntBetween(5, 15);
        for (int i = 0; i < iters; i++) {
            cluster().wipeIndices("test");
            assertAcked(prepareCreate("test")
                    .addMapping(mappingType, mapping));
            ensureYellow();
            int numDocs = scaledRandomIntBetween(10, 100);
            final CountDownLatch latch = new CountDownLatch(numDocs);
            final List<Throwable> throwable = new CopyOnWriteArrayList<>();
            int currentID = 0;
            for (int j = 0; j < numDocs; j++) {
                Map<String, Object> source = new HashMap<>();
                source.put(fieldName, "test-user");
                client().prepareIndex("test", mappingType, Integer.toString(currentID++)).setSource(source).execute(new ActionListener<IndexResponse>() {
                    @Override
                    public void onResponse(IndexResponse response) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        throwable.add(e);
                        latch.countDown();
                    }
                });
            }
            latch.await();
            assertThat(throwable, emptyIterable());
            refresh();
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test-user")).get(), numDocs);
            assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.matchQuery(fieldName, "test user")).get(), 0);

        }
    }



    @Test // see #3544
    public void testConcurrentDynamicMappingIssue3544() throws Exception {

        String setingsSource =
                "    {\"analysis\": {\n" +
                "      \"analyzer\": {\n" +
                "        \"nGram_analyzer\": {\n" +
                "          \"alias\": \"default_index\",\n" +
                "          \"tokenizer\": \"nGram_tokenizer\",\n" +
                "          \"filter\": [\n" +
                "            \"lowercase\",\n" +
                "            \"asciifolding\"\n" +
                "          ]\n" +
                "        },\n" +
                "        \"_autocomplete_en\": {\n" +
                "          \"tokenizer\": \"standard\",\n" +
                "          \"filter\": [\n" +
                "            \"lowercase\"\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"tokenizer\": {\n" +
                "        \"nGram_tokenizer\": {\n" +
                "          \"type\": \"nGram\",\n" +
                "          \"min_gram\": 3,\n" +
                "          \"max_gram\": 25,\n" +
                "          \"token_chars\": [ \"letter\", \"digit\", \"symbol\", \"punctuation\" ]\n" +
                "        }\n" +
                "      }" +
                "    }}\n" ;

        String page_mapping= "{\n" +
                "  \"page\": {\n" +
                "    \"properties\": {\n" +
                "      \"_lang\": {\n" +
                "        \"type\": \"string\",\n" +
                "        \"index\": \"not_analyzed\"\n" +
                "      }\n" +
                "    },\n" +
                "    \"dynamic_templates\" : [\n" +
                "      {\n" +
                "        \"autocomplete_for_lang\" : {\n" +
                "          \"match\" : \"_autocomplete_*\",\n" +
                "          \"mapping\" : {\n" +
                "            \"type\" : \"string\",\n" +
                "            \"index_analyzer\" : \"{name}\",\n" +
                "            \"search_analyzer\": \"standard\"\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ]"+
                "  }\n" +
                "}";

        String doc = "{\n" +
                "  \"_autocomplete_en\": \"this is some content to reproduce the behavior elasticsearch\"\n" +
                "}\n";
        String searchSource = "{\n" +
                "  \"query\": {\n" +
                "    \"match_all\": { }\n" +
                "  },\n" +
                "  \"facets\" : {\n" +
                "    \"all_autocomplete\" : {\n" +
                "      \"terms\" : {\n" +
                "        \"field\": \"_autocomplete_en\",\n" +
                "        \"regex\": \"^s.*\", \n" +
                "        \"size\": 5\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        client().admin().indices().prepareCreate("test_dynamic_template").setSettings(setingsSource).addMapping("page", page_mapping).get();

       // client().admin().indices().preparePutMapping("test_dynamic_template").setType("page").setSource(page_mapping).get();
        ensureGreen();
        for (int i = 0; i< 2; i++) {
            client().prepareIndex("test_dynamic_template", "page").setSource(doc).get();
            refresh();
        }



        SearchResponse response = client().prepareSearch("test_dynamic_template").setSource(searchSource).get();
        assertThat(((TermsFacet)response.getFacets().facetsAsMap().get("all_autocomplete")).getEntries().size(), equalTo(1));


    }


    @Test
    public void testDynamicMappingIntroductionPropagatesToAll() throws Exception {
        int numDocs = randomIntBetween(100, 1000);
        int numberOfFields = scaledRandomIntBetween(1, 50);
        Set<Integer> fieldsIdx = Sets.newHashSet();
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];

        XContentBuilder mappings = XContentFactory.jsonBuilder().startObject().startObject("_default_");
        mappings.startArray("dynamic_templates")
                .startObject()
                .startObject("template-strings")
                .field("match_mapping_type", "string")
                .startObject("mapping")
                .startObject("fielddata")
                .field(FieldDataType.FORMAT_KEY, randomFrom("paged_bytes", "fst"))
                .field(FieldMapper.Loading.KEY, FieldMapper.Loading.LAZY) // always use lazy loading
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .startObject("template-longs")
                .field("match_mapping_type", "long")
                .startObject("mapping")
                .startObject("fielddata")
                .field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values"))
                .field(FieldMapper.Loading.KEY, FieldMapper.Loading.LAZY)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .startObject("template-doubles")
                .field("match_mapping_type", "double")
                .startObject("mapping")
                .startObject("fielddata")
                .field(FieldDataType.FORMAT_KEY, randomFrom("array", "doc_values"))
                .field(FieldMapper.Loading.KEY, FieldMapper.Loading.LAZY)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endArray();
        mappings.endObject().endObject();

        assertAcked(client().admin().indices().prepareCreate("idx").addMapping("_default_", mappings));
        ensureGreen("idx");
        for (int i = 0; i < numDocs; ++i) {
            int fieldIdx = i % numberOfFields;
            fieldsIdx.add(fieldIdx);
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field("str_value_" + fieldIdx, "s" + i)
                    .field("l_value_" + fieldIdx, i)
                    .field("d_value_" + fieldIdx, (double)i + 0.01)
                    .endObject());
        }
        indexRandom(false, builders);
        for (Integer fieldIdx : fieldsIdx) {
            waitForConcreteMappingsOnAll("idx", "type", "str_value_" + fieldIdx, "l_value_" + fieldIdx, "d_value_" + fieldIdx);
        }
    }
}