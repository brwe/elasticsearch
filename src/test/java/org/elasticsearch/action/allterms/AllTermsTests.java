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

package org.elasticsearch.action.allterms;

import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

public class AllTermsTests extends ElasticsearchIntegrationTest {

    @Test
    public void testSimpleTestOneDoc() throws Exception {
        client().prepareIndex("test", "type", "1").setSource("field", "foo bar").execute().actionGet();
        client().prepareIndex("test", "type", "2").setSource("field", "I am sam bar").execute().actionGet();
        client().prepareIndex("test", "type", "3").setSource("field", "blah blah").execute().actionGet();
        client().prepareIndex("test", "type", "4").setSource("field", "I am blah blah foo bar sam bar").execute().actionGet();
        refresh();
        AllTermsResponse response = client().prepareAllTerms().index("test").field("field").size(10).execute().actionGet(1000000);
        String[] expected = {"am", "bar", "blah", "foo", "i", "sam"};
        assertArrayEquals(response.allTerms.toArray(new String[2]), expected);
    }

}
