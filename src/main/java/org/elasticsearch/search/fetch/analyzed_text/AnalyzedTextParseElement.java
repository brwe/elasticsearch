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
package org.elasticsearch.search.fetch.analyzed_text;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parses field names and thresholds from the {@code abalyzed_text} parameter in a
 * search request.
 * <p/>
 * <pre>
 *  GET movie-reviews/_search
 *  {
 *      "analyzed_text": [
 *      {
 *          "field": "text",  <- the field for which we want the analyzed text
 *          "idf_threshold": 0.5, <- skip terms with idf below idf_threshold (the, a, who, ....)
 *          "df_threshold": 5 <- skip terms with df below df_threshold because they are so rare that they might be typos or uninteresting
 *      },
 *      ...
 *      ]
 *  }
 * </pre>
 */
public class AnalyzedTextParseElement implements SearchParseElement {
    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new ElasticsearchParseException("Must be an array of objects");
                }
                String analyzedFieldName = null;
                float idf_threshold = 0;
                float df_threshold = 0;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    String fieldName;
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.text();
                    } else {
                        throw new ElasticsearchParseException("Excepted a field name");
                    }
                    parser.nextToken();

                    if (fieldName.equals("field")) {
                        analyzedFieldName = parser.text();
                    } else if (fieldName.equals("idf_threshold")) {
                        idf_threshold = parser.floatValue();
                    } else if (fieldName.equals("df_threshold")) {
                        df_threshold = parser.floatValue();
                    }else {
                        throw new ElasticsearchParseException("no field with name " + fieldName + "known");
                    }

                }
                context.analyzedTextFields().add(new AnalyzedTextContext.AnalyzedTextField(analyzedFieldName, idf_threshold, df_threshold));
            }
        } else {
            throw new ElasticsearchIllegalStateException("Expected START_ARRAY but got " + token);
        }
    }
}
