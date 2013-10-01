/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.payloads.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class DelimitedPayloadTokenFilterFactory extends AbstractTokenFilterFactory {

    char delimiter = '|';
    PayloadEncoder encoder = new FloatEncoder();
    static String enodeString = "encoding";

    @Inject
    public DelimitedPayloadTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, @Assisted String name,
            @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        String delimiterConf = settings.get("delimiter");
        if (delimiterConf != null) {
            delimiter = delimiterConf.charAt(0);
        }
        if (settings.get(enodeString) != null) {
            if (settings.get(enodeString).equals("float")) {
                encoder = new FloatEncoder();
            } else if (settings.get(enodeString).equals("int")) {
                encoder = new IntegerEncoder();
            } else if (settings.get(enodeString).equals("identity")) {
                encoder = new IdentityEncoder();
            } 
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        DelimitedPayloadTokenFilter filter = new DelimitedPayloadTokenFilter(tokenStream, delimiter, encoder);
        return filter;
    }
}
