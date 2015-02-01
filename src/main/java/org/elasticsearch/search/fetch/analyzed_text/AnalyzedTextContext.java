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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * All the required context to pull analyzed text from the term vectors.
 */
public class AnalyzedTextContext {

    public static class AnalyzedTextField {
        private final String name;
        private double idfThreshold;
        private double dfThreshold;
        private String tokenCountField;

        public AnalyzedTextField(String name, double idfThreshold, double dfThreshold, String tokenCountField) {
            this.name = name;
            this.idfThreshold = Math.exp(idfThreshold);
            this.dfThreshold = dfThreshold;
            this.tokenCountField = tokenCountField;
        }

        public String name() {
            return name;
        }

        public double getIdfThreshold() {
            return idfThreshold;
        }

        public double getDfThreshold() {
            return dfThreshold;
        }

        public String getTokenCountField() {
            return tokenCountField;
        }
    }

    private List<AnalyzedTextField> fields = Lists.newArrayList();

    public AnalyzedTextContext() {
    }

    public void add(AnalyzedTextField field) {
        this.fields.add(field);
    }

    public List<AnalyzedTextField> fields() {
        return this.fields;
    }
}
