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

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AllTermsResponse extends ActionResponse implements ToXContent {

    List<String> allTerms=new ArrayList<>();
    public AllTermsResponse() {

    }

    public AllTermsResponse(AllTermsSingleShardResponse[] responses, long size) {
        for(AllTermsSingleShardResponse singleShardResponse : responses) {
            for(String term : singleShardResponse.shardTerms) {
                allTerms.add(term);
            }
        }
        Collections.sort(allTerms);
        allTerms = allTerms.subList(0, Math.min((int)size, allTerms.size()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TERMS, allTerms);
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString TERMS = new XContentBuilderString("docs");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        allTerms = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(allTerms.toArray(new String[allTerms.size()]));
    }
}