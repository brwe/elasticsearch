package org.elasticsearch.action.termvector;
/*
 * Licensed to ElasticSearch under one
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


import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.action.termvector.TermVectorRequest.Flag;

public class MultiTermVectorsRequest extends ActionRequest<MultiTermVectorsRequest> {

    /**
     * A single Term term vector item.
     */
    public static class Item implements Streamable {
        private String index;
        private String type;
        private String id;
        private String routing;
        private String[] selectedFields;

        private EnumSet<Flag> flagsEnum = EnumSet.of(Flag.Positions, Flag.Offsets, Flag.Payloads,
                Flag.FieldStatistics);

        public Item(String index, @Nullable String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        Item() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            index = in.readString();
            type = in.readOptionalString();
            id = in.readString();
            routing = in.readOptionalString();
            long flags = in.readVLong();

            flagsEnum.clear();
            for (Flag flag : Flag.values()) {
                if ((flags & (1 << flag.ordinal())) != 0) {
                    flagsEnum.add(flag);
                }
            }

            selectedFields = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalString(type);
            out.writeString(id);
            out.writeOptionalString(routing);
            long longFlags = 0;
            for (Flag flag : flagsEnum) {
                longFlags |= (1 << flag.ordinal());
            }
            out.writeVLong(longFlags);
            out.writeStringArrayNullable(selectedFields);
        }

        public String index() {
            return index;
        }

        public String type() {
            return type;
        }

        public String id() {
            return id;
        }

        public String routing() {
            return routing;
        }

        public Item routing(String routing) {
            this.routing = routing;
            return this;
        }

        public String[] selectedFields() {
            return selectedFields;
        }

        public Item selectedFields(String[] selectedFields) {
            this.selectedFields = selectedFields;
            return this;
        }

        public static Item readItem(StreamInput in) throws IOException {
            Item item = new Item();
            item.readFrom(in);
            return item;
        }
    }

    String preference;
    List<Item> items = new ArrayList<Item>();

    public MultiTermVectorsRequest add(Item item) {
        items.add(item);
        return this;
    }

    public MultiTermVectorsRequest add(String index, @Nullable String type, String id) {
        items.add(new Item(index, type, id));
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiTermVectorsRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (items.isEmpty()) {
            validationException = ValidateActions.addValidationError("no documents to get", validationException);
        } else {
            for (int i = 0; i < items.size(); i++) {
                Item item = items.get(i);
                if (item.index() == null) {
                    validationException = ValidateActions.addValidationError("index is missing for doc " + i, validationException);
                }
                if (item.id() == null) {
                    validationException = ValidateActions.addValidationError("id is missing for doc " + i, validationException);
                }
            }
        }
        return validationException;
    }

    public void add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, byte[] data, int from, int length) throws Exception {
        add(defaultIndex, defaultType, defaultFields, new BytesArray(data, from, length));
    }

    public void add(@Nullable String defaultIndex, @Nullable String defaultType, @Nullable String[] defaultFields, BytesReference data) throws Exception {
        XContentParser parser = XContentFactory.xContent(data).createParser(data);
        try {
            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("docs".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token != XContentParser.Token.START_OBJECT) {
                                throw new ElasticSearchIllegalArgumentException("docs array element should include an object");
                            }
                            String index = defaultIndex;
                            String type = defaultType;
                            String id = null;
                            String routing = null;
                            List<String> fields = null;
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if ("_index".equals(currentFieldName)) {
                                        index = parser.text();
                                    } else if ("_type".equals(currentFieldName)) {
                                        type = parser.text();
                                    } else if ("_id".equals(currentFieldName)) {
                                        id = parser.text();
                                    } else if ("_routing".equals(currentFieldName) || "routing".equals(currentFieldName)) {
                                        routing = parser.text();
                                    }
                                } else if (token == XContentParser.Token.START_ARRAY) {
                                    if ("fields".equals(currentFieldName)) {
                                        fields = new ArrayList<String>();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            fields.add(parser.text());
                                        }
                                    }
                                }
                            }
                            String[] aFields;
                            if (fields != null) {
                                aFields = fields.toArray(new String[fields.size()]);
                            } else {
                                aFields = defaultFields;
                            }
                            add(new Item(index, type, id).routing(routing).selectedFields(aFields));
                        }
                    } else if ("ids".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (!token.isValue()) {
                                throw new ElasticSearchIllegalArgumentException("ids array element should only contain ids");
                            }
                            add(new Item(defaultIndex, defaultType, parser.text()).selectedFields(defaultFields));
                        }
                    }
                }
            }
        } finally {
            parser.close();
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        preference = in.readOptionalString();
        int size = in.readVInt();
        items = new ArrayList<Item>(size);
        for (int i = 0; i < size; i++) {
            items.add(Item.readItem(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(preference);
        out.writeVInt(items.size());
        for (Item item : items) {
            item.writeTo(out);
        }
    }
}
