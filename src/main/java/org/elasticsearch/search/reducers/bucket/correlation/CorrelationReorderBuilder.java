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

package org.elasticsearch.search.reducers.bucket.correlation;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.ReductionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CorrelationReorderBuilder extends ReductionBuilder<CorrelationReorderBuilder> {

    private CorrelationReorderParser.CurveXY reference = null;
    private List<CorrelationReorderParser.CurveXY> curves = new ArrayList<>();

    public CorrelationReorderBuilder(String name) {
        super(name, InternalCorrelationReorder.TYPE.name());
    }

    public CorrelationReorderBuilder reference(String ys, String xs) {
        this.reference = new CorrelationReorderParser.CurveXY(xs, ys);
        return this;
    }

    public CorrelationReorderBuilder curve(String ys, String xs) {
        this.curves.add(new CorrelationReorderParser.CurveXY(xs, ys));
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (reference != null) {
            builder.field(CorrelationReorderParser.REFERENCE_FIELD.getPreferredName());
            toXContent(builder, reference);
        }
        if (curves != null) {
            builder.startArray(CorrelationReorderParser.CURVES_FIELD.getPreferredName());
            for (CorrelationReorderParser.CurveXY curve : curves) {
                toXContent(builder, curve);
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    private void toXContent(XContentBuilder builder, CorrelationReorderParser.CurveXY curveXY) throws IOException {
        builder.startObject();
        builder.field(CorrelationReorderParser.XS_FIELD.getPreferredName(), curveXY.getXPath());
        builder.field(CorrelationReorderParser.YS_FIELD.getPreferredName(), curveXY.getYPath());
        builder.endObject();
    }
}
