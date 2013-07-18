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

package org.elasticsearch.index.query.functionscoring.distancescoring;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscoring.ScoreFunctionBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiplyingFunctionBuilder implements ScoreFunctionBuilder {

    List<Var> vars = new ArrayList<Var>();
    public static String REFERNECE = "reference";
    public static String SCALE = "scale";
    public static String SCALE_WEIGHT = "scale_weight";
    public static String SCALE_DEFAULT = "0.5";

    public void addVariable(String fieldName, String reference, String scale, String scaleWeight) {
        vars.add(new Var(fieldName, reference, scale, scaleWeight));
    }

    public void addVariable(String fieldName, String reference, String scale) {
        addVariable(fieldName, reference, scale, SCALE_DEFAULT);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (Var var : vars) {
            builder.field(var.fieldName);
            builder.startObject();
            builder.field(REFERNECE, var.reference);
            builder.field(SCALE, var.scale);
            builder.field(SCALE_WEIGHT, var.scaleWeight);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    class Var {
        String fieldName;
        String reference;
        String scale;
        String scaleWeight;

        public Var(String fieldName, String reference, String scale, String scaleWeight) {
            this.fieldName = fieldName;
            this.reference = reference;
            this.scale = scale;
            this.scaleWeight = scaleWeight;
        }
    }

    public void addGeoVariable(String fieldName, double lat, double lon, String scale) {
        addGeoVariable(fieldName, lat, lon, scale, SCALE_DEFAULT);
    }

    public void addGeoVariable(String fieldName, double lat, double lon, String scale, String scaleWeight) {
        String geoLoc = Double.toString(lat) + ", " + Double.toString(lon);
        addVariable(fieldName, geoLoc, scale, scaleWeight);
    }
}