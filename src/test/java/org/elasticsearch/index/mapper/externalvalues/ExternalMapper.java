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

package org.elasticsearch.index.mapper.externalvalues;

import com.spatial4j.core.shape.Point;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.*;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;

/**
 * This mapper add a new sub fields
 * .bin Binary type
 * .bool Boolean type
 * .point GeoPoint type
 * .shape GeoShape type
 */
public class ExternalMapper extends AbstractFieldMapper<Object> {


    /**
     * Returns the actual value of the field.
     *
     * @param value
     */
    @Override
    public Object value(Object value) {
        return null;
    }

    public static class Names {
        public static final String FIELD_BIN = "bin";
        public static final String FIELD_BOOL = "bool";
        public static final String FIELD_POINT = "point";
        public static final String FIELD_SHAPE = "shape";
        public static final String FIELD_FLOAT = "float";
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, ExternalMapper> {

        private BinaryFieldMapper.Builder binBuilder = new BinaryFieldMapper.Builder(Names.FIELD_BIN);
        private BooleanFieldMapper.Builder boolBuilder = new BooleanFieldMapper.Builder(Names.FIELD_BOOL);
        private GeoPointFieldMapper.Builder pointBuilder = new GeoPointFieldMapper.Builder(Names.FIELD_POINT);
        private GeoShapeFieldMapper.Builder shapeBuilder = new GeoShapeFieldMapper.Builder(Names.FIELD_SHAPE);
        private FloatFieldMapper.Builder floatBuilder = new FloatFieldMapper.Builder(Names.FIELD_FLOAT);
        private StringFieldMapper.Builder stringBuilder;
        private String generatedValue;
        private String mapperName;

        public Builder(String name, String generatedValue, String mapperName) {
            super(name, new FieldType(Defaults.FIELD_TYPE));
            this.builder = this;
            this.stringBuilder = stringField(name).store(false);
            this.generatedValue = generatedValue;
            this.mapperName = mapperName;
        }

        public Builder string(StringFieldMapper.Builder content) {
            this.stringBuilder = content;
            return this;
        }

        @Override
        public ExternalMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(ContentPath.Type.FULL);

            context.path().add(name);
            BinaryFieldMapper binMapper = binBuilder.build(context);
            BooleanFieldMapper boolMapper = boolBuilder.build(context);
            GeoPointFieldMapper pointMapper = pointBuilder.build(context);
            GeoShapeFieldMapper shapeMapper = shapeBuilder.build(context);
            FloatFieldMapper floatMapper = floatBuilder.build(context);
            StringFieldMapper stringMapper = stringBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            return new ExternalMapper(buildNames(context), generatedValue, mapperName, binMapper, boolMapper, pointMapper, shapeMapper, floatMapper, stringMapper,
                    multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private String generatedValue;
        private String mapperName;

        TypeParser(String mapperName, String generatedValue) {
            this.mapperName = mapperName;
            this.generatedValue = generatedValue;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ExternalMapper.Builder builder = new ExternalMapper.Builder(name, generatedValue, mapperName);
            parseField(builder, name, node, parserContext);
            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String propName = Strings.toUnderscoreCase(entry.getKey());
                Object propNode = entry.getValue();

                parseMultiField(builder, name, node, parserContext, propName, propNode);
            }

            return builder;
        }
    }

    private final String generatedValue;
    private final String mapperName;

    private final BinaryFieldMapper binMapper;
    private final BooleanFieldMapper boolMapper;
    private final GeoPointFieldMapper pointMapper;
    private final GeoShapeFieldMapper shapeMapper;
    private final FloatFieldMapper floatMapper;
    private final StringFieldMapper stringMapper;

    public ExternalMapper(FieldMapper.Names names,
                          String generatedValue, String mapperName,
                          BinaryFieldMapper binMapper, BooleanFieldMapper boolMapper, GeoPointFieldMapper pointMapper,
                          GeoShapeFieldMapper shapeMapper, FloatFieldMapper floatMapper, StringFieldMapper stringMapper, MultiFields multiFields, CopyTo copyTo) {
        super(names, 1.0f, Defaults.FIELD_TYPE, false, null, null, null, null, null, null, null, ImmutableSettings.EMPTY,
                multiFields, copyTo);
        this.generatedValue = generatedValue;
        this.mapperName = mapperName;
        this.binMapper = binMapper;
        this.boolMapper = boolMapper;
        this.pointMapper = pointMapper;
        this.shapeMapper = shapeMapper;
        this.floatMapper = floatMapper;
        this.stringMapper = stringMapper;

    }

    @Override
    public FieldType defaultFieldType() {
        return Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        createField(context, null, new ValueAndBoost(null, 1.0f));
        if (copyTo != null) {
            copyTo.parse(context);
        }
    }

    @Override
    protected void createField(ParseContext context, List<Field> fields, ValueAndBoost valueAndBoost) throws IOException {
        byte[] bytes = "Hello world".getBytes(Charset.defaultCharset());
        binMapper.addValue(context, new ValueAndBoost(bytes, 1.0f));

        boolMapper.addValue(context, new ValueAndBoost(true, 1.0f));

        // Let's add a Dummy Point
        Double lat = 42.0;
        Double lng = 51.0;
        GeoPoint point = new GeoPoint(lat, lng);
        pointMapper.addValue(context, new ValueAndBoost(point, 1.0f));

        // Let's add a Dummy Shape
        Point shape = ShapeBuilder.newPoint(-100, 45).build();
        shapeMapper.addValue(context, new ValueAndBoost(shape, 1.0f));

        // add a dummy float value
        floatMapper.addValue(context, new ValueAndBoost(1.234f, 1.0f));

        // Let's add a Original String
        stringMapper.addValue(context, new ValueAndBoost(generatedValue, 1.0f));

        multiFields.addValue(this, context, new ValueAndBoost(generatedValue, 1.0f));
    }

    @Override
    protected ValueAndBoost parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // ignore this for now
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        binMapper.traverse(fieldMapperListener);
        boolMapper.traverse(fieldMapperListener);
        pointMapper.traverse(fieldMapperListener);
        shapeMapper.traverse(fieldMapperListener);
        floatMapper.traverse(fieldMapperListener);
        stringMapper.traverse(fieldMapperListener);

    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
        binMapper.close();
        boolMapper.close();
        pointMapper.close();
        shapeMapper.close();
        floatMapper.close();
        stringMapper.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", mapperName);
        multiFields.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected String contentType() {
        return mapperName;
    }
}
