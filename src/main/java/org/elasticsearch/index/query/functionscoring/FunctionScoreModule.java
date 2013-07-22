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
package org.elasticsearch.index.query.functionscoring;

import org.elasticsearch.index.query.functionscoring.distancescoring.ExponentialDecayFunctionParser;
import org.elasticsearch.index.query.functionscoring.distancescoring.GaussDecayFunctionParser;
import org.elasticsearch.index.query.functionscoring.distancescoring.LinearDecayFunctionParser;

import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.Multibinder;
import org.elasticsearch.index.query.functionscoring.customboostscoring.CustomBoostFactorQueryParser;
import org.elasticsearch.index.query.functionscoring.customscriptscoring.CustomScoreQueryParser;

import java.util.List;

/**
 *
 */
public class FunctionScoreModule extends AbstractModule {

    private List<Class<? extends ScoreFunctionParser>> parsers = Lists.newArrayList();

    public FunctionScoreModule() {
        registerParser(LinearDecayFunctionParser.class);
        registerParser(GaussDecayFunctionParser.class);
        registerParser(ExponentialDecayFunctionParser.class);
        registerParser(CustomBoostFactorQueryParser.class);
        registerParser(CustomScoreQueryParser.class);
    }

    public void registerParser(Class<? extends ScoreFunctionParser> parser) {
        parsers.add(parser);
    }

    @Override
    protected void configure() {
        Multibinder<ScoreFunctionParser> parserMapBinder = Multibinder.newSetBinder(binder(), ScoreFunctionParser.class);
        for (Class<? extends ScoreFunctionParser> clazz : parsers) {
            parserMapBinder.addBinding().to(clazz);
        }
        bind(ScoreFunctionParserMapper.class);
    }
}
