package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.functionscoring.multiplydistancescores.MultiplyingFunctionBuilder;

public class CustomDistanceScoreBuilder extends MultiplyingFunctionBuilder {

    private static String NAME = new String("linear_mult");

    @Override
    public String getName() {
        return NAME;
    }

}
