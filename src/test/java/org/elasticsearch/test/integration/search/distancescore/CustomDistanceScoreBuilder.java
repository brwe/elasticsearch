package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.functionscoring.multiplydistancescores.MultiplyingFunctionBuilder;

public class CustomDistanceScoreBuilder extends MultiplyingFunctionBuilder {


    @Override
    public String getName() {
        return CustomDistanceScoreParser.NAMES[0];
    }

}
