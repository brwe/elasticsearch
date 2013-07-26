package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.functionscoring.distancescoring.DistanceFunctionBuilder;

public class CustomDistanceScoreBuilder extends DistanceFunctionBuilder {


    @Override
    public String getName() {
        return CustomDistanceScoreParser.NAMES[0];
    }

}
