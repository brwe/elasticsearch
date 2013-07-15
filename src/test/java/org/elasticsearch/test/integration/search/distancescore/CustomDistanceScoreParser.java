package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.distancescoring.multiplydistancescores.CustomDecayFuntion;
import org.elasticsearch.index.query.distancescoring.multiplydistancescores.MultiplyingFunctionParser;

import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;

public class CustomDistanceScoreParser extends MultiplyingFunctionParser {

    public static String NAME = "linear_mult";

    @Override
    public String getName() {
        return NAME;
    }

    static CustomDecayFuntion distanceFunction;

    public CustomDistanceScoreParser() {
        distanceFunction = new LinearMultScoreFunction();
    }

    @Override
    public CustomDecayFuntion getDecayFunction() {
        return distanceFunction;
    }

    static class LinearMultScoreFunction implements CustomDecayFuntion {
        LinearMultScoreFunction() {
        }

        @Override
        public double evaluate(double value, double scale) {
            return (float) Math.abs(value);
        }

        @Override
        public Explanation explainFunction(String distanceString, double distanceVal, double scale) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setDescription("No explanaition");
            return ce;
        }
    }
}
