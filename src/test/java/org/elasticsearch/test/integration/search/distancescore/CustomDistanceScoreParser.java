package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.functionscoring.multiplydistancescores.MultiplyingFunctionParser;

import org.elasticsearch.index.query.functionscoring.multiplydistancescores.CustomDecayFunction;

import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;

public class CustomDistanceScoreParser extends MultiplyingFunctionParser {

    public static String NAME = "linear_mult";

    @Override
    public String getName() {
        return NAME;
    }

    static CustomDecayFunction distanceFunction;

    public CustomDistanceScoreParser() {
        distanceFunction = new LinearMultScoreFunction();
    }

    @Override
    public CustomDecayFunction getDecayFunction() {
        return distanceFunction;
    }

    static class LinearMultScoreFunction implements CustomDecayFunction {
        LinearMultScoreFunction() {
        }

        @Override
        public double evaluate(double value, double scale) {
            return Math.abs(value);
        }

        @Override
        public Explanation explainFunction(String distanceString, double distanceVal, double scale) {
            ComplexExplanation ce = new ComplexExplanation();
            ce.setDescription("" + distanceVal);
            return ce;
        }

        @Override
        public double processScale(double userGivenScale, double userGivenValue) {
            return userGivenScale;
        }
    }
}
