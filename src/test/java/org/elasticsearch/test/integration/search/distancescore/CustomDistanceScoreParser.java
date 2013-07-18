package org.elasticsearch.test.integration.search.distancescore;

import org.elasticsearch.index.query.functionscoring.distancescoring.CustomDecayFunction;
import org.elasticsearch.index.query.functionscoring.distancescoring.MultiplyingFunctionParser;


import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;

public class CustomDistanceScoreParser extends MultiplyingFunctionParser {

    public static String[] NAMES = {"linear_mult"};

    @Override
    public String[] getNames() {
        return NAMES;
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
