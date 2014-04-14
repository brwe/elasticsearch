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
package org.elasticsearch.search.aggregations.bucket.significant;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.*;

/**
 *
 */
public abstract class InternalSignificantTerms extends InternalAggregation implements SignificantTerms, ToXContent, Streamable {

    protected int requiredSize;
    protected long minDocCount;
    protected Collection<Bucket> buckets;
    protected Map<String, Bucket> bucketMap;
    protected long subsetSize;
    protected long supersetSize;
    protected String significanceMethod;
    protected boolean excludeNegatives;


    protected InternalSignificantTerms() {} // for serialization

    // TODO updateScore call in constructor to be cleaned up as part of adding pluggable scoring algos
    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public static abstract class Bucket extends SignificantTerms.Bucket {

        protected boolean excludeNegatives;
        protected String significanceMethod;
        long bucketOrd;
        protected InternalAggregations aggregations;
        double score;

        protected Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations, String significanceMethod, boolean excludeNegatives) {
            super(subsetDf, subsetSize, supersetDf, supersetSize);
            this.aggregations = aggregations;
            this.significanceMethod = significanceMethod;
            this.excludeNegatives = excludeNegatives;
            assert subsetDf <= supersetDf;
            updateScore();
        }

        @Override
        public long getSubsetDf() {
            return subsetDf;
        }

        @Override
        public long getSupersetDf() {
            return supersetDf;
        }

        @Override
        public long getSupersetSize() {
            return supersetSize;
        }

        @Override
        public long getSubsetSize() {
            return subsetSize;
        }

        /**
         * Calculates the significance of a term in a sample against a background of
         * normal distributions by comparing the changes in frequency. This is the heart
         * of the significant terms feature.
         * <p/>
         * TODO - allow pluggable scoring implementations
         *
         * @param subsetFreq   The frequency of the term in the selected sample
         * @param subsetSize   The size of the selected sample (typically number of docs)
         * @param supersetFreq The frequency of the term in the superset from which the sample was taken
         * @param supersetSize The size of the superset from which the sample was taken  (typically number of docs)
         * @return a "significance" score
         */
        public static double getDefaultTermSignificance(long subsetFreq, long subsetSize, long supersetFreq, long supersetSize) {
            if ((subsetSize == 0) || (supersetSize == 0)) {
                // avoid any divide by zero issues
                return 0;
            }

            double subsetProbability = (double) subsetFreq / (double) subsetSize;
            double supersetProbability = (double) supersetFreq / (double) supersetSize;

            // Using absoluteProbabilityChange alone favours very common words e.g. you, we etc
            // because a doubling in popularity of a common term is a big percent difference 
            // whereas a rare term would have to achieve a hundred-fold increase in popularity to
            // achieve the same difference measure.
            // In favouring common words as suggested features for search we would get high
            // recall but low precision.
            double absoluteProbabilityChange = subsetProbability - supersetProbability;
            if (absoluteProbabilityChange <= 0) {
                return 0;
            }
            // Using relativeProbabilityChange tends to favour rarer terms e.g.mis-spellings or 
            // unique URLs.
            // A very low-probability term can very easily double in popularity due to the low
            // numbers required to do so whereas a high-probability term would have to add many
            // extra individual sightings to achieve the same shift. 
            // In favouring rare words as suggested features for search we would get high
            // precision but low recall.
            double relativeProbabilityChange = (subsetProbability / supersetProbability);

            // A blend of the above metrics - favours medium-rare terms to strike a useful
            // balance between precision and recall.
            return absoluteProbabilityChange * relativeProbabilityChange;
        }

        public void updateScore() {
            if (significanceMethod.equals("DEFAULT")) {
                score = getDefaultTermSignificance(subsetDf, subsetSize, supersetDf, supersetSize);
            } else {
                score = mutual(subsetDf, subsetSize, supersetDf, supersetSize, excludeNegatives);
            }

        }
        /** see "Information Retrieval", Manning et al., Eq. 13.17 */
        private static double mutual(long subsetDf, long subsetSize, long supersetDf, long supersetSize, boolean excludeNegatives) {

            //documents not in class and do not contain term
            double N00 = supersetSize - supersetDf - (subsetSize - subsetDf);
            //documents in class and do not contain term
            double N01 = (subsetSize - subsetDf);
            // documents not in class and do contain term
            double N10 = supersetDf - subsetDf;
            // documents in class and do contain term
            double N11 = subsetDf;
            //documents that do not contain term
            double N0_ = supersetSize - supersetDf;
            //documents that contain term
            double N1_ = supersetDf;
            //documents that are not in class
            double N_0 = supersetSize - subsetSize;
            //documents that are in class
            double N_1 = subsetSize;
            //all docs
            double N = supersetSize;

            double score =
                      N11/N * Math.log((N*N11)/(N1_*N_1))
                    + N01/N * Math.log((N*N01)/(N0_*N_1))
                    + N10/N * Math.log((N*N10)/(N1_*N_0))
                    + N00/N * Math.log((N*N00)/(N0_*N_0));
            if (Double.isNaN(score)) {
                score = -1.0 * Float.MAX_VALUE;
            }
            if (excludeNegatives && N11/N_1 < N10/N_0) {
                score = -1.0 * Float.MAX_VALUE;
            }
            return score;

        }

        @Override
        public long getDocCount() {
            return subsetDf;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, BigArrays bigArrays) {
            if (buckets.size() == 1) {
                return buckets.get(0);
            }
            Bucket reduced = null;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.subsetDf += bucket.subsetDf;
                    reduced.supersetDf += bucket.supersetDf;
                    reduced.updateScore();
                }
                aggregationsList.add(bucket.aggregations);
            }
            assert reduced.subsetDf <= reduced.supersetDf;
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, bigArrays);
            return reduced;
        }

        @Override
        public double getSignificanceScore() {
            return score;
        }
    }

    protected InternalSignificantTerms(long subsetSize, long supersetSize, String name, int requiredSize, long minDocCount, Collection<Bucket> buckets, String significanceMethod, boolean excludeNegatives) {
        super(name);
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
        this.buckets = buckets;
        this.subsetSize = subsetSize;
        this.supersetSize = supersetSize;
        this.significanceMethod = significanceMethod;
        this.excludeNegatives = excludeNegatives;
    }

    @Override
    public Iterator<SignificantTerms.Bucket> iterator() {
        Object o = buckets.iterator();
        return (Iterator<SignificantTerms.Bucket>) o;
    }

    @Override
    public Collection<SignificantTerms.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<SignificantTerms.Bucket>) o;
    }

    @Override
    public SignificantTerms.Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = Maps.newHashMapWithExpectedSize(buckets.size());
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
        }
        return bucketMap.get(term);
    }

    @Override
    public InternalSignificantTerms reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();
        if (aggregations.size() == 1) {
            InternalSignificantTerms terms = (InternalSignificantTerms) aggregations.get(0);
            terms.trimExcessEntries();
            final int size = Math.min(requiredSize, terms.buckets.size());
            BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
            for (Bucket b : terms.buckets) {

                    ordered.insertWithOverflow(b);
            }
            Bucket[] list = new Bucket[ordered.size()];
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = (Bucket) ordered.pop();
            }
            terms.buckets = Arrays.asList(list);
            return terms;
        }
        InternalSignificantTerms reduced = null;

        long globalSubsetSize = 0;
        long globalSupersetSize = 0;
        // Compute the overall result set size and the corpus size using the
        // top-level Aggregations from each shard
        for (InternalAggregation aggregation : aggregations) {
            InternalSignificantTerms terms = (InternalSignificantTerms) aggregation;
            globalSubsetSize += terms.subsetSize;
            globalSupersetSize += terms.supersetSize;
        }
        Map<String, List<InternalSignificantTerms.Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalSignificantTerms terms = (InternalSignificantTerms) aggregation;
            if (terms instanceof UnmappedSignificantTerms) {
                continue;
            }
            if (reduced == null) {
                reduced = terms;
            }
            if (buckets == null) {
                buckets = new HashMap<>(terms.buckets.size());
            }
            for (Bucket bucket : terms.buckets) {
                List<Bucket> existingBuckets = buckets.get(bucket.getKey());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getKey(), existingBuckets);
                }
                // Adjust the buckets with the global stats representing the
                // total size of the pots from which the stats are drawn
                bucket.subsetSize = globalSubsetSize;
                bucket.supersetSize = globalSupersetSize;
                bucket.updateScore();
                existingBuckets.add(bucket);
            }
        }

        if (reduced == null) {
            // there are only unmapped terms, so we just return the first one
            // (no need to reduce)
            return (UnmappedSignificantTerms) aggregations.get(0);
        }

        final int size = Math.min(requiredSize, buckets.size());
        BucketSignificancePriorityQueue ordered = new BucketSignificancePriorityQueue(size);
        for (Map.Entry<String, List<Bucket>> entry : buckets.entrySet()) {
            List<Bucket> sameTermBuckets = entry.getValue();
            final Bucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext.bigArrays());
            if ((b.score > 0) && (b.subsetDf >= minDocCount)) {
                ordered.insertWithOverflow(b);
            }
        }
        Bucket[] list = new Bucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = (Bucket) ordered.pop();
        }
        reduced.buckets = Arrays.asList(list);
        reduced.subsetSize = globalSubsetSize;
        reduced.supersetSize = globalSupersetSize;
        return reduced;
    }

    final void trimExcessEntries() {
        final List<Bucket> newBuckets = Lists.newArrayList();
        for (Bucket b : buckets) {
            if (newBuckets.size() >= requiredSize) {
                break;
            }
            if (b.subsetDf >= minDocCount) {
                newBuckets.add(b);
            }
        }
        buckets = newBuckets;
    }

}
