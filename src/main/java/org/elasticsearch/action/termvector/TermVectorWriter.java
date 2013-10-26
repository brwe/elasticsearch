/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this 
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
package org.elasticsearch.action.termvector;

import com.google.common.collect.Sets;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.termvector.TermVectorRequest.Flag;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

// package only - this is an internal class!
final class TermVectorWriter {
    final List<String> fields = new ArrayList<String>();
    final List<Long> fieldOffset = new ArrayList<Long>();
    final BytesStreamOutput output = new BytesStreamOutput(1); // can we somehow
    // predict the
    // size here?
    private static final String HEADER = "TV";
    private static final int CURRENT_VERSION = -1;
    TermVectorResponse response = null;

    TermVectorWriter(TermVectorResponse termVectorResponse) throws IOException {
        response = termVectorResponse;
    }

    /**
     * This method will write each term in the selectedTerms array if it
     * contains any elements. If the term does not appear in a field, it is
     * added with freq 0. If <code>selectedTerms</code> does not contain
     * elements or is null, we simple write all terms as they appear in the
     * fields. <code>selectedTerms</code> is assumed to be a sorted array of
     * Strings with unique elements.
     * */
    void setFields(Fields termVectorsByField, String[] selectedFields, String[] selectedTerms, EnumSet<Flag> flags, Fields topLevelFields)
            throws IOException {

        int numFieldsWritten = 0;
        TermsEnum iterator = null;
        DocsAndPositionsEnum docsAndPosEnum = null;
        DocsEnum docsEnum = null;
        TermsEnum topLevelIterator = null;
        Set<String> selectedFieldsSet = null;
        if (!containsElements(selectedFields)) {
            selectedFieldsSet = Sets.newHashSet();
        } else {
            selectedFieldsSet = Sets.newHashSet(selectedFields);
        }

        for (String field : termVectorsByField) {
            if (!shouldRetrieveField(selectedFieldsSet, field)) {
                continue;
            }

            Terms fieldTermVector = termVectorsByField.terms(field);
            Terms topLevelTerms = topLevelFields.terms(field);

            topLevelIterator = topLevelTerms.iterator(topLevelIterator);
            boolean positions = flags.contains(Flag.Positions) && fieldTermVector.hasPositions();
            boolean offsets = flags.contains(Flag.Offsets) && fieldTermVector.hasOffsets();
            boolean payloads = flags.contains(Flag.Payloads) && fieldTermVector.hasPayloads();
            if (containsElements(selectedTerms)) {
                startField(field, selectedTerms.length, positions, offsets, payloads);
            } else {
                startField(field, fieldTermVector.size(), positions, offsets, payloads);
            }
            if (flags.contains(Flag.FieldStatistics)) {
                writeFieldStatistics(topLevelTerms);
            }
            iterator = fieldTermVector.iterator(iterator);
            final boolean useDocsAndPos = positions || offsets || payloads;
            int selectedTermsCounter = 0;

            // we assume that selectedTerms is a sorted list and also that the
            // terms are in order.
            // we intersect these two lists now.
            while ((iterator.next() != null) && hasMoreSelectedTerms(selectedTerms, selectedTermsCounter)) {
                BytesRef term = iterator.term();
                String termAsString = term.utf8ToString();
                boolean foundTerm = topLevelIterator.seekExact(term);
                assert (foundTerm);
                
                // write freq 0 for all terms in the selected list that are lexicographically less than the current term
                selectedTermsCounter = writeSelectedTermsSmallerThanCurrent(selectedTerms, selectedTermsCounter, termAsString, flags);
                
                if (shouldWriteTerm(selectedTerms, selectedTermsCounter, termAsString)) {
                    selectedTermsCounter++;
                    startTerm(term);
                    if (flags.contains(Flag.TermStatistics)) {
                        writeTermStatistics(topLevelIterator);
                    }
                    if (useDocsAndPos) {
                        // given we have pos or offsets
                        docsAndPosEnum = writeTermWithDocsAndPos(iterator, docsAndPosEnum, positions, offsets, payloads);
                    } else {
                        // if we do not have the positions stored, we need to
                        // get the frequency from a DocsEnum.
                        docsEnum = writeTermWithDocsOnly(iterator, docsEnum);
                    }
                }

            }
            //if some selected terms are not in this field, we add them with frequency 0
            if (selectedTerms != null) {
                while (selectedTermsCounter < selectedTerms.length) {
                    // write the empty term
                    writeEmptyTerm(selectedTerms[selectedTermsCounter], flags);
                    selectedTermsCounter++;
                }
            }
            numFieldsWritten++;
        }
        response.setTermVectorField(output);
        response.setHeader(writeHeader(numFieldsWritten, flags.contains(Flag.TermStatistics), flags.contains(Flag.FieldStatistics)));
    }

    private void writeEmptyTerm(String term, EnumSet<Flag> flags) throws IOException {

        startTerm(new BytesRef(term));
        if (flags.contains(Flag.TermStatistics)) {
            writePotentiallyNegativeVInt(0);
            writePotentiallyNegativeVLong(0);
        }
        writeFreq(0);
    }

    private boolean containsElements(String[] elementArray) {
        if (elementArray == null) {
            return false;
        }
        if (elementArray.length == 0) {
            return false;
        }
        return true;
    }

    private boolean shouldWriteTerm(String[] selectedTerms, int selectedTermsCounter, String termAsString) {
        boolean writeTerm = false;
        if (containsElements(selectedTerms)) {
            if ((selectedTerms.length > selectedTermsCounter)) {//are we at the end of the selected terms list already?
                if (selectedTerms[selectedTermsCounter].compareTo(termAsString) == 0) {
                    //the current term is in the selectedTerms so we should write it
                    writeTerm = true;
                }
            }

        } else {
            //if no selected terms are given we should write the term
            writeTerm = true;
        }
        return writeTerm;
    }

    private int writeSelectedTermsSmallerThanCurrent(String[] selectedTerms, int selectedTermsCounter, String termAsString,
            EnumSet<Flag> flags) throws IOException {
        if (containsElements(selectedTerms)) {
            while ((selectedTermsCounter < selectedTerms.length) && (selectedTerms[selectedTermsCounter].compareTo(termAsString) < 0)) {
                // the current term lexicographically greater than the next term in the selected terms list
                // -> write the empty term
                writeEmptyTerm(selectedTerms[selectedTermsCounter], flags);
                selectedTermsCounter++;
            }
        }
        return selectedTermsCounter;
    }

    private boolean shouldRetrieveField(Set<String> selectedFieldsSet, String field) {
        if (selectedFieldsSet.size() == 0) {
            return true;
        } else {
            return (selectedFieldsSet.contains(field));
        }
    }

    private boolean hasMoreSelectedTerms(String[] selectedTerms, int selectedTermsCounter) {
        // selected terms can be null or just have length 0. In both cases we
        // want to return all terms and therefore return true.
        if (!containsElements(selectedTerms)) {
            return true;
        } else {
            return (selectedTermsCounter < selectedTerms.length);
        }
    }

    private BytesReference writeHeader(int numFieldsWritten, boolean getTermStatistics, boolean getFieldStatistics) throws IOException {
        // now, write the information about offset of the terms in the
        // termVectors field
        BytesStreamOutput header = new BytesStreamOutput();
        header.writeString(HEADER);
        header.writeInt(CURRENT_VERSION);
        header.writeBoolean(getTermStatistics);
        header.writeBoolean(getFieldStatistics);
        header.writeVInt(numFieldsWritten);
        for (int i = 0; i < fields.size(); i++) {
            header.writeString(fields.get(i));
            header.writeVLong(fieldOffset.get(i).longValue());
        }
        header.close();
        return header.bytes();
    }

    private DocsEnum writeTermWithDocsOnly(TermsEnum iterator, DocsEnum docsEnum) throws IOException {
        docsEnum = iterator.docs(null, docsEnum);
        int nextDoc = docsEnum.nextDoc();
        assert nextDoc != DocsEnum.NO_MORE_DOCS;
        writeFreq(docsEnum.freq());
        nextDoc = docsEnum.nextDoc();
        assert nextDoc == DocsEnum.NO_MORE_DOCS;
        return docsEnum;
    }

    private DocsAndPositionsEnum writeTermWithDocsAndPos(TermsEnum iterator, DocsAndPositionsEnum docsAndPosEnum, boolean positions,
            boolean offsets, boolean payloads) throws IOException {
        docsAndPosEnum = iterator.docsAndPositions(null, docsAndPosEnum);
        // for each term (iterator next) in this field (field)
        // iterate over the docs (should only be one)
        int nextDoc = docsAndPosEnum.nextDoc();
        assert nextDoc != DocsEnum.NO_MORE_DOCS;
        final int freq = docsAndPosEnum.freq();
        writeFreq(freq);
        for (int j = 0; j < freq; j++) {
            int curPos = docsAndPosEnum.nextPosition();
            if (positions) {
                writePosition(curPos);
            }
            if (offsets) {
                writeOffsets(docsAndPosEnum.startOffset(), docsAndPosEnum.endOffset());
            }
            if (payloads) {
                writePayload(docsAndPosEnum.getPayload());
            }
        }
        nextDoc = docsAndPosEnum.nextDoc();
        assert nextDoc == DocsEnum.NO_MORE_DOCS;
        return docsAndPosEnum;
    }

    private void writePayload(BytesRef payload) throws IOException {
        if (payload != null) {
            output.writeVInt(payload.length);
            output.writeBytes(payload.bytes, payload.offset, payload.length);
        } else {
            output.writeVInt(0);
        }
    }

    private void writeFreq(int termFreq) throws IOException {

        writePotentiallyNegativeVInt(termFreq);
    }

    private void writeOffsets(int startOffset, int endOffset) throws IOException {
        assert (startOffset >= 0);
        assert (endOffset >= 0);
        if ((startOffset >= 0) && (endOffset >= 0)) {
            output.writeVInt(startOffset);
            output.writeVInt(endOffset);
        }
    }

    private void writePosition(int pos) throws IOException {
        assert (pos >= 0);
        if (pos >= 0) {
            output.writeVInt(pos);
        }
    }

    private void startField(String fieldName, long termsSize, boolean writePositions, boolean writeOffsets, boolean writePayloads)
            throws IOException {
        fields.add(fieldName);
        fieldOffset.add(output.position());
        output.writeVLong(termsSize);
        // add information on if positions etc. are written
        output.writeBoolean(writePositions);
        output.writeBoolean(writeOffsets);
        output.writeBoolean(writePayloads);
    }

    private void startTerm(BytesRef term) throws IOException {
        output.writeVInt(term.length);
        output.writeBytes(term.bytes, term.offset, term.length);

    }

    private void writeTermStatistics(TermsEnum topLevelIterator) throws IOException {
        int docFreq = topLevelIterator.docFreq();
        assert (docFreq >= 0);
        writePotentiallyNegativeVInt(docFreq);
        long ttf = topLevelIterator.totalTermFreq();
        assert (ttf >= 0);
        writePotentiallyNegativeVLong(ttf);

    }

    private void writeFieldStatistics(Terms topLevelTerms) throws IOException {
        long sttf = topLevelTerms.getSumTotalTermFreq();
        assert (sttf >= 0);
        writePotentiallyNegativeVLong(sttf);
        long sdf = topLevelTerms.getSumDocFreq();
        assert (sdf >= 0);
        writePotentiallyNegativeVLong(sdf);
        int dc = topLevelTerms.getDocCount();
        assert (dc >= 0);
        writePotentiallyNegativeVInt(dc);

    }

    private void writePotentiallyNegativeVInt(int value) throws IOException {
        // term freq etc. can be negative if not present... we transport that
        // further...
        output.writeVInt(Math.max(0, value + 1));
    }

    private void writePotentiallyNegativeVLong(long value) throws IOException {
        // term freq etc. can be negative if not present... we transport that
        // further...
        output.writeVLong(Math.max(0, value + 1));
    }

}
