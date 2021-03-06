/**
 * FoundationDB Lucene Layer
 * Copyright (c) 2013 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.foundationdb.lucene;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;

import java.util.EnumSet;

public final class FDBCodec extends FilterCodec
{
    static final String CONFIG_PROP_NAME = "FDBCodec.formats";
    static final String CONFIG_VALUE_ALL = "ALL";
    static final String CONFIG_VALUE_NONE = "NONE";
    static final String CONFIG_VALUE_DEFAULT = "DEFAULT";


    private final DocValuesFormat docValues;
    private final FieldInfosFormat fieldInfos;
    private final LiveDocsFormat liveDocs;
    private final NormsFormat norms;
    private final PostingsFormat postings;
    private final SegmentInfoFormat segmentInfo;
    private final StoredFieldsFormat storedFields;
    private final TermVectorsFormat termVectors;


    public FDBCodec() {
        this(System.getProperty(CONFIG_PROP_NAME, CONFIG_VALUE_DEFAULT));
    }

    public FDBCodec(String formatOptStr) {
        this(deriveFormatOpts(formatOptStr));
    }

    public FDBCodec(EnumSet<FormatOpts> opts) {
        super(FDBCodec.class.getSimpleName(), new Lucene46Codec());
        this.docValues = opts.contains(FormatOpts.DOC_VALUES) ? new FDBDocValuesFormat() : super.docValuesFormat();
        this.fieldInfos = opts.contains(FormatOpts.FIELD_INFOS) ? new FDBFieldInfosFormat() : super.fieldInfosFormat();
        this.liveDocs = opts.contains(FormatOpts.LIVE_DOCS) ? new FDBLiveDocsFormat() : super.liveDocsFormat();
        this.norms = opts.contains(FormatOpts.NORMS) ? new FDBNormsFormat() : super.normsFormat();
        this.postings = opts.contains(FormatOpts.POSTINGS) ? new FDBPostingsFormat() : super.postingsFormat();
        this.segmentInfo = opts.contains(FormatOpts.SEGMENT_INFO) ? new FDBSegmentInfoFormat() : super.segmentInfoFormat();
        this.storedFields = opts.contains(FormatOpts.STORED_FIELDS) ? new FDBStoredFieldsFormat() : super.storedFieldsFormat();
        this.termVectors = opts.contains(FormatOpts.TERM_VECTORS) ? new FDBTermVectorsFormat() : super.termVectorsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValues;
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return fieldInfos;
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return liveDocs;
    }

    @Override
    public NormsFormat normsFormat() {
        return norms;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postings;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfo;
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return termVectors;
    }


    //
    // Helpers
    //

    private static enum FormatOpts
    {
        DOC_VALUES,
        FIELD_INFOS,
        LIVE_DOCS,
        NORMS,
        POSTINGS,
        SEGMENT_INFO,
        STORED_FIELDS,
        TERM_VECTORS,
    }

    public static EnumSet<FormatOpts> deriveFormatOpts(String configStr) {
        if(CONFIG_VALUE_ALL.equals(configStr.toUpperCase())) {
            return EnumSet.allOf(FormatOpts.class);
        }
        if(CONFIG_VALUE_NONE.equals(configStr.toUpperCase())) {
            return EnumSet.noneOf(FormatOpts.class);
        }
        if(CONFIG_VALUE_DEFAULT.equals(configStr.toUpperCase())) {
            return EnumSet.of(
                    FormatOpts.DOC_VALUES,
                    FormatOpts.FIELD_INFOS,
                    FormatOpts.LIVE_DOCS,
                    FormatOpts.NORMS,
                    FormatOpts.POSTINGS,
                    FormatOpts.SEGMENT_INFO,
                    FormatOpts.STORED_FIELDS,
                    FormatOpts.TERM_VECTORS
            );
        }
        EnumSet<FormatOpts> enumSet = EnumSet.noneOf(FormatOpts.class);
        String[] optNames = configStr.split(",");
        for(String name : optNames) {
            FormatOpts opt = FormatOpts.valueOf(name.toUpperCase());
            enumSet.add(opt);
        }
        return enumSet;
    }
}
