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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.SegmentInfoReader;
import org.apache.lucene.codecs.SegmentInfoWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.foundationdb.lucene.Util.getBool;
import static com.foundationdb.lucene.Util.set;
import static com.foundationdb.lucene.Util.setMap;

public class FDBSegmentInfoFormat extends SegmentInfoFormat
{
    private static final String SEGMENT_INFO_EXT = "si";
    private static final String DOC_COUNT = "doc_count";
    private static final String VERSION = "version";
    private static final String IS_COMPOUND_FILE = "is_compound_file";
    private static final String DIAG = "diag";
    private static final String ATTR = "attr";
    private static final String FILE = "file";


    //
    // SegmentInfoFormat
    //

    @Override
    public SegmentInfoReader getSegmentInfoReader() {
        return new FDBSegmentInfoReader();
    }

    @Override
    public SegmentInfoWriter getSegmentInfoWriter() {
        return new FDBSegmentInfoWriter();
    }


    //
    // SegmentInfoReader
    //

    private static class FDBSegmentInfoReader extends SegmentInfoReader
    {
        @Override
        public SegmentInfo read(Directory dirIn, String segmentName, IOContext context) throws IOException {
            final FDBDirectory dir = Util.unwrapDirectory(dirIn);
            final Tuple segmentTuple = dir.subspace.add(segmentName).add(SEGMENT_INFO_EXT);

            String version = null;
            Integer docCount = null;
            Boolean isCompoundFile = null;
            Map<String, String> diagnostics = new HashMap<String, String>();
            Map<String, String> attributes = new HashMap<String, String>();
            Set<String> files = new HashSet<String>();

            for(KeyValue kv : dir.txn.getRange(segmentTuple.range())) {
                Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                Tuple valueTuple = Tuple.fromBytes(kv.getValue());
                String key = keyTuple.getString(segmentTuple.size());
                if(keyTuple.size() == (segmentTuple.size() + 1)) {
                    if(VERSION.equals(key)) {
                        version = valueTuple.getString(0);
                    } else if(DOC_COUNT.equals(key)) {
                        docCount = (int)valueTuple.getLong(0);
                    } else if(IS_COMPOUND_FILE.equals(key)) {
                        isCompoundFile = getBool(valueTuple, 0);
                    } else {
                        notFound(key);
                    }
                } else if(keyTuple.size() == (segmentTuple.size() + 2)) {
                    if(DIAG.equals(key)) {
                        diagnostics.put(keyTuple.getString(segmentTuple.size() + 1), valueTuple.getString(0));
                    } else if(ATTR.equals(key)) {
                        attributes.put(keyTuple.getString(segmentTuple.size() + 1), valueTuple.getString(0));
                    } else if(FILE.equals(key)) {
                        files.add(keyTuple.getString(segmentTuple.size() + 1));
                    } else {
                        notFound(key);
                    }
                } else {
                    notFound(key);
                }
            }

            if(version == null) {
                throw required(segmentName, VERSION);
            }
            if(docCount == null) {
                throw required(segmentName, DOC_COUNT);
            }
            if(isCompoundFile == null) {
                throw required(segmentName, IS_COMPOUND_FILE);
            }

            SegmentInfo info = new SegmentInfo(
                    dirIn,
                    version,
                    segmentName,
                    docCount,
                    isCompoundFile,
                    null,
                    diagnostics,
                    Collections.unmodifiableMap(attributes)
            );
            info.setFiles(files);

            return info;
        }

        private static void notFound(String key) {
            throw new IllegalStateException("Unexpected key: " + key);
        }

        private static IllegalStateException required(String segmentName, String keyName) {
            return new IllegalStateException("Segment " + segmentName + " missing key: " + keyName);
        }
    }

    //
    // SegmentInfoWriter
    //

    private static class FDBSegmentInfoWriter extends SegmentInfoWriter
    {
        @Override
        public void write(Directory dirIn, SegmentInfo si, FieldInfos fis, IOContext ioContext) throws IOException {
            final FDBDirectory dir = Util.unwrapDirectory(dirIn);
            final Tuple segmentTuple = dir.subspace.add(si.name).add(SEGMENT_INFO_EXT);

            set(dir.txn, segmentTuple, DOC_COUNT, si.getDocCount());
            set(dir.txn, segmentTuple, IS_COMPOUND_FILE, si.getUseCompoundFile());
            set(dir.txn, segmentTuple, VERSION, si.getVersion());

            setMap(dir.txn, segmentTuple.add(DIAG), si.getDiagnostics());
            setMap(dir.txn, segmentTuple.add(ATTR), si.attributes());

            Set<String> files = si.files();
            if(files != null && !files.isEmpty()) {
                Tuple fileTuple = segmentTuple.add(FILE);
                for(String fileName : files) {
                    set(dir.txn, fileTuple, fileName);
                }
            }
        }
    }
}
