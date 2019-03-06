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

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.MatchAllDocsQuery;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SortTest extends TestBase
{
    @Test
    public void testString() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new FDBCodec());
        FDBDirectory dir = createDirectoryForMethod();
        IndexWriter writer = new IndexWriter(dir, config);

        Document doc = new Document();
        doc.add(new SortedDocValuesField("value", new BytesRef("foo")));
        doc.add(new StringField("value", "foo", Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new SortedDocValuesField("value", new BytesRef("bar")));
        doc.add(new StringField("value", "bar", Store.YES));
        writer.addDocument(doc);
        writer.close();

        IndexReader ir = DirectoryReader.open(dir);
        
        IndexSearcher searcher = new IndexSearcher(ir);
        Sort sort = new Sort(new SortField("value", SortField.Type.STRING));

        TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
        ScoreDoc[] hits = td.scoreDocs;
        assertEquals(2, hits.length);
        // 'bar' comes before 'foo'
        assertEquals("bar", searcher.doc(hits[0].doc).get("value"));
        assertEquals("foo", searcher.doc(hits[1].doc).get("value"));
        
        ir.close();
        dir.close();
    }

    @Test 
    public void testInt() throws IOException {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new FDBCodec());
        FDBDirectory dir = createDirectoryForMethod();
        IndexWriter writer = new IndexWriter(dir, config);

        Document doc = new Document();
        doc.add(new NumericDocValuesField("value", 300000));
        doc.add(new StringField("value", "300000", Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new NumericDocValuesField("value", -1));
        doc.add(new StringField("value", "-1", Store.YES));
        writer.addDocument(doc);
        doc = new Document();
        doc.add(new NumericDocValuesField("value", 4));
        doc.add(new StringField("value", "4", Store.YES));
        writer.addDocument(doc);
        
        writer.close();
        
        IndexReader ir = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(ir);

        Sort sort = new Sort(new SortField("value", SortField.Type.INT));

        TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
        ScoreDoc[] hits = td.scoreDocs;
        assertEquals(3, hits.length);
        // numeric order
        assertEquals("-1", searcher.doc(hits[0].doc).get("value"));
        assertEquals("4", searcher.doc(hits[1].doc).get("value"));
        assertEquals("300000", searcher.doc(hits[2].doc).get("value"));

        ir.close();
        dir.close();
    }

}
