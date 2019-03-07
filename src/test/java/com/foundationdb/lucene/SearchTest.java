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
import org.apache.lucene.queryparser.classic.QueryParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SearchTest extends TestBase
{
    @Test
    public void basicSearchTest() throws Exception {
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);
        // recreate the index on each execution
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new FDBCodec());
        FDBDirectory dir = createDirectoryForMethod();
        IndexWriter writer = new IndexWriter(dir, config);
        try {
            writer.addDocument(
                    Arrays.asList(
                            new TextField("title", "The title of my first document", Store.YES),
                            new TextField("content", "Some content", Store.YES)
                    )
            );
        } finally {
            writer.close();
        }
        queryDocuments(dir);
    }

    private void queryDocuments(Directory dir) throws IOException {
        int hitsPerPage = 10;
        IndexReader reader = DirectoryReader.open(dir);
        StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
        String searchString = "first";
        
        try {
            QueryParser queryParser = new QueryParser(Version.LUCENE_46, "title", analyzer);
            Query q = queryParser.parse(searchString);
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs docs = searcher.search(q, hitsPerPage);
            ScoreDoc[] hits = docs.scoreDocs;
            System.out.println("Found " + hits.length + " hits.");
            for(int i=0;i<hits.length;++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                System.out.println((i + 1) + ". " + d.get("title") + d.get("content"));
            }
        } catch (Exception e) {
            System.out.println("Caught Exceptione e: " + e);
        } finally {
            reader.close();
        }
    }

}