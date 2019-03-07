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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.CountFacetRequest;
import org.apache.lucene.facet.search.DrillDownQuery;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
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

public class FacetTest extends TestBase
{
    private void add(IndexWriter indexWriter, FacetFields facetFields,
            String ... categoryPaths) throws IOException {
        Document doc = new Document();
        
        List<CategoryPath> paths = new ArrayList<CategoryPath>();
        for (String categoryPath : categoryPaths) {
          paths.add(new CategoryPath(categoryPath, '/'));
        }
        facetFields.addFields(doc, paths);
        indexWriter.addDocument(doc);
    }
    
    @Test
    public void testBasicFacetSearch() throws IOException {
        FDBDirectory indexDir = createDirectoryForMethod();
        FDBDirectory taxoDir = createDirectory("taxonomy_dir");
        index(indexDir, taxoDir);
        List<FacetResult> results = search(indexDir, taxoDir);

        assertEquals(2, results.size());
    }

    @Test
    public void testDrillDownSearch() throws IOException {
        FDBDirectory indexDir = createDirectoryForMethod();
        FDBDirectory taxoDir = createDirectory("taxonomy_dir");
        index(indexDir, taxoDir);
        List<FacetResult> results = drillDown(indexDir, taxoDir);

        assertEquals(1, results.size());
    }
    
    private void index(FDBDirectory indexDir, FDBDirectory taxoDir) throws IOException {
        WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(Version.LUCENE_46);
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46, analyzer);

        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setCodec(new FDBCodec());
        IndexWriter indexWriter = new IndexWriter(indexDir, config);

        // Writes facet ords to a separate directory from the main index
        DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir,
            IndexWriterConfig.OpenMode.CREATE);

        // Reused across documents, to add the necessary facet fields
        FacetFields facetFields = new FacetFields(taxoWriter);

        add(indexWriter, facetFields, "Author/Bob", "Publish Date/2010/10/15");
        add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2010/10/20");
        add(indexWriter, facetFields, "Author/Lisa", "Publish Date/2012/1/1");
        add(indexWriter, facetFields, "Author/Susan", "Publish Date/2012/1/7");
        add(indexWriter, facetFields, "Author/Frank", "Publish Date/1999/5/5");
        
        indexWriter.close();
        taxoWriter.close();
    }

    /** User runs a query and counts facets. */
    private List<FacetResult> search(FDBDirectory indexDir, FDBDirectory taxoDir) throws IOException {
        DirectoryReader indexReader = DirectoryReader.open(indexDir);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

        // Count both "Publish Date" and "Author" dimensions
        FacetSearchParams fsp = new FacetSearchParams(
        new CountFacetRequest(new CategoryPath("Publish Date"), 10), 
        new CountFacetRequest(new CategoryPath("Author"), 10));

        // Aggregates the facet counts
        FacetsCollector fc = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);

        // MatchAllDocsQuery is for "browsing" (counts facets
        // for all non-deleted docs in the index); normally
        // you'd use a "normal" query, and use MultiCollector to
        // wrap collecting the "normal" hits and also facets:
        searcher.search(new MatchAllDocsQuery(), fc);

        // Retrieve results
        List<FacetResult> facetResults = fc.getFacetResults();

        indexReader.close();
        taxoReader.close();

        return facetResults;
    }

    /** User drills down on 'Publish Date/2010'. */
    private List<FacetResult> drillDown(FDBDirectory indexDir, FDBDirectory taxoDir) throws IOException {
        DirectoryReader indexReader = DirectoryReader.open(indexDir);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

        // Now user drills down on Publish Date/2010:
        FacetSearchParams fsp = new FacetSearchParams(new CountFacetRequest(new CategoryPath("Author"), 10));

        // Passing no baseQuery means we drill down on all
        // documents ("browse only"):
        DrillDownQuery q = new DrillDownQuery(fsp.indexingParams);
        q.add(new CategoryPath("Publish Date/2010", '/'));
        FacetsCollector fc = FacetsCollector.create(fsp, searcher.getIndexReader(), taxoReader);
        searcher.search(q, fc);

        // Retrieve results
        List<FacetResult> facetResults = fc.getFacetResults();

        indexReader.close();
        taxoReader.close();

        return facetResults;
    }


}
