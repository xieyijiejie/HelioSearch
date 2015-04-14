/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.HS;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.SolrIndexSearcher.DocsEnumState;
import org.apache.solr.search.facet.UnInvertedField;
import org.apache.solr.search.field.FieldUtil;
import org.apache.solr.util.RefCounted;
import org.noggit.ObjectBuilder;


public class JoinQParserPlugin extends QParserPlugin {
  public static final String NAME = "join";

  @Override
  public void init(NamedList args) {
  }

  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        String fromField = getParam("from");
        String fromIndex = getParam("fromIndex");
        String toField = getParam("to");
        String v = localParams.get("v");
        Query fromQuery;
        long fromCoreOpenTime = 0;

        if (fromIndex != null && !fromIndex.equals(req.getCore().getCoreDescriptor().getName()) ) {
          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();

          final SolrCore fromCore = container.getCore(fromIndex);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
          }

          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
          try {
            QParser parser = QParser.getParser(v, "lucene", otherReq);
            fromQuery = parser.getQuery();
            fromHolder = fromCore.getRegisteredSearcher();
            if (fromHolder != null) fromCoreOpenTime = fromHolder.get().getOpenTime();
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
        } else {
          QParser fromQueryParser = subQuery(v, null);
          fromQuery = fromQueryParser.getQuery();
        }

        JoinQuery jq = new JoinQuery(fromField, toField, fromIndex, fromQuery);
        jq.fromCoreOpenTime = fromCoreOpenTime;
        return jq;
      }
    };
  }
}


class JoinQuery extends Query {
  List<Map<String, Object>> joinList = new ArrayList<Map<String, Object>>();
  String fromField;
  String toField;
  String fromIndex;
  Query q;
  long fromCoreOpenTime;

  public JoinQuery(String fromField, String toField, String fromIndex, Query subQuery) {
    this.fromField = fromField;
    this.toField = toField;
    this.fromIndex = fromIndex;
    this.q = subQuery;
  }

  public Query getQuery() { return q; }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    // don't rewrite the subQuery
    return this;
  }

  @Override
  public void extractTerms(Set terms) {
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new JoinQueryWeight((SolrIndexSearcher)searcher);
  }

  private class JoinQueryWeight extends Weight {
    Map<String, SolrIndexSearcher> searcherMap = new HashMap<String, SolrIndexSearcher>();
    SolrIndexSearcher fromSearcher;
    RefCounted<SolrIndexSearcher> fromRef;
    SolrIndexSearcher toSearcher;
    private Similarity similarity;
    private float queryNorm;
    private float queryWeight;
    ResponseBuilder rb;

    public JoinQueryWeight(SolrIndexSearcher searcher) {
      this.fromSearcher = searcher;
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        rb = info.getResponseBuilder();
      }

      if (fromIndex == null) {
        this.fromSearcher = searcher;
      } else {
        if (info == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join must have SolrRequestInfo");
        }

        CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
        final SolrCore fromCore = container.getCore(fromIndex);

        if (fromCore == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndex);
        }

        if (info.getReq().getCore() == fromCore) {
          // if this is the same core, use the searcher passed in... otherwise we could be warming and
          // get an older searcher from the core.
          fromSearcher = searcher;
        } else {
          // This could block if there is a static warming query with a join in it, and if useColdSearcher is true.
          // Deadlock could result if two cores both had useColdSearcher and had joins that used eachother.
          // This would be very predictable though (should happen every time if misconfigured)
          fromRef = fromCore.getSearcher(false, true, null);

          // be careful not to do anything with this searcher that requires the thread local
          // SolrRequestInfo in a manner that requires the core in the request to match
          fromSearcher = fromRef.get();
        }

        if (fromRef != null) {
          final RefCounted<SolrIndexSearcher> ref = fromRef;
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              ref.decref();
            }
          });
        }

        info.addCloseHook(new Closeable() {
          @Override
          public void close() {
            fromCore.close();
          }
        });

      }
      this.toSearcher = searcher;

      if(rb.req.getParams().get("json.join") != null){
        try {
          joinList = (List<Map<String,Object>>)ObjectBuilder.fromJSON(rb.req.getParams().get("json.join"));
        } catch (IOException e) {
          // impossible
        }

        for(Map<String, Object> joinInfo : joinList){
          try {
            SolrQueryRequest req = new LocalSolrQueryRequest(searcher.getCore().getCoreDescriptor().getCoreContainer().getCore((String)joinInfo.get("fromIndex")), rb.req.getParams());
            QParser parser = QParser.getParser((String)joinInfo.get("q"), "lucene", req);
            Query q = parser.getQuery();
            joinInfo.put("q", q);
          } catch (SyntaxError e) {
            throw new RuntimeException();
          }
//          try {
//            StandardQueryParser queryParser = new StandardQueryParser();
//            Query q = queryParser.parse((String)joinInfo.get("q"), "id");
//            joinInfo.put("q", q);
//          } catch (QueryNodeException e) {
//            throw new RuntimeException();
//          }

          String fromIndexName = (String)joinInfo.get("fromIndex");
          String toIndexName = (String)joinInfo.get("toIndex");

          if(!searcherMap.containsKey(fromIndexName)){
            CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
            final SolrCore fromCore = container.getCore(fromIndexName);
            if (fromCore == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndexName);
            }
            if (info.getReq().getCore() == fromCore) {
              searcherMap.put(fromIndexName, searcher);
            } else {
              fromRef = fromCore.getSearcher(false, true, null);
              searcherMap.put(fromIndexName, fromRef.get());
              final RefCounted<SolrIndexSearcher> ref = fromRef;
              info.addCloseHook(new Closeable() {
                @Override
                public void close() {
                  ref.decref();
                }
              });
            }
            info.addCloseHook(new Closeable() {
              @Override
              public void close() {
                fromCore.close();
              }
            });
          }
          if(!searcherMap.containsKey(toIndexName)){
            CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
            final SolrCore fromCore = container.getCore(toIndexName);
            if (fromCore == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + toIndexName);
            }
            if (info.getReq().getCore() == fromCore) {
              searcherMap.put(toIndexName, searcher);
            } else {
              fromRef = fromCore.getSearcher(false, true, null);
              searcherMap.put(toIndexName, fromRef.get());
              final RefCounted<SolrIndexSearcher> ref = fromRef;
              info.addCloseHook(new Closeable() {
                @Override
                public void close() {
                  ref.decref();
                }
              });
            }
            info.addCloseHook(new Closeable() {
              @Override
              public void close() {
                fromCore.close();
              }
            });
          }
        }
      }
    }

    @Override
    public Query getQuery() {
      return JoinQuery.this;
    }

    @Override
    public float getValueForNormalization() throws IOException {
      queryWeight = getBoost();
      return queryWeight * queryWeight;
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      this.queryNorm = norm * topLevelBoost;
      queryWeight *= this.queryNorm;
    }

    DocSet resultSet;
    Filter filter;



    @Override
    public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
      if (filter == null) {
        boolean debug = rb != null && rb.isDebug();
        long start = debug ? System.currentTimeMillis() : 0;
        
        if(rb.req.getParams().get("json.join") != null){
          resultSet = getDocSetFromJoinChain();
        }else{
          if(rb.req.getParams().getBool("newJoin", false)){
            if(rb.req.getParams().getBool("joinCache", false)){
              resultSet = getDocSetNewWithCache(); 
            }else{
              resultSet = getDocSetNew();
            }
          }else{
            resultSet = getDocSetOld();
          }
        }

        long end = debug ? System.currentTimeMillis() : 0;

        if (debug) { // TODO: the debug process itself causes the query to be re-executed and this info is added multiple times!
          SimpleOrderedMap<Object> dbg = new SimpleOrderedMap<Object>();
          dbg.add("time", (end-start));
          dbg.add("fromSetSize", fromSetSize);  // the input
          dbg.add("toSetSize", resultSet.size());    // the output

          dbg.add("fromTermCount", fromTermCount);
          dbg.add("fromTermTotalDf", fromTermTotalDf);
          dbg.add("fromTermDirectCount", fromTermDirectCount);
          dbg.add("fromTermHits", fromTermHits);
          dbg.add("fromTermHitsTotalDf", fromTermHitsTotalDf);
          dbg.add("toTermHits", toTermHits);
          dbg.add("toTermHitsTotalDf", toTermHitsTotalDf);
          dbg.add("toTermDirectCount", toTermDirectCount);
          dbg.add("smallSetsDeferred", smallSetsDeferred);
          dbg.add("toSetDocsAdded", resultListDocs);

          // TODO: perhaps synchronize  addDebug in the future...
          rb.addDebug(dbg, "join", JoinQuery.this.toString());
        }

        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, acceptDocs);
      return new JoinScorer(this, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), getBoost());
    }


    int fromSetSize;          // number of docs in the fromSet (that match the from query)
    long resultListDocs;      // total number of docs collected
    int fromTermCount;
    long fromTermTotalDf;
    int fromTermDirectCount;  // number of fromTerms that were too small to use the filter cache
    int fromTermHits;         // number of fromTerms that intersected the from query
    long fromTermHitsTotalDf; // sum of the df of the matching terms
    int toTermHits;           // num if intersecting from terms that match a term in the to field
    long toTermHitsTotalDf;   // sum of the df for the toTermHits
    int toTermDirectCount;    // number of toTerms that we set directly on a bitset rather than doing set intersections
    int smallSetsDeferred;    // number of small sets collected to be used later to intersect w/ bitset or create another small set

    private FixedBitSet fetchDocsFromTerm(Bits liveDocs, DocsEnumState deState, TermsEnum termEnum, BytesRef term) throws IOException{
      FixedBitSet resultBits = new FixedBitSet(liveDocs.length());
      deState.docsEnum = termEnum.docs(liveDocs, deState.docsEnum, DocsEnum.FLAG_NONE);
      DocsEnum docsEnum = deState.docsEnum;
      if (docsEnum instanceof MultiDocsEnum) {
        MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
        int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
        for (int subindex = 0; subindex<numSubs; subindex++) {
          MultiDocsEnum.EnumWithSlice sub = subs[subindex];
          if (sub.docsEnum == null) continue;
          int base = sub.slice.start;
          int docid;
          while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            resultBits.set(docid + base);
          }
        }
      } else {
        int docid;
        while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          resultBits.set(docid);
        }
      }
      return resultBits;
    }
    
    private int[][] buildJoinResultCache(){
      JoinQueryResultKey jqrk = new JoinQueryResultKey(toSearcher.getName(), fromField, toField);
      if(!rb.req.getParams().getBool("refreshCache", false) && toSearcher.joinQueryResultCache.get(jqrk) != null){
        System.out.println("Cache Hit");
        return toSearcher.joinQueryResultCache.get(jqrk);
      }else{
        System.out.println("Cache Not Hit");
//        DocSet[] docJoinResult = new DocSet[fromSearcher.maxDoc()];
        int[][] docJoinResult = new int[fromSearcher.maxDoc()][];
//        Set<Integer>[] docJoinResultTemp = new Set[fromSearcher.maxDoc()];
        
        try {
          Fields fromFields = fromSearcher.getAtomicReader().fields();
          Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getAtomicReader().fields();
          Terms fromTerms = fromFields.terms(fromField);
          Terms toTerms = toFields.terms(toField);
          TermsEnum  fromTermsEnum = fromTerms.iterator(null);
          TermsEnum  toTermsEnum = toTerms.iterator(null);
          BytesRef term = fromTermsEnum.next();
          
          Bits fromLiveDocs = fromSearcher.getAtomicReader().getLiveDocs();
          Bits toLiveDocs = toSearcher.getAtomicReader().getLiveDocs();
          
          SolrIndexSearcher.DocsEnumState fromDeState = null;
          fromDeState = new SolrIndexSearcher.DocsEnumState();
          fromDeState.fieldName = fromField;
          fromDeState.liveDocs = fromLiveDocs;
          fromDeState.termsEnum = fromTermsEnum;
          fromDeState.docsEnum = null;
          
          SolrIndexSearcher.DocsEnumState toDeState = null;
          toDeState = new SolrIndexSearcher.DocsEnumState();
          toDeState.fieldName = toField;
          toDeState.liveDocs = toLiveDocs;
          toDeState.termsEnum = toTermsEnum;
          toDeState.docsEnum = null;
//          toDeState.minSetSizeCached = minDocFreqTo;
          
//          UnInvertedField uif = UnInvertedField.getUnInvertedField(fromField, fromSearcher);
//          SortedSetDocValues ssdv = uif.iterator(fromSearcher.getAtomicReader());
//          DocSet fromSet = fromSearcher.getDocSet(q);
//          DocIterator docIterator = fromSet.iterator();
//          while(docIterator.hasNext()){
//            int fromDocID = docIterator.nextDoc();
//            ssdv.setDocument(fromDocID);
//            FixedBitSet resultBits = new FixedBitSet(toSearcher.maxDoc());
//            for(long tempOrd = ssdv.nextOrd() ; tempOrd != SortedSetDocValues.NO_MORE_ORDS ; tempOrd = ssdv.nextOrd()){
//              if(toDeState.termsEnum.seekExact(ssdv.lookupOrd(tempOrd))){
//                toDeState.docsEnum = toDeState.termsEnum.docs(toDeState.liveDocs, toDeState.docsEnum, DocsEnum.FLAG_NONE);
//                DocsEnum docsEnum = toDeState.docsEnum;
//                if (docsEnum instanceof MultiDocsEnum) {
//                  MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
//                  int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
//                  for (int subindex = 0; subindex<numSubs; subindex++) {
//                    MultiDocsEnum.EnumWithSlice sub = subs[subindex];
//                    if (sub.docsEnum == null) continue;
//                    int base = sub.slice.start;
//                    int docid;
//                    while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
////                      docArray[++count] = docid + base;
//                      resultBits.set(docid + base);
//                    }
//                  }
//                } else {
//                  int docid;
//                  while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
////                    docArray[++count] = docid;
//                    resultBits.set(docid);
//                  }
//                }
//              }
//            }
//            int[] docArray = new int[resultBits.cardinality()];
//            DocIdSetIterator iterator = resultBits.iterator();
//            int docid, index = 0;
//            while ((docid = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
//              docArray[index++] = docid;
//            }
//            SortedIntDocSetNative sidn = new SortedIntDocSetNative(docArray);
//            docJoinResult[fromDocID] = sidn;
//          }
//          Runtime runtime = Runtime.getRuntime();
          int termCount = 0;
//          long lastUsedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024*1024);
          while(term != null){
//            if(termCount++ % 100 == 0){
//              System.out.println("Term Count:" + termCount);
//            }
//            termCount++;
            
            if(toTermsEnum.seekExact(term)){
//              if((runtime.totalMemory() - runtime.freeMemory()) / (1024*1024) - lastUsedMemory > 5){
//                System.out.println("Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / (1024*1024));
//                System.out.println("Term: " + term);
//                System.out.println("Term Count: " + termCount);
//              }
//              lastUsedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024*1024);
              DocSet fromResultDocSet = fromSearcher.getDocSet(fromDeState);
              DocSet toResultDocSet = toSearcher.getDocSet(toDeState);
              DocIterator toDocIterator = toResultDocSet.iterator();
              int toDocArray[] = new int[toResultDocSet.size()];
//              Set<Integer> toDocSet = new HashSet<Integer>(0, 1);
              int index = 0;
              while(toDocIterator.hasNext()){
//                docJoinResult.get(fromDocID).add(toDocIterator.nextDoc());
                toDocArray[index++] = toDocIterator.nextDoc();
//                toDocSet.add(toDocIterator.nextDoc());
              }
//              System.out.println("toDocArray length:" + toDocArray.length);
              
              DocIterator fromDocIterator = fromResultDocSet.iterator();
              int fromDocNumber = 0;
              while(fromDocIterator.hasNext()){
                fromDocNumber++;
                int fromDocID = fromDocIterator.nextDoc();
                if(docJoinResult[fromDocID] == null){
//                  docJoinResultTemp[fromDocID] = toDocSet;
//                  docJoinResult[fromDocID] = new SortedIntDocSet(toDocArray);
//                  docJoinResult[fromDocID] = new HashDocSet(toDocArray, 0, toDocArray.length);
                  docJoinResult[fromDocID] = toDocArray;
//                  docJoinResult[fromDocID] = new SortedIntDocSet(toDocArray);
//                  docJoinResult.put(fromDocID, new BitDocSet(new FixedBitSet(toSearcher.maxDoc())));
                }else{
//                  List<Integer> list = Arrays.asList(docJoinResult[fromDocID]);
//                  FixedBitSet target = new FixedBitSet(toSearcher.maxDoc());
//                  docJoinResult[fromDocID].setBitsOn(target);
//                  docJoinResultTemp[fromDocID].addAll(toDocSet);
//                  for(int i = 0 ; i < toDocArray.length ; i++){
//                    docJoinResult[fromDocID].exists(toDocArray[i]);
//                    target.set(toDocArray[i]);
//                  }
//                  docJoinResult[fromDocID] = docJoinResult[fromDocID].union(new SortedIntDocSet(toDocArray));
                  
                  HashDocSet tempDocSet = (HashDocSet) (new HashDocSet(toDocArray, 0, toDocArray.length)).union((new HashDocSet(docJoinResult[fromDocID], 0, docJoinResult[fromDocID].length)));
                  int[] newToDocArray = new int[tempDocSet.size()];
                  int newindex = 0;
                  for(DocIterator docIterator = tempDocSet.iterator(); docIterator.hasNext(); newindex++){
                    newToDocArray[newindex] = docIterator.next();
                  }
                  docJoinResult[fromDocID] = newToDocArray;
//                  docJoinResult[fromDocID] = docJoinResult[fromDocID].union(new HashDocSet(toDocArray, 0, toDocArray.length));
//                  docJoinResult[fromDocID] = (new SortedIntDocSet(toDocArray)).union(docJoinResult[fromDocID]);
//                  (new SortedIntDocSetNative(toDocArray)).addAllTo(docJoinResult[fromDocID]);
//                  docJoinResult[fromDocID] = ArrayUtils.addAll(docJoinResult[fromDocID], toDocArray);
                }
//                System.out.println(ObjectSizeFetcher.getObjectSize(docJoinResult[fromDocID]));
//                System.out.println("docJoinResult[fromDocID] length:" + docJoinResult[fromDocID].size());
              }
//              System.out.println("fromDocNumber length:" + fromDocNumber);
//              FixedBitSet fromDocResultBits = fetchDocsFromTerm(fromLiveDocs, fromDeState, fromTermsEnum, term);
//              FixedBitSet toDocResultBits = fetchDocsFromTerm(toLiveDocs, toDeState, toTermsEnum, term);
//              DocIdSetIterator iterator = fromDocResultBits.iterator();
//              int docid;
//              while ((docid = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
//                if(docJoinResult.get(docid) == null){
//                  docJoinResult.put(docid, new BitDocSet());
//                }
//                docJoinResult.get(docid).
//              }
//              int totalSize = 0, hitDocSize = 0;;
//              for(int i = 0 ; i < docJoinResult.length ; i++){
//                if(docJoinResult[i] != null){
//                  totalSize += docJoinResult[i].length;
//                  hitDocSize++;
//                }
//              }
//              System.out.println("hitDocSize: " + hitDocSize + ", Total Size:" + totalSize);
            }
            term = fromTermsEnum.next();
          }
          int totalSize = 0, hitDocSize = 0;;
          for(int i = 0 ; i < docJoinResult.length ; i++){
            if(docJoinResult[i] != null){
              totalSize += docJoinResult[i].length;
              hitDocSize++;
            }
          }
          System.out.println("hitDocSize: " + hitDocSize + ", Total Size:" + totalSize);
//          for(int i = 0 ; i < docJoinResultTemp.length ; i++){
//            int[] a = ArrayUtils.toPrimitive(docJoinResultTemp[i].toArray(new Integer[0]));
//            docJoinResult[i] = new HashDocSet(a, 0, a.length);
//          }

          
        } catch (IOException e) {
          throw new RuntimeException();
        }
        toSearcher.joinQueryResultCache.put(jqrk, docJoinResult);
        return docJoinResult;
      }
    }
    
    private int[][] buildJoinResultCache(SolrIndexSearcher fromSearcher, SolrIndexSearcher toSearcher, String fromField, String toField){
      JoinQueryResultKey jqrk = new JoinQueryResultKey(toSearcher.getName(), fromField, toField);
      if(!rb.req.getParams().getBool("refreshCache", false) && toSearcher.joinQueryResultCache.get(jqrk) != null){
        System.out.println("Cache Hit");
        return toSearcher.joinQueryResultCache.get(jqrk);
      }else{
        System.out.println("Cache Not Hit");
        int[][] docJoinResult = new int[fromSearcher.maxDoc()][];
        try {
          Fields fromFields = fromSearcher.getAtomicReader().fields();
          Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getAtomicReader().fields();
          Terms fromTerms = fromFields.terms(fromField);
          Terms toTerms = toFields.terms(toField);
          TermsEnum  fromTermsEnum = fromTerms.iterator(null);
          TermsEnum  toTermsEnum = toTerms.iterator(null);
          BytesRef term = fromTermsEnum.next();
          
          Bits fromLiveDocs = fromSearcher.getAtomicReader().getLiveDocs();
          Bits toLiveDocs = toSearcher.getAtomicReader().getLiveDocs();
          
          SolrIndexSearcher.DocsEnumState fromDeState = null;
          fromDeState = new SolrIndexSearcher.DocsEnumState();
          fromDeState.fieldName = fromField;
          fromDeState.liveDocs = fromLiveDocs;
          fromDeState.termsEnum = fromTermsEnum;
          fromDeState.docsEnum = null;
          
          SolrIndexSearcher.DocsEnumState toDeState = null;
          toDeState = new SolrIndexSearcher.DocsEnumState();
          toDeState.fieldName = toField;
          toDeState.liveDocs = toLiveDocs;
          toDeState.termsEnum = toTermsEnum;
          toDeState.docsEnum = null;
          int termCount = 0;
          while(term != null){           
            if(toTermsEnum.seekExact(term)){
              DocSet fromResultDocSet = fromSearcher.getDocSet(fromDeState);
              DocSet toResultDocSet = toSearcher.getDocSet(toDeState);
              DocIterator toDocIterator = toResultDocSet.iterator();
              int toDocArray[] = new int[toResultDocSet.size()];
              int index = 0;
              while(toDocIterator.hasNext()){
                toDocArray[index++] = toDocIterator.nextDoc();
              }
              DocIterator fromDocIterator = fromResultDocSet.iterator();
              int fromDocNumber = 0;
              while(fromDocIterator.hasNext()){
                fromDocNumber++;
                int fromDocID = fromDocIterator.nextDoc();
                if(docJoinResult[fromDocID] == null){
                  docJoinResult[fromDocID] = toDocArray;
                }else{   
                  HashDocSet tempDocSet = (HashDocSet) (new HashDocSet(toDocArray, 0, toDocArray.length)).union((new HashDocSet(docJoinResult[fromDocID], 0, docJoinResult[fromDocID].length)));
                  int[] newToDocArray = new int[tempDocSet.size()];
                  int newindex = 0;
                  for(DocIterator docIterator = tempDocSet.iterator(); docIterator.hasNext(); newindex++){
                    newToDocArray[newindex] = docIterator.next();
                  }
                  docJoinResult[fromDocID] = newToDocArray;
                }
              }
            }
            term = fromTermsEnum.next();
          }
          int totalSize = 0, hitDocSize = 0;;
          for(int i = 0 ; i < docJoinResult.length ; i++){
            if(docJoinResult[i] != null){
              totalSize += docJoinResult[i].length;
              hitDocSize++;
            }
          }
        } catch (IOException e) {
          throw new RuntimeException();
        }
        toSearcher.joinQueryResultCache.put(jqrk, docJoinResult);
        return docJoinResult;
      }
    }
    
    public DocSet getDocSetFromJoinChain() throws IOException{
      SolrIndexSearcher fromSearcher;
      SolrIndexSearcher toSearcher;
      String fromField;
      String toField;
      Query q;
      DocSet joinResult = null;
      for(Map<String, Object> joinQueryMap : joinList){
        fromSearcher = searcherMap.get((String)joinQueryMap.get("fromIndex"));
        toSearcher = searcherMap.get((String)joinQueryMap.get("toIndex"));
        fromField = (String)joinQueryMap.get("fromField");
        toField = (String)joinQueryMap.get("toField");
        q = (Query)joinQueryMap.get("q");
        if(joinResult == null){
          joinResult = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, q, null);
        }else{
          joinResult = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, q, joinResult);
        }
      }
      return joinResult;
    }
    
    public DocSet getDocSetNewWithCache(SolrIndexSearcher fromSearcher, SolrIndexSearcher toSearcher, String fromField, String toField, Query q, DocSet filterDocSet) throws IOException{
      DocSet fromSet = (filterDocSet==null?fromSearcher.getDocSet(q):fromSearcher.getDocSet(q, filterDocSet));
//      fromSetSize = fromSet.size();
      DocIterator docIterator = fromSet.iterator();
      int[][]docJoinAllResult = buildJoinResultCache(fromSearcher, toSearcher, fromField, toField);
      FixedBitSet joinResultBitSet = new FixedBitSet(toSearcher.maxDoc());
      while(docIterator.hasNext()){
        int[] toDocSet = docJoinAllResult[docIterator.nextDoc()];
        if(toDocSet != null){
          for(int i = 0 ; i < toDocSet.length ; i++){
            joinResultBitSet.set(toDocSet[i]);
          }
        }
      }
      return new BitDocSetNative(joinResultBitSet);
    }
    
    public DocSet getDocSetNewWithCache() throws IOException{
//      SchemaField sf = fromSearcher.getSchema().getField(fromField);
//      QueryContext qcontext = QueryContext.newContext(fromSearcher);
//      long time1 = System.currentTimeMillis();
//      SortedDocValues sortedDocValues = FieldUtil.getSortedDocValues(qcontext, sf, null);
//      long time2 = System.currentTimeMillis();
//      System.out.println("=======Zhitao Log======= FieldUtil.getSortedDocValues:" + (time2 - time1));
      DocSet fromSet = fromSearcher.getDocSet(q);
      fromSetSize = fromSet.size();
//      Fields fromFields = fromSearcher.getAtomicReader().fields();
//      Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getAtomicReader().fields();
//      Terms toTerms = toFields.terms(toField);
//      TermsEnum  toTermsEnum = toTerms.iterator(null);
      DocIterator docIterator = fromSet.iterator();
//      Set<Integer> termSet = new HashSet<Integer>();      
      
//      long time3 = System.currentTimeMillis();
//      UnInvertedField uif = UnInvertedField.getUnInvertedField(fromField, fromSearcher);
//      SortedSetDocValues ssdv = uif.iterator(fromSearcher.getAtomicReader());
//      FixedBitSet termSetBit = new FixedBitSet(uif.numTerms());
      
      int[][]docJoinAllResult = buildJoinResultCache();
      FixedBitSet joinResultBitSet = new FixedBitSet(toSearcher.maxDoc());
      while(docIterator.hasNext()){
        int[] toDocSet = docJoinAllResult[docIterator.nextDoc()];
        if(toDocSet != null){
          for(int i = 0 ; i < toDocSet.length ; i++){
            joinResultBitSet.set(toDocSet[i]);
          }
        }
      }
      return new BitDocSet(joinResultBitSet);
      
//      DocSet[] docJoinAllResult = buildJoinResultCache();
//      FixedBitSet joinResultBitSet = new FixedBitSet(toSearcher.maxDoc());
//      while(docIterator.hasNext()){
//        DocSet toDocSet = docJoinAllResult[docIterator.nextDoc()];
//        if(toDocSet != null){
//          DocIterator iterator = toDocSet.iterator();
//          while(iterator.hasNext()){
//              joinResultBitSet.set(iterator.nextDoc());
//          }
//
//        }
//      }
//      return new BitDocSet(joinResultBitSet);
    }

    public DocSet getDocSetNew() throws IOException{
      
      
      SchemaField sf = fromSearcher.getSchema().getField(fromField);
      QueryContext qcontext = QueryContext.newContext(fromSearcher);
      long time1 = System.currentTimeMillis();
      SortedDocValues sortedDocValues = FieldUtil.getSortedDocValues(qcontext, sf, null);
      long time2 = System.currentTimeMillis();
      System.out.println("=======Zhitao Log======= FieldUtil.getSortedDocValues:" + (time2 - time1));
      DocSet fromSet = fromSearcher.getDocSet(q);
      fromSetSize = fromSet.size();
      Fields fromFields = fromSearcher.getAtomicReader().fields();
      Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getAtomicReader().fields();
      Terms toTerms = toFields.terms(toField);
      TermsEnum  toTermsEnum = toTerms.iterator(null);
      DocIterator docIterator = fromSet.iterator();
      Set<Integer> termSet = new HashSet<Integer>();      
      
      long time3 = System.currentTimeMillis();
      UnInvertedField uif = UnInvertedField.getUnInvertedField(fromField, fromSearcher);
      SortedSetDocValues ssdv = uif.iterator(fromSearcher.getAtomicReader());
      FixedBitSet termSetBit = new FixedBitSet(uif.numTerms());
      
//      Map<Integer, Set<Integer>> docJoinAllResult = buildJoinResultCache();
//      FixedBitSet joinResultBitSet = new FixedBitSet(toSearcher.maxDoc());
//      while(docIterator.hasNext()){
//        Set<Integer> toDocSet = docJoinAllResult.get(docIterator.nextDoc());
//        if(toDocSet != null){
//          for(int docid : toDocSet){
//            joinResultBitSet.set(docid);
//          }
//        }
//      }
//      return new BitDocSet(joinResultBitSet);
      
      while(docIterator.hasNext()){
        ssdv.setDocument(docIterator.nextDoc());
        for(long tempOrd = ssdv.nextOrd() ; tempOrd != SortedSetDocValues.NO_MORE_ORDS ; tempOrd = ssdv.nextOrd()){
          termSet.add((int)tempOrd);
          termSetBit.set((int)tempOrd);
        }
      }

      

      long time4 = System.currentTimeMillis();
      System.out.println("=======Zhitao Log======= DocIterator Loop:" + (time4 - time3));
      FixedBitSet resultBits = null;
      // minimum docFreq to use the cache
      int minDocFreqFrom = Math.max(5, fromSearcher.maxDoc() >> 13);
      int minDocFreqTo = Math.max(5, toSearcher.maxDoc() >> 13);
      // use a smaller size than normal since we will need to sort and dedup the results
      int maxSortedIntSize = Math.max(10, toSearcher.maxDoc() >> 10);
      Bits fromLiveDocs = fromSearcher.getAtomicReader().getLiveDocs();
      Bits toLiveDocs = fromSearcher == toSearcher ? fromLiveDocs : toSearcher.getAtomicReader().getLiveDocs();
      SolrIndexSearcher.DocsEnumState toDeState = null;
      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.docsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;
      LinkedList<DocSet> resultList = new LinkedList<DocSet>();
      System.out.println("TermSetSize:" + termSet.size());
      long time5 = System.currentTimeMillis();
      
      long timeFirstCondition = 0, timeSecondCondition = 0, timeSecondFirstCondition = 0, timeSecondSecondCondition = 0;
      long seekPart = 0, dfPart = 0;
      int first = 0, second = 0, firstsecond = 0, secondsecond = 0;
      
      DocIdSetIterator termIterator = termSetBit.iterator();
      for(int termOrd = termIterator.nextDoc() ; termOrd != DocIdSetIterator.NO_MORE_DOCS ; termOrd = termIterator.nextDoc()){
        long timeSeekStart = System.currentTimeMillis();
        TermsEnum.SeekStatus status = toTermsEnum.seekCeil(sortedDocValues.lookupOrd(termOrd));
        long timeSeekEnd = System.currentTimeMillis();
        seekPart += timeSeekEnd - timeSeekStart;
        if (status == TermsEnum.SeekStatus.END) break;
        if (status == TermsEnum.SeekStatus.FOUND) {
          
          long getdfStart = System.currentTimeMillis();
          toTermHits++;
          int df = toTermsEnum.docFreq();
          toTermHitsTotalDf += df;
          if (resultBits==null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
            resultBits = new FixedBitSet(toSearcher.maxDoc());
          }
          long getdfEnd = System.currentTimeMillis();
          dfPart += getdfEnd - getdfStart;
          // if we don't have a bitset yet, or if the resulting set will be too large
          // use the filterCache to get a DocSet
          long time11 = System.currentTimeMillis();
          if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
            first++;
            // use filter cache
            DocSet toTermSet = toSearcher.getDocSet(toDeState);
            resultListDocs += toTermSet.size();
            if (resultBits != null) {
              toTermSet.setBitsOn(resultBits);
              toTermSet.decref();
            } else {
              if (toTermSet instanceof BitDocSetNative) {
                resultBits = ((BitDocSetNative)toTermSet).toFixedBitSet();
                toTermSet.decref();
              } else if (toTermSet instanceof BitDocSet) {
                // shouldn't happen any more?
                resultBits = (FixedBitSet)((BitDocSet)toTermSet).bits.clone();
              } else {
                // should be SortedIntDocSetNative
                resultList.add(toTermSet);
              }
            }
            long time22 = System.currentTimeMillis();
            timeFirstCondition += time22 - time11;
          } else {
            toTermDirectCount++;
            second++;
            // need to use liveDocs here so we don't map to any deleted ones
            toDeState.docsEnum = toDeState.termsEnum.docs(toDeState.liveDocs, toDeState.docsEnum, DocsEnum.FLAG_NONE);
            DocsEnum docsEnum = toDeState.docsEnum;
            long time111 = System.currentTimeMillis();
            if (docsEnum instanceof MultiDocsEnum) {
              firstsecond++;
              MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
              int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
              for (int subindex = 0; subindex<numSubs; subindex++) {
                MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                if (sub.docsEnum == null) continue;
                int base = sub.slice.start;
                int docid;
                while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid + base);
                }
              }
              long time222 = System.currentTimeMillis();
              timeSecondFirstCondition += time222 - time111;
            } else {
              secondsecond++;
              int docid;
              while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                resultListDocs++;
                resultBits.set(docid);
              }
              long time222 = System.currentTimeMillis();
              timeSecondSecondCondition += time222 - time111;
            }
            long time22 = System.currentTimeMillis();
            timeSecondCondition += time22 - time11;
          }
          
        }
      }

      long time6 = System.currentTimeMillis();
      System.out.println("=======Zhitao Log======= TermSet Loop:" + (time6 - time5));
      System.out.println("=======Zhitao Log======= SeekPart:" + seekPart);
      System.out.println("=======Zhitao Log======= dfPart:" + dfPart);
      System.out.println("=======Zhitao Log======= timeFirstCondition:" + timeFirstCondition);
      System.out.println("=======Zhitao Log======= timeSecondCondition:" + timeSecondCondition);
      System.out.println("=======Zhitao Log======= timeSecondFirstCondition:" + timeSecondFirstCondition);
      System.out.println("=======Zhitao Log======= timeSecondSecondCondition:" + timeSecondSecondCondition);
      System.out.println("=======Zhitao Log======= first:" + first);
      System.out.println("=======Zhitao Log======= second:" + second);
      System.out.println("=======Zhitao Log======= firstsecond:" + firstsecond);
      System.out.println("=======Zhitao Log======= secondsecond:" + secondsecond);

      smallSetsDeferred = resultList.size();

      if (resultBits != null) {

        for(;;) {
          DocSet set = resultList.pollFirst();
          if (set == null) break;
          set.setBitsOn(resultBits);
          set.decref();
        }
        return new BitDocSet(resultBits);
      }

      if (resultList.size()==0) {
        return DocSet.EMPTY;
      }

      /** This could be off-heap, and we don't want to have to try and free it later
       if (resultList.size() == 1) {
       return resultList.get(0);
       }
       **/

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;

      for(;;) {
        DocSet set = resultList.pollFirst();
        if (set == null) break;
        if (set instanceof SortedIntDocSet) {
          System.arraycopy(((SortedIntDocSet)set).getDocs(), 0, docs, pos, set.size());
        } else {
          HS.copyInts(((SortedIntDocSetNative)set).getIntArrayPointer(), 0, docs, pos, set.size());
        }
        pos += set.size();
        set.decref();
      }

      Arrays.sort(docs);  // TODO: try switching to timsort or something like a bucket sort for numbers...
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }
      long time7 = System.currentTimeMillis();
      System.out.println("=======Zhitao Log======= Other After TermSet Loop:" + (time7 - time6));
      System.out.println("=======Zhitao Log======= GetDocSetNew Total:" + (time7 - time1));
      return new SortedIntDocSet(dedup, dedup.length);
    }

    public DocSet getDocSetOld() throws IOException {
//    log.info("====Log By Zhitao==== Time of Total getFieldCacheCounts " + (System.currentTimeMillis() - time1));
    long intersectTime = 0L, findDocTime = 0L, findDocCond1Time = 0L, findDocCond2Time = 0L, seekTermTime = 0;
    
    long time1 = System.currentTimeMillis();
    FixedBitSet resultBits = null;

    // minimum docFreq to use the cache
    int minDocFreqFrom = Math.max(5, fromSearcher.maxDoc() >> 13);
    int minDocFreqTo = Math.max(5, toSearcher.maxDoc() >> 13);

    // use a smaller size than normal since we will need to sort and dedup the results
    int maxSortedIntSize = Math.max(10, toSearcher.maxDoc() >> 10);

    // TODO: set new SolrRequestInfo???
    DocSet fromSet = fromSearcher.getDocSet(q);
    fromSetSize = fromSet.size();
    long time2 = System.currentTimeMillis();
    LinkedList<DocSet> resultList = new LinkedList<DocSet>();
    try {

      // make sure we have a set that is fast for random access, if we will use it for that
      DocSet fastForRandomSet = fromSet;
      if (minDocFreqFrom>0 && fromSet instanceof SortedIntDocSetNative) {
        SortedIntDocSetNative sset = (SortedIntDocSetNative)fromSet;
        fastForRandomSet = new HashDocSet(sset.getIntArrayPointer(), 0, sset.size(), HashDocSet.DEFAULT_INVERSE_LOAD_FACTOR);
      }

      Fields fromFields = fromSearcher.getAtomicReader().fields();
      Fields toFields = fromSearcher==toSearcher ? fromFields : toSearcher.getAtomicReader().fields();
      if (fromFields == null) return DocSet.EMPTY;
      Terms terms = fromFields.terms(fromField);
      Terms toTerms = toFields.terms(toField);
      if (terms == null || toTerms==null) return DocSet.EMPTY;
      String prefixStr = TrieField.getMainValuePrefix(fromSearcher.getSchema().getFieldType(fromField));
      BytesRef prefix = prefixStr == null ? null : new BytesRef(prefixStr);

      BytesRef term = null;
      TermsEnum  termsEnum = terms.iterator(null);
      TermsEnum  toTermsEnum = toTerms.iterator(null);
      SolrIndexSearcher.DocsEnumState fromDeState = null;
      SolrIndexSearcher.DocsEnumState toDeState = null;

      if (prefix == null) {
        term = termsEnum.next();
      } else {
        if (termsEnum.seekCeil(prefix) != TermsEnum.SeekStatus.END) {
          term = termsEnum.term();
        }
      }

      Bits fromLiveDocs = fromSearcher.getAtomicReader().getLiveDocs();
      Bits toLiveDocs = fromSearcher == toSearcher ? fromLiveDocs : toSearcher.getAtomicReader().getLiveDocs();

      fromDeState = new SolrIndexSearcher.DocsEnumState();
      fromDeState.fieldName = fromField;
      fromDeState.liveDocs = fromLiveDocs;
      fromDeState.termsEnum = termsEnum;
      fromDeState.docsEnum = null;
      fromDeState.minSetSizeCached = minDocFreqFrom;

      toDeState = new SolrIndexSearcher.DocsEnumState();
      toDeState.fieldName = toField;
      toDeState.liveDocs = toLiveDocs;
      toDeState.termsEnum = toTermsEnum;
      toDeState.docsEnum = null;
      toDeState.minSetSizeCached = minDocFreqTo;
      long time3 = System.currentTimeMillis();
      while (term != null) {
        if (prefix != null && !StringHelper.startsWith(term, prefix))
          break;

        fromTermCount++;

        long timeBeforeIntersect = System.currentTimeMillis();
        boolean intersects = false;
        int freq = termsEnum.docFreq();
        fromTermTotalDf++;

        if (freq < minDocFreqFrom) {
          fromTermDirectCount++;
          // OK to skip liveDocs, since we check for intersection with docs matching query
          fromDeState.docsEnum = fromDeState.termsEnum.docs(null, fromDeState.docsEnum, DocsEnum.FLAG_NONE);
          DocsEnum docsEnum = fromDeState.docsEnum;

          if (docsEnum instanceof MultiDocsEnum) {
            MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
            int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
            outer: for (int subindex = 0; subindex<numSubs; subindex++) {
              MultiDocsEnum.EnumWithSlice sub = subs[subindex];
              if (sub.docsEnum == null) continue;
              int base = sub.slice.start;
              int docid;
              while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (fastForRandomSet.exists(docid+base)) {
                  intersects = true;
                  break outer;
                }
              }
            }
          } else {
            int docid;
            while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
              if (fastForRandomSet.exists(docid)) {
                intersects = true;
                break;
              }
            }
          }
        } else {
          // use the filter cache
          DocSet fromTermSet = fromSearcher.getDocSet(fromDeState);
          intersects = fromSet.intersects(fromTermSet);
          fromTermSet.decref();
        }
        intersectTime += System.currentTimeMillis() - timeBeforeIntersect;

        if (intersects) {
          fromTermHits++;
          fromTermHitsTotalDf++;
          long findDocStartTime = System.currentTimeMillis();
          TermsEnum.SeekStatus status = toTermsEnum.seekCeil(term);
          seekTermTime += System.currentTimeMillis() - findDocStartTime;
          if (status == TermsEnum.SeekStatus.END) break;
          if (status == TermsEnum.SeekStatus.FOUND) {
            toTermHits++;
            int df = toTermsEnum.docFreq();
            toTermHitsTotalDf += df;
            if (resultBits==null && df + resultListDocs > maxSortedIntSize && resultList.size() > 0) {
              resultBits = new FixedBitSet(toSearcher.maxDoc());
            }

            // if we don't have a bitset yet, or if the resulting set will be too large
            // use the filterCache to get a DocSet
            if (toTermsEnum.docFreq() >= minDocFreqTo || resultBits == null) {
              long findDocCond1StartTime = System.currentTimeMillis();
              // use filter cache
              DocSet toTermSet = toSearcher.getDocSet(toDeState);
              resultListDocs += toTermSet.size();
              if (resultBits != null) {
                toTermSet.setBitsOn(resultBits);
                toTermSet.decref();
              } else {
                if (toTermSet instanceof BitDocSetNative) {
                  resultBits = ((BitDocSetNative)toTermSet).toFixedBitSet();
                  toTermSet.decref();
                } else if (toTermSet instanceof BitDocSet) {
                  // shouldn't happen any more?
                  resultBits = (FixedBitSet)((BitDocSet)toTermSet).bits.clone();
                } else {
                  // should be SortedIntDocSetNative
                  resultList.add(toTermSet);
                }
              }
              findDocCond1Time += System.currentTimeMillis() - findDocCond1StartTime;
            } else {
              toTermDirectCount++;
              long findDocCond2StartTime = System.currentTimeMillis();
              // need to use liveDocs here so we don't map to any deleted ones
              toDeState.docsEnum = toDeState.termsEnum.docs(toDeState.liveDocs, toDeState.docsEnum, DocsEnum.FLAG_NONE);
              DocsEnum docsEnum = toDeState.docsEnum;

              if (docsEnum instanceof MultiDocsEnum) {
                MultiDocsEnum.EnumWithSlice[] subs = ((MultiDocsEnum)docsEnum).getSubs();
                int numSubs = ((MultiDocsEnum)docsEnum).getNumSubs();
                for (int subindex = 0; subindex<numSubs; subindex++) {
                  MultiDocsEnum.EnumWithSlice sub = subs[subindex];
                  if (sub.docsEnum == null) continue;
                  int base = sub.slice.start;
                  int docid;
                  while ((docid = sub.docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    resultListDocs++;
                    resultBits.set(docid + base);
                  }
                }
              } else {
                int docid;
                while ((docid = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  resultListDocs++;
                  resultBits.set(docid);
                }
              }
              findDocCond2Time += System.currentTimeMillis() - findDocCond2StartTime;
            }

          }
          findDocTime += System.currentTimeMillis() - findDocStartTime;
        }

        term = termsEnum.next();
      }
      smallSetsDeferred = resultList.size();

      System.out.println("====Log By Zhitao==== intersectTime " + intersectTime);
      System.out.println("====Log By Zhitao==== findDocTime " + findDocTime);
      System.out.println("====Log By Zhitao==== seekTermTime " + seekTermTime);
      System.out.println("====Log By Zhitao==== findDocCond1Time " + findDocCond1Time);
      System.out.println("====Log By Zhitao==== findDocCond2Time " + findDocCond2Time);

      long createResultStartTime = System.currentTimeMillis();
      if (resultBits != null) {

        for(;;) {
          DocSet set = resultList.pollFirst();
          if (set == null) break;
          set.setBitsOn(resultBits);
          set.decref();
        }
        System.out.println("====Log By Zhitao==== createResultTime resultBits!=null " + (System.currentTimeMillis() - createResultStartTime));
        return new BitDocSet(resultBits);
      }

      if (resultList.size()==0) {
        return DocSet.EMPTY;
      }

      /** This could be off-heap, and we don't want to have to try and free it later
       if (resultList.size() == 1) {
       return resultList.get(0);
       }
       **/

      int sz = 0;

      for (DocSet set : resultList)
        sz += set.size();

      int[] docs = new int[sz];
      int pos = 0;

      for(;;) {
        DocSet set = resultList.pollFirst();
        if (set == null) break;
        if (set instanceof SortedIntDocSet) {
          System.arraycopy(((SortedIntDocSet)set).getDocs(), 0, docs, pos, set.size());
        } else {
          HS.copyInts(((SortedIntDocSetNative)set).getIntArrayPointer(), 0, docs, pos, set.size());
        }
        pos += set.size();
        set.decref();
      }

      Arrays.sort(docs);  // TODO: try switching to timsort or something like a bucket sort for numbers...
      int[] dedup = new int[sz];
      pos = 0;
      int last = -1;
      for (int doc : docs) {
        if (doc != last)
          dedup[pos++] = doc;
        last = doc;
      }

      if (pos != dedup.length) {
        dedup = Arrays.copyOf(dedup, pos);
      }
      System.out.println("====Log By Zhitao==== createResultTime " + (System.currentTimeMillis() - createResultStartTime));
      return new SortedIntDocSet(dedup, dedup.length);

    } finally {
      fromSet.decref();
      // resultList should be empty, except if an exception happened somewhere
      for (DocSet set : resultList) {
        set.decref();
      }
    }
    }

    @Override
    public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
      Scorer scorer = scorer(context, context.reader().getLiveDocs());
      boolean exists = scorer.advance(doc) == doc;

      ComplexExplanation result = new ComplexExplanation();

      if (exists) {
        result.setDescription(this.toString()
        + " , product of:");
        result.setValue(queryWeight);
        result.setMatch(Boolean.TRUE);
        result.addDetail(new Explanation(getBoost(), "boost"));
        result.addDetail(new Explanation(queryNorm,"queryNorm"));
      } else {
        result.setDescription(this.toString()
        + " doesn't match id " + doc);
        result.setValue(0);
        result.setMatch(Boolean.FALSE);
      }
      return result;
    }
  }


  protected static class JoinScorer extends Scorer {
    final DocIdSetIterator iter;
    final float score;
    int doc = -1;

    public JoinScorer(Weight w, DocIdSetIterator iter, float score) throws IOException {
      super(w);
      this.score = score;
      this.iter = iter==null ? DocIdSetIterator.empty() : iter;
    }

    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }

    @Override
    public int docID() {
      return iter.docID();
    }

    @Override
    public float score() throws IOException {
      return score;
    }
    
    @Override
    public int freq() throws IOException {
      return 1;
    }

    @Override
    public int advance(int target) throws IOException {
      return iter.advance(target);
    }

    @Override
    public long cost() {
      return iter.cost();
    }
  }


  @Override
  public String toString(String field) {
    return "{!join from="+fromField+" to="+toField
        + (fromIndex != null ? " fromIndex="+fromIndex : "")
        +"}"+q.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    JoinQuery other = (JoinQuery)o;
    return this.fromField.equals(other.fromField)
           && this.toField.equals(other.toField)
           && this.getBoost() == other.getBoost()
           && this.q.equals(other.q)
           && (this.fromIndex == other.fromIndex || this.fromIndex != null && this.fromIndex.equals(other.fromIndex))
           && this.fromCoreOpenTime == other.fromCoreOpenTime
        ;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = h * 31 + q.hashCode();
    h = h * 31 + (int)fromCoreOpenTime;
    h = h * 31 + fromField.hashCode();
    h = h * 31 + toField.hashCode();
    return h;
  }

}
