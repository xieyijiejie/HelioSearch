package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.JoinQuery.JoinScorer;
import org.apache.solr.util.RefCounted;
import org.noggit.ObjectBuilder;

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

public class JoinJsonQParserPlugin extends QParserPlugin{
  public static final String NAME = "jsonjoin";
  
  @Override
  public void init(NamedList args) {}
  
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      private void parseJoinQueryObject(Map<String, Object> joinQueryObject) throws SyntaxError{
        
        String op = "";
        if(joinQueryObject.containsKey("$and")){
          op = "$and";
          
        }else if(joinQueryObject.containsKey("$or")){
          op = "$or";
        }else if(joinQueryObject.containsKey("$chain")){
          op = "$chain";
        }
        if(op.isEmpty()){
          String fromIndexName = (String)joinQueryObject.get("fromIndex");
          String toIndexName = (String)joinQueryObject.get("toIndex");
          
          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();
          final SolrCore fromCore = container.getCore(fromIndexName);
          RefCounted<SolrIndexSearcher> fromHolder = null;

          if (fromCore == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndexName);
          }
          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
          try {
            QParser parser = QParser.getParser(joinQueryObject.containsKey("fromQuery")?(String)joinQueryObject.get("fromQuery"):"*:*", "lucene", otherReq);
            Query q = parser.getQuery();
            joinQueryObject.put("fromq", q);
          } finally {
            otherReq.close();
            fromCore.close();
            if (fromHolder != null) fromHolder.decref();
          }
          
          if(joinQueryObject.containsKey("toQuery")){
            final SolrCore toCore = container.getCore(toIndexName);
            RefCounted<SolrIndexSearcher> toHolder = null;

            if (toCore == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + toIndexName);
            }
            LocalSolrQueryRequest otherReqTo = new LocalSolrQueryRequest(toCore, params);
            try {
              QParser parser = QParser.getParser((String)joinQueryObject.get("toQuery"), "lucene", otherReqTo);
              Query q = parser.getQuery();
              joinQueryObject.put("toq", q);
            } finally {
              otherReqTo.close();
              toCore.close();
              if (toHolder != null) toHolder.decref();
            }
          }
        }else{
          List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)joinQueryObject.get(op);
          for(Map<String, Object> joinQuerySubObject : joinQueryList){
            parseJoinQueryObject(joinQuerySubObject);
          }
        }
      }
      
      @Override
      public Query parse() throws SyntaxError {
        Map<String, Object> joinQueryObject = new HashMap<String, Object>();
        
        String v = localParams.get("v");
        try {
          joinQueryObject = (HashMap<String,Object>)ObjectBuilder.fromJSON(v);
          parseJoinQueryObject(joinQueryObject);
        } catch (IOException e) {
          // impossible
        }
        parseJoinQueryObject(joinQueryObject);

//        for(Map<String, Object> joinInfo : joinList){
//
//          String fromIndexName = (String)joinInfo.get("fromIndex");
//          String toIndexName = (String)joinInfo.get("toIndex");
//          
//          CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();
//          final SolrCore fromCore = container.getCore(fromIndexName);
//          RefCounted<SolrIndexSearcher> fromHolder = null;
//
//          if (fromCore == null) {
//            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndexName);
//          }
//          LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(fromCore, params);
//          try {
//            QParser parser = QParser.getParser((String)joinInfo.get("q"), "lucene", otherReq);
//            Query q = parser.getQuery();
//            joinInfo.put("q", q);
//          } finally {
//            otherReq.close();
//            fromCore.close();
//            if (fromHolder != null) fromHolder.decref();
//          }
//        }
        return new JoinJsonQuery(joinQueryObject);
      }
    };
  }
}

class JoinJsonQuery extends Query {
  Map<String, Object> joinQueryObject = new HashMap<String, Object>();
  long fromCoreOpenTime;
  
  public JoinJsonQuery(Map<String, Object> joinQueryObject){
    this.joinQueryObject = joinQueryObject;
  }
  
  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new JoinJsonQueryWeight();
  }
  
  private class JoinJsonQueryWeight extends Weight {
    Map<String, SolrIndexSearcher> searcherMap = new HashMap<String, SolrIndexSearcher>();
    private float queryNorm;
    private float queryWeight;
    ResponseBuilder rb;
    DocSet resultSet;
    Filter filter;
    
    public JoinJsonQueryWeight(){
      SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
      if (info != null) {
        rb = info.getResponseBuilder();
      }
      buildSearcherMap(joinQueryObject);
    }
    
    private void buildSearcherMap(Map<String, Object> _joinQueryObject){
      String op = "";
      if(_joinQueryObject.containsKey("$and")){
        op = "$and";
        
      }else if(_joinQueryObject.containsKey("$or")){
        op = "$or";
      }else if(_joinQueryObject.containsKey("$chain")){
        op = "$chain";
      }
      if(op.isEmpty()){
        String fromIndexName = (String)_joinQueryObject.get("fromIndex");
        String toIndexName = (String)_joinQueryObject.get("toIndex");
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        if (info != null) {
          rb = info.getResponseBuilder();
        }
        if(!searcherMap.containsKey(fromIndexName)){
          CoreContainer container = rb.req.getCore().getCoreDescriptor().getCoreContainer();
          final SolrCore core = container.getCore(fromIndexName);
          if (core == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + fromIndexName);
          }
          RefCounted<SolrIndexSearcher> coreRef = core.getSearcher(false, true, null);
          searcherMap.put(fromIndexName, coreRef.get());
          final RefCounted<SolrIndexSearcher> ref = coreRef;
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              ref.decref();
            }
          });
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              core.close();
            }
          });
        }
        if(!searcherMap.containsKey(toIndexName)){
          CoreContainer container = rb.req.getCore().getCoreDescriptor().getCoreContainer();
          final SolrCore core = container.getCore(toIndexName);
          if (core == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + toIndexName);
          }
          RefCounted<SolrIndexSearcher> coreRef = core.getSearcher(false, true, null);
          searcherMap.put(toIndexName, coreRef.get());
          final RefCounted<SolrIndexSearcher> ref = coreRef;
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              ref.decref();
            }
          });
          info.addCloseHook(new Closeable() {
            @Override
            public void close() {
              core.close();
            }
          });
        }
      }else{
        List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)_joinQueryObject.get(op);
        for(Map<String, Object> joinQuerySubObject : joinQueryList){
          buildSearcherMap(joinQuerySubObject);
        }
      }
    }
    
    @Override
    public Explanation explain(AtomicReaderContext context, int doc)
        throws IOException {
      return null;
    }

    @Override
    public Query getQuery() {
      return JoinJsonQuery.this;
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

    @Override
    public Scorer scorer(AtomicReaderContext context, Bits acceptDocs)
        throws IOException {
      if (filter == null) {
        resultSet = getDocSet(joinQueryObject, null);
        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, acceptDocs);
      return new JoinScorer(this, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), getBoost());

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
    
    public DocSet getDocSetNewWithCache(SolrIndexSearcher fromSearcher, SolrIndexSearcher toSearcher, String fromField, String toField, DocSet fromDocSet) throws IOException{
//      DocSet fromSet = (filterDocSet==null?fromSearcher.getDocSet(q):fromSearcher.getDocSet(q, filterDocSet));
//      fromSetSize = fromSet.size();
      DocIterator docIterator = fromDocSet.iterator();
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
    
    public DocSet getDocSet(Map<String, Object> _joinQueryObject, DocSet filterDocSet) throws IOException{
      String op = "";
      if(_joinQueryObject.containsKey("$and")){
        op = "$and";
        
      }else if(_joinQueryObject.containsKey("$or")){
        op = "$or";
      }else if(_joinQueryObject.containsKey("$chain")){
        op = "$chain";
      }
      DocSet resultDocSet = null;
      if(op.isEmpty()){
        SolrIndexSearcher fromSearcher = searcherMap.get((String)_joinQueryObject.get("fromIndex"));
        SolrIndexSearcher toSearcher = searcherMap.get((String)_joinQueryObject.get("toIndex"));
        String fromField = (String)_joinQueryObject.get("fromField");
        String toField = (String)_joinQueryObject.get("toField");
        Query fromq = (Query)_joinQueryObject.get("fromq");
        DocSet toSet = null;
        if(_joinQueryObject.containsKey("toq")){
          Query toq = (Query)_joinQueryObject.get("toq");
          toSet = toSearcher.getDocSet(toq);
        }
        DocSet fromSet = (filterDocSet==null?fromSearcher.getDocSet(fromq):fromSearcher.getDocSet(fromq, filterDocSet));
        resultDocSet = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, fromSet);
        fromSet.decref();
        if(toSet != null){
          DocSet newResultDocSet = resultDocSet.intersection(toSet);
          resultDocSet.decref();
          toSet.decref();
          resultDocSet = newResultDocSet;
        }
        if("true".equals(_joinQueryObject.get("$not"))){
          DocSet allToSet = toSearcher.getPositiveDocSet(new MatchAllDocsQuery());
          DocSet newResultDocSet = allToSet.andNot(resultDocSet);
          allToSet.decref();
          resultDocSet.decref();
          resultDocSet = newResultDocSet;
        }
        
      }else{
        List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)_joinQueryObject.get(op);
        DocSet prevJoinResult = null;
        for(Map<String, Object> joinQuerySubObject : joinQueryList){
          DocSet subDocSet = getDocSet(joinQuerySubObject, prevJoinResult);
          if(prevJoinResult != null)prevJoinResult.decref();
          if(resultDocSet == null){
            resultDocSet = subDocSet;
          }else{
            if(op.equals("$and")){
              DocSet tempDocSet = resultDocSet.intersection(subDocSet);
              resultDocSet.decref();
              subDocSet.decref();
              resultDocSet = tempDocSet;
            }else if(op.equals("$or")){
              DocSet tempDocSet = resultDocSet.union(subDocSet);
              resultDocSet.decref();
              subDocSet.decref();
              resultDocSet = tempDocSet;
            }else if(op.equals("$chain")){
              prevJoinResult = resultDocSet = subDocSet;
            }
          }
          
        }
      }
      return resultDocSet;
    }
    
//    public DocSet getDocSetFromJoinChain() throws IOException{
//      SolrIndexSearcher fromSearcher;
//      SolrIndexSearcher toSearcher;
//      String fromField;
//      String toField;
//      Query q;
//      DocSet joinResult = null;
//      for(Map<String, Object> joinQueryMap : joinList){
//        fromSearcher = searcherMap.get((String)joinQueryMap.get("fromIndex"));
//        toSearcher = searcherMap.get((String)joinQueryMap.get("toIndex"));
//        fromField = (String)joinQueryMap.get("fromField");
//        toField = (String)joinQueryMap.get("toField");
//        q = (Query)joinQueryMap.get("q");
//        if(joinResult == null){
//          joinResult = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, q, null);
//        }else{
//          joinResult = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, q, joinResult);
//        }
//      }
//      return joinResult;
//    }
    
  }
  
  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) return false;
    JoinJsonQuery other = (JoinJsonQuery)o;
    return joinQueryObject.equals(other.joinQueryObject)
        && this.fromCoreOpenTime == other.fromCoreOpenTime
        ;
  }

  @Override
  public int hashCode() {
    int h = super.hashCode();
    h = h * 31 + joinQueryObject.hashCode();
    h = h * 31 + (int)fromCoreOpenTime;
    return h;
  }
  
  @Override
  public String toString(String field) {
    return "jsonjoin";
  }
  
}
