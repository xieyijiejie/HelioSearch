package org.apache.solr.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
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


/* Query Example
{!jsonjoin cache=false}{"$not": {"$or":[{"index":"metrix_clinicaltrials", "q":"study_type_id:8"}, {"$join":{"from":{"$join":{"from":{"index":"metrix_organization", "q":"province:11"}, "to":{"index":"metrix_hcp"}, "on":"id->institution_list"}}, "to":{"index":"metrix_clinicaltrials", "q":"study_type_id:9"}, "on":"id->link_hcp"}}]}}
 */

public class JoinJsonQParserPlugin extends QParserPlugin{
  public static final String NAME = "jsonjoin";
  
  @Override
  public void init(NamedList args) {}
  
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      private void parseJoinQueryObject(Map<String, Object> queryObject) throws SyntaxError{
        
        if(queryObject.containsKey("$join")){
          Map<String, Object> joinQueryObject = (Map<String, Object>)queryObject.get("$join");
          parseJoinQueryObject((Map<String, Object>)joinQueryObject.get("from"));
          parseJoinQueryObject((Map<String, Object>)joinQueryObject.get("to"));
        }else if(queryObject.containsKey("$not")){
          parseJoinQueryObject((Map<String, Object>)queryObject.get("$not"));
        }else{
          String op = "";
          if(queryObject.containsKey("$and")){
            op = "$and";
          }else if(queryObject.containsKey("$or")){
            op = "$or";
          }
          if(op.isEmpty()){
            String indexName = (String)queryObject.get("index");
            CoreContainer container = req.getCore().getCoreDescriptor().getCoreContainer();
            final SolrCore core = container.getCore(indexName);
            RefCounted<SolrIndexSearcher> holder = null;

            if (core == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + indexName);
            }
            LocalSolrQueryRequest otherReq = new LocalSolrQueryRequest(core, params);
            try {
              if(queryObject.get("q") == null){
                
              }else{
                if(queryObject.get("q") instanceof String){
                  QParser parser = QParser.getParser((String)queryObject.get("q"), "lucene", otherReq);
                  Query q = parser.getQuery();
                  List<Query> qList = new ArrayList<Query>();
                  qList.add(q);
                  queryObject.put("parsedq", qList);
                }else if(queryObject.get("q") instanceof List){
                  List<Query> qList = new ArrayList<Query>();
                  for(Object qObject : (List<Object>)queryObject.get("q")){
                    QParser parser = QParser.getParser(String.valueOf(qObject), "lucene", otherReq);
                    qList.add(parser.getQuery());
                  }
                  queryObject.put("parsedq", qList);
                }

              }
            } finally {
              otherReq.close();
              core.close();
              if (holder != null) holder.decref();
            }
          }else{
            List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)queryObject.get(op);
            for(Map<String, Object> joinQuerySubObject : joinQueryList){
              parseJoinQueryObject(joinQuerySubObject);
            }
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
    
    private void buildSearcherMap(Map<String, Object> _queryObject){
      if(_queryObject.containsKey("$join")){
        Map<String, Object> _joinQueryObject = (Map<String, Object>)_queryObject.get("$join");
        buildSearcherMap((Map<String, Object>)_joinQueryObject.get("from"));
        buildSearcherMap((Map<String, Object>)_joinQueryObject.get("to"));
      }else if(_queryObject.containsKey("$not")){
        buildSearcherMap((Map<String, Object>)_queryObject.get("$not"));
      }else{
        String op = "";
        if(_queryObject.containsKey("$and")){
          op = "$and";
        }else if(_queryObject.containsKey("$or")){
          op = "$or";
        }
        if(op.isEmpty()){
          String indexName = (String)_queryObject.get("index");
          SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
          if (info != null) {
            rb = info.getResponseBuilder();
          }
          if(!searcherMap.containsKey(indexName)){
            CoreContainer container = rb.req.getCore().getCoreDescriptor().getCoreContainer();
            final SolrCore core = container.getCore(indexName);
            if (core == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + indexName);
            }
            RefCounted<SolrIndexSearcher> coreRef = core.getSearcher(false, true, null);
            searcherMap.put(indexName, coreRef.get());
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
          List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)_queryObject.get(op);
          for(Map<String, Object> joinQuerySubObject : joinQueryList){
            buildSearcherMap(joinQuerySubObject);
          }
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
        resultSet = getDocSet(joinQueryObject);
        filter = resultSet.getTopFilter();
      }

      // Although this set only includes live docs, other filters can be pushed down to queries.
      DocIdSet readerSet = filter.getDocIdSet(context, acceptDocs);
      return new JoinScorer(this, readerSet == null ? DocIdSetIterator.empty() : readerSet.iterator(), getBoost());

    }
    
    private int[][] buildJoinResultCache(SolrIndexSearcher fromSearcher, SolrIndexSearcher toSearcher, String fromField, String toField){
      JoinQueryResultKey jqrk = new JoinQueryResultKey(fromSearcher.getCore().getName(), fromField, toField);
      System.out.println("===============" + toSearcher + "===============, " + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ssZZ"));
      if(!rb.req.getParams().getBool("refreshCache", false) && toSearcher.joinQueryResultCache.get(jqrk) != null){
        System.out.println("Cache Hit - " + jqrk.toString() + ", on " + toSearcher + "@" + toSearcher.hashCode());
        return toSearcher.joinQueryResultCache.get(jqrk);
      }else{
        System.out.println("Waiting for locker:" + toSearcher + "@" + toSearcher.hashCode() + ", at " + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ssZZ"));
        synchronized(toSearcher){
          System.out.println("Got locker:" + toSearcher + "@" + toSearcher.hashCode() + ", at " + DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd'T'HH:mm:ssZZ"));
          if(!rb.req.getParams().getBool("refreshCache", false) && toSearcher.joinQueryResultCache.get(jqrk) != null){
            System.out.println("Cache Hit - " + jqrk.toString() + ", on " + toSearcher + "@" + toSearcher.hashCode());
            return toSearcher.joinQueryResultCache.get(jqrk);
          }
          System.out.println("Cache Not Hit - " + jqrk.toString() + ", on " + toSearcher + "@" + toSearcher.hashCode());
          long startTime = System.currentTimeMillis();
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
//                  int fromDocNumber = 0;
                while(fromDocIterator.hasNext()){
//                    fromDocNumber++;
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
            System.out.println("totalSize:" + totalSize + ", hitDocSize:" + hitDocSize + ", " + jqrk.toString() + ", on " + toSearcher + "@" + toSearcher.hashCode());
          } catch (IOException e) {
            throw new RuntimeException();
          }
          toSearcher.joinQueryResultCache.put(jqrk, docJoinResult);
          System.out.println("Build Cache Elapsed Time: " + (System.currentTimeMillis() - startTime) + "ms, " + jqrk.toString() + ", on " + toSearcher + "@" + toSearcher.hashCode());
          return docJoinResult;
        }
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
    
    public DocSet getDocSet(Map<String, Object> _queryObject) throws IOException{
      DocSet resultDocSet = null;
      if(_queryObject.containsKey("$join")){
        Map<String, Object> _joinQueryObject = (Map<String, Object>)_queryObject.get("$join");
        DocSet fromDocSet = getDocSet((Map<String, Object>)_joinQueryObject.get("from"));
        String fromIndex = getLastIndexName((Map<String, Object>)_joinQueryObject.get("from"));
        String toIndex = getLastIndexName((Map<String, Object>)_joinQueryObject.get("to"));
        SolrIndexSearcher fromSearcher = searcherMap.get(fromIndex);
        SolrIndexSearcher toSearcher = searcherMap.get(toIndex);
        
        String[] onArray = ((String)_joinQueryObject.get("on")).split("\\-\\>");
        String fromField = onArray[0];
        String toField = onArray[1];
        resultDocSet = getDocSetNewWithCache(fromSearcher, toSearcher, fromField, toField, fromDocSet);
        
        DocSet toDocSet = getDocSet((Map<String, Object>)_joinQueryObject.get("to"));
        if(toDocSet != null){
          DocSet tempDocSet = resultDocSet.intersection(toDocSet);
          resultDocSet.decref();
          toDocSet.decref();
          resultDocSet = tempDocSet;
        }
      }else if(_queryObject.containsKey("$not")){
        DocSet toDocSet = getDocSet((Map<String, Object>)_queryObject.get("$not"));
        String index = getLastIndexName((Map<String, Object>)_queryObject.get("$not"));
        DocSet allDocSet = searcherMap.get(index).getDocSet(new MatchAllDocsQuery());
        resultDocSet = allDocSet.andNot(toDocSet);
        allDocSet.decref();
        toDocSet.decref();
      }else{
        String op = "";
        if(_queryObject.containsKey("$and")){
          op = "$and";
        }else if(_queryObject.containsKey("$or")){
          op = "$or";
        }
        
        if(op.isEmpty()){
          SolrIndexSearcher searcher = searcherMap.get((String)_queryObject.get("index"));
          if(_queryObject.containsKey("parsedq")){
            for(Query parsedq : (List<Query>)_queryObject.get("parsedq")){
              if(resultDocSet == null){
                resultDocSet = searcher.getDocSet(parsedq);
              }else{
                resultDocSet = resultDocSet.intersection(searcher.getDocSet(parsedq));
              }
            }
//            Query parsedq = (Query)_queryObject.get("parsedq");
//            resultDocSet = searcher.getDocSet(parsedq);
          }
        }else{
          List<Map<String, Object>> joinQueryList = (List<Map<String, Object>>)_queryObject.get(op);
          for(Map<String, Object> joinQuerySubObject : joinQueryList){
            DocSet subDocSet = getDocSet(joinQuerySubObject);
            DocSet tempDocSet = null;
            if(resultDocSet == null){
              resultDocSet = subDocSet;
            }else{
              switch(op){
                case "$and":
                  tempDocSet = resultDocSet.intersection(subDocSet);
                  resultDocSet.decref();
                  subDocSet.decref();
                  resultDocSet = tempDocSet;
                  break;
                case "$or":
                  tempDocSet = resultDocSet.union(subDocSet);
                  resultDocSet.decref();
                  subDocSet.decref();
                  resultDocSet = tempDocSet;
                  break;
              }
            }    
          }
        }
      }
      return resultDocSet;
    }
    
    private String getLastIndexName(Map<String, Object> _queryObject){
      if(_queryObject.containsKey("$join")){
        return getLastIndexName((Map<String, Object>)((Map<String, Object>)_queryObject.get("$join")).get("to"));
      }else if(_queryObject.containsKey("$not")){
        return getLastIndexName((Map<String, Object>)_queryObject.get("$not"));
      }else{
        String op = "";
        if(_queryObject.containsKey("$and")){
          op = "$and";
        }else if(_queryObject.containsKey("$or")){
          op = "$or";
        }
        
        if(op.isEmpty()){
          return (String)_queryObject.get("index");
        }else{
          List<Map<String, Object>> queryList = (List<Map<String, Object>>)_queryObject.get(op);
          if(!queryList.isEmpty()){
            return getLastIndexName(queryList.get(0));
          }
        }
      }
      return null;
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
