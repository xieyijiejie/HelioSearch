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

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
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
import org.apache.solr.schema.TrieField;
import org.apache.solr.util.RefCounted;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


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
        resultSet = getDocSet();
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


    public DocSet getDocSet() throws IOException {
//      log.info("====Log By Zhitao==== Time of Total getFieldCacheCounts " + (System.currentTimeMillis() - time1));
      long intersectTime = 0L, findDocTime = 0L, findDocCond1Time = 0L, findDocCond2Time = 0L;
      
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
                findDocCond1Time += System.currentTimeMillis() - findDocCond2StartTime;
              }

            }
            findDocTime += System.currentTimeMillis() - findDocStartTime;
          }

          term = termsEnum.next();
        }
        smallSetsDeferred = resultList.size();

        System.out.println("====Log By Zhitao==== intersectTime " + intersectTime);
        System.out.println("====Log By Zhitao==== findDocTime " + findDocTime);
        System.out.println("====Log By Zhitao==== findDocCond1Time " + findDocCond1Time);
        System.out.println("====Log By Zhitao==== findDocCond2Time " + findDocCond2Time);

        long createResultStartTime = 0L;
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
