package org.apache.solr.search.facet;

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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocTermOrds;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.FieldFacetStats;
import org.apache.solr.handler.component.StatsValues;
import org.apache.solr.handler.component.StatsValuesFactory;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.BitDocSetNative;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.JoinJsonQParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryContext;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.facet.SimpleFacetStats.Slot;
import org.apache.solr.util.LongPriorityQueue;
import org.apache.solr.util.PrimUtils;
import org.apache.solr.util.RefCounted;

/**
 *
 * Final form of the un-inverted field:
 *   Each document points to a list of term numbers that are contained in that document.
 *
 *   Term numbers are in sorted order, and are encoded as variable-length deltas from the
 *   previous term number.  Real term numbers start at 2 since 0 and 1 are reserved.  A
 *   term number of 0 signals the end of the termNumber list.
 *
 *   There is a single int[maxDoc()] which either contains a pointer into a byte[] for
 *   the termNumber lists, or directly contains the termNumber list if it fits in the 4
 *   bytes of an integer.  If the first byte in the integer is 1, the next 3 bytes
 *   are a pointer into a byte[] where the termNumber list starts.
 *
 *   There are actually 256 byte arrays, to compensate for the fact that the pointers
 *   into the byte arrays are only 3 bytes long.  The correct byte array for a document
 *   is a function of it's id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with it's corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField extends DocTermOrds {
  private static int TNUM_OFFSET=2;

  static class TopTerm {
    BytesRef term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
             8 + 8 +term.length +  //term
             4;    // int
    }
  }

  long memsz;
  final AtomicLong use = new AtomicLong(); // number of uses

  int[] maxTermCounts = new int[1024];

  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<Integer,TopTerm>();

  private SolrIndexSearcher.DocsEnumState deState;
  private final SolrIndexSearcher searcher;
  private final boolean isPlaceholder;

  private static UnInvertedField uifPlaceholder = new UnInvertedField();

  private UnInvertedField() { // Dummy for synchronization.
    super("fake", 0, 0); // cheapest initialization I can find.
    isPlaceholder = true;
    searcher = null;
   }

  @Override
  protected void visitTerm(TermsEnum te, int termNum) throws IOException {

    if (termNum >= maxTermCounts.length) {
      // resize by doubling - for very large number of unique terms, expanding
      // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
      int[] newMaxTermCounts = new int[maxTermCounts.length*2];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
      maxTermCounts = newMaxTermCounts;
    }

    final BytesRef term = te.term();

    if (te.docFreq() > maxTermDocFreq) {
      TopTerm topTerm = new TopTerm();
      topTerm.term = BytesRef.deepCopyOf(term);
      topTerm.termNum = termNum;
      bigTerms.put(topTerm.termNum, topTerm);

      if (deState == null) {
        deState = new SolrIndexSearcher.DocsEnumState();
        deState.fieldName = field;
        deState.liveDocs = searcher.getAtomicReader().getLiveDocs();
        deState.termsEnum = te;  // TODO: check for MultiTermsEnum in SolrIndexSearcher could now fail?
        deState.docsEnum = docsEnum;
        deState.minSetSizeCached = maxTermDocFreq;
      }

      docsEnum = deState.docsEnum;
      DocSet set = searcher.getDocSet(deState);
      maxTermCounts[termNum] = set.size();
      set.decref();
    }
  }

  @Override
  protected void setActualDocFreq(int termNum, int docFreq) {
    maxTermCounts[termNum] = docFreq;
  }

  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = super.ramBytesUsed();
    sz += 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    if (indexedTermsArray != null) {
      // assume 8 byte references?
      sz += 8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings;
    }
    memsz = sz;
    return sz;
  }

  public UnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    super(field,
          // threshold, over which we use set intersections instead of counting
          // to (1) save memory, and (2) speed up faceting.
          // Add 2 for testing purposes so that there will always be some terms under
          // the threshold even when the index is very
          // small.
          searcher.maxDoc()/20 + 2,
          DEFAULT_INDEX_INTERVAL_BITS);
    //System.out.println("maxTermDocFreq=" + maxTermDocFreq + " maxDoc=" + searcher.maxDoc());

    isPlaceholder = false;
    final String prefix = TrieField.getMainValuePrefix(searcher.getSchema().getFieldType(field));
    this.searcher = searcher;
    try {
      AtomicReader r = searcher.getAtomicReader();
      uninvert(r, r.getLiveDocs(), prefix == null ? null : new BytesRef(prefix));
    } catch (IllegalStateException ise) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ise.getMessage());
    }
    if (tnums != null) {
      for(byte[] target : tnums) {
        if (target != null && target.length > (1<<24)*.9) {
          SolrCore.log.warn("Approaching too many values for UnInvertedField faceting on field '"+field+"' : bucket size=" + target.length);
        }
      }
    }

    // free space if outrageously wasteful (tradeoff memory/cpu) 
    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
    }

    SolrCore.log.info("UnInverted multi-valued field " + toString());
    //System.out.println("CREATED: " + toString() + " ti.index=" + ti.index);
  }

  public int getNumTerms() {
    return numTermsInField;
  }


  public class DocToTerm implements Closeable {
    private final DocSet[] bigTermSets;
    private final int[] bigTermNums;

    public DocToTerm() throws IOException {
      bigTermSets = new DocSet[bigTerms.size()];
      bigTermNums = new int[bigTerms.size()];
      int i=0;
      for (TopTerm tt : bigTerms.values()) {
        bigTermSets[i] = searcher.getDocSet(new TermQuery(new Term(field, tt.term)));
        bigTermNums[i] = tt.termNum;
        i++;
      }
    }

    public void getTerms(int doc, Callback target) throws IOException {
      if (bigTermSets != null) {
        for (int i=0; i<bigTermSets.length; i++) {
          if (bigTermSets[i].exists(doc)) {
            target.call( bigTermNums[i] );
          }
        }
      }

      getNonBigTerms(doc, target);
    }

    public void getNonBigTerms(int doc, Callback target) {
      if (termInstances > 0) {
        int code = index[doc];

        if ((code & 0xff)==1) {
          int pos = code>>>8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            target.call(tnum);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              target.call(tnum);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      for (DocSet set : bigTermSets) {
        set.decref();
      }
    }
  }

  public interface Callback {
    public void call(int termNum);
  }

  //Zhitao - 转字串用Set<String>效率太低，需要改进
  public int[] getBytesRef(SolrIndexSearcher searcher, DocSet baseDocs){
//    BytesRefHash bytesRefHash = new BytesRefHash();
    Set<String> bytesSet = new HashSet<String>();
    use.incrementAndGet();

    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();
    
    final int[] counts = new int[numTermsInField + 1];
    try {
      final int[] index = this.index;
      // tricky: we add more more element than we need because we will reuse this array later
      // for ordering term ords before converting to term labels.
      

      //
      // If there is prefix, find it's start and end term numbers
      //
      int startTerm = 0;
      int endTerm = numTermsInField;  // one past the end

      TermsEnum te = getOrdTermsEnum(searcher.getAtomicReader());
//      boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0
//          && startTerm==0 && endTerm==numTermsInField
//          && (docs instanceof BitDocSet || docs instanceof BitDocSetNative);
//
//      if (doNegative) {
//        // TODO: when iterator across negative elements is available, use that
//        // instead of creating a new bitset and inverting.
//
//        if (docs instanceof BitDocSet) {
//          FixedBitSet bs = ((BitDocSet)docs).getBits().clone();
//          bs = bs.clone(); // don't mess with internal obs of BitDocSet
//          bs.flip(0, maxDoc);
//          docs = new BitDocSet(bs, maxDoc - baseSize);
//        } else {
//          BitDocSetNative negSet = ((BitDocSetNative)docs).clone();
//          negSet.flip(0, maxDoc);
//          negSet.setSize(maxDoc - baseSize);
//          docs = negSet;
//        }
//
//        // simply negating will mean that we have deleted docs in the set.
//        // that should be OK, as their entries in our table should be empty.
//        //System.out.println("  NEG");
//      }

      // For the biggest terms, do straight set intersections
      for (TopTerm tt : bigTerms.values()) {
        //System.out.println("  do big termNum=" + tt.termNum + " term=" + tt.term.utf8ToString());
        // TODO: counts could be deferred if sorted==false
        if (tt.termNum >= startTerm && tt.termNum < endTerm) {
          counts[tt.termNum] = searcher.numDocs(new TermQuery(new Term(field, tt.term)), docs);
//          bytesRefHash.add(this.getTermValue(te, tt.termNum));
          //System.out.println("    count=" + counts[tt.termNum]);
        } else {
          //System.out.println("SKIP term=" + tt.termNum);
        }
      }

      // TODO: we could short-circuit counting altogether for sorted faceting
      // where we already have enough terms from the bigTerms

      // TODO: we could shrink the size of the collection array, and
      // additionally break when the termNumber got above endTerm, but
      // it would require two extra conditionals in the inner loop (although
      // they would be predictable for the non-prefix case).
      // Perhaps a different copy of the code would be warranted.

      if (termInstances > 0) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext()) {
          int doc = iter.nextDoc();
          //System.out.println("iter doc=" + doc);
          int code = index[doc];

          if ((code & 0xff)==1) {
            //System.out.println("  ptr");
            int pos = code>>>8;
            int whichArray = (doc >>> 16) & 0xff;
            byte[] arr = tnums[whichArray];
            int tnum = 0;
            for(;;) {
              int delta = 0;
              for(;;) {
                byte b = arr[pos++];
                delta = (delta << 7) | (b & 0x7f);
                if ((b & 0x80) == 0) break;
              }
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              //System.out.println("    tnum=" + tnum);
              counts[tnum]++;
//              BytesRef br = BytesRef.deepCopyOf(this.getTermValue(te, tnum));
//              bytesRefHash.add(this.getTermValue(te, tnum));
            }
          } else {
            //System.out.println("  inlined");
            int tnum = 0;
            int delta = 0;
            for (;;) {
              delta = (delta << 7) | (code & 0x7f);
              if ((code & 0x80)==0) {
                if (delta==0) break;
                tnum += delta - TNUM_OFFSET;
                //System.out.println("    tnum=" + tnum);
                counts[tnum]++;
//                BytesRef br = BytesRef.deepCopyOf(this.getTermValue(te, tnum));
//                bytesRefHash.add(this.getTermValue(te, tnum));
                delta = 0;
              }
              code >>>= 8;
            }
          }
        }
//        for(int i = 0 ; i < numTermsInField ; i++ ){
//          if(counts[i] > 0){
//            bytesSet.add(this.getTermValue(te, i).utf8ToString());
//          }
//        }
      }

    } catch (IOException e) {
      throw new RuntimeException();
    } finally {
      if (docs != baseDocs) {
        // if doNegative, release the negative set
        docs.decref();
        docs = null;
      }
    }
    
    return counts;
  }

  public NamedList<Integer> getCounts(SolrIndexSearcher searcher, DocSet baseDocs, int offset, int limit, Integer mincount, boolean missing, String sort, String prefix) throws IOException {
    use.incrementAndGet();

    FieldType ft = searcher.getSchema().getFieldType(field);

    NamedList<Integer> res = new NamedList<Integer>();  // order is important

    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    try {

      //System.out.println("GET COUNTS field=" + field + " baseSize=" + baseSize + " minCount=" + mincount + " maxDoc=" + maxDoc + " numTermsInField=" + numTermsInField);
      if (baseSize >= mincount) {

        final int[] index = this.index;
        // tricky: we add more more element than we need because we will reuse this array later
        // for ordering term ords before converting to term labels.
        final int[] counts = new int[numTermsInField + 1];

        //
        // If there is prefix, find it's start and end term numbers
        //
        int startTerm = 0;
        int endTerm = numTermsInField;  // one past the end

        TermsEnum te = getOrdTermsEnum(searcher.getAtomicReader());
        if (te != null && prefix != null && prefix.length() > 0) {
          final BytesRef prefixBr = new BytesRef(prefix);
          if (te.seekCeil(prefixBr) == TermsEnum.SeekStatus.END) {
            startTerm = numTermsInField;
          } else {
            startTerm = (int) te.ord();
          }
          prefixBr.append(UnicodeUtil.BIG_TERM);
          if (te.seekCeil(prefixBr) == TermsEnum.SeekStatus.END) {
            endTerm = numTermsInField;
          } else {
            endTerm = (int) te.ord();
          }
        }

        /***********
         // Alternative 2: get the docSet of the prefix (could take a while) and
         // then do the intersection with the baseDocSet first.
         if (prefix != null && prefix.length() > 0) {
         docs = searcher.getDocSet(new ConstantScorePrefixQuery(new Term(field, ft.toInternal(prefix))), docs);
         // The issue with this method are problems of returning 0 counts for terms w/o
         // the prefix.  We can't just filter out those terms later because it may
         // mean that we didn't collect enough terms in the queue (in the sorted case).
         }
         ***********/

        boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0
            && startTerm==0 && endTerm==numTermsInField
            && (docs instanceof BitDocSet || docs instanceof BitDocSetNative);

        if (doNegative) {
          // TODO: when iterator across negative elements is available, use that
          // instead of creating a new bitset and inverting.

          if (docs instanceof BitDocSet) {
            FixedBitSet bs = ((BitDocSet)docs).getBits().clone();
            bs = bs.clone(); // don't mess with internal obs of BitDocSet
            bs.flip(0, maxDoc);
            docs = new BitDocSet(bs, maxDoc - baseSize);
          } else {
            BitDocSetNative negSet = ((BitDocSetNative)docs).clone();
            negSet.flip(0, maxDoc);
            negSet.setSize(maxDoc - baseSize);
            docs = negSet;
          }

          // simply negating will mean that we have deleted docs in the set.
          // that should be OK, as their entries in our table should be empty.
          //System.out.println("  NEG");
        }

        // For the biggest terms, do straight set intersections
        for (TopTerm tt : bigTerms.values()) {
          //System.out.println("  do big termNum=" + tt.termNum + " term=" + tt.term.utf8ToString());
          // TODO: counts could be deferred if sorted==false
          if (tt.termNum >= startTerm && tt.termNum < endTerm) {
            counts[tt.termNum] = searcher.numDocs(new TermQuery(new Term(field, tt.term)), docs);
            //System.out.println("    count=" + counts[tt.termNum]);
          } else {
            //System.out.println("SKIP term=" + tt.termNum);
          }
        }

        // TODO: we could short-circuit counting altogether for sorted faceting
        // where we already have enough terms from the bigTerms

        // TODO: we could shrink the size of the collection array, and
        // additionally break when the termNumber got above endTerm, but
        // it would require two extra conditionals in the inner loop (although
        // they would be predictable for the non-prefix case).
        // Perhaps a different copy of the code would be warranted.

        if (termInstances > 0) {
          DocIterator iter = docs.iterator();
          while (iter.hasNext()) {
            int doc = iter.nextDoc();
            //System.out.println("iter doc=" + doc);
            int code = index[doc];

            if ((code & 0xff)==1) {
              //System.out.println("  ptr");
              int pos = code>>>8;
              int whichArray = (doc >>> 16) & 0xff;
              byte[] arr = tnums[whichArray];
              int tnum = 0;
              for(;;) {
                int delta = 0;
                for(;;) {
                  byte b = arr[pos++];
                  delta = (delta << 7) | (b & 0x7f);
                  if ((b & 0x80) == 0) break;
                }
                if (delta == 0) break;
                tnum += delta - TNUM_OFFSET;
                //System.out.println("    tnum=" + tnum);
                counts[tnum]++;
              }
            } else {
              //System.out.println("  inlined");
              int tnum = 0;
              int delta = 0;
              for (;;) {
                delta = (delta << 7) | (code & 0x7f);
                if ((code & 0x80)==0) {
                  if (delta==0) break;
                  tnum += delta - TNUM_OFFSET;
                  //System.out.println("    tnum=" + tnum);
                  counts[tnum]++;
                  delta = 0;
                }
                code >>>= 8;
              }
            }
          }
        }
        final CharsRef charsRef = new CharsRef();

        int off=offset;
        int lim=limit>=0 ? limit : Integer.MAX_VALUE;

        if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
          int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
          maxsize = Math.min(maxsize, numTermsInField);
          LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

          int min=mincount-1;  // the smallest value in the top 'N' values
          //System.out.println("START=" + startTerm + " END=" + endTerm);
          for (int i=startTerm; i<endTerm; i++) {
            int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
            if (c>min) {
              // NOTE: we use c>min rather than c>=min as an optimization because we are going in
              // index order, so we already know that the keys are ordered.  This can be very
              // important if a lot of the counts are repeated (like zero counts would be).

              // smaller term numbers sort higher, so subtract the term number instead
              long pair = (((long)c)<<32) + (Integer.MAX_VALUE - i);
              boolean displaced = queue.insert(pair);
              if (displaced) min=(int)(queue.top() >>> 32);
            }
          }

          // now select the right page from the results

          // if we are deep paging, we don't have to order the highest "offset" counts.
          int collectCount = Math.max(0, queue.size() - off);
          assert collectCount <= lim;

          // the start and end indexes of our list "sorted" (starting with the highest value)
          int sortedIdxStart = queue.size() - (collectCount - 1);
          int sortedIdxEnd = queue.size() + 1;
          final long[] sorted = queue.sort(collectCount);

          final int[] indirect = counts;  // reuse the counts array for the index into the tnums array
          assert indirect.length >= sortedIdxEnd;

          for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
            long pair = sorted[i];
            int c = (int)(pair >>> 32);
            int tnum = Integer.MAX_VALUE - (int)pair;

            indirect[i] = i;   // store the index for indirect sorting
            sorted[i] = tnum;  // reuse the "sorted" array to store the term numbers for indirect sorting

            // add a null label for now... we'll fill it in later.
            res.add(null, c);
          }

          // now sort the indexes by the term numbers
          PrimUtils.sort(sortedIdxStart, sortedIdxEnd, indirect, new PrimUtils.IntComparator() {
            @Override
            public int compare(int a, int b) {
              return (int)sorted[a] - (int)sorted[b];
            }

            @Override
            public boolean lessThan(int a, int b) {
              return sorted[a] < sorted[b];
            }

            @Override
            public boolean equals(int a, int b) {
              return sorted[a] == sorted[b];
            }
          });

          // convert the term numbers to term values and set
          // as the label
          //System.out.println("sortStart=" + sortedIdxStart + " end=" + sortedIdxEnd);
          for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
            int idx = indirect[i];
            int tnum = (int)sorted[idx];
            final String label = getReadableValue(getTermValue(te, tnum), ft, charsRef);
            //System.out.println("  label=" + label);
            res.setName(idx - sortedIdxStart, label);
          }

        } else {
          // add results in index order
          int i=startTerm;
          if (mincount<=0) {
            // if mincount<=0, then we won't discard any terms and we know exactly
            // where to start.
            i=startTerm+off;
            off=0;
          }

          for (; i<endTerm; i++) {
            int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
            if (c<mincount || --off>=0) continue;
            if (--lim<0) break;

            final String label = getReadableValue(getTermValue(te, i), ft, charsRef);
            res.add(label, c);
          }
        }
      }

    } finally {
      if (docs != baseDocs) {
        // if doNegative, release the negative set
        docs.decref();
        docs = null;
      }
    }


    if (missing) {
      // TODO: a faster solution for this?
      res.add(null, SimpleFacets.getFieldMissingCount(searcher, baseDocs, field));
    }

    //System.out.println("  res=" + res);

    return res;
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // TODO: remove after JSON facets are ready...
  public SimpleOrderedMap<Object> getCounts(final SimpleFacetStats facetStats, DocSet baseDocs, int offset, int limit, int mincount, boolean missing, String prefix, boolean unique) throws IOException {
    use.incrementAndGet();

    SchemaField sf = searcher.getSchema().getField(field);
    FieldType ft = sf.getType();

    SimpleOrderedMap<Object> res = new SimpleOrderedMap<>();

    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();
    int uniqueTerms = 0;

    try {

      //System.out.println("GET COUNTS field=" + field + " baseSize=" + baseSize + " minCount=" + mincount + " maxDoc=" + maxDoc + " numTermsInField=" + numTermsInField);
      if (baseSize >= mincount) {

        final int[] index = this.index;
        // tricky: we add more more element than we need because we will reuse this array later
        // for ordering term ords before converting to term labels.
        final int[] counts = new int[numTermsInField + 1];

        //
        // If there is prefix, find it's start and end term numbers
        //
        int startTerm = 0;
        int endTerm = numTermsInField;  // one past the end

        TermsEnum te = getOrdTermsEnum(searcher.getAtomicReader());
        if (te != null && prefix != null && prefix.length() > 0) {
          final BytesRef prefixBr = new BytesRef(prefix);
          if (te.seekCeil(prefixBr) == TermsEnum.SeekStatus.END) {
            startTerm = numTermsInField;
          } else {
            startTerm = (int) te.ord();
          }
          prefixBr.append(UnicodeUtil.BIG_TERM);
          if (te.seekCeil(prefixBr) == TermsEnum.SeekStatus.END) {
            endTerm = numTermsInField;
          } else {
            endTerm = (int) te.ord();
          }
        }


        int nDocs = baseDocs.size();
        int nTerms = endTerm - startTerm;  // TODO: handle 0


        facetStats.createAccs(nDocs, nTerms);


        for (TopTerm tt : bigTerms.values()) {
          if (tt.termNum >= startTerm && tt.termNum < endTerm) {
            // handle the biggest terms
            try ( DocSet intersection = searcher.getDocSet(new TermQuery(new Term(field, tt.term)), docs); )
            {
              int collected = facetStats.collect(tt.termNum, intersection);
              if (collected > 0) {
                uniqueTerms++;
              }
            }
          }
        }

        if (termInstances > 0) {

          final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
          final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
          AtomicReaderContext ctx = null;
          int segBase = 0;
          int segMax;
          int adjustedMax = 0;

          DocIterator iter = docs.iterator();
          while (iter.hasNext()) {
            int doc = iter.nextDoc();

            if (doc >= adjustedMax) {
              do {
                ctx = ctxIt.next();
                if (ctx == null) {
                  // should be impossible
                  throw new RuntimeException("INTERNAL FACET ERROR");
                }
                segBase = ctx.docBase;
                segMax = ctx.reader().maxDoc();
                adjustedMax = segBase + segMax;
              } while (doc >= adjustedMax);
              assert doc >= ctx.docBase;
              facetStats.setNextReader(ctx);
            }
            int segDoc = doc - segBase;



            //System.out.println("iter doc=" + doc);
            int code = index[doc];

            if ((code & 0xff)==1) {
              //System.out.println("  ptr");
              int pos = code>>>8;
              int whichArray = (doc >>> 16) & 0xff;
              byte[] arr = tnums[whichArray];
              int tnum = 0;
              for(;;) {
                int delta = 0;
                for(;;) {
                  byte b = arr[pos++];
                  delta = (delta << 7) | (b & 0x7f);
                  if ((b & 0x80) == 0) break;
                }
                if (delta == 0) break;
                tnum += delta - TNUM_OFFSET;
                facetStats.collect(tnum, segDoc);
              }
            } else {
              //System.out.println("  inlined");
              int tnum = 0;
              int delta = 0;
              for (;;) {
                delta = (delta << 7) | (code & 0x7f);
                if ((code & 0x80)==0) {
                  if (delta==0) break;
                  tnum += delta - TNUM_OFFSET;
                  facetStats.collect(tnum, segDoc);
                  delta = 0;
                }
                code >>>= 8;
              }
            }
          }
        }



        // TODO: this is mostly repeated code... figure out how to extract it...


        int off=offset;
        int lim=limit>=0 ? limit : Integer.MAX_VALUE;

        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, nTerms);

        PriorityQueue<Slot> queue = new PriorityQueue<Slot>(maxsize) {
          final SlotAcc acc = facetStats.sortAcc;
          final int mul = facetStats.sortMul;
          @Override
          protected boolean lessThan(Slot a, Slot b) {
            int cmp = acc.compare(a.slot, b.slot) * mul;
            return cmp == 0 ? b.slot < a.slot : cmp < 0;
          }
        };

        Slot bottom = null;
        for (int i=startTerm; i<endTerm; i++) {
          // TODO: screen out mincount?  Other filters on stats?

          if (bottom != null) {
            if (facetStats.sortAcc.compare(bottom.slot, i) < 0) {
              bottom.slot = i;
              bottom = queue.updateTop();
            }
          } else {
            // queue not full
            Slot s = new Slot();
            s.slot = i;
            queue.add(s);
            if (queue.size() >= maxsize) {
              bottom = queue.top();
            }
          }
        }


        // if we are deep paging, we don't have to order the highest "offset" counts.
        int collectCount = Math.max(0, queue.size() - off);
        assert collectCount <= lim;
        int[] sortedSlots = new int[collectCount];
        for (int i=collectCount-1; i>=0; i--) {
          sortedSlots[i] = queue.pop().slot;
        }

        SimpleOrderedMap<Object> globalStats = new SimpleOrderedMap<>();
        facetStats.addGlobalStats(globalStats);
        res.add("stats", globalStats);

        ArrayList bucketList = new ArrayList(collectCount);
        res.add("buckets", bucketList);
        for (int slotNum : sortedSlots) {
          SimpleOrderedMap<Object> bucket = new SimpleOrderedMap<>();

          // get the ord of the slot...
          int ord = slotNum;

          BytesRef br = getTermValue(te, slotNum);
          Object val = ft.toObject(sf, br);

          bucket.add("val", val);

          facetStats.addStats(bucket, slotNum);

          bucketList.add(bucket);

          facetStats.addSubFacets(bucket, null, new TermQuery(new Term(field, br.clone())) );  // TODO: missing query...
        }


      }
    } finally {

    }

    return res;
  }


  // called from FieldFacetProcessor
  // TODO: do a callback version that can be specialized!
  public void collectDocs(FacetFieldProcessorUIF processor) throws IOException {
    use.incrementAndGet();

    DocSet docs = processor.fcontext.base;
    int startTermIndex = processor.startTermIndex;
    int endTermIndex = processor.endTermIndex;
    int nTerms = processor.nTerms;//这个field上所有term的个数，不是当前query下的
    int[] termStatus = new int[nTerms];
    int uniqueTerms = 0;

    for (TopTerm tt : bigTerms.values()) {  //bitTerms就是文档数大于一定值的term,由maxTermDocFreq指定
      if (tt.termNum >= startTermIndex && tt.termNum < endTermIndex) {
        // handle the biggest terms
        try ( DocSet intersection = searcher.getDocSet(new TermQuery(new Term(field, tt.term)), docs); )
        {
          int collected = processor.collect(tt.termNum - startTermIndex, intersection);
          processor.countAcc.incrementCount(tt.termNum - startTermIndex, collected);
          if (collected > 0) {
            uniqueTerms++;
          }
        }
      }
    }

    if (termInstances > 0) {

      final List<AtomicReaderContext> leaves = searcher.getIndexReader().leaves();
      final Iterator<AtomicReaderContext> ctxIt = leaves.iterator();
      AtomicReaderContext ctx = null;
      int segBase = 0;
      int segMax;
      int adjustedMax = 0;


      // TODO: handle facet.prefix here!!!

      
      // Zhitao: handle facet filter here
//      BytesRefHash termHash = null;
//      Set<String> termSet = null;
      int[] termCounts = null;
      TermsEnum filterTermsEnum = null;
      DocSet resultDocSet = null;
      if(processor.freq.filter != null){
        String indexName = processor.freq.filter.get("index");
        CoreContainer container = searcher.getCore().getCoreDescriptor().getCoreContainer();
        final SolrCore core = container.getCore(indexName);
        if (core == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cross-core join: no such core " + indexName);
        }
        RefCounted<SolrIndexSearcher> coreRef = core.getSearcher(false, true, null);
//        searcherMap.put(indexName, coreRef.get());
       
        try {
          long s1 = System.currentTimeMillis();
//          QParser parser = QParser.getParser(processor.freq.filter.get("q")
//              , "lucene", new LocalSolrQueryRequest(core, processor.fcontext.req.getParams()));
//          Query q = parser.getQuery();
          SolrIndexSearcher filterSearcher = coreRef.get();
//          filterTermsEnum = filterSearcher.getAtomicReader().terms(processor.freq.filter.get("field")).iterator(null);
          String jsonjoinquery = String.format("{!jsonjoin}{$join:{from:{index:%s,q:\"%s\"},to:{index:%s,q:\"%s\"},on:\"%s->%s\"}}"
              , processor.fcontext.req.getCore().getName(), processor.fcontext.req.getParams().get("q")
              , indexName, processor.freq.filter.get("q")
              , field, processor.freq.filter.get("field"));
          System.out.println("jsonjoinquery: " + jsonjoinquery);
          QParser joinParser = QParser.getParser(jsonjoinquery
              , "lucene", new LocalSolrQueryRequest(core, processor.fcontext.req.getParams()));
          Query jjq = joinParser.getQuery();
          resultDocSet = filterSearcher.getDocSet(jjq);
          
//          resultDocSet = filterSearcher.getDocSet(q);
          long s2 = System.currentTimeMillis();
          System.out.println("getDocSet Time: " + (s2-s1));
          UnInvertedField filteruif =  UnInvertedField.getUnInvertedField(processor.freq.filter.get("field"), filterSearcher);
          filterTermsEnum = filteruif.getOrdTermsEnum(filterSearcher.getAtomicReader());
          long s3 = System.currentTimeMillis();
          System.out.println("getUnInvertedField Time: " + (s3-s2));
          termCounts = filteruif.getBytesRef(filterSearcher, resultDocSet);
          long s4 = System.currentTimeMillis();
          System.out.println("getBytesRef Time: " + (s4-s3));
        } catch (SyntaxError e) {
          throw new RuntimeException();
        }
        coreRef.decref();
        core.close();
      }
      TermsEnum te = this.getOrdTermsEnum(searcher.getAtomicReader());
      DocIterator iter = docs.iterator();
//      long s5 = 0L;
      while (iter.hasNext()) {
        int doc = iter.nextDoc();

        if (doc >= adjustedMax) {
          do {
            ctx = ctxIt.next();
            if (ctx == null) {
              // should be impossible
              throw new RuntimeException("INTERNAL FACET ERROR");
            }
            segBase = ctx.docBase;
            segMax = ctx.reader().maxDoc();
            adjustedMax = segBase + segMax;
          } while (doc >= adjustedMax);
          assert doc >= ctx.docBase;
          processor.setNextReader(ctx);
        }
        int segDoc = doc - segBase;


        int code = index[doc];//univerted索引，index里面有每个doc在该field上的term信息

        if ((code & 0xff)==1) {                 //如果第一位是1，则剩下的三个字节是表示指针，a pointer into a byte[] where the termNumber list starts
          int pos = code>>>8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            int arrIdx = tnum - startTermIndex;
            if (arrIdx < 0) continue;
            if (arrIdx >= nTerms) break;
            
            //TODO 验证term
            if(resultDocSet != null){
              if(termStatus[arrIdx] == 1){
                //要
                processor.countAcc.incrementCount(arrIdx, 1);
                processor.collect(arrIdx, segDoc);
              }else if(termStatus[arrIdx] == 0){
                //不知道
                long s11 = System.currentTimeMillis();
//                if(termHash.find(getTermValue(te, tnum)) != -1){
                if(filterTermsEnum.seekExact(getTermValue(te, tnum)) && termCounts[(int) filterTermsEnum.ord()] > 0){
//                  s5 += (System.currentTimeMillis() - s11);
//                if(termSet.contains(getTermValue(te, tnum).utf8ToString())){
                  termStatus[arrIdx] = 1;
                  processor.countAcc.incrementCount(arrIdx, 1);
                  processor.collect(arrIdx, segDoc);
                }else{
                  termStatus[arrIdx] = -1;
                }
              }else{
                //不要
              }
//              if(termStatus[arrIdx] == 1/*termHash.find(getTermValue(te, tnum)) != -1*/){
//
//              }
            }else{
              processor.countAcc.incrementCount(arrIdx, 1);
              processor.collect(arrIdx, segDoc);
            }
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f); //delta的右边第一位拼上code的右边7位
            if ((code & 0x80)==0) {               //如果code的第一位是0
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;

              int arrIdx = tnum - startTermIndex;
              if (arrIdx < 0) continue;
              if (arrIdx >= nTerms) break;
              
              //TODO 验证term
              if(resultDocSet != null){
                if(termStatus[arrIdx] == 1){
                  //要
                  processor.countAcc.incrementCount(arrIdx, 1);
                  processor.collect(arrIdx, segDoc);
                }else if(termStatus[arrIdx] == 0){
                  //不知道
//                  if(termHash.find(getTermValue(te, tnum)) != -1){
                  long s11 = System.currentTimeMillis();
                  if(filterTermsEnum.seekExact(getTermValue(te, tnum)) && termCounts[(int) filterTermsEnum.ord()] > 0){
//                    s5 += (System.currentTimeMillis() - s11);
//                  if(termSet.contains(getTermValue(te, tnum).utf8ToString())){
                    termStatus[arrIdx] = 1;
                    processor.countAcc.incrementCount(arrIdx, 1);
                    processor.collect(arrIdx, segDoc);
                  }else{
                    termStatus[arrIdx] = -1;
                  }
                }else{
                  //不要
                }
              }else{
                processor.countAcc.incrementCount(arrIdx, 1);
                processor.collect(arrIdx, segDoc);
              }
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
//      System.out.println("s5: " + s5);
    }


  }











  /**
   * Collect statistics about the UninvertedField.  Code is very similar to {@link #getCounts(org.apache.solr.search.SolrIndexSearcher, org.apache.solr.search.DocSet, int, int, Integer, boolean, String, String)}
   * It can be used to calculate stats on multivalued fields.
   * <p/>
   * This method is mainly used by the {@link org.apache.solr.handler.component.StatsComponent}.
   *
   * @param searcher The Searcher to use to gather the statistics
   * @param baseDocs The {@link org.apache.solr.search.DocSet} to gather the stats on
   * @param calcDistinct whether distinct values should be collected and counted
   * @param facet One or more fields to facet on.
   * @return The {@link org.apache.solr.handler.component.StatsValues} collected
   * @throws java.io.IOException If there is a low-level I/O error.
   */
  public StatsValues getStats(SolrIndexSearcher searcher, DocSet baseDocs, boolean calcDistinct, String[] facet) throws IOException {
    //this function is ripped off nearly wholesale from the getCounts function to use
    //for multiValued fields within the StatsComponent.  may be useful to find common
    //functionality between the two and refactor code somewhat
    use.incrementAndGet();

    SchemaField sf = searcher.getSchema().getField(field);
   // FieldType ft = sf.getType();

    QueryContext qcontext = QueryContext.newContext(searcher);
    StatsValues allstats = StatsValuesFactory.createStatsValues(qcontext, sf, calcDistinct);


    DocSet docs = baseDocs;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    if (baseSize <= 0) return allstats;

    DocSet all = searcher.getDocSet(new TermRangeQuery(field, null, null, false, false));
    DocSet missing = docs.andNot( all );
    all.decref();

    try {

      int i = 0;
      final FieldFacetStats[] finfo = new FieldFacetStats[facet.length];
      //Initialize facetstats, if facets have been passed in
      SortedDocValues si;
      for (String f : facet) {
        SchemaField facet_sf = searcher.getSchema().getField(f);
        finfo[i] = new FieldFacetStats(searcher, f, sf, facet_sf, calcDistinct);
        i++;
      }

      final int[] index = this.index;
      final int[] counts = new int[numTermsInField];//keep track of the number of times we see each word in the field for all the documents in the docset

      TermsEnum te = getOrdTermsEnum(searcher.getAtomicReader());

      boolean doNegative = false;
      if (finfo.length == 0) {
        //if we're collecting statistics with a facet field, can't do inverted counting
        doNegative = baseSize > maxDoc >> 1 && termInstances > 0
            && (docs instanceof BitDocSet || docs instanceof BitDocSetNative);
      }

      if (doNegative) {
        FixedBitSet bs = docs.getBits();
        if (docs instanceof BitDocSet) {
          bs = bs.clone(); // don't mess with internal obs of BitDocSet
        }
        bs.flip(0, maxDoc);
        // TODO: when iterator across negative elements is available, use that
        // instead of creating a new bitset and inverting.
        docs = new BitDocSet(bs, maxDoc - baseSize);
        // simply negating will mean that we have deleted docs in the set.
        // that should be OK, as their entries in our table should be empty.
      }

      // For the biggest terms, do straight set intersections
      for (TopTerm tt : bigTerms.values()) {
        // TODO: counts could be deferred if sorted==false
        if (tt.termNum >= 0 && tt.termNum < numTermsInField) {
          final Term t = new Term(field, tt.term);
          if (finfo.length == 0) {
            counts[tt.termNum] = searcher.numDocs(new TermQuery(t), docs);
          } else {
            //COULD BE VERY SLOW
            //if we're collecting stats for facet fields, we need to iterate on all matching documents
            try (
                DocSet tdocs = searcher.getDocSet(new TermQuery(t));
                DocSet bigTermDocSet = tdocs.intersection(docs);
            ) {
              DocIterator iter = bigTermDocSet.iterator();
              while (iter.hasNext()) {
                int doc = iter.nextDoc();
                counts[tt.termNum]++;
                for (FieldFacetStats f : finfo) {
                  f.facetTermNum(doc, tt.termNum);
                }
              }
            }  // end try-with

          }
        }
      }


      if (termInstances > 0) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext()) {
          int doc = iter.nextDoc();
          int code = index[doc];

          if ((code & 0xff) == 1) {
            int pos = code >>> 8;
            int whichArray = (doc >>> 16) & 0xff;
            byte[] arr = tnums[whichArray];
            int tnum = 0;
            for (; ;) {
              int delta = 0;
              for (; ;) {
                byte b = arr[pos++];
                delta = (delta << 7) | (b & 0x7f);
                if ((b & 0x80) == 0) break;
              }
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              counts[tnum]++;
              for (FieldFacetStats f : finfo) {
                f.facetTermNum(doc, tnum);
              }
            }
          } else {
            int tnum = 0;
            int delta = 0;
            for (; ;) {
              delta = (delta << 7) | (code & 0x7f);
              if ((code & 0x80) == 0) {
                if (delta == 0) break;
                tnum += delta - TNUM_OFFSET;
                counts[tnum]++;
                for (FieldFacetStats f : finfo) {
                  f.facetTermNum(doc, tnum);
                }
                delta = 0;
              }
              code >>>= 8;
            }
          }
        }
      }

      // add results in index order
      for (i = 0; i < numTermsInField; i++) {
        int c = doNegative ? maxTermCounts[i] - counts[i] : counts[i];
        if (c == 0) continue;
        BytesRef value = getTermValue(te, i);

        allstats.accumulate(value, c);
        //as we've parsed the termnum into a value, lets also accumulate fieldfacet statistics
        for (FieldFacetStats f : finfo) {
          f.accumulateTermNum(i, value);
        }
      }

      int c = missing.size();
      allstats.addMissing(c);

      if (finfo.length > 0) {
        for (FieldFacetStats f : finfo) {
          Map<String, StatsValues> facetStatsValues = f.facetStatsValues;
          FieldType facetType = searcher.getSchema().getFieldType(f.name);
          for (Map.Entry<String,StatsValues> entry : facetStatsValues.entrySet()) {
            String termLabel = entry.getKey();
            int missingCount = searcher.numDocs(new TermQuery(new Term(f.name, facetType.toInternal(termLabel))), missing);
            entry.getValue().addMissing(missingCount);
          }
          allstats.addFacet(f.name, facetStatsValues);
        }
      }

      return allstats;

    } finally {
      missing.decref();
    }

  }

  String getReadableValue(BytesRef termval, FieldType ft, CharsRef charsRef) {
    return ft.indexedToReadable(termval, charsRef).toString();
  }

  /** may return a reused BytesRef */
  BytesRef getTermValue(TermsEnum te, int termNum) throws IOException {
    //System.out.println("getTermValue termNum=" + termNum + " this=" + this + " numTerms=" + numTermsInField);
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        //System.out.println("  return big " + tt.term);
        return tt.term;
      }
    }

    return lookupTerm(te, termNum);
  }

  @Override
  public String toString() {
    final long indexSize = indexedTermsArray == null ? 0 : (8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings); // assume 8 byte references?
    return "{field=" + field
            + ",memSize="+memSize()
            + ",tindexSize="+indexSize
            + ",time="+total_time
            + ",phase1="+phase1_time
            + ",nTerms="+numTermsInField
            + ",bigTerms="+bigTerms.size()
            + ",termInstances="+termInstances
            + ",uses="+use.get()
            + "}";
  }

  //////////////////////////////////////////////////////////////////
  //////////////////////////// caching /////////////////////////////
  //////////////////////////////////////////////////////////////////

  public static UnInvertedField getUnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    SolrCache<String,UnInvertedField> cache = searcher.getFieldValueCache();
    if (cache == null) {
      return new UnInvertedField(field, searcher);
    }
    UnInvertedField uif = null;
    Boolean doWait = false;
    synchronized (cache) {
      uif = cache.get(field);
      if (uif == null) {
        cache.put(field, uifPlaceholder); // This thread will load this field, don't let other threads try.
      } else {
        if (uif.isPlaceholder == false) {
          return uif;
        }
        doWait = true; // Someone else has put the place holder in, wait for that to complete.
      }
    }
    while (doWait) {
      try {
        synchronized (cache) {
          uif = cache.get(field); // Should at least return the placeholder, NPE if not is OK.
          if (uif.isPlaceholder == false) { // OK, another thread put this in the cache we should be good.
            return uif;
          }
          cache.wait();
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted in getUninvertedField.");
      }
    }

    uif = new UnInvertedField(field, searcher);
    synchronized (cache) {
      cache.put(field, uif); // Note, this cleverly replaces the placeholder.
      cache.notifyAll();
    }

    return uif;
  }
}
