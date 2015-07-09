package org.apache.solr.search;

import org.apache.lucene.search.SortField;

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

public class JoinQueryResultKey {
  String fromIndex = "";
  String fromField = "";
  String toField = "";
  
  private final int hc;  // cached hashCode
  
  public JoinQueryResultKey(String fromIndex, String fromField, String toField){
    this.fromIndex = fromIndex;
    this.fromField = fromField;
    this.toField = toField;
    hc = this.fromIndex.hashCode() + this.fromField.hashCode() + this.toField.hashCode();
  }
  
  @Override
  public int hashCode() {
    return hc;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o==this) return true;
    if (!(o instanceof JoinQueryResultKey)) return false;
    JoinQueryResultKey other = (JoinQueryResultKey)o;

    // fast check of the whole hash code... most hash tables will only use
    // some of the bits, so if this is a hash collision, it's still likely
    // that the full cached hash code will be different.
    if (this.hc != other.hc) return false;

    // check for the thing most likely to be different (and the fastest things)
    // first.
    if (!this.fromIndex.equals(other.fromIndex)) return false;
    if (!this.fromField.equals(other.fromField)) return false;
    if (!this.toField.equals(other.toField)) return false;

    return true;
  }
}
