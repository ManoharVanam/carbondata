/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.datastorage.store.columnar;

import org.carbondata.core.util.ByteUtil.UnsafeComparer;

public class ColumnWithIntIndexForHighCard extends ColumnWithIntIndex
    implements Comparable<ColumnWithIntIndex> {

  public ColumnWithIntIndexForHighCard(byte[] column, int index) {
    super(column, index);
  }

  @Override public int compareTo(ColumnWithIntIndex o) {
    return UnsafeComparer.INSTANCE
        .compareTo(column, 2, column.length - 2, o.column, 2, o.column.length - 2);
  }

}
