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

package org.carbondata.query.processor;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class FileBasedLimitProcessor implements DataProcessorExt {
  private DataProcessorExt processor;

  private int limit;

  private int counter;

  public FileBasedLimitProcessor(DataProcessorExt processor) {
    this.processor = processor;
  }

  @Override public void initialise(DataProcessorInfo model) throws DataProcessorException {
    this.processor.initialise(model);
    this.limit = model.getLimit();
  }

  @Override public void processRow(byte[] key, MeasureAggregator[] value)
      throws DataProcessorException {
    if (limit == -1 || counter < limit) {
      processor.processRow(key, value);
      counter++;
    }
  }

  @Override public void finish() throws DataProcessorException {
    processor.finish();

  }

  @Override public CarbonIterator<QueryResult> getQueryResultIterator() {
    return processor.getQueryResultIterator();
  }

  @Override public void processRow(ByteArrayWrapper key, MeasureAggregator[] value)
      throws DataProcessorException {
    if (limit == -1 || counter < limit) {
      processor.processRow(key, value);
      counter++;
    }

  }

}
