/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.examples.batchWindowApplication;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.examples.batchWindowApplication.FileControlTuple.EndFileControlTuple;
import org.apache.apex.examples.batchWindowApplication.FileControlTuple.StartFileControlTuple;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;

public class BatchedFileInputOperator extends LineByLineFileInputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(BatchedFileInputOperator.class);

  private int count = 0;
  public final transient ControlAwareDefaultOutputPort<String> out = new ControlAwareDefaultOutputPort<String>();

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    this.isBatchEnd = false;
  }

  @Override
  public void openPendingFile(String newPathString) throws IllegalArgumentException, IOException
  {
    count = 0;
    LOG.warn("Emitting start batch - {}", count);
    emitStartBatch();
    super.openPendingFile(newPathString);
    LOG.warn("Emitting start file - 1");
    emitStartFileControlTuple();
  }

  @Override
  protected void emit(String tuple)
  {
    out.emit(tuple);
    count++;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    LOG.warn("Emitting end file - 1");
    emitEndFileControlTuple();
    super.closeFile(is);
    LOG.warn("Emitting end batch - {}", count);
    emitEndBatch();
    this.isBatchEnd = true;
  }

  protected void emitStartFileControlTuple()
  {
    LOG.info("Emitting start file control tuple : {} : Wid : {}", this.currentFile, this.currentWindowId);
    this.out.emitControl(new StartFileControlTuple(this.currentFile));
  }

  protected void emitEndFileControlTuple()
  {
    LOG.info("Emitting end batch control tuple : {}, wID : {}", this.currentFile, this.currentWindowId);
    this.out.emitControl(new EndFileControlTuple(this.currentFile));
  }

  public void emitStartBatch()
  {
    out.emitControl(new BatchControlTupleImpl.StartBatchImpl());
  }

  public void emitEndBatch()
  {
    out.emitControl(new BatchControlTupleImpl.EndBatchImpl());
  }

}
