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
package org.apache.apex.malhar.lib.fs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.malhar.lib.batch.EndApplicationControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.EndFileControlTuple;
import org.apache.apex.malhar.lib.batch.FileControlTuple.StartFileControlTuple;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

/**
 * This is an extension of the {@link AbstractFileInputOperator} that outputs the contents of a file line by line.&nbsp;
 * Each line is emitted as a separate tuple in string format.
 * <p>
 * The directory path where to scan and read files from should be specified using the {@link #directory} property.
 * </p>
 * @displayName Line-by-line File Input
 * @category Input
 * @tags fs, file, line, lines, input operator
 *
 * @since 3.4.0
 */
public class LineByLineFileInputOperator extends AbstractFileInputOperator<String>
{
  private static final Logger LOG = LoggerFactory.getLogger(LineByLineFileInputOperator.class);

  public final transient ControlAwareDefaultOutputPort<String> output = new ControlAwareDefaultOutputPort<String>();

  protected transient BufferedReader br;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    super.closeFile(is);
    br.close();
    br = null;
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
  }

  @Override
  protected void emit(String tuple)
  {
    output.emit(tuple);
  }

  @Override
  protected void emitStartBatchControlTuple()
  {
    LOG.debug("Emitting start batch control tuple : {}", this.currentFile);
    output.emitControl(new StartFileControlTuple(this.currentFile));
  }

  @Override
  protected void emitEndBatchControlTuple()
  {
    LOG.debug("Emitting end batch control tuple : {}", this.currentFile);
    output.emitControl(new EndFileControlTuple(this.currentFile));
  }

  @Override
  protected void handleEndOfInputData()
  {
    output.emitControl(new EndApplicationControlTuple.EndApplicationControlTupleImpl());
  }
}
