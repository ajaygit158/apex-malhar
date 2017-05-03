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
package com.datatorrent.lib.testbench;

import java.util.ArrayList;
import java.util.List;

import org.apache.apex.api.operator.ControlTuple;

import com.datatorrent.api.ControlTupleEnabledSink;

/**
 * A sink implementation to collect expected test results.
 * <p>
 * @displayName Collector Test Sink
 * @category Test Bench
 * @tags sink
 * @since 0.3.2
 */
public class CollectorTestSink<T> implements ControlTupleEnabledSink<T>
{
  public final List<T> collectedTuples = new ArrayList<T>();
  public final List<ControlTuple> collectedControlTuples = new ArrayList<>();

  /**
   * clears data
   */
  public void clear()
  {
    this.collectedTuples.clear();
  }

  @Override
  public void put(T payload)
  {
    synchronized (collectedTuples) {
      collectedTuples.add(payload);
      collectedTuples.notifyAll();
    }
  }

  @Override
  public boolean putControl(ControlTuple controlTuple)
  {
    synchronized (collectedControlTuples) {
      collectedControlTuples.add(controlTuple);
      collectedControlTuples.notifyAll();
    }
    return false;
  }

  public void waitForResultCount(int count, long timeoutMillis) throws InterruptedException
  {
    while (collectedTuples.size() < count && timeoutMillis > 0) {
      timeoutMillis -= 20;
      synchronized (collectedTuples) {
        if (collectedTuples.size() < count) {
          collectedTuples.wait(20);
        }
      }
    }
  }

  @Override
  public int getCount(boolean reset)
  {
    synchronized (collectedTuples) {
      try {
        return collectedTuples.size();
      } finally {
        if (reset) {
          collectedTuples.clear();
        }
      }
    }
  }
}
