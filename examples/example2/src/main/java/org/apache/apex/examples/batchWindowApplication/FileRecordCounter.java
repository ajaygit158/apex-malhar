package org.apache.apex.examples.batchWindowApplication;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.ControlAwareDefaultOutputPort;
import org.apache.apex.api.operator.ControlTuple;
import org.apache.apex.examples.batchWindowApplication.FileControlTuple.EndFileControlTuple;
import org.apache.apex.examples.batchWindowApplication.FileControlTuple.StartFileControlTuple;
import org.apache.apex.malhar.lib.batch.BatchControlTuple.EndBatchControlTuple;
import org.apache.apex.malhar.lib.batch.BatchControlTuple.StartBatchControlTuple;

import com.datatorrent.common.util.BaseOperator;

public class FileRecordCounter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(FileRecordCounter.class);

  Map<String, Integer> fileRecordCountMap = new HashMap<>();
  private String currentFile;
  private int recordCount = 0;
  public final transient ControlAwareDefaultOutputPort<String> output = new ControlAwareDefaultOutputPort<String>();
  public final transient ControlAwareDefaultInputPort<String> input = new ControlAwareDefaultInputPort<String>()
  {
    @Override
    public void process(String obj)
    {
      recordCount++;
    }

    @Override
    public boolean processControl(ControlTuple tuple)
    {
      if (tuple instanceof StartFileControlTuple) {
        currentFile = ((StartFileControlTuple)tuple).getFileName();
        LOG.warn("Received StartFileControlTuple : {}", currentFile);
        recordCount = 0;
      } else if (tuple instanceof EndFileControlTuple) {
        currentFile = ((EndFileControlTuple)tuple).getFileName();
        LOG.warn("Received EndFileControlTuple : {}", currentFile);
        fileRecordCountMap.put(currentFile, recordCount);
        LOG.warn("Count : {}", recordCount);
      } else if (tuple instanceof StartBatchControlTuple) {
        LOG.warn("Received StartBatch");
        fileRecordCountMap.clear();
      } else if (tuple instanceof EndBatchControlTuple) {
        LOG.warn("Received EndBatch");
        for (String file : fileRecordCountMap.keySet()) {
          LOG.warn("Count : {} {}", file, recordCount);
          output.emit(file + " : " + fileRecordCountMap.get(file));
        }
      }
      return false;
    }
  };
  public int getRecordCount()
  {
    return recordCount;
  }
  public void setRecordCount(int recordCount)
  {
    this.recordCount = recordCount;
  }
}
