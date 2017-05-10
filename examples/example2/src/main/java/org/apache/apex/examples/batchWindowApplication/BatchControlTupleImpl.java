package org.apache.apex.examples.batchWindowApplication;

import org.apache.apex.malhar.lib.batch.BatchControlTuple.EndBatchControlTuple;
import org.apache.apex.malhar.lib.batch.BatchControlTuple.StartBatchControlTuple;

public class BatchControlTupleImpl
{
  public static class StartBatchImpl implements StartBatchControlTuple
  {
    @Override
    public DeliveryType getDeliveryType()
    {
      return DeliveryType.IMMEDIATE;
    }
  }

  public static class EndBatchImpl implements EndBatchControlTuple
  {
    @Override
    public DeliveryType getDeliveryType()
    {
      return DeliveryType.END_WINDOW;
    }
  }
}
