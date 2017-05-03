package org.apache.apex.malhar.lib.batch;

import org.apache.apex.api.operator.ControlTuple;
import org.apache.hadoop.classification.InterfaceStability.Evolving;

@Evolving
public interface EndApplicationControlTuple extends ControlTuple
{
  @Evolving
  public static class EndApplicationControlTupleImpl implements EndApplicationControlTuple
  {
    @Override
    public DeliveryType getDeliveryType()
    {
      return DeliveryType.END_WINDOW;
    }
  }
}
