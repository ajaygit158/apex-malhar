package org.apache.apex.examples.batchWindowApplication;

import org.apache.apex.api.operator.ControlTuple;

public class FileControlTuple implements ControlTuple
{
  @Override
  public DeliveryType getDeliveryType()
  {
    return DeliveryType.IMMEDIATE;
  }

  public static class StartFileControlTuple extends FileControlTuple
  {
    private String fileName;

    public StartFileControlTuple()
    {
      // TODO Auto-generated constructor stub
    }

    public StartFileControlTuple(String filename)
    {
      this.fileName = filename;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }
  }

  public static class EndFileControlTuple extends FileControlTuple
  {
    private String fileName;

    public EndFileControlTuple()
    {
      // TODO Auto-generated constructor stub
    }

    public EndFileControlTuple(String filename)
    {
      this.fileName = filename;
    }

    public String getFileName()
    {
      return fileName;
    }

    public void setFileName(String fileName)
    {
      this.fileName = fileName;
    }
  }
}
