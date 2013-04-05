package com.senseidb.search.relevance.storage;

import java.io.IOException;

import com.senseidb.search.relevance.message.MsgDispatcher;

public class DistributedStorageFactory
{
  
  private DistributedStorageFactory(){}
  
  private static DistributedStorage _distributedStorage = null;
  private static MsgDispatcher _msgDispatcher = null;
  
  public static void initialization(DistributedStorage distributedStorage, MsgDispatcher msgDispatcher) throws IOException
  {
    _distributedStorage = distributedStorage;
    _msgDispatcher = msgDispatcher;
    _msgDispatcher.initialization();
    _distributedStorage.init(msgDispatcher);
  }
  
  public static MsgDispatcher getMsgDispatcher() throws Exception
  {
    if(_msgDispatcher == null)
      throw new Exception("No message dispatcher was initialized.");
    else
      return _msgDispatcher;
  }
  
  public static DistributedStorage getDistributedStorage() throws Exception
  {
    if(_distributedStorage == null)
      throw new Exception("No distributed model storage accesor was initialized.");
    else
      return _distributedStorage;
  }
}
