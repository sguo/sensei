package com.senseidb.search.relevance.storage;

import java.io.IOException;
import java.util.Map;

import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.message.MsgDispatcher;

public interface DistributedStorage {

  public static String REL_STORE_ROOT = "relevanceStore";
  
  public void init(MsgDispatcher cacheMsgDispatcher) throws IOException;
  
  public void addModel(String name, String model, boolean overwrite);
  public void delModel(String name);
  public Map<String, RuntimeRelevanceFunctionFactory> loadAllModels();
  public void emptyModels();
}
