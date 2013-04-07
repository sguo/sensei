package com.senseidb.search.relevance.storage;

import java.io.IOException;
import java.util.Map;

import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.message.MsgDispatcher;

public interface DistributedStorage {

  public static String REL_STORE_ROOT = "relevanceStore";
  
  public void init(MsgDispatcher cacheMsgDispatcher) throws IOException;
  
  public boolean addModel(String name, String model, boolean overwrite) throws IOException;
  public boolean delModel(String name) throws IOException;
  public Map<String, RuntimeRelevanceFunctionFactory> loadAllModels();
  public boolean emptyAllModels() throws IOException;
}
