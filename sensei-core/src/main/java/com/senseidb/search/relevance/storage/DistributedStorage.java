package com.senseidb.search.relevance.storage;

import java.util.Map;

import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;

public interface DistributedStorage {

  public void addModel(String name, String model, boolean overwrite);
  public void delModel(String name);
  public Map<String, RuntimeRelevanceFunctionFactory> loadAllModels();
  public void emptyModels();
}
