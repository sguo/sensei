package com.senseidb.search.relevance.storage;

import java.util.HashMap;
import java.util.Map;

import com.senseidb.search.relevance.CustomRelevanceFunction;
import com.senseidb.search.relevance.RuntimeRelevanceFunction;
import com.senseidb.search.relevance.CustomRelevanceFunction.CustomRelevanceFunctionFactory;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;

public class InMemModelStorage
{

  /**
   * 
   *   A model is a relevance function factory, which can build() a CustomRelevanceFunction Object;
   * 
   * 
   * **/
  
  
  // preloaded models; custom relevance function factory
  private static Map<String, CustomRelevanceFunctionFactory> preloadedCRFMap = new HashMap<String, CustomRelevanceFunctionFactory>();
  
  
  // runtime models; runtime relevance function factory
  private static Map<String, RuntimeRelevanceFunctionFactory> runtimeCRFMap = new HashMap<String, RuntimeRelevanceFunctionFactory>();
  
  
  
  public static void injectPreloadedModel(String name, CustomRelevanceFunctionFactory crf)
  {
    preloadedCRFMap.put(name, crf);
  }
  
  public static void injectRuntimeModel(String name, RuntimeRelevanceFunctionFactory rrf)
  {
    runtimeCRFMap.put(name, rrf);
  }
  
  public static void injectRuntimeModel(Map<String, RuntimeRelevanceFunctionFactory> crfMap)
  {
    runtimeCRFMap.putAll(crfMap);
  }
  
  public static boolean hasRuntimeModel(String modelName)
  {
    return runtimeCRFMap.containsKey(modelName);
  }
  
  public static boolean hasPreloadedModel(String modelName)
  {
    return preloadedCRFMap.containsKey(modelName);
  }
  
  public static RuntimeRelevanceFunctionFactory getRuntimeModel(String modelName)
  {
    return runtimeCRFMap.get(modelName);
  }
  
  public static CustomRelevanceFunctionFactory getPreloadedModel(String modelName)
  {
    return preloadedCRFMap.get(modelName);
  }
  
  
}
