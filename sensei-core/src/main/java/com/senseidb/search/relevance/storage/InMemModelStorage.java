package com.senseidb.search.relevance.storage;

import java.util.HashMap;
import java.util.Map;

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
  
  
  // (1) Read operations;
  
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
  
  
  // (2) write operations;
  public static synchronized boolean injectPreloadedModel(String name, CustomRelevanceFunctionFactory crf, boolean overwrite)
  {
    if(preloadedCRFMap.containsKey(name) && (overwrite == false))
      return false;
    else
    {
      preloadedCRFMap.put(name, crf);
      return true;
    }
  }
  
  public static synchronized boolean injectRuntimeModel(String name, RuntimeRelevanceFunctionFactory rrf, boolean overwrite)
  {
    if(runtimeCRFMap.containsKey(name) && (overwrite == false))
      return false;
    else
    {
      runtimeCRFMap.put(name, rrf);
      return true;
    }
  }
  
  public static synchronized boolean injectRuntimeModel(Map<String, RuntimeRelevanceFunctionFactory> crfMap, boolean overwrite)
  {
    boolean conflict = false;
    for(Map.Entry<String, RuntimeRelevanceFunctionFactory> entry : crfMap.entrySet())
    {
      String name = entry.getKey();
      if(runtimeCRFMap.containsKey(name))
      {
        conflict = true;
        break;
      }
    }

    if(conflict == true && overwrite == false)
    {
      return false;
    }
    else
    {
      runtimeCRFMap.putAll(crfMap);
      return true;
    }
  }
  
  public static synchronized void delRuntimeModel(String modelName)
  {
     runtimeCRFMap.remove(modelName);
  }
  
  public static synchronized void delPreloadedModel(String modelName)
  {
     preloadedCRFMap.remove(modelName);
  }
  
  public static synchronized void delAllPreloadedModel()
  {
     preloadedCRFMap.clear();
  }
  
  public static synchronized void delAllRuntimeModel()
  {
    runtimeCRFMap.clear();
  }
}
