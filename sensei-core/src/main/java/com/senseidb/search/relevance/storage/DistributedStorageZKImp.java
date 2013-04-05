package com.senseidb.search.relevance.storage;


import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.senseidb.search.relevance.RelevanceFunctionBuilder;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;


/**
 * Relevance Storage Handler Implementation based on Zookeeper;
 * @author Sheng Guo
 */
public class DistributedStorageZKImp implements DistributedStorage
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageZKImp.class);
  
  private static ZkModelDataAccessor _zkDataAccessor = null;

  public DistributedStorageZKImp(ZkModelDataAccessor zkDataAccessor)
  {
    _zkDataAccessor = zkDataAccessor; 
  }
  
  @Override
  public void addModel(String name, String model, boolean overwrite) {
    _zkDataAccessor.addZookeeperData(name, model, overwrite);
  }

  @Override
  public void delModel(String name) {
    _zkDataAccessor.removeZookeeperData(name);
  }

  @Override
  public Map<String, RuntimeRelevanceFunctionFactory> loadAllModels() {
    
    HashMap<String, String> jsonModels = _zkDataAccessor.getZookeeperData();
    Map<String, RuntimeRelevanceFunctionFactory> models = new HashMap<String, RuntimeRelevanceFunctionFactory>();
    for(Map.Entry<String, String> entry : jsonModels.entrySet())
    {
      String modelName = entry.getKey();
      String modelJsonString = entry.getValue();
      
      JSONObject modelJson;
      try {
        modelJson = new JSONObject(modelJsonString);
        RuntimeRelevanceFunctionFactory rrfFactory = (RuntimeRelevanceFunctionFactory) RelevanceFunctionBuilder.buildModelFactoryFromModelJSON(modelJson);
        models.put(modelName, rrfFactory);
      } catch (JSONException e) {
        LOGGER.error("Can not convert the loaded json string model to json object", e);
      }
    }
    return models;
  }

  @Override
  public void emptyModels() {
    _zkDataAccessor.emptyZookeeperData();
  }

  
  public static class DistributedStorageFactory{
    private DistributedStorageFactory()
    {
    }
    
    private static DistributedStorage _distributedStorage = null;
    
    public static void initialization(DistributedStorage distributedStorage)
    {
      _distributedStorage = distributedStorage;
    }
    public static DistributedStorage getDistributedStorage() throws Exception
    {
      if(_distributedStorage == null)
        throw new Exception("No distributed model storage accesor was initialized.");
      else
        return _distributedStorage;
    }
  }
}
