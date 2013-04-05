package com.senseidb.search.relevance.storage;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.senseidb.search.relevance.RelevanceFunctionBuilder;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.message.MsgDispatcher;
import com.senseidb.search.relevance.message.MsgReceiver;


/**
 * Relevance Storage Handler Implementation based on Zookeeper;
 * @author Sheng Guo
 */
public class DistributedStorageZKImp implements DistributedStorage, MsgReceiver
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageZKImp.class);
  
  private static ZkModelDataAccessor _zkDataAccessor = null;
  private static MsgDispatcher _msgDispatcher = null;

  public DistributedStorageZKImp(ZkModelDataAccessor zkDataAccessor)
  {
    _zkDataAccessor = zkDataAccessor; 
  }
  
  public void init(MsgDispatcher cacheMsgDispatcher) throws IOException
  {
    _msgDispatcher = cacheMsgDispatcher;
    // Put callback registration in initialization after the object is constructed;
    _msgDispatcher.registerCallback(this);
  }
  
  @Override
  public void onMessage(String msgType, boolean isSender, String message)
  {
    // TODO Auto-generated method stub
    if(!isSender)
    {
      // modify the in-memory model storage only after receiving the message;
      
    }
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
    
    InMemModelStorage.injectRuntimeModel(models); // load the models into the in-memory storage;
    return models;
  }

  @Override
  public void emptyModels() {
    _zkDataAccessor.emptyZookeeperData();
  }
}
