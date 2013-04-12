package com.senseidb.search.relevance.storage;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.senseidb.search.relevance.RelevanceFunctionBuilder;
import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;
import com.senseidb.search.relevance.message.MsgConstant;
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
  
  private static Lock _exeOrderLock = new ReentrantLock();
  private static Lock _inMemStoreLock = new ReentrantLock();

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
    if(!isSender)
    {
      // modify the in-memory model storage only after receiving the message
      // and the receiver is not the message sender;
      try {
        handleLocalStorage(msgType, message);
      } catch (IOException e) {
        LOGGER.error("Can not handle notified model change message. Type:" + msgType + " message:" + message);
      }
    }
  }
  
  @Override
  public boolean addModel(String name, String model, boolean overwrite) throws IOException {
    _exeOrderLock.lock();
    try
    {
      // prevent user-side mistake of sending out too many same model create statement;
      if(checkModelExistence(name, model))
        return true;
      
      // update central storage;
      boolean success = _zkDataAccessor.addZookeeperData(name, model, overwrite);
  
      if(success)
      {
        String msgType = (overwrite == true) ? MsgConstant.UPDATE : MsgConstant.ADD;
        String message = name + MsgConstant.MSG_SEPARATOR + model;
  
        // update local storage;
        handleLocalStorage(msgType, message);
        
        // send out message;
        _msgDispatcher.dispatchMessage(msgType, message);
        
        return true;
      }
      else 
        return false;
    }
    finally
    {
      _exeOrderLock.unlock();
    }
  }


  @Override
  public boolean delModel(String name) throws IOException {
    _exeOrderLock.lock();
    try
    {
      if(!checkModelExistence(name))
        return true;
      // update central storage;
      boolean success = _zkDataAccessor.removeZookeeperData(name);
      
      if(success)
      {
        String msgType = MsgConstant.DEL;
        String message = name;
        
        // update local storage;
        handleLocalStorage(msgType, message);
        
        // send out message;
        _msgDispatcher.dispatchMessage(msgType, message);
        return true;
      }
      else 
        return false;
    }
    finally
    {
      _exeOrderLock.unlock();
    }
  }

  @Override
  public Map<String, RuntimeRelevanceFunctionFactory> loadAllModels() {
    _exeOrderLock.lock();
    try
    {
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
      
      InMemModelStorage.injectRuntimeModel(models, true); // load the models into the in-memory storage;
      return models;
    }
    finally
    {
      _exeOrderLock.unlock();
    }
  }

  @Override
  public boolean emptyAllModels() throws IOException {
    _exeOrderLock.lock();
    try
    {
      // update central storage;
      boolean success = _zkDataAccessor.emptyZookeeperData();
      
      if(success)
      {
        String msgType = MsgConstant.EMPTY;
        String message = "";
        
        // update local storage;
        handleLocalStorage(msgType, message);
        
        // send out message;
        _msgDispatcher.dispatchMessage(msgType, message);
        
        return true;
      }
      else 
        return false;
    }
    finally
    {
      _exeOrderLock.unlock();
    }
  }
  

  private void handleLocalStorage(String msgType, String message) throws IOException
  {
    _inMemStoreLock.lock();
    try
    {
      if(msgType.equals(MsgConstant.DEL))
      {
        InMemModelStorage.delPreloadedModel(message);
        InMemModelStorage.delRuntimeModel(message);
      }
      else if(msgType.equals(MsgConstant.ADD))
      {
        String name = getModelName(message);
        String model = getModelJSONString(message);
        try{
          JSONObject modelJson = new JSONObject(model);
          RuntimeRelevanceFunctionFactory rrf = (RuntimeRelevanceFunctionFactory) RelevanceFunctionBuilder.buildModelFactoryFromModelJSON(modelJson);
          InMemModelStorage.injectRuntimeModel(name, rrf, false);
        }catch(Exception e)
        {
          throw new IOException("can not create the model factory.");
        }
      }
      else if(msgType.equals(MsgConstant.UPDATE))
      {
        String name = getModelName(message);
        String model = getModelJSONString(message);
        try{
          JSONObject modelJson = new JSONObject(model);
          RuntimeRelevanceFunctionFactory rrf = (RuntimeRelevanceFunctionFactory) RelevanceFunctionBuilder.buildModelFactoryFromModelJSON(modelJson);
          InMemModelStorage.injectRuntimeModel(name, rrf, true);
        }catch(Exception e)
        {
          throw new IOException("can not create the model factory.");
        }
      }
      else if(msgType.equals(MsgConstant.EMPTY))
      {
        InMemModelStorage.delAllPreloadedModel();
        InMemModelStorage.delAllRuntimeModel();
      }
      else
        LOGGER.error("unsupported model operation: " + msgType);
    }
    finally
    {
      _inMemStoreLock.unlock();
    }
  }

  private String getModelName(String message) {
    int loc = message.indexOf(MsgConstant.MSG_SEPARATOR);
    return message.substring(0, loc);
  }
  
  private String getModelJSONString(String message) {
    int loc = message.indexOf(MsgConstant.MSG_SEPARATOR);
    return message.substring(loc + MsgConstant.MSG_SEPARATOR.length());
  }
  

  private boolean checkModelExistence(String name, String model) throws IOException {
    try{
      JSONObject modelJson = new JSONObject(model);
      RuntimeRelevanceFunctionFactory rrfNew = (RuntimeRelevanceFunctionFactory) RelevanceFunctionBuilder.buildModelFactoryFromModelJSON(modelJson);
      RuntimeRelevanceFunctionFactory rrfOld = InMemModelStorage.getRuntimeModel(name);
      if(rrfOld != null && rrfOld.equals(rrfNew))
        return true;
    }catch(Exception e)
    {
      throw new IOException("can not create the model factory.");
    }
    return false;
  }
  
  private boolean checkModelExistence(String name) {
    return InMemModelStorage.hasRuntimeModel(name);
  }
}
