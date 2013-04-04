package com.senseidb.search.relevance.storage;


import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.senseidb.search.relevance.RuntimeRelevanceFunction.RuntimeRelevanceFunctionFactory;


/**
 * Relevance Storage Handler Implementation based on Zookeeper;
 * @author Sheng Guo
 */
public class DistributedStorageZKImp implements DistributedStorage
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedStorageZKImp.class);
  
  private final ZkModelDataAccessor _zkDataAccessor;

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
    
    // TODO: convert the json to java object model;
    HashMap<String, String> jsonModels = _zkDataAccessor.getZookeeperData();

    return null;
  }

  @Override
  public void emptyModels() {
    _zkDataAccessor.emptyZookeeperData();
  }
}
