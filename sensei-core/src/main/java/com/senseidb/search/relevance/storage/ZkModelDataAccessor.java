package com.senseidb.search.relevance.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ZkModelDataAccessor
{

  private static final Logger LOGGER = LoggerFactory.getLogger(ZkModelDataAccessor.class);


  private final AtomicLong _idGenerator = new AtomicLong(0);  // global msgDispatcher ID generator;

  private String    _id;        // global identity;
  private ZooKeeper _zk;
  private String    _zkPath;    // the root path to store the relevance models;
  
  
  public ZkModelDataAccessor(String zkURL, int zkTimeOut, String zkPath) throws IOException
  {
    this(zkURL.substring(0, zkURL.indexOf(":")), 
         zkURL.substring(zkURL.indexOf(":")+1).trim(), 
         zkTimeOut, 
         zkPath);
  }
  
  public ZkModelDataAccessor(String zkAddress, String zkPort, int zkTimeOut, String zkPath) throws IOException
  {
    _zkPath = zkPath.startsWith("/")? zkPath: ("/"+zkPath);;
    _id = InetAddress.getLocalHost().getHostName() + _idGenerator.getAndIncrement();
    
    // access to zookeeper;
    try
    {
      _zk = new ZooKeeper(zkAddress + ":" + zkPort, zkTimeOut, new Watcher()
      {
        public void process(WatchedEvent event)
        {;}
      });
     
      LOGGER.info("ZookeeperDataAccessor _id: {}", _id);
      LOGGER.info("ZookeeperDataAccessor _zkAddress: {}", zkAddress);
      LOGGER.info("ZookeeperDataAccessor _zkPort: {}", zkPort);
      LOGGER.info("ZookeeperDataAccessor _zkTimeOut: {}", zkTimeOut);
    }
    catch (IOException e)
    {
      throw new IOException("Can not create zookeeper object for ZookeeperDataAccessor. " + e.getMessage());
    }  
    
    // create the model root node if it does not exist;
    try
    {
      _zk.create(_zkPath, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOGGER.info("ZookeeperDataAccessor _zkPath: {}", _zkPath);
    }
    catch (KeeperException e)
    {
      if(KeeperException.Code.NODEEXISTS == e.code())
      {
        LOGGER.info("Node already created by others, will not be created again.");
      }
      else
      {
        throw new IOException("error to initialize the disptcher, can not create node, got keeperException. " + e.getMessage());
      }
    }
    catch (InterruptedException e)
    {
      // propagate the InterruptedException to the caller is not helpful, just re-try and if still fail, no way to initialize this dispatcher.
      Thread.currentThread().interrupt();
      shutdown();
      throw new IOException("error to initialize the ZookeeperDataAccessor, can not create node, got InterruptedException. " + e.getMessage());
    } 
    
  }
  
  public void shutdown()
  {
    try
    {    
      if(_zk != null)
        _zk.close();
    }
    catch (InterruptedException e)
    {
      LOGGER.warn("Interrupted", e);
    }
  }
  
  public static byte[] compress(String text) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OutputStream out = new DeflaterOutputStream(baos);
      out.write(text.getBytes("UTF-8"));
      out.close();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    return baos.toByteArray();
  }

  public static String decompress(byte[] bytes) {
    InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) > 0)
        baos.write(buffer, 0, len);
      return new String(baos.toByteArray(), "UTF-8");
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  public HashMap<String, String> getZookeeperData()
  {
    //TODO need to get lock;
    LinkedHashMap<String, String> lhm = new LinkedHashMap<String, String>();
    try{
      List<String> models = _zk.getChildren(_zkPath, false);
      for(String name : models)
      {
        byte[] modelBytes = _zk.getData(_zkPath + "/" + name, false, null);
        String model = decompress(modelBytes);
        lhm.put(name, model);
      }
      return lhm;
    }catch(Exception e)
    {
      LOGGER.error("error when reading all the models.", e);
    }
    return lhm;
  }
  
  public boolean emptyZookeeperData()
  {
    //TODO need to get lock;
    try{
      List<String> models = _zk.getChildren(_zkPath, false);
      for(String name : models)
      {
        _zk.delete(_zkPath + "/" + name, -1);
      }
      return true;
    }catch(Exception e)
    {
      LOGGER.error("error when deleting all the models.", e);
    }
    return false;
  }
  
  public boolean addZookeeperData(String name, String value, boolean overwrite)
  {
    //TODO need to get lock;
    try{
      List<String> models = _zk.getChildren(_zkPath, false);
      
      if(!models.contains(name))
      {    
        _zk.create(_zkPath + "/" + name, compress(value), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        return true;
      }
      else if(overwrite == true)
      {  
        _zk.setData(_zkPath + "/" + name, compress(value), -1);  
        return true;
      }
      else
      {
        LOGGER.error("model already exists, has to be overwritten.");
      }
    }catch(Exception e)
    {
      LOGGER.error("error when adding new model.", e);
    }
    return false;
  }
  
  public boolean removeZookeeperData(String name)
  {
    //TODO need to get lock;
    try{
      List<String> models = _zk.getChildren(_zkPath, false);
      
      if(models.contains(name))
      {    
        _zk.delete(_zkPath + "/" + name, -1);
      }
      return true;
    }catch(Exception e)
    {
      LOGGER.error("error when deleting model.", e);
    }
    return false;
  }
  
  public static void main(String[] args) throws IOException
  {
    String zkAddress = "127.0.0.1";
    String zkPort = "2121";
    int zkTimeOut = 30000;
    String zkPath = "SenseiRelevance";
    ZkModelDataAccessor zDataAccessor = new ZkModelDataAccessor(zkAddress, zkPort, zkTimeOut, zkPath);
    
    {
      Map<String, String> map = zDataAccessor.getZookeeperData();
      System.out.println(map.size());
      for(Map.Entry<String, String> entry : map.entrySet())
      {
        System.out.println("Model   name:" + entry.getKey());
        System.out.println("Model detail:" + entry.getValue());
      }
    }
    zDataAccessor.addZookeeperData("model_A", "this is a model A", true);
    
    {
      Map<String, String> map = zDataAccessor.getZookeeperData();
      System.out.println(map.size());
      for(Map.Entry<String, String> entry : map.entrySet())
      {
        System.out.println("Model   name: " + entry.getKey());
        System.out.println("Model detail: " + entry.getValue());
      }
    }
    
    zDataAccessor.addZookeeperData("model_B", "this is a model A", false);
    
    {
      Map<String, String> map = zDataAccessor.getZookeeperData();
      System.out.println(map.size());
      for(Map.Entry<String, String> entry : map.entrySet())
      {
        System.out.println("Model   name: " + entry.getKey());
        System.out.println("Model detail: " + entry.getValue());
      }
    }
  }

}