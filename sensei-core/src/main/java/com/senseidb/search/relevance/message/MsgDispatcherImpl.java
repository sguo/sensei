package com.senseidb.search.relevance.message;


import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation for message dispatching based on zookeeper.
 * 
 * @author Sheng Guo <enigmaguo@hotmail.com>
 *
 */
public class MsgDispatcherImpl implements MsgDispatcher, Watcher
{

  private static final Logger _logger = LoggerFactory.getLogger(MsgDispatcherImpl.class);
  private static final AtomicLong _idGenerator = new AtomicLong(0);  // global msgDispatcher ID generator;
  private final List<MsgReceiver> _msgReceivers;   // container to host all the message receivers;

  private boolean   _initialized;
  private String    _id;        // global identity;
  private ZooKeeper _zk;
  private String    _zkAddress; // zookeeper address;
  private String    _zkPort;    // zookeeper port;
  private int       _zkTimeOut; // session time out;
  private String    _zkPath;    // zookeeper node path for notification;
  
  
  
  public MsgDispatcherImpl(String zkURL, int zkTimeOut, String zkPath) throws IOException
  {
    this(zkURL.substring(0, zkURL.indexOf(":")), 
         zkURL.substring(zkURL.indexOf(":")+1).trim(), 
         zkTimeOut, 
         zkPath);
  }
  
  public MsgDispatcherImpl(String zkAddress, String zkPort, int zkTimeOut, String zkPath)
  {
    _msgReceivers = new ArrayList<MsgReceiver>();
    _initialized = false;
    _zk = null;
    _zkAddress = zkAddress;
    _zkPort = zkPort;
    _zkTimeOut = zkTimeOut;
    
    _zkPath = zkPath.startsWith("/")? zkPath: ("/"+zkPath);
  }

  /* 
   * Initialization has to happen after the object is constructed.
   * Must be called before any other operation;
   */
  @Override
  public void initialization() throws IOException
  {
    if(_initialized == true)
    {
      _logger.info("MsgDispatcherImpl is already initialized.");
      return;
    }
    
    _id = InetAddress.getLocalHost().getHostName() + _idGenerator.getAndIncrement();
    
    // access to zookeeper;
    try
    {
      _zk = new ZooKeeper(_zkAddress + ":" + _zkPort, _zkTimeOut, this);
     
      _logger.info("MsgDispatcher _id: {}",         _id);
      _logger.info("MsgDispatcher _zkAddress: {}",  _zkAddress);
      _logger.info("MsgDispatcher _zkPort: {}",     _zkPort);
      _logger.info("MsgDispatcher _zkTimeOut: {}",  _zkTimeOut);
    }
    catch (IOException e)
    {
      throw new IOException("Can not create zookeeper object. " + e.getMessage());
    }  
    
    // create the monitoring node if it does not exist;
    try
    {
      _zk.create(_zkPath, MsgConstant.NONE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      
      _logger.info("MsgDispatcher _zkPath: {}",  _zkPath);
    }
    catch (KeeperException e)
    {
      if(KeeperException.Code.NODEEXISTS == e.code())
      {
        _logger.info("Node already created by others, will not be created again.");
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
      throw new IOException("error to initialize the disptcher, can not create node, got InterruptedException. " + e.getMessage());
    } 
    
    // Monitoring the change;
    try
    {
      _zk.getData(_zkPath, true, null);
      _initialized = true;
    }
    catch (KeeperException e)
    {
      throw new IOException("error to initialize the disptcher, got keeperException. " + e.getMessage());
    }
    catch (InterruptedException e)
    {
      throw new IOException("error to initialize the disptcher, got InterruptedException. " + e.getMessage());
    }
    
    _logger.info("MsgDispatcherImpl is successfully initialized.");
  }
  
  @Override
  public void shutdown()
  {
    try
    {    
      if(_zk != null)
        _zk.close();
    }
    catch (InterruptedException e)
    {
      // No need to do anything;
      ;
    }
  }
  
  @Override
  public void dispatchMessage(String msgType, String message) throws IOException
  {
    if(_initialized == false)
    {
      _logger.error("Message dispatcher has not been initialized to be used!");
      return;
    }

    // Currently we only support three types of relevance model operation message;
    if(msgType.equals(MsgConstant.ADD) ||
        msgType.equals(MsgConstant.DEL) ||
        msgType.equals(MsgConstant.EMPTY))
    {
      try
      {
        // send out the message so that everyone knows that something happened;
        if(message == null)
          message = "";
        String rawMessage = msgType + MsgConstant.MSG_SEPARATOR + _id + MsgConstant.MSG_SEPARATOR + message;
        _zk.setData(_zkPath, rawMessage.getBytes(),-1);
      }
      catch (KeeperException e)
      {
        throw new IOException("error to send out message, got keeperException. " + e.getMessage());
      }
      catch (InterruptedException e)
      {
        throw new IOException("error to send out message, got InterruptedException. " + e.getMessage());
      }
    }
    else{
      _logger.error("Message type unknown:" + msgType);
    }
  }

  public void onMessage(String msgType, boolean isSender, String message)
  {
    // Do nothing for dummy message;
    if(MsgConstant.NONE.equals(msgType))
      return;
    
    // Iterating all the receivers and trigger the onMessage() method;
    for(MsgReceiver r: _msgReceivers)
    {
      r.onMessage(msgType, isSender, message);
    }
  }

  @Override
  public void registerCallback(MsgReceiver receiver) throws IOException
  {
    if(_initialized == false)
    {
      _logger.error("Not initialized dispatcher to be used!");
      throw new IOException("Message dispatcher has not been initialized to be used! Can not register message receiver.");
    }
    _msgReceivers.add(receiver);    
  }

  /* 
   * When the event comes, check what kind of message it is, and trigger corresponding receivers to take action;
   */
  @Override
  public void process(WatchedEvent event)
  {
    if(_initialized == false)
    {
      // If the user forget to initialize it;
      if(! event.getType().equals(Watcher.Event.EventType.None))
        _logger.error("Message dispatcher has not been initialized to be used! Can not process event: " + event.getType().toString());
      return;
    }
    
    // Keep watching the change there;
    try
    {
      _zk.exists(_zkPath, true);
      _logger.info("re-attached the watcher");
    }
    catch (KeeperException e)
    {
      _logger.error(e.getMessage());
    }
    catch (InterruptedException e)
    {
      _logger.error(e.getMessage());
    }
    
    _logger.info("Got notification event coming:" + event.getType());
    
    
    // Handle the incoming event;
    if(event.getType().equals(Watcher.Event.EventType.NodeDataChanged))
    {
      try
      {
        String rawMessage = new String(_zk.getData(_zkPath, false, null));
        int idx1 = rawMessage.indexOf(MsgConstant.MSG_SEPARATOR);        
        String msgType = rawMessage.substring(0, idx1);   // type
        int idx2 = rawMessage.indexOf(MsgConstant.MSG_SEPARATOR, idx1+1);
        String id = rawMessage.substring(idx1+1, idx2);      // sender's ID;
        String message = ( (idx2+1) == rawMessage.length())? "": rawMessage.substring(idx2+1);  // message payload. (could be empty string)
        onMessage(msgType, _id.equals(id), message);  // for now only consider invalidation message;
      }
      catch (KeeperException e)
      {
        _logger.error(e.getMessage());
      }
      catch (InterruptedException e)
      {
        _logger.error(e.getMessage());
      }
    }
    else
    {
      // for other event type, just ignore now;
      ;
    }

  }
  
  // Dummy test class for local and two machine test, this can not be easily unit-tested;
  public static class TestReceiver implements MsgReceiver
  {

    @Override
    public void onMessage(String msgType, boolean isSender, String message)
    {
      System.out.println("Got message:" + message);
      System.out.println("From sender:" + isSender);
      System.out.println("Message type:" + msgType);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException
  {
    TestReceiver tester = new TestReceiver();
    String zkAddress = "127.0.0.1";
    String zkPort = "2181";
    int zkTimeOut = 30000;
    String zkPath = "SenseiDBMessage";
    MsgDispatcherImpl dispatcher = new MsgDispatcherImpl(zkAddress, zkPort, zkTimeOut, zkPath);    
    
    dispatcher.initialization();
    dispatcher.registerCallback(tester);
    
    // send out message;
    System.out.println("sleeping.");
    Thread.sleep(1000);
    System.out.println("\nsending out message 1");
    dispatcher.dispatchMessage(MsgConstant.ADD, "");
    System.out.println();
    Thread.sleep(1000);
    System.out.println("\nsending out message 2");
    dispatcher.dispatchMessage(MsgConstant.DEL, "testing message.");
    System.out.println();
    Thread.sleep(1000);
    System.out.println("\nsending out message 3");
    dispatcher.dispatchMessage("", "testing message empty type.");
    System.out.println();
    Thread.sleep(3000);
    System.out.println("Done.");
  }

}
