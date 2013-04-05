package com.senseidb.search.relevance.message;

import java.io.IOException;


/**
 * A message dispatcher interface for broadcasting messages.
 * 
 * @author Sheng Guo <enigmaguo@hotmail.com>
 *
 */
public interface MsgDispatcher
{
  public static final String MSG_ROOT = "senseiMsg";
  
  void initialization() throws IOException;
  
  void dispatchMessage(String msgType, String message) throws IOException;
  
  void registerCallback(MsgReceiver cache) throws IOException;
  
  void shutdown();

}
