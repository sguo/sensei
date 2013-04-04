package com.senseidb.search.relevance.message;



public interface MsgReceiver
{

  public void onMessage(String msgType, boolean isSender, String message);

}
