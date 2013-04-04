package com.senseidb.search.relevance.message;


public interface MsgConstant
{

  // Message meta data separator
  public static final String  MSG_SEPARATOR   = ":";
  
  /*
   * Message types to be sent out. 
   * 
   * Attn:
   * It can NOT contain a separator.
   * It can NOT be an empty string.
   * 
   * */

  public static final String    NONE          = "none";  // dummy message for anything requires no action;
  public static final String    ADD           = "add";    // add one relevance model;
  public static final String    DEL           = "del";    // delete one relevance model;
  public static final String    EMPTY         = "empty";  // empty all the relevance models;
  
}
