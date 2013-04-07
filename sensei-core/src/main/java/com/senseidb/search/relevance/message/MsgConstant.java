package com.senseidb.search.relevance.message;


public interface MsgConstant
{

  // Message meta data separator
  public static final String  MSG_SEPARATOR   = "@SEP"; //refactor this part later;
  
  /*
   * Message types to be sent out. 
   * 
   * Attn:
   * It can NOT contain a separator.
   * It can NOT be an empty string.
   * 
   * */

  public static final String    MODEL_OP_PREFIX   = "model_";      // attn: all model operation name must have this prefix;
  public static final String    NONE              = "model_none";  // dummy message for anything requires no action;
  public static final String    ADD               = "model_add";    // add one relevance model, won't overwrite;
  public static final String    UPDATE            = "model_update"; // update one relevance model, will overwrite;
  public static final String    DEL               = "model_del";    // delete one relevance model;
  public static final String    EMPTY             = "model_empty";  // empty all the relevance models;
  
  
  
}
