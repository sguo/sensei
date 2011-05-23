package com.sensei.search.nodes;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.DataProvider;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;

import com.browseengine.bobo.api.BoboIndexReader;
import com.browseengine.bobo.facets.FacetHandler;
import com.browseengine.bobo.facets.RuntimeFacetHandlerFactory;

import com.sensei.search.jmx.JmxUtil;
import com.sensei.search.req.SenseiSystemInfo;


public class SenseiCore{
  private static final Logger logger = Logger.getLogger(SenseiServer.class);

  private final MBeanServer mbeanServer = java.lang.management.ManagementFactory.getPlatformMBeanServer();

  private final List<ObjectName> _registeredMBeans;
  private SenseiZoieFactory<?> _zoieFactory;
  private SenseiIndexingManager _indexManager;
  private SenseiQueryBuilderFactory _queryBuilderFactory;
  private final HashSet<Zoie<BoboIndexReader,?>> zoieSystems = new HashSet<Zoie<BoboIndexReader,?>>();
  
  private final int[] _partitions;
  private final int _id;
  private final Map<Integer,SenseiQueryBuilderFactory> _builderFactoryMap;
  private final Map<Integer,Zoie<BoboIndexReader,?>> _readerFactoryMap;
  private SenseiSystemInfo _senseiSystemInfo;
  private volatile boolean _started;
    
  public SenseiCore(int id,int[] partitions,
            SenseiZoieFactory<?> zoieSystemFactory,
            SenseiIndexingManager indexManager,
            SenseiQueryBuilderFactory queryBuilderFactory){

    _registeredMBeans = new LinkedList<ObjectName>();
    _zoieFactory = zoieSystemFactory;
    _indexManager = indexManager;
    _queryBuilderFactory = queryBuilderFactory;
    _partitions = partitions;
    _id = id;
    
    _builderFactoryMap = new HashMap<Integer,SenseiQueryBuilderFactory>();
    _readerFactoryMap = new HashMap<Integer,Zoie<BoboIndexReader,?>>();
    _started = false;
  }
  
  public int getNodeId(){
    return _id;
  }
  
  public int[] getPartitions(){
    return _partitions;
  }

  public SenseiSystemInfo getSystemInfo()
  {
    if (_senseiSystemInfo == null)
      _senseiSystemInfo = new SenseiSystemInfo();

    if (_senseiSystemInfo.getClusterInfo() == null)
    {
      List<Integer> partitionList = new ArrayList<Integer>(_partitions.length);

      for (int i=0; i<_partitions.length; ++i)
      {
        partitionList.add(_partitions[i]);
      }

      Map<Integer, List<Integer>> clusterInfo = new HashMap<Integer, List<Integer>>();
      clusterInfo.put(_id, partitionList);
      _senseiSystemInfo.setClusterInfo(clusterInfo);
    }

    if (_senseiSystemInfo.getFacetInfos() == null)
    {
      Set<SenseiSystemInfo.SenseiFacetInfo> facetInfos = new HashSet<SenseiSystemInfo.SenseiFacetInfo>();
      if (_zoieFactory.getDecorator() != null && _zoieFactory.getDecorator().getFacetHandlerList() != null)
      {
        for (FacetHandler<?> facetHandler : _zoieFactory.getDecorator().getFacetHandlerList())
        {
          facetInfos.add(new SenseiSystemInfo.SenseiFacetInfo(facetHandler.getName()));
        }
      }

      if (_zoieFactory.getDecorator() != null && _zoieFactory.getDecorator().getFacetHandlerFactories() != null)
      {
        for (RuntimeFacetHandlerFactory<?,?> runtimeFacetHandlerFactory : _zoieFactory.getDecorator().getFacetHandlerFactories())
        {
          SenseiSystemInfo.SenseiFacetInfo facetInfo = new SenseiSystemInfo.SenseiFacetInfo(runtimeFacetHandlerFactory.getName());
          facetInfo.setRunTime(true);
          facetInfos.add(facetInfo);
        }
      }
      _senseiSystemInfo.setFacetInfos(facetInfos);
    }

    Date lastModified = new Date(0L);
    String version = null;
    for(Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
    {
      if (version == null || _zoieFactory.getVersionComparator().compare(version, zoieSystem.getVersion()) < 0)
        version = zoieSystem.getVersion();
    }

    for (ObjectName name : _registeredMBeans) {
      try
      {
        Date lastModifiedB = (Date)mbeanServer.getAttribute(name, "LastDiskIndexModifiedTime");
        if (lastModified.compareTo(lastModifiedB) < 0)
          lastModified = lastModifiedB;
      }
      catch (Exception e)
      {
        // Simplely ignore.
      }
    }
    
    _senseiSystemInfo.setLastModified(lastModified.getTime());
    if (version != null)
      _senseiSystemInfo.setVersion(version);

    return _senseiSystemInfo;
  }

  public void setSystemInfo(SenseiSystemInfo senseiSystemInfo)
  {
    _senseiSystemInfo = senseiSystemInfo;
  }
  
  public void start() throws Exception{
    if (_started) return;
      for (int part : _partitions){
        //in simple case query builder is the same for each partition
        _builderFactoryMap.put(part, _queryBuilderFactory);
        
        Zoie<BoboIndexReader,?> zoieSystem = _zoieFactory.getZoieInstance(_id,part);
        
        // register ZoieSystemAdminMBean

        String[] mbeannames = zoieSystem.getStandardMBeanNames();
        for(String name : mbeannames)
        {
          ObjectName oname = new ObjectName(JmxUtil.Domain, "name", name + "-" + _id+"-"+part);
          try
          {
            mbeanServer.registerMBean(zoieSystem.getStandardMBean(name), oname);
            _registeredMBeans.add(oname);
            logger.info("registered mbean " + oname);
          } catch(Exception e)
          {
            logger.error(e.getMessage(),e);
            if (e instanceof InstanceAlreadyExistsException)
            {
              _registeredMBeans.add(oname);
            }
          }        
        }
              
        if(!zoieSystems.contains(zoieSystem))
        {
          zoieSystem.start();
          zoieSystems.add(zoieSystem);
        }

        _readerFactoryMap.put(part, zoieSystem);
      }

    logger.info("initializing index manager...");
      _indexManager.initialize(_readerFactoryMap);
      logger.info("starting index manager...");
      _indexManager.start();
      logger.info("index manager started...");
      _started = true;
  }
  
  public void shutdown(){
    if (!_started) return;
    logger.info("unregistering mbeans...");
      try{
        if (_registeredMBeans.size()>0){
          for (ObjectName name : _registeredMBeans){
            mbeanServer.unregisterMBean(name);
          }
          _registeredMBeans.clear();
        }
      }
      catch(Exception e){
        logger.error(e.getMessage(),e);
      }
     
        // shutdown the index manager

    logger.info("shutting down index manager...");
      _indexManager.shutdown();
    logger.info("index manager shutdown...");
      
        // shutdown the zoieSystems
        for(Zoie<BoboIndexReader,?> zoieSystem : zoieSystems)
        {
          zoieSystem.shutdown();
        }
        zoieSystems.clear();
        _started =false;
  }

  public DataProvider getDataProvider()
  {
    return _indexManager.getDataProvider();
  }
  
  public IndexReaderFactory<ZoieIndexReader<BoboIndexReader>> getIndexReaderFactory(int partition){
    return _readerFactoryMap.get(partition);
  }
  
  public SenseiQueryBuilderFactory getQueryBuilderFactory(int partition){
    return _builderFactoryMap.get(partition);
  }

  public void syncWithVersion(long timeToWait, String version) throws ZoieException
  {
    _indexManager.syncWithVersion(timeToWait, version);
  }

}