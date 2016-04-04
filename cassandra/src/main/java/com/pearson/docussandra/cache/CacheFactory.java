
package com.pearson.docussandra.cache;

import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Cache objects. Must already be define in ehcache.xml.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class CacheFactory
{

    /**
     * CacheManger for this entire application.
     */
    private static CacheManager manager = null;
    /**
     * Map containing all of our Caches.
     */
    private static ConcurrentHashMap<String, Cache> cacheMap = new ConcurrentHashMap<>();
    /**
     * Flag indicating if the CacheManager has been established or not.
     */
    private static boolean cacheManagerEstablished = false;
    /**
     * Logger.
     */
    private static final Logger logger = LoggerFactory.getLogger(CacheFactory.class);

    /**
     * Constructor. Private for public static access only to methods.
     */
    private CacheFactory()
    {
        ;
    }

    /**
     * Establishes the cache if it doesn't exist.
     */
    private synchronized static void establishCacheManager()
    {
        URL url = PreparedStatementFactory.class.getResource("/ehcache.xml");
        logger.debug("Establishing cache manager with config file: " + url.getPath());
        if (manager == null)
        {
            manager = CacheManager.newInstance(url);
        }
        cacheManagerEstablished = true;
    }

    public static Cache getCache(String cacheName)
    {
        if (!cacheManagerEstablished)//establish the manager if we don't have one yet
        {
            establishCacheManager();
        }
        //synchronized (CacheSynchronizer.getLockingObject(cacheName, Cache.class))
        //{
            Cache c = cacheMap.get(cacheName);//try to pull the cache from our map
            if (c == null)//it doesn't exist yet
            {
                //lets create a new cache
                c = manager.getCache(cacheName);
                if (c == null)
                {
                    throw new RuntimeException("Cache is not defined: "+cacheName+". This is a programming error.");
                }
                cacheMap.put(cacheName, c);
            }
            return c;
       // }        
    }

    /**
     * Shuts down the cache; only call upon application shutdown.
     */
    public static void shutdownCacheManger()
    {
        if (manager != null)
        {
            logger.info("Shutting down cache manager.");
            manager.shutdown();
            cacheMap = new ConcurrentHashMap<>();
            cacheManagerEstablished = false;
        }
    }
    
    /**
     * Clears all caches we currently have. For testing use only.
     */
    public static void clearAllCaches(){
        Enumeration e = cacheMap.elements();
        while(e.hasMoreElements()){
            Cache c = (Cache)e.nextElement();
            c.removeAll();
        }
    }
}
