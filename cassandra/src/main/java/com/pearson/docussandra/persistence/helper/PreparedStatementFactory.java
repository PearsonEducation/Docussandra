package com.pearson.docussandra.persistence.helper;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.pearson.docussandra.cache.CacheFactory;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating and reusing PreparedStatements.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PreparedStatementFactory
{
    //private static Cache preparedStatementCache = null;
    // private static boolean established = false;

    private static final Logger logger = LoggerFactory.getLogger(PreparedStatementFactory.class);

    private static final Object LOCK = new Object();

//    /**
//     * Establishes the cache if it doesn't exist.
//     */
//    private synchronized static void establishCache()
//    {     
//        logger.debug("Establishing prepared statement cache...");
//        if (preparedStatementCache == null)
//        {
//            preparedStatementCache = CacheFactory.getCache("preparedStatements");
//        }
//        established = true;
//    }
    /**
     * Gets a prepared statement. Could be new, or from the cache.
     *
     * @param query The query to get the statement for.
     * @param session The session to create the statement in.
     * @return a PreparedStatement to use.
     */
    public static PreparedStatement getPreparedStatement(String query, Session session)
    {
//        if (!established)
//        {
//            establishCache();
//        }
        //StopWatch sw = new StopWatch();
        //sw.start();
        if (query == null || query.trim().equals(""))
        {
            throw new IllegalArgumentException("Query must be populated.");
        }
        if (session == null)
        {
            throw new IllegalArgumentException("Session cannot be null.");
        }
        query = query.trim();
        Cache c = CacheFactory.getCache("preparedStatements");
        Element e = null;
//        synchronized (LOCK)
        //{
        e = c.get(query);
        if (e == null || e.getObjectValue() == null)
        {
            logger.debug("Creating new Prepared Statement for: " + query);
            try
            {
                e = new Element(query, session.prepare(query));
                c.put(e);
            } catch (InvalidQueryException ex)
            {
                logger.error("Serious problem when attempting to prepare query: "
                        + query + " This is likely a fatal application problem.", ex);
                throw ex;//this wasn't getting logged for some reason, so we will manually log ^^
            }
        } else
        {
            if (logger.isTraceEnabled())
            {
                PreparedStatement ps = (PreparedStatement) e.getObjectValue();
                logger.trace("Pulling PreparedStatement from Cache: " + ps.getQueryString());
            }
        }
        //}
        //sw.stop();
        //logger.debug("Time to fetch prepared statement (" + query + "): " + sw.getTime());
        return (PreparedStatement) e.getObjectValue();

    }

}
