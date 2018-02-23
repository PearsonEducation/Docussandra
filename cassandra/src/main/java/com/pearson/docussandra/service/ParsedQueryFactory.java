
package com.pearson.docussandra.service;

import com.datastax.driver.core.Session;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.ParsedQuery;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.Query;
import com.pearson.docussandra.domain.objects.WhereClause;
import com.pearson.docussandra.exception.FieldNotIndexedException;
import com.pearson.docussandra.persistence.IndexRepository;
import com.pearson.docussandra.persistence.impl.IndexRepositoryImpl;
import com.pearson.docussandra.persistence.helper.PreparedStatementFactory;
import java.util.ArrayList;
import java.util.List;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating ParsedQueries.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class ParsedQueryFactory {

  private static final Logger logger = LoggerFactory.getLogger(PreparedStatementFactory.class);

  /**
   * Gets a parsed query for the passed in parameters. If the ParsedQuery requested has already been
   * created on this app node, it will retrieve it from a cache instead of recreating it. Use this
   * method instead of parseQuery if a cached copy is acceptable (almost always).
   *
   * @param db Database that the query will run against
   * @param toParse Query to be parsed.
   * @param session Database session.
   * @return a PreparedStatement to use.
   */
  public static ParsedQuery getParsedQuery(String db, Query toParse, Session session)
      throws FieldNotIndexedException {

    if (db == null || db.trim().equals("")) {
      throw new IllegalArgumentException("Query must be populated.");
    }
    if (toParse == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    final String key = db + ":" + toParse.getTable() + ":" + toParse.getWhere();
    // StopWatch pull = new StopWatch();
    // pull.start();
    Cache c = CacheFactory.getCache("parsedQuery");
    // synchronized (CacheSynchronizer.getLockingObject(key, ParsedQuery.class))
    // {
    Element e = c.get(key);
    // pull.stop();
    // logger.debug("Time to pull a parsed query from cache: " + pull.getTime());
    if (e == null) {
      logger.debug("Creating new ParsedQuery for: " + key);
      // StopWatch sw = new StopWatch();
      // sw.start();
      e = new Element(key, parseQuery(db, toParse, session));
      c.put(e);
      // sw.stop();
      // logger.debug("Time to create a new parsed query: " + sw.getTime());
    } else {
      logger.trace("Pulling ParsedQuery from Cache: " + e.getObjectValue().toString());
    }
    return (ParsedQuery) e.getObjectValue();
    // }
  }

  /**
   * Parses a query to determine if it is valid and determine the information we actually need to
   * perform the query.
   *
   * @param db Database that the query will run against
   * @param toParse Query to be parsed.
   * @param session Database session.
   * @return A ParsedQuery object for the query.
   * @throws FieldNotIndexedException
   */
  public static ParsedQuery parseQuery(String db, Query toParse, Session session)
      throws FieldNotIndexedException {
    // let's parse the where clause so we know what we are actually searching for
    WhereClause where = new WhereClause(toParse.getWhere());
    // determine if the query is valid; in other words is it searching on valid getFields that we
    // have indexed
    List<String> fieldsToQueryOn = where.getFields();
    IndexRepository indexRepo = new IndexRepositoryImpl(session);
    List<Index> indices = indexRepo.readAllCached(new Identifier(db, toParse.getTable()));
    Index indexToUse = null;
    for (Index index : indices) {
      // if (index.isActive())//only use active indexes
      // {
      if (Utils.equalLists(index.getFieldsValues(), fieldsToQueryOn)) {
        indexToUse = index;// we have a perfect match; the index matches the query exactly
        break;
      }
      // }
    }
    if (indexToUse == null) {// whoops, no perfect match, let try for a partial match (ie, the index
                             // has more getFields than the query)
                             // querying on non-primary getFields will lead to us being unable to
                             // determine which bucket to search
      for (Index index : indices) {
        // if (index.isActive())//only use active indexes
        // {
        // make a copy of the fieldsToQueryOn so we don't mutate the original
        ArrayList<String> fieldsToQueryOnCopy = new ArrayList<>(fieldsToQueryOn);
        ArrayList<String> indexFields = new ArrayList<>(index.getFieldsValues());// make a copy here
                                                                                 // too
        fieldsToQueryOnCopy.removeAll(indexFields);// we remove all the getFields we have, from the
                                                   // getFields we want
        // if there are not any getFields left in getFields we want
        if (fieldsToQueryOnCopy.isEmpty() && fieldsToQueryOn.contains(indexFields.get(0))) {// second
                                                                                            // clause
                                                                                            // in
                                                                                            // this
                                                                                            // statement
                                                                                            // is
                                                                                            // what
                                                                                            // ensure
                                                                                            // we
                                                                                            // have
                                                                                            // a
                                                                                            // primary
                                                                                            // index;
                                                                                            // see
                                                                                            // above.
                                                                                            // we
                                                                                            // have
                                                                                            // an
                                                                                            // index
                                                                                            // that
                                                                                            // will
                                                                                            // work
                                                                                            // (even
                                                                                            // though
                                                                                            // we
                                                                                            // have
                                                                                            // extra
                                                                                            // getFields
                                                                                            // in
                                                                                            // it)
          indexToUse = index;
          break;
        }
        // }
      }
    }
    if (indexToUse == null) {
      throw new FieldNotIndexedException(fieldsToQueryOn);
    }
    ParsedQuery toReturn = new ParsedQuery(toParse, where, indexToUse);
    return toReturn;
  }

}
