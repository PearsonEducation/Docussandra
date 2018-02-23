package com.pearson.docussandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.mongodb.DBObject;
import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexField;
import com.pearson.docussandra.domain.objects.IndexIdentifier;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.exception.IndexParseFieldException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.*;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of public static helper methods for various docussandra related tasks.
 *
 * @author https://github.com/JeffreyDeYoung
 * @since Feb 12, 2015
 */
public class Utils {

  private static Logger logger = LoggerFactory.getLogger(Utils.class);

  public static final String EMPTY_STRING = "";

  /**
   * Calculates the getIndexName of an iTable based on the dataBaseName, the getTableName, and the
   * getIndexName.
   *
   * Note: No null checks.
   *
   * @param databaseName database getIndexName for the iTable.
   * @param tableName setTable getIndexName for the iTable.
   * @param indexName index getIndexName for the iTable.
   *
   * @return The getIndexName of the iTable for that index.
   */
  public static String calculateITableName(String databaseName, String tableName,
      String indexName) {
    String key = databaseName + ":" + tableName + ":" + indexName;
    Cache c = CacheFactory.getCache("iTableName");
    // synchronized (CacheSynchronizer.getLockingObject(key, "iTableName"))
    // {
    Element e = c.get(key);
    if (e == null || e.getObjectValue() == null)// if its not set, or set, but null, re-read
    {
      // not cached; let's create it
      StringBuilder sb = new StringBuilder();
      sb.append(databaseName);
      sb.append('_');
      sb.append(tableName);
      sb.append('_');
      sb.append(indexName);
      // return sb.toString().toLowerCase();
      e = new Element(key, sb.toString().toLowerCase());
      c.put(e);
    }
    return (String) e.getObjectValue();
    // }
  }

  /**
   * Calculates the getIndexName of an iTable based on an index.
   *
   * Note: No null checks.
   *
   * @param index Index whose iTable getIndexName you would like.
   * @return The getIndexName of the iTable for that index.
   */
  public static String calculateITableName(Index index) {
    return calculateITableName(index.getDatabaseName(), index.getTableName(), index.getName());
  }

  /**
   * Calculates the getIndexName of an iTable based on the IDENTIFIER of an index.
   *
   * Note: No null checks.
   *
   * @param indexId index id whose iTable getIndexName you would like.
   * @return The getIndexName of the iTable for that index.
   */
  public static String calculateITableName(IndexIdentifier indexId) {
    return calculateITableName(indexId.getDatabaseName(), indexId.getTableName(),
        indexId.getIndexName());
  }

  /**
   * Creates the database based off of a passed in CQL file. WARNING: Be careful, this could erase
   * data if you are not cautious. Ignores comment lines (lines that start with "//").
   *
   * @param cqlPath path to the CQl file you wish to use to init the database.
   * @param session Database session
   *
   * @throws IOException if it can't read from the CQL file for some reason.
   */
  @Deprecated
  public static void initDatabase(String cqlPath, Session session) throws IOException {
    logger.warn("Initing database from CQL file: " + cqlPath);
    InputStream cqlStream = Utils.class.getResourceAsStream(cqlPath);
    String cql = IOUtils.toString(cqlStream);
    String[] statements = cql.split("\\Q;\\E");
    for (String statement : statements) {
      statement = statement.trim();
      statement = statement.replaceAll("\\Q\n\\E", " ");
      if (!statement.equals("") && !statement.startsWith("//"))// don't count comments
      {
        logger.info("Executing CQL statement: " + statement);
        session.execute(statement);
      }
    }
  }

  /**
   * Creates the initial Docussandra database.
   *
   * @param dropDb Start the database dropDb; will <b>DROP THE EXISTING DATABASE</b>. Do not pass
   *        true except for testing.
   * @param replicationString Replication string to use when creating the keyspace.
   * @param session Cassandra session to use to connect in order to execute the db create.
   */
  public static void initDatabase(boolean dropDb, String replicationString, Session session) {
    // check that the replication string looks at least a little valid so we don't SQL inject that
    // easily
    if (replicationString.startsWith("{") && replicationString.endsWith("}")
        && replicationString.contains("Strategy") && replicationString.contains("'class'")) {
      logger.info("Initing Docussandra Cassandra Database with replication factor: \""
          + replicationString + "\".");
    } else {
      throw new IllegalArgumentException(
          "Replication String: \"" + replicationString + "\" does not appear to be valid.");
    }
    if (dropDb) {
      // Fix with?: https://github.com/PearsonEducation/Docussandra/issues/1
      logger.warn(
          "WARNING: DROPPING EXISTING DOCUSSANDRA database! You have 5 seconds to kill this process before the data will be dropped.");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      executeWithLog(session, "drop keyspace if exists docussandra;");
    }
    executeWithLog(session,
        "create keyspace IF NOT EXISTS docussandra with replication = " + replicationString + ";");
    executeWithLog(session, "use docussandra;");
    executeWithLog(session,
        "create table IF NOT EXISTS sys_meta (\n" + "	id text,\n" + "	version text,\n"
            + "	created_at timestamp,\n" + "	updated_at timestamp,\n"
            + "	primary key ((id), updated_at))\n" + "with clustering order by (updated_at DESC);");

    executeWithLog(session,
        "create table IF NOT EXISTS sys_db (\n" + "	db_name text primary key,\n"
            + "	description text,\n" + "	created_at timestamp,\n" + "	updated_at timestamp\n"
            + ");");
    executeWithLog(session,
        "create table IF NOT EXISTS sys_tbl (\n" + "	db_name text,\n" + "	tbl_name text,\n"
            + "	description text,\n" + "	created_at timestamp,\n" + "	updated_at timestamp,\n"
            + "	primary key ((db_name), tbl_name)\n" + ");");
    executeWithLog(session,
        "create table IF NOT EXISTS sys_idx (\n" + "	db_name text,\n" + "	tbl_name text,\n"
            + "	name text,\n" + "	is_unique boolean,\n" + "	fields list<text>,\n"
            + "	fields_type list<text>,\n" + "	only list<text>,\n" + "     is_active boolean,\n"
            + "	created_at timestamp,\n" + "	updated_at timestamp,\n"
            + "	primary key ((db_name), tbl_name, name)\n" + ");");
    executeWithLog(session,
        "create table IF NOT EXISTS sys_idx_status (\n" + "     id uuid,\n" + "	db_name text,\n"
            + "	tbl_name text,\n" + "	index_name text,\n" + "	records_completed bigint,\n"
            + "	total_records bigint,\n" + "	started_at timestamp,\n"
            + "	updated_at timestamp,\n" + "     errors list<text>,\n" + "     fatal_error text,\n"
            + "	primary key (id)\n" + ");");
    executeWithLog(session,
        "create table IF NOT EXISTS sys_idx_not_done (\n" + "    id uuid primary key\n" + ");");
  }

  /**
   * Creates the initial Docussandra database with a replication factor of 1 for local deploys and
   * testing.
   *
   * @param dropDb Start the database dropDb; will <b>DROP THE EXISTING DATABASE</b>. Do not pass
   *        true except for testing.
   * @param session Cassandra session to use to connect in order to execute the db create.
   */
  public static void initDatabaseSingleReplication(boolean dropDb, Session session) {
    initDatabase(dropDb, "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1}", session);
  }

  /**
   * Logs a statement, then executes it on the session.
   *
   * @param session Session to execute the statement on.
   * @param statement Statement to execute.
   */
  private static void executeWithLog(Session session, String statement) {
    logger.info("Executing CQL statement: " + statement);
    session.execute(statement);
  }

  public static void setField(String value, IndexField fieldData, BoundStatement bs, int index)
      throws IndexParseException {
    try {
      if (value == null) {
        bs.setToNull(index);
      } else if (fieldData.getType().equals(FieldDataType.BINARY)) {
        bs.setBytes(index, ParseUtils.parseBase64StringAsByteBuffer(value));
      } else if (fieldData.getType().equals(FieldDataType.BOOLEAN)) {
        bs.setBool(index, ParseUtils.parseStringAsBoolean(value));
      } else if (fieldData.getType().equals(FieldDataType.DATE_TIME)
          || fieldData.getType().equals(FieldDataType.TIMEPOINT)) {
        bs.setDate(index, ParseUtils.parseStringAsDate(value));
      } else if (fieldData.getType().equals(FieldDataType.DOUBLE)) {
        bs.setDouble(index, ParseUtils.parseStringAsDouble(value));
      } else if (fieldData.getType().equals(FieldDataType.INTEGER)) {
        bs.setInt(index, ParseUtils.parseStringAsInt(value));
      } else if (fieldData.getType().equals(FieldDataType.TEXT)) {
        bs.setString(index, value);
      } else if (fieldData.getType().equals(FieldDataType.UUID)) {
        bs.setUUID(index, ParseUtils.parseStringAsUUID(value));
      } else if (fieldData.getType().equals(FieldDataType.LONG)) {
        bs.setLong(index, ParseUtils.parseStringAsLong(value));
      } else {
        throw new IndexParseFieldException(fieldData.getField(), new Exception(
            fieldData.getType().toString() + " is an unsupported type. Please contact support."));
      }
    } catch (IndexParseFieldException parseException) {
      throw new IndexParseException(fieldData, parseException);
    }
  }

  /**
   * Sets a field into a BoundStatement.
   *
   * @param jsonObject Object to pull the value from
   * @param fieldData Object describing the field to pull the value from.
   * @param bs BoundStatement to set the field value to.
   * @param index Index in the BoundStatement to set.
   * @return false if the field is null and should cause this bound statement not to be entered into
   *         the batch, true otherwise (normal)
   * @throws IndexParseException If there is a problem parsing the field that indicates the entire
   *         document should not be indexed.
   */
  public static boolean setField(DBObject jsonObject, IndexField fieldData, BoundStatement bs,
      int index) throws IndexParseException {
    Object jObject = jsonObject.get(fieldData.getField());
    String jsonValue = null;
    if (jObject != null) {
      jsonValue = jObject.toString();
    }
    if (jsonValue == null) {
      /*
       * we can't index on this field, it is null, so we just won't create an index on THIS FIELD
       * throw an exception indicating this (just don't add this to the batch)
       */
      return false;
    } else if (jsonValue.isEmpty() && !fieldData.getType().equals(
        FieldDataType.TEXT)) { /*
                                * if we have an empty string for a non-text field by definition,
                                * this means that we can't parse it into a useful non-text value so
                                * there's nothing we can do to index this document, and we can't
                                * ignore it because this indicates that the field isn't in the
                                * expected format, so we need to throw an exception and not index
                                * this document AT ALL
                                */

      throw new IndexParseException(fieldData, new IndexParseFieldException(jsonValue));
    } else {
      // nothing odd here; set the field
      setField(jsonValue, fieldData, bs, index);
      return true;
    }
  }

  public static String join(String delimiter, Object... objects) {
    return join(delimiter, Arrays.asList(objects));
  }

  public static String join(String delimiter, Collection<? extends Object> objects) {
    if (objects == null || objects.isEmpty()) {
      return EMPTY_STRING;
    }
    Iterator<? extends Object> iterator = objects.iterator();
    StringBuilder builder = new StringBuilder();
    builder.append(iterator.next());
    while (iterator.hasNext()) {
      builder.append(delimiter).append(iterator.next());
    }
    return builder.toString();
  }

  public static boolean equalLists(List<String> one, List<String> two) {
    if (one == null && two == null) {
      return true;
    }
    if ((one == null && two != null) || one != null && two == null || one.size() != two.size()) {
      return false;
    }
    ArrayList<String> oneCopy = new ArrayList<>(one);
    ArrayList<String> twoCopy = new ArrayList<>(two);
    Collections.sort(oneCopy);
    Collections.sort(twoCopy);
    return one.equals(twoCopy);
  }

  /**
   * Writes a file to the file system. Used mainly for testing and some utils. Could probably be
   * better.
   *
   * @param toWrite String to write to the file system.
   * @param fileName Name of the file to write.
   * @throws FileNotFoundException
   * @throws UnsupportedEncodingException
   */
  public static void writeFile(String toWrite, String fileName)
      throws FileNotFoundException, UnsupportedEncodingException {
    logger.debug("Writing file: " + fileName);
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(fileName, "UTF-8");
      writer.println(toWrite);
    } finally {
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * Reads a string in from a file on the classpath.
   *
   * @param fileName File path relative to the classpath.
   * @return The String contents of this file.
   * @throws IOException If something goes wrong.
   */
  public static String readFile(String fileName) throws IOException {
    String lineInFile;
    InputStreamReader inStreamReader = null;
    BufferedReader br = null;
    try {
      inStreamReader = new InputStreamReader(Utils.class.getResourceAsStream(fileName));
      URL url = Utils.class.getResource(fileName);
      logger.debug("Reading file from classpath; this is the file path : " + url.getFile());
      br = new BufferedReader(inStreamReader);
      StringBuilder toReturn = new StringBuilder();
      while ((lineInFile = br.readLine()) != null) {
        toReturn.append(lineInFile).append("\n");
      }
      return toReturn.toString();
    } finally {
      if (inStreamReader != null) {
        inStreamReader.close();
      }
      if (br != null) {
        br.close();
      }
    }
  }

  /**
   * Method to convert a 1 row file to long arraylist values fileName needs to be the entire
   * location of the file example : src/test/resources/bucketsDate.csv
   */
  public static List<Long> traverse1RowFile(String fileName) throws IOException {
    logger.debug("Parsing bucket file from: " + fileName);
    ArrayList<Long> fileToArraylist = new ArrayList<>();
    String[] splitString = readFile(fileName).trim().split(",");
    logger.debug("Bucket list contains " + splitString.length + " buckets.");
    for (int i = 0; i < splitString.length; i++) {
      try {
        fileToArraylist.add(new Long(splitString[i]));
      } catch (NumberFormatException e) {
        logger.error("Could not parse token: " + splitString[i] + " from file: " + fileName
            + " at index: " + i, e);
        throw e;
      }
    }
    logger.debug("Done parsing bucket file from: " + fileName);
    return fileToArraylist;
  }

}
