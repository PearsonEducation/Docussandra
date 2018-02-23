
package com.pearson.docussandra.persistence.parent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.pearson.docussandra.domain.objects.Identifier;

/**
 * Super class for our repositories.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class AbstractCassandraRepository {

  /**
   * A pre-configured Session instance.
   */
  private Session session;
  /**
   * The name of the Cassandra table entities are stored in.
   */
  private String table;

  /**
   * Default constructor.
   */
  public AbstractCassandraRepository() {

  }

  /**
   * Constructor.
   *
   * @param session a pre-configured Session instance.
   * @param tableName the name of the Cassandra table entities are stored in.
   */
  public AbstractCassandraRepository(Session session, String tableName) {
    this.session = session;
    this.table = tableName;
  }

  /**
   * Constructor.
   *
   * @param session a pre-configured Session instance.
   */
  public AbstractCassandraRepository(Session session) {
    this.session = session;
  }

  /**
   * Gets the database session.
   *
   * @return
   */
  protected Session getSession() {
    return session;
  }

  /**
   * Gets the database table.
   *
   * @return
   */
  protected String getTable() {
    return table;
  }

  /**
   * Binds an identifier object.
   *
   * @param bs
   * @param identifier
   */
  protected void bindIdentifier(BoundStatement bs, Identifier identifier) {
    bs.bind(identifier.components().toArray());
  }

}
