
package com.pearson.docussandra.persistence.parent;

import com.datastax.driver.core.Session;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.parent.Identifiable;

import java.util.List;

/**
 * Abstract class for our CRUD repositories. Mainly just to enforce naming conventions for right
 * now.
 *
 * @author https://github.com/JeffreyDeYoung
 * @param <T> Object type for this repo.
 */
public abstract class AbstractCRUDRepository<T extends Identifiable>
    extends AbstractCassandraRepository {

  /**
   * @param session a pre-configured Session instance.
   * @param tableName the name of the Cassandra table entities are stored in.
   */
  public AbstractCRUDRepository(Session session, String tableName) {
    super(session, tableName);
  }

  /**
   * @param session a pre-configured Session instance.
   */
  public AbstractCRUDRepository(Session session) {
    super(session);
  }

  public abstract T create(T entity);

  public abstract T update(T entity);

  public List<T> readAll() {
    throw new UnsupportedOperationException("Not valid for this class.");
  }

  public List<T> readAll(Identifier id) {
    throw new UnsupportedOperationException("Not valid for this class.");
  }

  public abstract boolean exists(Identifier id);

  public abstract T read(Identifier id);

  public abstract void delete(T entity);

  public abstract void delete(Identifier id);
}
