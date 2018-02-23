
package com.pearson.docussandra.persistence;

import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Identifier;
import java.util.List;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface DatabaseRepository {

  Database create(Database entity);

  void delete(Database entity);

  void delete(Identifier identifier);

  boolean exists(Identifier identifier);

  Database read(Identifier identifier);

  List<Database> readAll();

  List<Database> readAll(Identifier id);

  Database update(Database entity);

}
