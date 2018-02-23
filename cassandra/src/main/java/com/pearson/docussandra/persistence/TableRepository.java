
package com.pearson.docussandra.persistence;

import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Table;
import java.util.List;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface TableRepository {

  long countAllTables(String database);

  long countTableSize(String database, String tableName);

  Table create(Table entity);

  void delete(Table entity);

  void delete(Identifier id);

  boolean exists(Identifier identifier);

  Table read(Identifier identifier);

  List<Table> readAll(Identifier id);

  List<Table> readAll();

  Table update(Table entity);

}
