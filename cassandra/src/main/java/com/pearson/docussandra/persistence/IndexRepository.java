
package com.pearson.docussandra.persistence;

import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Index;
import java.util.List;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface IndexRepository {

  long countAll(Identifier id);

  Index create(Index entity);

  void delete(Identifier id);

  void delete(Index entity);

  boolean exists(Identifier identifier);

  /**
   * Marks an index as "active" meaning that indexing has completed on it.
   *
   * @param entity Index to mark active.
   */
  void markActive(Index entity);

  Index read(Identifier identifier);

  List<Index> readAll(Identifier id);

  List<Index> readAll();

  /**
   * Same as readAll, but will read from the cache if available.
   *
   * @return
   */
  List<Index> readAllCached(Identifier id);

  Index update(Index entity);

}
