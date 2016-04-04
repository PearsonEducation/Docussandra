
package com.pearson.docussandra.persistence;

import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.QueryResponseWrapper;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface DocumentRepository
{

    Document create(Document entity);

    void delete(Document entity);

    void delete(Identifier id);

    boolean exists(Identifier identifier);

    Document read(Identifier identifier);

    QueryResponseWrapper readAll(String database, String tableString, int limit, long offset);

    Document update(Document entity);

}
