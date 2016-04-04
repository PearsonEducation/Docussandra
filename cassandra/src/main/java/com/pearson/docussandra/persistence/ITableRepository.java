
package com.pearson.docussandra.persistence;

import com.pearson.docussandra.domain.objects.Index;
import com.pearson.docussandra.domain.objects.IndexIdentifier;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public interface ITableRepository
{

    /**
     * Creates an iTable for the specified index.
     *
     * @param index Index that needs an iTable created for it.
     */
    void createITable(Index index);

    /**
     * Deletes an iTable
     *
     * @param index index whose iTable should be deleted
     */
    void deleteITable(Index index);

    /**
     * Deletes an iTable
     *
     * @param indexId index id whose iTable should be deleted
     */
    void deleteITable(IndexIdentifier indexId);

    /**
     * Deletes an iTable
     *
     * @param tableName iTable getIndexName to delete.
     */
    void deleteITable(String tableName);

    /**
     * Checks to see if an iTable exists for the specified index.
     *
     * @param index Index that you want to check if it has a corresponding
     * iTable.
     * @return True if the iTable exists for the index, false otherwise.
     */
    boolean iTableExists(Index index);

    /**
     * Checks to see if an iTable exists for the specified index.
     *
     * @param indexId Index Id that you want to check if it has a corresponding
     * iTable.
     * @return True if the iTable exists for the index, false otherwise.
     */
    boolean iTableExists(IndexIdentifier indexId);

}
