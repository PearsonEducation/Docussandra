package com.pearson.docussandra.service;

import com.pearson.docussandra.cache.CacheFactory;
import com.pearson.docussandra.domain.objects.Document;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.exception.IndexParseException;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.DocumentRepository;
import com.pearson.docussandra.persistence.TableRepository;
import com.pearson.docussandra.plugininterfaces.NotifierPlugin;
import com.strategicgains.syntaxe.ValidationEngine;
import java.util.ArrayList;
import java.util.UUID;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

/**
 * Service for CRUD operations on documents.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DocumentService {

  /**
   * Table repository for interacting with Tables.
   */
  private TableRepository tableRepo;
  /**
   * Document repository for interacting with Documents.
   */
  private DocumentRepository docRepo;

  /**
   * Cache of our tables so we can avoid hitting the DB as frequently.
   */
  private final Cache tableCache = CacheFactory.getCache("tableExist");

  /**
   * Plugins that will be notified upon any document mutation.
   */
  private ArrayList<NotifierPlugin> plugins;

  /**
   * Constructor.
   *
   * @param databaseRepository Database repository to use.
   * @param documentRepository Document repository to use.
   * @param plugins List of plugins that should be notified of any document creation or mutation.
   */
  public DocumentService(TableRepository databaseRepository, DocumentRepository documentRepository,
      ArrayList<NotifierPlugin> plugins) {
    super();
    this.docRepo = documentRepository;
    this.tableRepo = databaseRepository;
    this.plugins = plugins;
  }

  /**
   * Creates a Document, ensuring that all business rules are met.
   *
   * @param database Database to insert the document into.
   * @param table Table to insert the document into.
   * @param json String of the JSON to insert.
   * @return The created document.
   * @throws IndexParseException If the document contains a field that should be indexable, but
   *         isn't for some reason (probably the wrong datatype).
   */
  public Document create(String database, String table, String json) throws IndexParseException {
    verifyTable(database, table);

    Document doc = new Document();
    doc.setTable(database, table);
    doc.setObjectAsString(json);
    doc.setUuid(UUID.randomUUID());// TODO: is this right? --
                                   // https://github.com/PearsonEducation/Docussandra/issues/4
    ValidationEngine.validateAndThrow(doc);
    try {
      Document created = docRepo.create(doc);
      notifyAllPlugins(NotifierPlugin.MutateType.CREATE, created);
      return created;
    } catch (RuntimeException e)// the framework does not allow us to throw the IndexParseException
                                // directly from the repository layer
    {
      if (e.getCause() != null && e.getCause() instanceof IndexParseException) {
        throw (IndexParseException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Reads a document out of the database.
   *
   * @param database The database to read from.
   * @param table The table to read from.
   * @param id The identifier of the document you are trying to read.
   * @return The document you requested.
   */
  public Document read(String database, String table, Identifier id) {
    verifyTable(database, table);
    return docRepo.read(id);
  }

  // public List<Document> readAll(String database, String table)
  // {
  // verifyTable(database, table);
  // return docs.readAll(database, table);
  // }
  //
  // public long countAll(String database, String table)
  // {
  // return docs.countAll(database, table);
  // }
  /**
   * Updates a document.
   *
   * @param entity The document you are trying to update with the changes you are trying to apply.
   */
  public void update(Document entity) {
    ValidationEngine.validateAndThrow(entity);
    docRepo.update(entity);
    notifyAllPlugins(NotifierPlugin.MutateType.UPDATE, entity);
  }

  /**
   * Deletes a document.
   * 
   * @param database Database in which the document you are trying to delete resides.
   * @param table Table in which the document you are trying to delete resides.
   * @param id Id of the document you are trying to delete.
   */
  public void delete(String database, String table, Identifier id) {
    verifyTable(database, table);
    docRepo.delete(id);
    notifyAllPlugins(NotifierPlugin.MutateType.DELETE, null);
  }

  /**
   * Verifies a table exists. Throws an ItemNotFoundException if it does not exist; does nothing if
   * it exists.
   * 
   * @param database Database in which you are checking for the table in.
   * @param table Table you are checking for existance.
   * @throws ItemNotFoundException If the table does not exist.
   */
  private void verifyTable(String database, String table) throws ItemNotFoundException {
    String key = database + table;
    Identifier tableId = new Identifier(database, table);
    // synchronized (CacheSynchronizer.getLockingObject(key, "tableExist"))
    // {
    Element e = tableCache.get(key);
    if (e == null || e.getObjectValue() == null)// if its not set, or set, but null, re-read
    {
      // not cached; let's read it
      e = new Element(key, (Boolean) tableRepo.exists(tableId));
    }
    if (!(Boolean) e.getObjectValue()) {
      throw new ItemNotFoundException("Table not found: " + tableId.toString());
    }
  }

  /**
   * Method to notify all the Notifier plugins that a specific mutation has occurred.
   *
   * @param type Type of mutation.
   * @param document Document in it's present post-mutation state.
   */
  private void notifyAllPlugins(NotifierPlugin.MutateType type, Document document) {
    for (NotifierPlugin plugin : plugins) {
      plugin.doNotify(type, document);
    }
  }
}
