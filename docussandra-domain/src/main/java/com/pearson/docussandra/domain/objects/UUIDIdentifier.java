package com.pearson.docussandra.domain.objects;

import java.util.UUID;

/**
 * Identifier for just using UUIDs.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class UUIDIdentifier extends Identifier {

  /**
   * Constructor.
   *
   * @param id
   */
  public UUIDIdentifier(UUID id) {
    super.add(id);
  }

  /*
   * Constructor.
   * 
   * @param id
   */
  public UUIDIdentifier(UUIDIdentifier id) {
    super(id);
  }

  /**
   * Do not call. Not valid for this class. Will throw UnsupportedOperationException.
   *
   * @return
   */
  @Override
  public String getDatabaseName() {
    throw new UnsupportedOperationException(
        "Not a valid call for this class. UUID Identifiers do not have an associated database.");
  }

  /**
   * Do not call. Not valid for this class. Will throw UnsupportedOperationException.
   *
   * @return
   */
  @Override
  public String getTableName() {
    throw new UnsupportedOperationException(
        "Not a valid call for this class. UUID Identifiers do not have an associated table.");
  }

  /**
   * Do not call. Not valid for this class. Will throw UnsupportedOperationException.
   *
   * @return
   */
  @Override
  public Table getTable() {
    throw new UnsupportedOperationException(
        "Not a valid call for this class. UUID Identifiers do not have an associated table.");
  }

  /**
   * Gets the UUID associated with this identifier.
   *
   * @return
   */
  public UUID getUUID() {
    return (UUID) super.components().get(0);
  }
}
