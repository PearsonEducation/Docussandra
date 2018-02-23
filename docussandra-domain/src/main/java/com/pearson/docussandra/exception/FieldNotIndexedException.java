package com.pearson.docussandra.exception;

import java.util.List;

/**
 * Exception that indicates an attempted query on a field that is not indexed.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class FieldNotIndexedException extends Exception {

  /**
   * Field that an index does not exist for.
   */
  private List<String> fields;

  /**
   * Constructor.
   *
   * @param fields List of fields of which at least one does not exist in a known index.
   */
  public FieldNotIndexedException(List<String> fields) {
    super("One of the following fields: ["
        + com.pearson.docussandra.domain.DomainUtils.join(", ", fields)
        + "] does not exist in any known indices (or may not be yet active). Try adding an index (if you understand the ramifications of this).");
    this.fields = fields;
  }

  /**
   * Fields that at least one of which does not exist an index.
   *
   * @return
   */
  public List<String> getFields() {
    return fields;
  }
}
