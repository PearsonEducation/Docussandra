package com.pearson.docussandra.domain.objects;

/**
 * Supports the concept of a compound identifier. An Identifier is made up of components, which are
 * Object instances. The components are kept in order of which they are added.
 *
 * For index identification only.
 *
 * @author https://github.com/tfredrich
 * @since Aug 29, 2013
 */
public class IndexIdentifier extends Identifier {

  /**
   * Create an identifier with the given components. Duplicate instances are not added--only one
   * instance of a component will exist in the identifier. Components should be passed in the order
   * of significance: Database -> Table -> Index -> Document
   *
   * @param components
   */
  public IndexIdentifier(Object... components) {
    super(components);
  }

  /**
   * Constructor.
   *
   * Creates an Identifier from another Identifier.
   *
   * @param id
   */
  public IndexIdentifier(Identifier id) {
    super(id);
  }

  /**
   * Gets the name of the index for this Identifier.
   *
   * @return
   */
  public String getIndexName() {
    if (super.size() >= 3) {
      return super.getComponentAsString(2);
    }
    return null;
  }
}
