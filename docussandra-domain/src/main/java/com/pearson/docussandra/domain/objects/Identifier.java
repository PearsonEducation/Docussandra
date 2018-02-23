package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.DomainUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Supports the concept of a compound identifier. An Identifier is made up of components, which are
 * Object instances. The components are kept in order of which they are added.
 *
 * @author https://github.com/tfredrich
 * @since Aug 29, 2013
 */
public class Identifier implements Comparable<Identifier> {

  private static final String SEPARATOR = ", ";

  private List<Object> components = new ArrayList<>();

  /**
   * Create an identifier with the given components. Duplicate instances are not added--only one
   * instance of a component will exist in the identifier. Components should be passed in the order
   * of significance: Database -> Table -> Index -> Document
   *
   * @param components
   */
  public Identifier(Object... components) {
    super();
    add(components);
  }

  /**
   * Creates an Identifier from another Identifier.
   *
   * @param id
   */
  public Identifier(Identifier id) {
    this.components = id.components();
  }

  /**
   * Add the given components, in order, to the identifier. Duplicate instances are not added--only
   * one instance of a component will exist in the identifier.
   *
   * @param components
   */
  protected void add(Object... components) {
    if (components == null) {
      return;
    }

    for (Object component : components) {
      add(component);
    }
  }

  /**
   * Add a single component to the identifier. The given component is added to the end of the
   * identifier. Duplicate instances are not added--only one instance of a component will exist in
   * the identifier.
   *
   * @param component
   */
  protected void add(Object component) {
    if (component == null) {
      return;
    }

    components.add(component);
  }

  /**
   * Get an unmodifiable list of the components that make up this identifier.
   *
   * @return an unmodifiable list of components.
   */
  public List<Object> components() {
    return Collections.unmodifiableList(components);
  }

  /**
   * Gets the database name for this Identifier
   *
   * @return
   */
  public String getDatabaseName() {
    if (size() >= 1) {
      return getComponentAsString(0);
    }
    return null;
  }

  /**
   * Gets the table name for this Identifier.
   *
   * @return
   */
  public String getTableName() {
    if (size() >= 2) {
      return getComponentAsString(1);
    }
    return null;
  }

  /**
   * Gets the Table object for this Identifier.
   *
   * @return
   */
  public Table getTable() {
    Table t = new Table();
    t.setDatabaseByObject(new Database(getDatabaseName()));
    t.setName(getTableName());
    return t;
  }

  /**
   * Get an item out of this identifier.
   *
   * @param index Index of the component to fetch.
   *
   * @return an unmodifiable list of components.
   */
  protected String getComponentAsString(int index) {
    return components.get(index).toString();
  }

  /**
   * Get an item out of this identifier.
   *
   * @param index Index of the component to fetch.
   *
   * @return an unmodifiable list of components.
   */
  protected Object getComponent(int index) {
    return components.get(index);
  }

  /**
   * Iterate the components of this identifier. Modifications to the underlying components are not
   * possible via this iterator.
   *
   * @return an iterator over the components of this identifier
   */
  public Iterator<Object> iterator() {
    return components().iterator();
  }

  /**
   * Indicates the number of components making up this identifier.
   *
   * @return the number of components in this identifier.
   */
  public int size() {
    return components.size();
  }

  /**
   * Check for equality between identifiers. Returns true if the identifiers contain equal
   * components. Otherwise, returns false.
   *
   * @return true if the identifiers are equivalent.
   */
  @Override
  public boolean equals(Object that) {
    return (compareTo((Identifier) that) == 0);
  }

  /**
   * Returns a hash code for this identifier.
   *
   * @return an integer hashcode
   */
  @Override
  public int hashCode() {
    return 1 + components.hashCode();
  }

  /**
   * Compares this identifier to another, returning -1, 0, or 1 depending on whether this identifier
   * is less-than, equal-to, or greater-than the other identifier, respectively.
   *
   * @return -1, 0, 1 to indicate less-than, equal-to, or greater-than, respectively.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public int compareTo(Identifier that) {
    if (that == null) {
      return 1;
    }
    if (this.size() < that.size()) {
      return -1;
    }
    if (this.size() > that.size()) {
      return 1;
    }

    int i = 0;
    int result = 0;

    while (result == 0 && i < size()) {
      Object cThis = this.components.get(i);
      Object cThat = that.components.get(i);

      if (Identifier.areComparable(cThis, cThat)) {
        result = ((Comparable) cThis).compareTo(((Comparable) cThat));
      } else {
        result = (cThis.toString().compareTo(cThat.toString()));
      }

      ++i;
    }

    return result;
  }

  /**
   * Returns a string representation of this identifier.
   *
   * @return a string representation of the identifier.
   */
  @Override
  public String toString() {
    if (components.isEmpty()) {
      return "";
    }
    return "(" + DomainUtils.join(SEPARATOR, components) + ")";
  }

  // /**
  // * Returns the first component of the identifier. Return null if the
  // * identifier is empty. Equivalent to components().get(0).
  // *
  // * @return the first component or null.
  // */
  // @Deprecated
  // public Object primaryKey()
  // {
  // return (isEmpty() ? null : components.get(0));
  // }
  /**
   * Return true if the identifier has no components.
   *
   * @return true if the identifier is empty.
   */
  public boolean isEmpty() {
    return components.isEmpty();
  }

  // stolen from the restexpress object utils
  /**
   * Determines is two objects are comparable to each other, in that they implement Comparable and
   * are of the same type. If either object is null, returns false.
   *
   * @param o1 an instance
   * @param o2 an instance
   * @return true if the instances can be compared to each other.
   */
  public static boolean areComparable(Object o1, Object o2) {
    if (o1 == null || o2 == null) {
      return false;
    }

    if ((isComparable(o1) && isComparable(o2)) && (o1.getClass().isAssignableFrom(o2.getClass())
        || o2.getClass().isAssignableFrom(o1.getClass()))) {
      return true;
    }

    return false;
  }

  /**
   * Returns true if the object implements Comparable. Otherwise, false.
   *
   * @param object an instance
   * @return true if the instance implements Comparable.
   */
  public static boolean isComparable(Object object) {
    return (object instanceof Comparable);
  }
}
