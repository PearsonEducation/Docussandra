package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.Constants;
import com.pearson.docussandra.domain.parent.Identifiable;
import com.pearson.docussandra.domain.parent.Timestamped;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.StringValidation;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;
import org.restexpress.plugin.hyperexpress.Linkable;

/**
 * Database domain object. Represents a database in Docussandra, but does not contain the actual
 * data inside of the database.
 *
 * @author Jeffrey DeYoung
 */
@ApiModel(value = "Database",
    description = "Model that defines a database including its name and description")
public class Database extends Timestamped implements Linkable, Identifiable {

  @JsonProperty
  @ApiModelProperty(value = "The database's name", notes = "This is the database's name",
      dataType = "String", required = true)
  @RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN,
      message = Constants.NAME_MESSAGE)
  @StringValidation(maxLength = 32)
  private String name;

  @JsonProperty
  @ApiModelProperty(value = "The database's description", notes = "Optional description.",
      dataType = "String", required = false)
  @StringValidation(maxLength = 1024)
  private String description;

  // TODO: add consistency & distro metadata here.
  public Database() {
    super();
  }

  /**
   * Constructor.
   *
   * @param name Name of the database.
   */
  public Database(String name) {
    this();
    setName(name);
  }

  /**
   * Gets the name of this database.
   *
   * @return
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the name of this database.
   *
   * @param name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets the Identifier of this DB.
   *
   * @return
   */
  @Override
  public Identifier getId() {
    return new Identifier(name);
  }

  /**
   * Returns true if there is a description for this DB.
   *
   * @return
   */
  public boolean hasDescription() {
    return (description != null);
  }

  /**
   * Gets the description for this Database.
   *
   * @return
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets the description for this database.
   *
   * @param description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets a string representation of this object.
   *
   * @return
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(name);

    if (hasDescription()) {
      sb.append(" (");
      sb.append(description);
      sb.append(")");
    }
    return sb.toString();
  }

  /**
   * Gets the hash code for this object.
   *
   * @return
   */
  @Override
  public int hashCode() {
    int hash = 5;
    hash = 59 * hash + Objects.hashCode(this.name);
    hash = 59 * hash + Objects.hashCode(this.description);
    return hash;
  }

  /**
   * Determines if this object is equal to another object.
   *
   * @param obj
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Database other = (Database) obj;
    if (!Objects.equals(this.name, other.name)) {
      return false;
    }
    if (!Objects.equals(this.description, other.description)) {
      return false;
    }
    return true;
  }

}
