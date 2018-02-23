package com.pearson.docussandra.domain.objects;

import java.util.ArrayList;
import java.util.Objects;

/**
 * Object that represents a CQL where clause to the extent that we need to process it.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class WhereClause {

  private static final int FIELD = 0;
  private static final int OPERATOR = 1;
  private static final int VALUE = 2;
  private static final int CONJUNCTION = 3;
  private static final int END = 4;

  private final String whereClause;

  private String boundStatementSyntax = "";

  /**
   * Field Names in a where clause. Corresponds to values.
   */
  private ArrayList<String> fields = new ArrayList<>();
  /**
   * Field Values in a where clause. Corresponds to fields.
   */
  private ArrayList<String> values = new ArrayList<>();

  public WhereClause(String whereClause) {
    this.whereClause = whereClause;
    parse(whereClause);
  }

  // TODO: this will need improvment; handle exceptions
  // https://github.com/PearsonEducation/Docussandra/issues/13
  private void parse(String whereClause) {

    StringBuilder boundStatementBuilder = new StringBuilder();
    // foo = 'fooish' AND bar < 'barish'... [ORDER BY] foo
    String[] tokens = whereClause.split("\\Q \\E");
    int currentOperator = FIELD;
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      switch (currentOperator) {
        case FIELD:
          fields.add(token);
          boundStatementBuilder.append(token);
          currentOperator = OPERATOR;
          break;
        case OPERATOR:
          boundStatementBuilder.append(token);
          currentOperator = VALUE;
          break;
        case VALUE:
          boundStatementBuilder.append("?");
          StringBuilder sb = new StringBuilder();
          if (token.charAt(0) == '\'') {
            token = token.substring(1);// take out the quote
          }
          while (token.charAt(token.length() - 1) != '\'') {
            sb.append(token);
            sb.append(" ");// put the spaces back in
            token = tokens[++i];
          }
          token = token.substring(0, token.length() - 1);// take out the quote
          sb.append(token);// last token
          values.add(sb.toString());
          currentOperator = CONJUNCTION;
          break;
        case CONJUNCTION:
          boundStatementBuilder.append(token);
          currentOperator = FIELD;
          // TODO: we could have CQL injection here; it pretty much just appends the end of the
          // statement; should check specifically
          break;
        default:// this should not run; defaults to conjunction
          boundStatementBuilder.append(token);
          break;
      }
      boundStatementBuilder.append(" ");// tokens need to be re-space seperated
    }
    boundStatementSyntax = boundStatementBuilder.toString().trim();

  }

  public String getBoundStatementSyntax() {
    return boundStatementSyntax;
  }

  /**
   * Field Names in a where clause. Corresponds to values.
   *
   * @return the fields
   */
  public ArrayList<String> getFields() {
    return fields;
  }

  /**
   * Field Values in a where clause. Corresponds to fields.
   *
   * @return the values
   */
  public ArrayList<String> getValues() {
    return values;
  }

  /**
   * @return the original where clause passed in. Use as a reference only.
   */
  public String getWhereClause() {
    return whereClause;
  }

  @Override
  public String toString() {
    return "WhereClause{" + "whereClause=" + whereClause + ", boundStatementSyntax="
        + boundStatementSyntax + ", fields=" + fields + ", values=" + values + '}';
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 23 * hash + Objects.hashCode(this.whereClause);
    hash = 23 * hash + Objects.hashCode(this.boundStatementSyntax);
    hash = 23 * hash + Objects.hashCode(this.fields);
    hash = 23 * hash + Objects.hashCode(this.values);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final WhereClause other = (WhereClause) obj;
    if (!Objects.equals(this.whereClause, other.whereClause)) {
      return false;
    }
    if (!Objects.equals(this.boundStatementSyntax, other.boundStatementSyntax)) {
      return false;
    }
    if (!Objects.equals(this.fields, other.fields)) {
      return false;
    }
    if (!Objects.equals(this.values, other.values)) {
      return false;
    }
    return true;
  }

}
