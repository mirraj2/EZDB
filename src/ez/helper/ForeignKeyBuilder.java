package ez.helper;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;

public class ForeignKeyBuilder {

  protected String constraintName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  protected ForeignKeyBuilder(String sourceTable) {
    this.sourceTable = sourceTable;
  }

  public ForeignKeyBuilder constraintName(String foreignKeyName) {
    this.constraintName = foreignKeyName;
    return this;
  }

  public ForeignKeyBuilder sourceColumn(String sourceColumn) {
    this.sourceColumnName = sourceColumn;
    return this;
  }

  public ForeignKeyBuilder foreignTable(String foreignTable) {
    this.foreignTable = foreignTable;
    return this;
  }

  public ForeignKeyBuilder foreignColumn(String foreignColumn) {
    this.foreignColumnName = foreignColumn;
    return this;
  }

  public ForeignKeyConstraint build() {
    if (Strings.isNullOrEmpty(constraintName)) {
      return new ForeignKeyConstraint(sourceTable, sourceColumnName, foreignTable, foreignColumnName);
    } else {
      checkState(constraintName.length() < 64, "The constraint name is too longer than 64 characters");
      return new ForeignKeyConstraint(sourceTable, sourceColumnName, foreignTable, foreignColumnName, constraintName);
    }
  }

  public static ForeignKeyConstraint create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    return createForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName).build();
  }

  public static ForeignKeyConstraint create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName, String foreignKeyName) {
    return createForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName)
        .constraintName(foreignKeyName).build();
  }

  private static ForeignKeyBuilder createForeignKeyBuilder(String sourceTable, String sourceColumnName,
      String foreignTable,
      String foreignColumnName) {
    return new ForeignKeyBuilder(sourceTable).sourceColumn(sourceColumnName).foreignTable(foreignTable)
        .foreignColumn(foreignColumnName);
  }

  public static ForeignKeyBuilder table(String sourceTable) {
    return new ForeignKeyBuilder(sourceTable);
  }

}
