package ez.helper;

public class ForeignKeyBuilder {

  protected String foreigKeyName, sourceTable, sourceColumnName, foreignTable, foreignColumnName;

  protected ForeignKeyBuilder(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    this.sourceTable = sourceTable;
    this.sourceColumnName = sourceColumnName;
    this.foreignTable = foreignTable;
    this.foreignColumnName = foreignColumnName;
  }

  public ForeignKeyBuilder name(String foreignKeyName) {
    this.foreigKeyName = foreignKeyName;
    return this;
  }

  public static ForeignKeyBuilder create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName) {
    return new ForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName);
  }

  public static ForeignKeyBuilder create(String sourceTable, String sourceColumnName, String foreignTable,
      String foreignColumnName, String foreignKeyName) {
    return new ForeignKeyBuilder(sourceTable, sourceColumnName, foreignTable, foreignColumnName).name(foreignKeyName);
  }

}
