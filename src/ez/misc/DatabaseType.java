package ez.misc;

public enum DatabaseType {
  MYSQL('`'), POSTGRES('"');

  private final char escapeChar;

  private DatabaseType(char escapeChar) {
    this.escapeChar = escapeChar;

  }

  public String escape(String s) {
    return escapeChar + s + escapeChar;
  }
}