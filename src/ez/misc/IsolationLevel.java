package ez.misc;

public enum IsolationLevel {
  READ_UNCOMMITTED(1),
  READ_COMMITTED(2),
  REPEATABLE_READ(4),
  SERIALIZABLE(8);

  public final int level;

  private IsolationLevel(int level) {
    this.level = level;
  }
}