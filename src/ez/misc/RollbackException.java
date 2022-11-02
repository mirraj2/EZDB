package ez.misc;

public interface RollbackException {

  /**
   * Gets whether this Exception should trigger a rollback.
   */
  public default boolean shouldRollback() {
    return true;
  }

}