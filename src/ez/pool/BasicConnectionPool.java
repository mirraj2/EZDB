package ez.pool;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.count;
import static ox.util.Utils.normalize;
import static ox.util.Utils.propagate;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Stopwatch;

import ez.misc.DatabaseType;

import ox.Log;
import ox.x.XList;

public class BasicConnectionPool {

  private final DatabaseType databaseType;
  private Driver driver;
  private String url, user, password;
  private int maxConnections = 4;
  private boolean autoCommit = true;

  private final XList<PooledConnection> allConnections = XList.create();
  private final BlockingQueue<PooledConnection> availableConnections = new LinkedBlockingQueue<>();

  public BasicConnectionPool(DatabaseType databaseType, String url, String user, String password) {
    this.databaseType = checkNotNull(databaseType);
    this.url = checkNotEmpty(normalize(url));
    this.user = checkNotEmpty(normalize(user));
    this.password = normalize(password);
  }

  public BasicConnectionPool initialize() {
    Stopwatch watch = Stopwatch.createStarted();
    checkState(driver == null);

    try {
      if (databaseType == DatabaseType.MYSQL) {
        driver = new com.mysql.cj.jdbc.Driver();
      } else {
        driver = new org.postgresql.Driver();
      }

      Properties properties = new Properties();
      properties.put("user", user);
      if (!password.isEmpty()) {
        properties.put("password", password);
      }

      Log.debug("Seeding pool with %s connections.", maxConnections);
      count(1, maxConnections).concurrentAll().forEach(n -> {
        try {
          Connection connection = driver.connect(url, properties);
          checkState(connection != null, "Failed to get connection for url: " + url);
          connection.setAutoCommit(autoCommit);
          allConnections.add(new PooledConnection(connection, this::onClose));
        } catch (Exception e) {
          throw propagate(e);
        }
      });
      availableConnections.addAll(allConnections);

    } catch (Exception e) {
      throw propagate(e);
    }

    Log.debug("Connection pool initialized in " + watch);

    return this;
  }

  private void onClose(PooledConnection connection) {
    Log.debug("Recycling connection: " + connection);
    availableConnections.add(connection);
  }

  public BasicConnectionPool maxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
    return this;
  }

  public BasicConnectionPool autoCommit(boolean b) {
    this.autoCommit = b;
    return this;
  }

  public synchronized void close() {
    allConnections.forEach(conn -> {
      // TODO rollback transaction if one is in-progress
      try {
        conn.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    });
  }

  public Connection getConnection() {
    try {
      PooledConnection ret = availableConnections.take();
      Log.debug("Giving out connection: " + ret);
      return ret;
    } catch (InterruptedException e) {
      throw propagate(e);
    }
  }

}
