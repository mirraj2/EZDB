package ez.helper;

import static ox.util.Utils.abbreviate;
import static ox.util.Utils.format;
import static ox.util.Utils.normalize;

import java.time.Duration;

import com.google.common.collect.ComparisonChain;

import ox.Log;
import ox.util.Regex;
import ox.x.XCollection;
import ox.x.XList;
import ox.x.XMultimap;

public class DebuggingData {

  public static int MAX_LENGTH = 128;

  private final XList<Query> queries = XList.create();

  public void store(String sqlQuery, XList<Object> args, Duration elapsed) {
    queries.add(new Query(sqlQuery, args, elapsed));
  }

  public void print() {
    if (queries.isEmpty()) {
      return;
    }

    XMultimap<String, Query> groupedQueries = queries.indexMultimap(q -> q.sqlQuery);
    Duration totalQueryTime = getTotal(queries);

    StringBuilder sb = new StringBuilder(
        format("\n\n######## DebuggingData ({0} queries) ({1} ms elapsed) ########\n", queries.size(),
            totalQueryTime.toMillis()));

    groupedQueries.keySet().toList()
        .sortSelf((a, b) -> {
          return ComparisonChain.start()
              .compare(groupedQueries.get(b).size(), groupedQueries.get(a).size())
              .compare(a, b)
              .result();
        })
        .forEach(sqlQuery -> {
          XList<Query> queries = groupedQueries.get(sqlQuery);

          int numDuplicates = countDuplicates(queries);
          long millis = getTotal(queries).toMillis();

          sb.append(queries.size()).append("x ");
          if (numDuplicates > 1) {
            sb.append("(").append(numDuplicates).append(") ");
          }
          if (millis >= 100) {
            sb.append("(").append(millis).append(" ms) ");
          }
          sb.append("of: ").append(getSQLQuery(sqlQuery)).append('\n');
        });

    sb.append("#################################\n\n");
    if (queries.size() > 1000) {
      sb.append("Summary: " + queries.size() + " queries.\n\n");
    }
    Log.debug(sb);
  }

  private String getSQLQuery(String s) {
    s = Regex.replaceAll("/\\*.*?\\*/", s, match -> "");
    return abbreviate(s, MAX_LENGTH);
  }

  private Duration getTotal(XCollection<Query> queries) {
    return queries.map(q -> q.elapsed == null ? Duration.ZERO : q.elapsed).reduce(Duration.ofNanos(0),
        (a, b) -> a.plus(b));
  }

  private int countDuplicates(XList<Query> queries) {
    int uniques = queries.toSet(q -> q.args).size();
    return queries.size() - uniques;
  }

  private static class Query {
    public final String sqlQuery;
    public final XList<Object> args;
    public final Duration elapsed;

    public Query(String sqlQuery, XList<Object> args, Duration elapsed) {
      this.sqlQuery = normalize(sqlQuery);
      this.args = args;
      this.elapsed = elapsed;
    }

    @Override
    public String toString() {
      return args.toString();
    }
  }

}
