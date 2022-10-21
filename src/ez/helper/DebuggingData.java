package ez.helper;

import static ox.util.Utils.abbreviate;
import static ox.util.Utils.f;
import static ox.util.Utils.normalize;

import java.time.Duration;

import com.google.common.collect.ComparisonChain;

import ox.Log;
import ox.x.XList;
import ox.x.XMultimap;

public class DebuggingData {

  private final XList<Query> queries = XList.create();

  public void store(String sqlQuery, XList<Object> args, Duration elapsed) {
    queries.add(new Query(sqlQuery, args, elapsed));
  }

  public void print() {
    if (queries.isEmpty()) {
      return;
    }

    XMultimap<String, Query> groupedQueries = queries.indexMultimap(q -> q.sqlQuery);

    StringBuilder sb = new StringBuilder("\n\n######## DebuggingData (" + queries.size() + " queries) ########\n");

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
          if (numDuplicates > 1) {
            sb.append(f("{0}x ({1} duplicates) of: {2}\n", queries.size(), numDuplicates, abbreviate(sqlQuery, 100)));
          } else {
            sb.append(f("{0}x of: {1}\n", queries.size(), abbreviate(sqlQuery, 100)));
          }
        });

    sb.append("#################################\n\n");
    if (queries.size() > 1000) {
      sb.append("Summary: " + queries.size() + " queries.\n\n");
    }
    Log.debug(sb);
  }

  private int countDuplicates(XList<Query> queries) {
    return queries.toSet(q -> q.args).size();
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
  }

}
