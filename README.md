# EZDB

Provides an easy-to-use API for accessing your database.

Getting started is this easy:

```java
//connect to the database
Db db = new DB("localhost", "username", "password", "schema");

//add a new table
db.addTable(new Table("hero")
  .idColumn()
  .column("name", String.class)
  .column("level", Integer.class)
  .column("alive", Boolean.class);

//insert a row
db.insert("hero",
  new Row()
  .with("name", "Sabriel")
  .with("level", 19)
  .with("alive", true));

//checks to see if the given hero is alive
public boolean isAlive(String heroName){
  Row row = db.selectSingleRow("SELECT alive FROM hero WHERE name = ?", heroName);
  return row.getBoolean("alive");
}
```

EZDB relies on our favorite Java utilities library called Ox. You can find it here: https://github.com/mirraj2/ox
