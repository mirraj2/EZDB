EZDB
====

A library that allows for easy read/writing to a database.

Getting started is this easy:

//connect to the database
Db db = new DB("localhost", "username", "password", "schema");

//add a new table
db.addTable(new Table("hero").idColumn().column("name", String.class).column("level", Integer.class).column("alive", Boolean.class);

//insert a row
db.insert("hero", new Row().with("name", "Sabriel").with("level", 19).with("alive", true));

//checks to see if the given hero is alive
public boolean isAlive(String heroName){
  Row row = db.selectSingleRow("SELECT alive FROM hero WHERE name = ?", heroName);
  return row.getBoolean("alive");
}
