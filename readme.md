
```java
final ODBCBridge bridge = ODBCBridge.INSTANCE;
        
System.out.println("-- Databases --");
String[] databases = bridge.listDatabases();
for (String database : databases) {
   System.out.println("Database: " + database);
}

final String dsn = "PostgreSQL30";
final long link = bridge.connect(dsn);
try {
   System.out.println("-- Tables --");
   String[] tables = bridge.listTables(link);
   for (String table : tables) {
         System.out.println("Table: " + table);
   }
   
   System.out.println("-- Query --");
   final String sql = "SELECT * FROM \"Product\" LIMIT 100";
   final long result = bridge.query(link, sql);
   try {
         Map<String, String> row;
         while ((row = bridge.fetchAssoc(result)) != null) {
            System.out.println(row);
         }
   } finally {
         bridge.free(result);
   }
   
} finally {
   bridge.close(link);
}
```
