
import odbcbridge.ODBCDataSource;
import odbcbridge.OdbcConnection;

public class Connect {
    
    public static void main(String[] args) throws Exception {
        ODBCDataSource dataSource = new ODBCDataSource()
                .setDsn("Postgre32");
        
        System.out.println(dataSource.buildConnectionString());
        
        try (OdbcConnection connection = dataSource.getConnection()) {
            System.out.println("-- Tables --");
            final String[] tables = connection.listTables();
            for (String table : tables) {
                System.out.println(table);
            }
        }
      
    }
}
