
import odbcbridge.ODBCDataSource;
import odbcbridge.ODBCField;
import odbcbridge.ODBCInfo;
import odbcbridge.ODBCConnection;
import odbcbridge.ODBCResultSet;

public class ODBCTest {
    
    public static void main(String[] args) throws Exception {
        System.out.println("-- Databases --");
        String[] databases = ODBCDataSource.listDatabases();
        for (String database : databases) {
            System.out.println("Database: " + database);
        }
        
        final ODBCDataSource dataSource = new ODBCDataSource()
                .setDsn("Postgre32");
        
        try (ODBCConnection connection = dataSource.getConnection()) {
            System.out.println("-- Info --");
            ODBCInfo info = connection.getDatabaseInfo();
            System.out.println(info);
            
            System.out.println("-- Tables --");
            String[] tables = connection.listTables();
            for (String table : tables) {
                System.out.println("Table: " + table);
            }
            
            System.out.println("-- Query --");
            final String sql = "SELECT * FROM \"Product\" LIMIT 1";
            System.out.println(sql);
            try (ODBCResultSet resultSet = connection.query(sql)) {
                
                ODBCField[] fields = resultSet.getFields();
                for (int col = 0; col < fields.length; col++) {
                    if (col > 0) System.out.print(",");
                    System.out.print(fields[col].name);
                }
                System.out.println("");
                    
                while (resultSet.next()) {
                    for (int col = 1; col <= resultSet.getColumnCount(); col++) {
                        if (col > 1) System.out.print(",");
                        System.out.print(resultSet.get(col));
                    }
                    System.out.println("");
                }
            } 
        } 
    }
}
