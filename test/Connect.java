
import odbcbridge.ODBCBridge;
import odbcbridge.ODBCDataSource;


public class Connect {
    
    public static void main(String[] args) throws Exception {
        ODBCDataSource dataSource = new ODBCDataSource()
                .setDsn("BODEGA CD")
                .setUser("sysdba")
                .setPassword("masterkey");
        
        System.out.println(dataSource.buildConnectionString());
        
        ODBCBridge bridge = ODBCBridge.INSTANCE;
        long link = bridge.connect(dataSource);
        try {
            System.out.println("-- Tables --");
            final String[] tables = bridge.listTables(link);
            for (String table : tables) {
                System.out.println(table);
            }
        } finally {
            bridge.close(link);
        }
    }
}
