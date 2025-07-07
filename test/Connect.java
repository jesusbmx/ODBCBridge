
import odbcbridge.ODBCBridge;


public class Connect {
    
    public static void main(String[] args) throws Exception {
        ODBCBridge bridge = ODBCBridge.INSTANCE;
        long link = bridge.connect("BODEGA CD", "sysdba", "masterkey");
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
