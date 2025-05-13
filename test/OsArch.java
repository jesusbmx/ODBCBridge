
import odbcbridge.ODBCBridge;

public class OsArch {
    
    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("os.arch"));
        
        final ODBCBridge bridge = ODBCBridge.INSTANCE;
        
        System.out.println("-- Databases --");
        String[] databases = bridge.listDatabases();
        for (String database : databases) {
            System.out.println("Database: " + database);
        }
    }
}
