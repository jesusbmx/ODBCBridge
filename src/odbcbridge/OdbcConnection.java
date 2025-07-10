package odbcbridge;

/**
 * Wrapper de nivel alto para ODBCBridge usando AutoCloseable.
 */
public class OdbcConnection implements AutoCloseable {
    private static final ODBCBridge nativeBridge = ODBCBridge.INSTANCE;
    private final long handle;

    public OdbcConnection(long handle) {
        this.handle = handle;
    }
    
    public static OdbcConnection connectWithString(String connectionString) throws Exception {
        long connectionPtr = nativeBridge.connectWithString(connectionString);
        try {
            return new OdbcConnection(connectionPtr);
        } catch (Exception e) {
            if (connectionPtr != 0) {
                ODBCBridge.INSTANCE.close(connectionPtr);
            }
            throw e;
        }
    }
    
    public static OdbcConnection connect(String dsn, String uid, String pwd) throws Exception {
        long connectionPtr = nativeBridge.connect(dsn, uid, pwd);
        try {
            return new OdbcConnection(connectionPtr);
        } catch (Exception e) {
            if (connectionPtr != 0) {
                ODBCBridge.INSTANCE.close(connectionPtr);
            }
            throw e;
        }
    }
    
    public static OdbcConnection connect(String dsn) throws Exception {
        long connectionPtr = nativeBridge.connect(dsn);
        try {
            return new OdbcConnection(connectionPtr);
        } catch (Exception e) {
            if (connectionPtr != 0) {
                ODBCBridge.INSTANCE.close(connectionPtr);
            }
            throw e;
        }
    }
  
    /** Lista tablas disponibles */
    public String[] listTables() throws Exception {
        return nativeBridge.listTables(handle);
    }

    /** Lista columnas de tabla */
    public ODBCField[] listColumns(String table) throws Exception {
        return nativeBridge.listColumns(handle, table);
    }
    
    /** Obtiene información de la base de datos. */
    public ODBCInfo getDatabaseInfo() throws Exception {
        return nativeBridge.getDatabaseInfo(handle);
    }

    /** Ejecuta query y devuelve un wrapper AutoCloseable */
    public OdbcResultSet query(String sql) throws Exception {
        long ptr = nativeBridge.query(handle, sql);
        return new OdbcResultSet(nativeBridge, ptr);
    }

    /** Cierra la conexión */
    @Override
    public void close() throws Exception {
        nativeBridge.close(handle);
    }
}
