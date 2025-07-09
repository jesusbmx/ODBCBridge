package odbcbridge;

/**
 * Wrapper de nivel alto para ODBCBridge usando AutoCloseable.
 */
public class OdbcConnection implements AutoCloseable {
    private static final ODBCBridge bridge = ODBCBridge.INSTANCE;
    private final long handle;

    public OdbcConnection(long handle) {
        this.handle = handle;
    }
    
    public OdbcConnection(String connectionString) throws Exception {
        this(bridge.connectWithString(connectionString));
    }
    
    /** Lista todos los DSNs disponibles. */
    public static String[] listDatabases() throws Exception {
        return bridge.listDatabases();
    }
  
    /** Lista tablas disponibles */
    public String[] listTables() throws Exception {
        return bridge.listTables(handle);
    }

    /** Lista columnas de tabla */
    public ODBCField[] listColumns(String table) throws Exception {
        return bridge.listColumns(handle, table);
    }
    
    /** Obtiene información de la base de datos. */
    public ODBCInfo getDatabaseInfo() throws Exception {
        return bridge.getDatabaseInfo(handle);
    }

    /** Ejecuta query y devuelve un wrapper AutoCloseable */
    public OdbcResultSet query(String sql) throws Exception {
        long ptr = bridge.query(handle, sql);
        return new OdbcResultSet(bridge, ptr);
    }

    /** Cierra la conexión */
    @Override
    public void close() throws Exception {
        bridge.close(handle);
    }
}
