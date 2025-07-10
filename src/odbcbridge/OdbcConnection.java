package odbcbridge;

/**
 * Wrapper de nivel alto para ODBCBridge usando AutoCloseable.
 */
public class ODBCConnection implements AutoCloseable {
    private static final ODBCBridge nativeBridge = ODBCBridge.INSTANCE;
    private final long handle;

    public ODBCConnection(long handle) {
        this.handle = handle;
    }
    
    public static ODBCConnection connectWithString(String connectionString) throws Exception {
        long connectionPtr = nativeBridge.connectWithString(connectionString);
        try {
            return new ODBCConnection(connectionPtr);
        } catch (Exception e) {
            if (connectionPtr != 0) {
                ODBCBridge.INSTANCE.close(connectionPtr);
            }
            throw e;
        }
    }
    
    public static ODBCConnection connect(String dsn, String uid, String pwd) throws Exception {
        long connectionPtr = nativeBridge.connect(dsn, uid, pwd);
        try {
            return new ODBCConnection(connectionPtr);
        } catch (Exception e) {
            if (connectionPtr != 0) {
                ODBCBridge.INSTANCE.close(connectionPtr);
            }
            throw e;
        }
    }
    
    public static ODBCConnection connect(String dsn) throws Exception {
        long connectionPtr = nativeBridge.connect(dsn);
        try {
            return new ODBCConnection(connectionPtr);
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
    public ODBCResultSet query(String sql, Object[] params) throws Exception {
        long ptr = -1;
        try  {
            ptr = nativeBridge.query(handle, sql, params);
            return new ODBCResultSet(nativeBridge, ptr);
        } catch (Exception e) {
            if (ptr != -1) nativeBridge.free(ptr);
            throw e;
        }
    }
    
    public ODBCResultSet query(String sql) throws Exception {
        return query(sql, null);
    }
    
    /**
     * Ejecuta una sentencia DML/DDL (INSERT, UPDATE, DELETE, CREATE, etc.)
     * y devuelve el número de filas afectadas.
     *
     * @param connectionPtr Puntero a la conexión JNI
     * @param sql           Sentencia SQL a ejecutar
     * @param params        Parámetros opcionales (en orden), o null si no hay
     * @return número de filas afectadas, o 0 si no se puede determinar
     * @throws Exception si ocurre algún error ODBC/JNI
     */
    public int execute(long connectionPtr, String sql, Object[] params) throws Exception {
        return nativeBridge.execute(handle, sql, params);
    }

    /**
     * Sobrecarga sin parámetros.
     */
    public int execute(long connectionPtr, String sql) throws Exception {
        return execute(connectionPtr, sql, null);
    }

    /** Cierra la conexión */
    @Override
    public void close() throws Exception {
        nativeBridge.close(handle);
    }
}
