// ODBCBridge.java
package odbcbridge;

/**
 * Clase ODBCBridge para establecer una interfaz Java con ODBC mediante JNI.
 * Esta clase proporciona métodos nativos para listar tablas y ejecutar consultas SQL en una fuente de datos ODBC.
 * 
 * # Generar archivo .h con todos los métodos nativos incluidos
 * - javac -h . ODBCBridge.java
 * 
 * # Instrucciones de instalación y compilación
 * 
 * 1.  Visual Studio 2019 Developer Command Prompt v16.11.39
 * 
 * 2. Compilar DLL para 64 bits en terminal Visual Studio 2019 Developer Command Prompt:
 *    # Abre la terminal:
 *    - "C:\Program Files\Microsoft Visual Studio\2019\Community\Common7\Tools\VsDevCmd.bat"
 * 
 *    # Genera DLL 64:
 *    - cl /I "%JAVA_HOME%\include" /I "%JAVA_HOME%\include\win32" /LD /Fe:odbc_bridge_win64.dll src/odbcbridge/ODBCBridge.c odbc32.lib odbccp32.lib
 * 
 * 3. Compilar DLL para 32 bits en terminal Visual Studio 2019 Developer Command Prompt:
 *    # Abre la terminal:
 *    - "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\Common7\Tools\VsDevCmd.bat" -arch=x86
 * 
 *    # Genera DLL 32:
 *    - cl /I "%JAVA_HOME%\include" /I "%JAVA_HOME%\include\win32" /LD /Fe:odbc_bridge_win32.dll src/odbcbridge/ODBCBridge.c odbc32.lib odbccp32.lib
 */
public class ODBCBridge {
    
    public static final ODBCBridge INSTANCE = new ODBCBridge();
    
    /**
     * Bloque de inicialización estática.
     * Detecta automáticamente el sistema operativo y arquitectura para
     * cargar la biblioteca nativa apropiada.
     */
    static {
        try {
            // Detectar arquitectura del sistema y cargar la biblioteca adecuada
            String osName = System.getProperty("os.name").toLowerCase();
            String libName = "odbc_bridge";

            if (osName.contains("win")) {
                String arch = System.getProperty("os.arch").contains("64") ? "win64" : "win32";
                loadLibrary("odbc_bridge_" + arch);
                
            } else if (osName.contains("mac")) {
                loadLibrary(libName);
                
            } else if (osName.contains("nux") || osName.contains("nix")) {
                loadLibrary(libName);
                
            } else {
                throw new UnsupportedOperationException("OS not supported");
            }
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            javax.swing.JOptionPane.showMessageDialog(null, 
                    e.getMessage(), "", javax.swing.JOptionPane.ERROR_MESSAGE);
            throw new RuntimeException("Failed to load native library", e);
        }
    }
    
    static void loadLibrary(String libName) {
        System.out.println("ODBCBridge.loadLibrary: " + libName);
        System.loadLibrary(libName);
    }
    
    private ODBCBridge() {
        
    }
    
    /**
     * Conecta usando una cadena de conexión ODBC completa.
     * 
     * <p>Permite especificar todos los parámetros de conexión en una sola cadena:</p>
     * <pre>
     * "DSN=MiDSN;UID=usuario;PWD=contraseña;DATABASE=MiDB;SERVER=localhost,1433"
     * </pre>
     * 
     * @param connectionString Cadena completa de conexión ODBC
     * @return Puntero a la conexión (handle)
     * @throws Exception Si la conexión falla
     */
    public native long connectWithString(String connectionString) throws Exception;

    /**
     * Conecta a una fuente de datos ODBC usando solo el nombre del DSN.
     * 
     * @param dsn Nombre de la fuente de datos ODBC configurada en el sistema
     * @return Puntero a la conexión (handle)
     * @throws Exception Si la conexión falla
     */
    public native long connect(String dsn) throws Exception;

    public long connect(ODBCDataSource dataSource) throws Exception {
        return connectWithString(dataSource.buildConnectionString());
    }
    
    /**
     * Conecta a una fuente de datos ODBC con usuario y contraseña.
     * 
     * <p>Método de conveniencia que construye la cadena de conexión automáticamente.</p>
     * 
     * @param dsn Nombre de la fuente de datos ODBC
     * @param uid Usuario para la conexión (puede ser null)
     * @param pwd Contraseña para la conexión (puede ser null)
     * @return Puntero a la conexión (handle)
     * @throws Exception Si la conexión falla
     */
    public long connect(String dsn, String uid, String pwd) throws Exception {
        final ODBCDataSource dataSource = new ODBCDataSource()
                .setDsn(dsn)
                .setUser(uid)
                .setPassword(pwd)
        ;
        return connect(dataSource);
    }
    
    /**
     * Cierra una conexión ODBC y libera los recursos asociados.
     * 
     * @param connectionPtr Puntero a la conexión a cerrar
     * @throws Exception Si ocurre un error al cerrar la conexión
     */
    public native void close(long connectionPtr) throws Exception;
    
    /**
     * Obtiene información detallada sobre la base de datos conectada.
     * 
     * @param connectionPtr Puntero a la conexión activa
     * @return Objeto ODBCInfo con información de la base de datos
     * @throws Exception Si ocurre un error al obtener la información
     */
    public native ODBCInfo getDatabaseInfo(long connectionPtr) throws Exception;
    
    /**
     * Lista todas las fuentes de datos ODBC disponibles en el sistema.
     * 
     * @return Array con los nombres de las fuentes de datos ODBC
     * @throws Exception Si ocurre un error al listar las fuentes de datos
     */
    public native String[] listDatabases() throws Exception;
    
    /**
     * Lista todas las tablas disponibles en la base de datos conectada.
     * 
     * @param connectionPtr Puntero a la conexión activa
     * @return Array con los nombres de las tablas
     * @throws Exception Si ocurre un error al listar las tablas
     */
    public native String[] listTables(long connectionPtr) throws Exception;
    
    /**
     * Lista todas las columnas de una tabla específica.
     * 
     * @param connectionPtr Puntero a la conexión activa
     * @param tableName Nombre de la tabla a consultar
     * @return Array de ODBCField con información de cada columna
     * @throws Exception Si ocurre un error al listar las columnas
     */
    public native ODBCField[] listColumns(long connectionPtr, String tableName) throws Exception;
    
     /** 
     * Ejecuta una consulta SELECT y devuelve un puntero a QueryState. 
     * @param connectionPtr puntero a la conexión JNI
     * @param sql           sentencia SQL
     * @param params        parámetros opcionales (Object[]), o null
     */
    public native long query(long connectionPtr, String sql, Object[] params) throws Exception;

    /** Sobrecarga para llamadas sin parámetros */
    public long query(long connectionPtr, String sql) throws Exception {
        return query(connectionPtr, sql, null);
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
    public native int execute(long connectionPtr, String sql, Object[] params) throws Exception;

    /**
     * Sobrecarga sin parámetros.
     */
    public int execute(long connectionPtr, String sql) throws Exception {
        return execute(connectionPtr, sql, null);
    }
    
    /**
     * Obtiene los metadatos de las columnas del resultado de una consulta.
     * 
     * @param queryPtr Puntero al resultado de la consulta
     * @return Array de ODBCField con información de cada columna
     * @throws Exception Si ocurre un error al obtener los metadatos
     */
    public native ODBCField[] fetchFields(long queryPtr) throws Exception;
    
    /**
     * Obtiene la siguiente fila del resultado como un array de objetos.
     * 
     * @param queryPtr Puntero al resultado de la consulta
     * @return Array de objetos con los valores de la fila, o null si no hay más filas
     * @throws Exception Si ocurre un error al obtener la fila
     */
    public native Object[] fetchArray(long queryPtr) throws Exception;
    
    /**
     * Libera los recursos asociados a una consulta.
     * 
     * <p><strong>Importante:</strong> Siempre debe llamarse después de procesar
     * los resultados para evitar fugas de memoria.</p>
     * 
     * @param queryPtr Puntero al resultado de la consulta a liberar
     * @throws Exception Si ocurre un error al liberar los recursos
     */
    public native void free(long queryPtr) throws Exception;
}
