// ODBCBridge.java
package odbcbridge;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.swing.JOptionPane;

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
            JOptionPane.showMessageDialog(null, 
                    e.getMessage(), "", JOptionPane.ERROR_MESSAGE);
            throw new RuntimeException("Failed to load native library", e);
        }
    }
    
    static void loadLibrary(String libName) {
        System.out.println("ODBCBridge.loadLibrary: " + libName);
        System.loadLibrary(libName);
    }
    
    private ODBCField[] _fields;
    private long _queryPtr;
    
    private ODBCBridge() {
        
    }

    // Métodos nativos
    public native long connect(String dsn) throws Exception;
    public native void close(long connectionPtr) throws Exception;
    public native ODBCInfo getDatabaseInfo(long connectionPtr) throws Exception;
    public native String[] listDatabases() throws Exception;
    public native String[] listTables(long connectionPtr) throws Exception;
    public native ODBCField[] listColumns(long connectionPtr, String tableName) throws Exception;
    public native long query(long connectionPtr, String sql) throws Exception;
    public native ODBCField[] fetchFields(long queryPtr) throws Exception;
    public native Object[] fetchArray(long queryPtr) throws Exception;
    public native void free(long queryPtr) throws Exception;
    
    public Map<String, Object> fetchAssoc(long queryPtr) throws Exception {
        Object[] values = fetchArray(queryPtr);
        if (values == null)  return null;
        
        if (_fields == null || _queryPtr != queryPtr) {
            _queryPtr = queryPtr;
            _fields = fetchFields(queryPtr);
        }
        
        final int len = values.length;
        Map<String, Object> row = new LinkedHashMap<>(len);
        for (int i = 0; i < len; i++) {
            row.put(_fields[i].name, values[i]);
        }
        
        return row;
    }
}
