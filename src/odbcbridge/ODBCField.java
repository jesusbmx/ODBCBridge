package odbcbridge;

import java.util.HashMap;
import java.util.Map;

public class ODBCField {
  
    // Mapa de código ODBC a nombre de tipo SQL
    private static final Map<Integer, String> TYPES = new HashMap<>();

    static {
        TYPES.put(1, "CHAR");
        TYPES.put(2, "NUMERIC");
        TYPES.put(3, "DECIMAL");
        TYPES.put(4, "INTEGER");
        TYPES.put(5, "SMALLINT");
        TYPES.put(6, "FLOAT");
        TYPES.put(7, "REAL");
        TYPES.put(8, "DOUBLE");
        TYPES.put(9, "DATE");
        TYPES.put(10, "TIME");
        TYPES.put(11, "TIMESTAMP");
        TYPES.put(12, "VARCHAR");
        TYPES.put(-1, "LONGVARCHAR");
        TYPES.put(-2, "BINARY");
        TYPES.put(-3, "VARBINARY");
        TYPES.put(-4, "LONGVARBINARY");
        TYPES.put(-5, "BIGINT");
        TYPES.put(-6, "TINYINT");
        TYPES.put(-7, "BIT");
        TYPES.put(-8, "WCHAR");
        TYPES.put(-9, "WVARCHAR");
        TYPES.put(-10, "WLONGVARCHAR");
        TYPES.put(91, "SQL_DATE");
        TYPES.put(92, "SQL_TIME");
        TYPES.put(93, "SQL_TIMESTAMP");
        // Puedes agregar más códigos según lo necesites
    }

    /**
     * Devuelve el nombre del tipo SQL dado su código ODBC.
     *
     * @param code Código entero del tipo de dato
     * @return Nombre legible del tipo SQL
     */
    public static String getTypeName(int code) {
        final String typeName = TYPES.get(code);
        return typeName == null ? ("UNKNOWN(" + code + ")") : typeName; 
    }
    
    public final String name;
    public final int type;
    public final int size;

    public ODBCField(String name, int type, int size) {
        this.name = name;
        this.type = type;
        this.size = size;
    }
    
    public String getTypeName() {
        return getTypeName(type);
    }

    @Override
    public String toString() {
        return "ODBCField{" + "name=" + name + ", type=" + getTypeName() + ", size=" + size + '}';
    }
    
    
}