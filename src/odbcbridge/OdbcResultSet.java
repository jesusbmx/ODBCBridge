package odbcbridge;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mini-ResultSet para iterar filas con try-with-resources.
 */
public class OdbcResultSet implements AutoCloseable {
    private final ODBCBridge nativeBridge;
    private final long ptr;
    private final ODBCField[] fields;
    private final Map<String,Integer> indexMap;
    private Object[] currentValues;

    /**
     * Constructor: precarga metadata y mapea nombres a índices.
     */
    public OdbcResultSet(ODBCBridge nativeBridge, long ptr) throws Exception {
        this.nativeBridge = nativeBridge;
        this.ptr = ptr;
        this.fields = nativeBridge.fetchFields(ptr);
        this.indexMap = new HashMap<>();
        for (int i = 0; i < fields.length; i++) {
            indexMap.put(fields[i].name, i);
        }
    }

    /**
     * Avanza al siguiente registro usando fetchArray (sin fetchAssoc).
     * @return true si hay fila, false al acabar.
     */
    public boolean next() throws Exception {
        currentValues = nativeBridge.fetchArray(ptr);
        return currentValues != null;
    }

    /** Devuelve todos los valores actuales. */
    public Object[] getValues() {
        return currentValues;
    }

    /** Devuelve metadata de columnas. */
    public ODBCField[] getFields() {
        return fields;
    }
    
    /**
     * Devuelve la fila actual como un Map<nombreColumna, valor>.
     * @throws IllegalStateException si no se ha llamado a next() o ya no hay fila.
     */
    public Map<String, Object> toMap() {
       if (currentValues == null) {
           throw new IllegalStateException("There is no current row. Did you forget to call next()?");
       }
       Map<String, Object> rowMap = new LinkedHashMap<>(fields.length);
       for (int i = 0; i < fields.length; i++) {
           rowMap.put(fields[i].name, currentValues[i]);
       }
       return rowMap;
    }

    /** Devuelve el número de columnas en el ResultSet. */
    public int getColumnCount() {
        return fields.length;
    }
    
    /** Obtiene valor por índice 1-based. */
    public Object get(int columnIndex) {
        return currentValues[columnIndex - 1];
    }

    /** Obtiene valor por nombre de columna. */
    public Object get(String columnName) {
        Integer idx = indexMap.get(columnName);
        if (idx == null) throw new IllegalArgumentException("Columna no encontrada: " + columnName);
        return currentValues[idx];
    }

    /** Libera recursos de la consulta. */
    @Override
    public void close() throws Exception {
        nativeBridge.free(ptr);
    }
}
