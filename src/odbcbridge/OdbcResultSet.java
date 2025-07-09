package odbcbridge;

import java.util.HashMap;
import java.util.Map;

/**
 * Mini-ResultSet para iterar filas con try-with-resources.
 */
public class OdbcResultSet implements AutoCloseable {
    private final ODBCBridge bridge;
    private final long ptr;
    private final ODBCField[] fields;
    private final Map<String,Integer> indexMap;
    private Object[] currentValues;

    /**
     * Constructor: precarga metadata y mapea nombres a índices.
     */
    public OdbcResultSet(ODBCBridge bridge, long ptr) throws Exception {
        this.bridge = bridge;
        this.ptr = ptr;
        this.fields = bridge.fetchFields(ptr);
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
        currentValues = bridge.fetchArray(ptr);
        return currentValues != null;
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

    /** Devuelve todos los valores actuales. */
    public Object[] getValues() {
        return currentValues;
    }

    /** Devuelve metadata de columnas. */
    public ODBCField[] getFields() {
        return fields;
    }
    
    /** Devuelve el número de columnas en el ResultSet. */
    public int getColumnCount() {
        return fields.length;
    }

    /** Libera recursos de la consulta. */
    @Override
    public void close() throws Exception {
        bridge.free(ptr);
    }
}
