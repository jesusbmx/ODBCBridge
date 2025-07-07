
import java.util.Arrays;
import java.util.Map;
import odbcbridge.ODBCBridge;
import odbcbridge.ODBCField;
import odbcbridge.ODBCInfo;

public class FirebirdTest {
    
    public static void main(String[] args) throws Exception {
        final ODBCBridge bridge = ODBCBridge.INSTANCE;
              
        final String dsn = "BODEGA CD";
        final long link = bridge.connect(dsn);
        try {

            System.out.println("-- Fields --");
            ODBCField[] columns = bridge.listColumns(link, "DOCTOS_CM");
            for (ODBCField column : columns) {
                System.out.println("Column: " + column);
            }
            
            System.out.println("-- Query --");
                final String sql = "SELECT FIRST 100\n" +
"  cm.\"DOCTO_CM_ID\",\n" +
"  cm.\"FOLIO\",\n" +
"  cm.\"TIPO_DOCTO\",\n" +
"  cm.\"SUBTIPO_DOCTO\",\n" +
"  cm.\"SUCURSAL_ID\",\n" +
"  cm.\"PROVEEDOR_ID\",\n" +
"  cm.\"FECHA\",\n" +
"  \n" +
"  art.\"NOMBRE\",\n" +
"\n" +
"  -- det.\"DOCTO_CM_DET_ID\",\n" +
"  det.\"CLAVE_ARTICULO\",\n" +
"  det.\"UNIDADES\",\n" +
"  det.\"UMED\",\n" +
"  det.\"CONTENIDO_UMED\",\n" +
"  det.\"PRECIO_UNITARIO\",\n" +
"  det.\"PRECIO_TOTAL_NETO\",\n" +
"  \n" +
"  lart.\"TARA\",\n" +
"  lart.\"DIAS_CADUCIDAD_EPELSA\",\n" +
"  lart.\"PLU\",\n" +
"  lart.\"TABLA\", \n" +
"  lart.\"FAMILIA\", \n" +
"  lart.\"CODIGO_1\", \n" +
"  lart.\"CODIGO_2\", \n" +
"  lart.\"CODIGO_3\"\n" +
"\n" +
"FROM \"DOCTOS_CM\" cm\n" +
"JOIN \"DOCTOS_CM_DET\" det ON cm.\"DOCTO_CM_ID\" = det.\"DOCTO_CM_ID\"\n" +
"JOIN \"ARTICULOS\" art ON det.\"ARTICULO_ID\" = art.\"ARTICULO_ID\"\n" +
"LEFT JOIN \"LIBRES_ARTICULOS\" lart ON det.\"ARTICULO_ID\" = lart.\"ARTICULO_ID\"\n" +
"\n" +
"WHERE cm.\"FOLIO\" = 'MTZ000023'"; 
                System.out.println(sql);
            final long result = bridge.query(link, sql);
            try {
                ODBCField[] fields = bridge.fetchFields(result);

                int index = 0;
                Map<String, Object> row;
                while ((row = bridge.fetchAssoc(result)) != null) {
                    if (index == 0) {
                        int v = 0;
                        for (Object value : row.values()) {
                            Class<?> classValue = value == null ? null : value.getClass();
                            System.out.println("[" + v + "]" + classValue + ": " + fields[v] + " {" + value + "}");
                            v++; 
                        }
                    }
                    System.out.println("--");
                    System.out.println(row);
                    index++;
                }
            } finally {
                bridge.free(result);
            }
            
        } finally {
            bridge.close(link);
        }
    }
}
