
import odbcbridge.ODBCBridge;
import odbcbridge.ODBCDataSource;
import odbcbridge.ODBCField;
import odbcbridge.ODBCInfo;

public class ODBCTest2 {
    
    private static final ODBCBridge bridge = ODBCBridge.INSTANCE;
    
    public static void main(String[] args) throws Exception {
        System.out.println("-- Databases --");
        String[] databases = bridge.listDatabases();
        for (String database : databases) {
            System.out.println("Database: " + database);
        }
        
        final ODBCDataSource dataSource = new ODBCDataSource()
                .setDsn("CARNES DELGADILLO A25");

        long link = bridge.connect(dataSource);
        try {
            System.out.println("-- Info --");
            ODBCInfo info = bridge.getDatabaseInfo(link);
            System.out.println(info);
            
            System.out.println("-- Tables --");
            String[] tables = bridge.listTables(link);
            for (String table : tables) {
                System.out.println("Table: " + table);
            }
            
            System.out.println("-- Query --");
            final String sql = "SELECT \n" +
"  --DOCTOS_IN\n" +
"  di.\"FOLIO\",\n" +
"  di.\"FECHA\",\n" +
"  di.\"DOCTO_IN_ID\",\n" +
"  di.\"ALMACEN_ID\",\n" +
"  di.\"CONCEPTO_IN_ID\",\n" +
"  di.\"SUCURSAL_ID\",\n" +
"  --CONCEPTOS_IN\n" +
"  ci.\"NOMBRE\" AS \"NOMBRE_CONCEPTO\",\n" +
"  ci.\"NOMBRE_ABREV\" AS \"CONCEPTO_ABREV\",\n" +
"  ci.\"TIPO\",\n" +
"  -- Clave Principal\n" +
"  (SELECT FIRST 1 ca.\"CLAVE_ARTICULO\"\n" +
"   FROM \"CLAVES_ARTICULOS\" ca\n" +
"   JOIN \"ROLES_CLAVES_ARTICULOS\" rc ON rc.\"ROL_CLAVE_ART_ID\" = ca.\"ROL_CLAVE_ART_ID\"\n" +
"   WHERE ca.\"ARTICULO_ID\" = a.\"ARTICULO_ID\"\n" +
"     AND rc.\"NOMBRE\" = 'Clave principal') AS \"CLAVE_PRINCIPAL\",\n" +
"  -- ARTICULOS\n" +
"  a.\"NOMBRE\" AS \"NOMBRE_ARTICULO\",\n" +
"  (CASE \n" +
"    WHEN a.\"UNIDAD_VENTA\" = 'Pieza' THEN 'Pza'\n" +
"    WHEN a.\"UNIDAD_VENTA\" = 'Kilogramo' THEN 'Kg'\n" +
"    WHEN a.\"UNIDAD_VENTA\" = 'Metro' THEN 'Mt'\n" +
"    WHEN a.\"UNIDAD_VENTA\" = 'Litro' THEN 'Lt'\n" +
"    ELSE a.\"UNIDAD_VENTA\"\n" +
"   END) AS \"UMED\",\n" +
"  --DOCTOS_IN_DET\n" +
"  did.\"CLAVE_ARTICULO\",\n" +
"  did.\"ARTICULO_ID\",\n" +
"  did.\"UNIDADES\",\n" +
"  did.\"UNIDADES\" AS \"UNIDADES_FORMAT\",\n" +
"  LPAD(CAST(CAST(did.\"UNIDADES\" * 1000 AS INTEGER) AS VARCHAR(7)), 7, '0') AS \"UNIDADES_STR_4_3\",\n" +
"  LPAD(CAST(CAST(did.\"UNIDADES\" * 100 AS INTEGER) AS VARCHAR(5)), 5, '0') AS \"UNIDADES_STR_3_2\",\n" +
"  (did.\"UNIDADES\" / 36) AS \"UNIDADES_ETI\",\n" +
"  (did.\"UNIDADES\" / 36) AS \"UNIDADES_ETI_FORMAT\",\n" +
"  LPAD(CAST(CAST((did.\"UNIDADES\" / 36) * 1000 AS INTEGER) AS VARCHAR(7)), 7, '0') AS \"UNIDADES_ETI_STR_4_3\",\n" +
"  LPAD(CAST(CAST((did.\"UNIDADES\" / 36) * 100 AS INTEGER) AS VARCHAR(5)), 5, '0') AS \"UNIDADES_ETI_STR_3_2\",\n" +
"  did.\"COSTO_UNITARIO\",\n" +
"  LPAD(CAST(CAST(did.\"COSTO_UNITARIO\" * 1000 AS INTEGER) AS VARCHAR(6)), 6, '0') AS \"COSTO_UNITARIO_STR_3_3\",\n" +
"  did.\"DOCTO_IN_DET_ID\",\n" +
"  did.\"ALMACEN_ID\" AS \"DET_ALMACEN_ID\",\n" +
"   --LIBRES_ARTICULOS\n" +
"  la.\"TARA\",\n" +
"  la.\"PLU\",\n" +
"  la.\"TABLA\",\n" +
"  la.\"FAMILIA\",\n" +
"  la.\"DIAS_CADUCIDAD_EPELSA\",\n" +
"  CAST(CURRENT_TIMESTAMP AS DATE) AS \"FECHA_ACTUAL\",\n" +
"  DATEADD(la.\"DIAS_CADUCIDAD_EPELSA\" DAY TO CURRENT_TIMESTAMP) AS \"FECHA_CADUCIDAD\",\n" +
"  LPAD(EXTRACT(DAY FROM CURRENT_DATE), 2, '0') || \n" +
"    LPAD(EXTRACT(MONTH FROM CURRENT_DATE), 2, '0') || \n" +
"    SUBSTRING(CAST(EXTRACT(YEAR FROM CURRENT_DATE) AS VARCHAR(4)) FROM 3 FOR 2) AS \"LOTE\",\n" +
"  la.\"CODIGO_LEYENDA\",\n" +
"  la.\"NOMBRE_LEYENDA\",\n" +
"  CAST(la.\"TEM_MIN\" AS INTEGER) || '°C a ' || CAST(la.\"TEM_MAX\" AS INTEGER) || '°C' AS \"TEMP_RANGO\",\n" +
"  -- Nombre de Familia\n" +
"  lin.\"NOMBRE\" AS \"NOMBRE_FAMILIA\",\n" +
"  -- Sucursales\n" +
"  s.\"NOMBRE\" AS \"NOMBRE_SUCURSAL\",\n" +
"  --ALMACENES\n" +
"  al.\"NOMBRE\" AS \"NOMBRE_ALMACEN\",\n" +
"  -- COMPONENTES DEL JUEGO (Nueva columna)\n" +
"  (SELECT FIRST 1 COALESCE(a_comp.\"NOMBRE\", 'N/A') FROM \"JUEGOS_DET\" jd\n" +
"   LEFT JOIN \"ARTICULOS\" a_comp ON jd.\"COMPONENTE_ID\" = a_comp.\"ARTICULO_ID\"\n" +
"   WHERE jd.\"ARTICULO_ID\" = did.\"ARTICULO_ID\"\n" +
"     AND a_comp.\"ES_JUEGO\" = 'N') AS \"NOMBRE_COMPONENTE\",\n" +
"  (SELECT FIRST 1 COALESCE(a_comp.\"UNIDAD_VENTA\", 'N/A') FROM \"JUEGOS_DET\" jd\n" +
"   LEFT JOIN \"ARTICULOS\" a_comp ON jd.\"COMPONENTE_ID\" = a_comp.\"ARTICULO_ID\"\n" +
"   WHERE jd.\"ARTICULO_ID\" = did.\"ARTICULO_ID\"\n" +
"     AND a_comp.\"ES_JUEGO\" = 'N') AS \"UNIDAD_COMPONENTE\",\n" +
"  (36) AS COPIES\n" +
"FROM \"DOCTOS_IN\" di\n" +
"LEFT JOIN \"DOCTOS_IN_DET\" did ON di.\"DOCTO_IN_ID\" = did.\"DOCTO_IN_ID\"\n" +
"LEFT JOIN \"ALMACENES\" al ON di.\"ALMACEN_ID\" = al.\"ALMACEN_ID\"\n" +
"LEFT JOIN \"CONCEPTOS_IN\" ci ON di.\"CONCEPTO_IN_ID\" = ci.\"CONCEPTO_IN_ID\"\n" +
"LEFT JOIN \"SUCURSALES\" s ON di.\"SUCURSAL_ID\" = s.\"SUCURSAL_ID\"\n" +
"LEFT JOIN \"ARTICULOS\" a ON did.\"ARTICULO_ID\" = a.\"ARTICULO_ID\"\n" +
"LEFT JOIN \"LIBRES_ARTICULOS\" la ON did.\"ARTICULO_ID\" = la.\"ARTICULO_ID\"\n" +
"LEFT JOIN \"LINEAS_ARTICULOS\" lin ON lin.\"LINEA_ARTICULO_ID\" = a.\"LINEA_ARTICULO_ID\"\n" +
"WHERE ci.\"TIPO\" = 'E'\n" +
"  AND di.\"CANCELADO\" = 'N'\n" +
"  AND (\n" +
"      di.\"FOLIO\" = 'BOG000083' \n" +
"      OR di.\"FOLIO\" = SUBSTRING('BOG000083' FROM 1 FOR 3) || LPAD(SUBSTRING('BOG000083' FROM 4 FOR 6), 6, '0')\n" +
"  );";
            System.out.println(sql);
            long result = bridge.query(link, sql);
            if (result == 0) {
    throw new RuntimeException("La función query devolvió puntero 0: hubo un error al preparar la consulta");
}
            try {                   
                ODBCField[] fields = bridge.fetchFields(result);
                for (int col = 0; col < fields.length; col++) {
                    if (col > 0) System.out.print(",");
                    System.out.print(fields[col].name);
                }
                System.out.println("");
                
                Object[] row;
                while ((row = bridge.fetchArray(result)) != null) {
                    for (int col = 0; col < row.length; col++) {
                        if (col > 0) System.out.print(",");
                        System.out.print(row[col]);
                    }
                    System.out.println("");
                }
            }  finally {
                bridge.free(result);
            }
            
        } finally {
            bridge.close(link);
        }
    }
}
