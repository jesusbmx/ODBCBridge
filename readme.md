# ODBCBridge

**ODBCBridge** es una librería Java que proporciona una interfaz sencilla para conectarse a bases de datos utilizando ODBC mediante código nativo (C) a través de JNI.

Permite:
- Listar fuentes de datos ODBC (DSNs)
- Listar tablas
- Ejecutar consultas SQL
- Leer resultados como mapas llave-valor

## Requisitos
- Sistema operativo Windows
- Java 8 o superior
- Controladores ODBC instalados para la base de datos objetivo (ej. PostgreSQL ODBC Driver)

## Archivos necesarios

Para integrar esta librería en tu proyecto, debes importar los siguientes archivos desde la carpeta `dist/`:

| Archivo | Descripción |
|:--------|:------------|
| [ODBCBridge.jar](dist/ODBCBridge.jar) | Librería Java que proporciona la interfaz de programación |
| [odbc_bridge_win32.dll](dist/odbc_bridge_win32.dll) | DLL para sistemas Windows de 32 bits |
| [odbc_bridge_win64.dll](dist/odbc_bridge_win64.dll) | DLL para sistemas Windows de 64 bits |

## Uso básico

```java
System.out.println("-- Databases --");
String[] databases = ODBCDataSource.listDatabases();
for (String database : databases) {
    System.out.println("Database: " + database);
}

final ODBCDataSource dataSource = new ODBCDataSource()
        .setDsn("Postgre32");

try (OdbcConnection connection = dataSource.getConnection()) {
    System.out.println("-- Info --");
    ODBCInfo info = connection.getDatabaseInfo();
    System.out.println(info);

    System.out.println("-- Tables --");
    String[] tables = connection.listTables();
    for (String table : tables) {
        System.out.println("Table: " + table);
    }

    System.out.println("-- Fields --");
    ODBCField[] columns = connection.listColumns("Product");
    for (ODBCField column : columns) {
        System.out.println("Column: " + column);
    }

    System.out.println("-- Query --");
    final String sql = "SELECT * FROM \"Product\" LIMIT 100";
    try (OdbcResultSet resultSet = connection.query(sql)) {

        while (resultSet.next()) {
            for (int col = 1; col <= resultSet.getColumnCount(); col++) {
                if (col > 1) System.out.print(",");
                System.out.print(resultSet.get(col));
            }
            System.out.println("");
        }
    } 
}
```

## Instalación

1. Copia `ODBCBridge.jar` en el classpath de tu proyecto.
2. Copia las DLLs `odbc_bridge_win32.dll` y `odbc_bridge_win64.dll` en alguna carpeta disponible.
3. Asegúrate que el directorio que contiene las DLLs esté en la variable de entorno `PATH`, o cópialas junto a tu ejecutable.

## Notas
- Detecta automáticamente la arquitectura del sistema para cargar la DLL correcta.
- No se soportan sistemas operativos que no sean Windows en esta versión.
- La conexión y consultas tienen timeout de 5 segundos configurado por defecto.

---

**Contacto**: Para dudas o soporte técnico puedes comunicarte con el equipo de desarrollo.

