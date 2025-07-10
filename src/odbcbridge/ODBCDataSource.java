// ODBCDataSource.java
package odbcbridge;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * Clase para configurar y construir cadenas de conexión ODBC.
 * 
 * <p>Esta clase proporciona una manera estructurada de configurar los parámetros
 * de conexión ODBC y generar automáticamente la cadena de conexión apropiada.</p>
 * 
 * <h2>Características:</h2>
 * <ul>
 *   <li>Configuración fluida mediante métodos encadenados</li>
 *   <li>Soporte para propiedades adicionales personalizadas</li>
 *   <li>Validación de parámetros básicos</li>
 *   <li>Generación automática de cadenas de conexión</li>
 *   <li>Compatibilidad con diferentes tipos de bases de datos</li>
 * </ul>
 * 
 * <h2>Ejemplo de uso básico:</h2>
 * <pre>
 * {@code
 * // Conexión simple
 * ODBCDataSource ds = new ODBCDataSource()
 *     .setDsn("MiDSN")
 *     .setUser("sa")
 *     .setPassword("secret");
 * 
 * String connStr = ds.buildConnectionString();
 * // Resultado: "DSN=MiDSN;UID=sa;PWD=secret"
 * 
 * // Conexión con propiedades adicionales
 * ODBCDataSource ds2 = new ODBCDataSource()
 *     .setDsn("MiDSN")
 *     .setUser("sa")
 *     .setPassword("secret")
 *     .setDatabase("MiBaseDatos")
 *     .setServer("localhost", 1433)
 *     .setProperty("MARS_Connection", "yes")
 *     .setProperty("ApplicationIntent", "ReadOnly");
 * 
 * String connStr2 = ds2.buildConnectionString();
 * }
 * </pre>
 * 
 * <h2>Propiedades comunes soportadas:</h2>
 * <ul>
 *   <li><strong>DSN</strong> - Nombre de la fuente de datos</li>
 *   <li><strong>UID</strong> - Usuario</li>
 *   <li><strong>PWD</strong> - Contraseña</li>
 *   <li><strong>DATABASE</strong> - Base de datos</li>
 *   <li><strong>SERVER</strong> - Servidor</li>
 *   <li><strong>PORT</strong> - Puerto</li>
 *   <li><strong>DRIVER</strong> - Controlador ODBC</li>
 *   <li><strong>Encrypt</strong> - Cifrado (yes/no)</li>
 *   <li><strong>TrustServerCertificate</strong> - Confiar en certificado (yes/no)</li>
 * </ul>
 * 
 * @author Tu Nombre
 * @version 1.0
 * @since 2025
 */
public class ODBCDataSource {
    
    // Propiedades principales
    private String dsn;
    private String user;
    private String password;
    private String database;
    private String server;
    private Integer port;
    private String driver;
    
    // Propiedades adicionales personalizadas
    private final Map<String, String> properties = new HashMap<>();
    
    /**
     * Constructor por defecto.
     */
    public ODBCDataSource() {
        // Constructor vacío
    }
    
    /**
     * Constructor con DSN.
     * 
     * @param dsn Nombre de la fuente de datos ODBC
     */
    public ODBCDataSource(String dsn) {
        this.dsn = dsn;
    }
    
    /**
     * Constructor con DSN, usuario y contraseña.
     * 
     * @param dsn Nombre de la fuente de datos ODBC
     * @param user Usuario para la conexión
     * @param password Contraseña para la conexión
     */
    public ODBCDataSource(String dsn, String user, String password) {
        this.dsn = dsn;
        this.user = user;
        this.password = password;
    }
    
    /**
     * Constructor con Properties.
     * 
     * @param props Propiedades de conexión
     */
    public ODBCDataSource(Properties props) {
        loadFromProperties(props);
    }
    
    /**
     * Constructor con Map de propiedades.
     * 
     * @param props Mapa de propiedades de conexión
     */
    public ODBCDataSource(Map<String, String> props) {
        loadFromMap(props);
    }
    
    // ==================== MÉTODOS FLUIDOS (BUILDER PATTERN) ====================
    
    /**
     * Establece el nombre de la fuente de datos (DSN).
     * 
     * @param dsn Nombre de la fuente de datos ODBC
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setDsn(String dsn) {
        this.dsn = dsn;
        return this;
    }
    
    /**
     * Establece el usuario para la conexión.
     * 
     * @param user Usuario para la conexión
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setUser(String user) {
        this.user = user;
        return this;
    }
    
    /**
     * Establece la contraseña para la conexión.
     * 
     * @param password Contraseña para la conexión
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setPassword(String password) {
        this.password = password;
        return this;
    }
    
    /**
     * Establece la base de datos a conectar.
     * 
     * @param database Nombre de la base de datos
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setDatabase(String database) {
        this.database = database;
        return this;
    }
    
    /**
     * Establece el servidor a conectar.
     * 
     * @param server Nombre o IP del servidor
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setServer(String server) {
        this.server = server;
        return this;
    }
    
    /**
     * Establece el servidor y puerto a conectar.
     * 
     * @param server Nombre o IP del servidor
     * @param port Puerto del servidor
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setServer(String server, int port) {
        this.server = server;
        this.port = port;
        return this;
    }
    
    /**
     * Establece el puerto del servidor.
     * 
     * @param port Puerto del servidor
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setPort(int port) {
        this.port = port;
        return this;
    }
    
    /**
     * Establece el controlador ODBC a usar.
     * 
     * @param driver Nombre del controlador ODBC
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setDriver(String driver) {
        this.driver = driver;
        return this;
    }
    
    /**
     * Establece si usar cifrado en la conexión.
     * 
     * @param encrypt true para habilitar cifrado, false para deshabilitarlo
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setEncrypt(boolean encrypt) {
        return setProperty("Encrypt", encrypt ? "yes" : "no");
    }
    
    /**
     * Establece si confiar en el certificado del servidor.
     * 
     * @param trust true para confiar en el certificado, false para no confiar
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setTrustServerCertificate(boolean trust) {
        return setProperty("TrustServerCertificate", trust ? "yes" : "no");
    }
    
    /**
     * Establece el timeout de conexión en segundos.
     * 
     * @param seconds Segundos para timeout de conexión
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setConnectionTimeout(int seconds) {
        return setProperty("Connection Timeout", String.valueOf(seconds));
    }
    
    /**
     * Establece el timeout de comando en segundos.
     * 
     * @param seconds Segundos para timeout de comando
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setCommandTimeout(int seconds) {
        return setProperty("Command Timeout", String.valueOf(seconds));
    }
    
    /**
     * Establece una propiedad personalizada.
     * 
     * @param key Nombre de la propiedad
     * @param value Valor de la propiedad
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setProperty(String key, String value) {
        if (key != null && value != null) {
            properties.put(key, value);
        }
        return this;
    }
    
    /**
     * Establece múltiples propiedades desde un Map.
     * 
     * @param props Mapa de propiedades
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setProperties(Map<String, String> props) {
        if (props != null) {
            properties.putAll(props);
        }
        return this;
    }
    
    /**
     * Establece múltiples propiedades desde un objeto Properties.
     * 
     * @param props Propiedades
     * @return Esta instancia para encadenamiento fluido
     */
    public ODBCDataSource setProperties(Properties props) {
        if (props != null) {
            for (String key : props.stringPropertyNames()) {
                properties.put(key, props.getProperty(key));
            }
        }
        return this;
    }
    
    // ==================== MÉTODOS GETTER ====================
    
    public String getDsn() { return dsn; }
    public String getUser() { return user; }
    public String getPassword() { return password; }
    public String getDatabase() { return database; }
    public String getServer() { return server; }
    public Integer getPort() { return port; }
    public String getDriver() { return driver; }
    
    /**
     * Obtiene una propiedad personalizada.
     * 
     * @param key Nombre de la propiedad
     * @return Valor de la propiedad o null si no existe
     */
    public String getProperty(String key) {
        return properties.get(key);
    }
    
    /**
     * Obtiene todas las propiedades personalizadas.
     * 
     * @return Mapa con todas las propiedades personalizadas
     */
    public Map<String, String> getProperties() {
        return new HashMap<>(properties);
    }
    
    // ==================== MÉTODOS DE CARGA ====================
    
    /**
     * Carga configuración desde un objeto Properties.
     * 
     * @param props Propiedades de conexión
     */
    public void loadFromProperties(Properties props) {
        if (props == null) return;
        
        // Propiedades principales
        dsn = props.getProperty("dsn", props.getProperty("DSN"));
        user = props.getProperty("user", props.getProperty("uid", props.getProperty("UID")));
        password = props.getProperty("password", props.getProperty("pwd", props.getProperty("PWD")));
        database = props.getProperty("database", props.getProperty("DATABASE"));
        server = props.getProperty("server", props.getProperty("SERVER"));
        driver = props.getProperty("driver", props.getProperty("DRIVER"));
        
        String portStr = props.getProperty("port", props.getProperty("PORT"));
        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                // Ignorar puerto inválido
            }
        }
        
        // Propiedades adicionales
        for (String key : props.stringPropertyNames()) {
            String lowerKey = key.toLowerCase();
            if (!lowerKey.equals("dsn") && !lowerKey.equals("uid") && !lowerKey.equals("user") && 
                !lowerKey.equals("pwd") && !lowerKey.equals("password") && !lowerKey.equals("database") &&
                !lowerKey.equals("server") && !lowerKey.equals("port") && !lowerKey.equals("driver")) {
                properties.put(key, props.getProperty(key));
            }
        }
    }
    
    /**
     * Carga configuración desde un Map.
     * 
     * @param props Mapa de propiedades de conexión
     */
    public void loadFromMap(Map<String, String> props) {
        if (props == null) return;
        
        // Propiedades principales
        dsn = props.get("dsn") != null ? props.get("dsn") : props.get("DSN");
        user = props.get("user") != null ? props.get("user") : 
               props.get("uid") != null ? props.get("uid") : props.get("UID");
        password = props.get("password") != null ? props.get("password") : 
                  props.get("pwd") != null ? props.get("pwd") : props.get("PWD");
        database = props.get("database") != null ? props.get("database") : props.get("DATABASE");
        server = props.get("server") != null ? props.get("server") : props.get("SERVER");
        driver = props.get("driver") != null ? props.get("driver") : props.get("DRIVER");
        
        String portStr = props.get("port") != null ? props.get("port") : props.get("PORT");
        if (portStr != null) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                // Ignorar puerto inválido
            }
        }
        
        // Propiedades adicionales
        for (Map.Entry<String, String> entry : props.entrySet()) {
            String key = entry.getKey();
            String lowerKey = key.toLowerCase();
            if (!lowerKey.equals("dsn") && !lowerKey.equals("uid") && !lowerKey.equals("user") && 
                !lowerKey.equals("pwd") && !lowerKey.equals("password") && !lowerKey.equals("database") &&
                !lowerKey.equals("server") && !lowerKey.equals("port") && !lowerKey.equals("driver")) {
                properties.put(key, entry.getValue());
            }
        }
    }
    
    /**
    * Carga configuración desde un string con formato key=value;key2=value2.
    * 
    * <p>Parsea un string de propiedades con formato ODBC estándar donde cada
    * propiedad está separada por punto y coma (;) y cada clave-valor está
    * separado por signo igual (=).</p>
    * 
    * <h3>Formato esperado:</h3>
    * <pre>
    * {@code
    * "database=MiDB;server=localhost;port=1433;encrypt=yes;ApplicationIntent=ReadOnly"
    * }
    * </pre>
    * 
    * <h3>Propiedades reconocidas automáticamente:</h3>
    * <ul>
    *   <li><strong>dsn</strong> - Nombre de la fuente de datos</li>
    *   <li><strong>uid/user</strong> - Usuario</li>
    *   <li><strong>pwd/password</strong> - Contraseña</li>
    *   <li><strong>database</strong> - Base de datos</li>
    *   <li><strong>server</strong> - Servidor</li>
    *   <li><strong>port</strong> - Puerto (convertido a entero)</li>
    *   <li><strong>driver</strong> - Controlador ODBC</li>
    *   <li><strong>encrypt</strong> - Cifrado (yes/no/true/false)</li>
    *   <li><strong>trustservercertificate</strong> - Confiar en certificado</li>
    *   <li><strong>connectiontimeout</strong> - Timeout de conexión</li>
    *   <li><strong>commandtimeout</strong> - Timeout de comando</li>
    * </ul>
    * 
    * <p>Cualquier otra propiedad se agrega como propiedad personalizada.</p>
    * 
    * @param propertiesString String con las propiedades en formato key=value;key2=value2
    * @throws IllegalArgumentException Si el formato del string es inválido
    */
   public void loadFrom(String propertiesString) {
       if (propertiesString == null || propertiesString.trim().isEmpty()) {
           return;
       }

       String[] propertyArray = propertiesString.split(";");

       for (String property : propertyArray) {
           if (property == null) continue;

           property = property.trim();
           if (property.isEmpty()) continue;

           try {
               int equalIndex = property.indexOf('=');
               if (equalIndex <= 0 || equalIndex >= property.length() - 1) {
                   // Formato inválido, saltar esta propiedad
                   continue;
               }

               String key = property.substring(0, equalIndex).trim();
               String value = property.substring(equalIndex + 1).trim();

               if (key.isEmpty() || value.isEmpty()) {
                   continue;
               }

               // Remover comillas si existen
               value = removeQuotes(value);

               // Aplicar propiedades específicas conocidas
               applyProperty(key, value);

           } catch (Exception e) {
               // Continuar con la siguiente propiedad si hay error
               System.err.println("Error procesando propiedad: " + property + " - " + e.getMessage());
           }
       }
   }

   /**
    * Carga configuración desde un string con formato key=value;key2=value2.
    * Versión que retorna la instancia para encadenamiento fluido.
    * 
    * @param propertiesString String con las propiedades
    * @return Esta instancia para encadenamiento fluido
    */
   public ODBCDataSource loadFromString(String propertiesString) {
       loadFrom(propertiesString);
       return this;
   }

   /**
    * Método privado para aplicar una propiedad específica.
    * 
    * @param key Clave de la propiedad
    * @param value Valor de la propiedad
    */
   private void applyProperty(String key, String value) {
       String lowerKey = key.toLowerCase();

       switch (lowerKey) {
           case "dsn":
               this.dsn = value;
               break;
           case "uid":
           case "user":
               this.user = value;
               break;
           case "pwd":
           case "password":
               this.password = value;
               break;
           case "database":
               this.database = value;
               break;
           case "server":
               this.server = value;
               break;
           case "port":
               try {
                   this.port = Integer.parseInt(value);
               } catch (NumberFormatException e) {
                   // Si no es un número válido, agregar como propiedad personalizada
                   this.properties.put(key, value);
               }
               break;
           case "driver":
               this.driver = value;
               break;
           case "encrypt":
               boolean encryptValue = "yes".equalsIgnoreCase(value) || 
                                    "true".equalsIgnoreCase(value) || 
                                    "1".equals(value);
               this.properties.put("Encrypt", encryptValue ? "yes" : "no");
               break;
           case "trustservercertificate":
               boolean trustValue = "yes".equalsIgnoreCase(value) || 
                                  "true".equalsIgnoreCase(value) || 
                                  "1".equals(value);
               this.properties.put("TrustServerCertificate", trustValue ? "yes" : "no");
               break;
           case "connectiontimeout":
           case "connection timeout":
               try {
                   int timeout = Integer.parseInt(value);
                   this.properties.put("Connection Timeout", String.valueOf(timeout));
               } catch (NumberFormatException e) {
                   this.properties.put(key, value);
               }
               break;
           case "commandtimeout":
           case "command timeout":
               try {
                   int timeout = Integer.parseInt(value);
                   this.properties.put("Command Timeout", String.valueOf(timeout));
               } catch (NumberFormatException e) {
                   this.properties.put(key, value);
               }
               break;
           default:
               // Cualquier otra propiedad personalizada
               this.properties.put(key, value);
               break;
       }
   }

   /**
    * Método privado para remover comillas de los valores.
    * 
    * @param value Valor que puede contener comillas
    * @return Valor sin comillas
    */
   private String removeQuotes(String value) {
       if (value.length() < 2) {
           return value;
       }

       if ((value.startsWith("\"") && value.endsWith("\"")) ||
           (value.startsWith("'") && value.endsWith("'"))) {
           return value.substring(1, value.length() - 1);
       }

       return value;
   }
    
    
    // ==================== MÉTODO PRINCIPAL ====================
    
    /**
     * Construye la cadena de conexión ODBC.
     * 
     * <p>Genera automáticamente la cadena de conexión basada en las propiedades
     * configuradas, siguiendo el formato estándar ODBC.</p>
     * 
     * <h3>Orden de precedencia:</h3>
     * <ol>
     *   <li>DSN (si está configurado)</li>
     *   <li>DRIVER (si no hay DSN)</li>
     *   <li>SERVER y PORT</li>
     *   <li>DATABASE</li>
     *   <li>UID y PWD</li>
     *   <li>Propiedades adicionales</li>
     * </ol>
     * 
     * @return Cadena de conexión ODBC formateada
     * @throws IllegalStateException Si no se proporciona DSN ni DRIVER
     */
    public String buildConnectionString() {
        StringJoiner joiner = new StringJoiner(";");
        
        // DSN tiene prioridad
        if (dsn != null && !dsn.trim().isEmpty()) {
            joiner.add("DSN=" + dsn);
        } else if (driver != null && !driver.trim().isEmpty()) {
            joiner.add("DRIVER={" + driver + "}");
        } else {
            throw new IllegalStateException("Debe proporcionar DSN o DRIVER para la conexión");
        }
        
        // Servidor y puerto
        if (server != null && !server.trim().isEmpty()) {
            if (port != null) {
                joiner.add("SERVER=" + server + "," + port);
            } else {
                joiner.add("SERVER=" + server);
            }
        }
        
        // Base de datos
        if (database != null && !database.trim().isEmpty()) {
            joiner.add("DATABASE=" + database);
        }
        
        // Usuario y contraseña
        if (user != null && !user.trim().isEmpty()) {
            joiner.add("UID=" + user);
        }
        
        if (password != null && !password.trim().isEmpty()) {
            joiner.add("PWD=" + password);
        }
        
        // Propiedades adicionales
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key != null && value != null && !key.trim().isEmpty() && !value.trim().isEmpty()) {
                joiner.add(key + "=" + value);
            }
        }
        
        return joiner.toString();
    }
    
    /**
     * Conecta usando esta configuración de fuente de datos.
     * 
     * @return Puntero a la conexión establecida
     * @throws Exception Si la conexión falla
     */
    public ODBCConnection getConnection() throws Exception {
        return ODBCConnection.connectWithString(buildConnectionString());
    }
    
    /** Lista todos los DSNs disponibles. */
    public static String[] listDatabases() throws Exception {
        return ODBCBridge.INSTANCE.listDatabases();
    }
    
    // ==================== MÉTODOS DE UTILIDAD ====================
    
    /**
     * Valida la configuración básica.
     * 
     * @return true si la configuración es válida, false en caso contrario
     */
    public boolean isValid() {
        return (dsn != null && !dsn.trim().isEmpty()) || 
               (driver != null && !driver.trim().isEmpty());
    }
    
    /**
     * Limpia todas las propiedades.
     */
    public void clear() {
        dsn = null;
        user = null;
        password = null;
        database = null;
        server = null;
        port = null;
        driver = null;
        properties.clear();
    }
    
    /**
     * Crea una copia de esta fuente de datos.
     * 
     * @return Nueva instancia con la misma configuración
     */
    public ODBCDataSource clone() {
        ODBCDataSource copy = new ODBCDataSource();
        copy.dsn = this.dsn;
        copy.user = this.user;
        copy.password = this.password;
        copy.database = this.database;
        copy.server = this.server;
        copy.port = this.port;
        copy.driver = this.driver;
        copy.properties.putAll(this.properties);
        return copy;
    }
    
    @Override
    public String toString() {
        return "ODBCDataSource{" +
               "dsn='" + dsn + '\'' +
               ", user='" + user + '\'' +
               ", database='" + database + '\'' +
               ", server='" + server + '\'' +
               ", port=" + port +
               ", driver='" + driver + '\'' +
               ", properties=" + properties.size() + " items" +
               '}';
    }
}