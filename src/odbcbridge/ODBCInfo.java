package odbcbridge;

public class ODBCInfo {
    public final String dbmsName;
    public final String dbmsVersion;
    public final String driverName;
    public final String driverVersion;
    public final String serverName;
    public final String userName;

    public ODBCInfo(String dbmsName, String dbmsVersion, String driverName, String driverVersion, String serverName, String userName) {
        this.dbmsName = dbmsName;
        this.dbmsVersion = dbmsVersion;
        this.driverName = driverName;
        this.driverVersion = driverVersion;
        this.serverName = serverName;
        this.userName = userName;
    }

    @Override
    public String toString() {
        return String.format("DBMS: %s %s, Driver: %s %s, Server: %s, User: %s",
            dbmsName, dbmsVersion, driverName, driverVersion, serverName, userName);
    }
}
