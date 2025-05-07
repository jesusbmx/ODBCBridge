// ODBCBridge.c
#include "ODBCBridge.h"
#include <windows.h>
#include <sql.h>
#include <sqlext.h>

// Estructura para almacenar el estado de la conexión
typedef struct {
    SQLHENV hEnv;
    SQLHDBC hDbc;
} ConnectionState;

// Estructura para almacenar el estado de la consulta
typedef struct {
    SQLHSTMT hStmt;
    ConnectionState* connectionState;
} QueryState;

// Función auxiliar para manejar errores de ODBC
void check_error(JNIEnv *env, SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle, const char* message) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR sqlState[6], errMsg[256];
        SQLINTEGER nativeError;
        SQLSMALLINT textLength;
        SQLGetDiagRec(handleType, handle, 1, sqlState, &nativeError, errMsg, sizeof(errMsg), &textLength);
        fprintf(stderr, "Error: %s, SQLState: %s, Message: %s\n", message, sqlState, errMsg);
        jclass exClass = (*env)->FindClass(env, "java/sql/SQLException");
        if (exClass != NULL) {
            (*env)->ThrowNew(env, exClass, (const char*)errMsg);
        }
    }
}

// Función para establecer el entorno ODBC
SQLHENV setup_environment(JNIEnv *env) {
    SQLHENV hEnv;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    check_error(env, ret, SQL_HANDLE_ENV, hEnv, "Failed to allocate ODBC environment handle");

    ret = SQLSetEnvAttr(hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    check_error(env, ret, SQL_HANDLE_ENV, hEnv, "Failed to set ODBC version");

    return hEnv;
}

// Función para establecer la conexión ODBC con timeout
SQLHDBC connect_to_dsn(JNIEnv *env, SQLHENV hEnv, const char *dsn) {
    SQLHDBC hDbc;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    check_error(env, ret, SQL_HANDLE_DBC, hDbc, "Failed to allocate ODBC connection handle");

    // Set connection timeout
    SQLSetConnectAttr(hDbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0); // 5 seconds timeout

    ret = SQLConnect(hDbc, (SQLCHAR *)dsn, SQL_NTS, NULL, 0, NULL, 0);
    check_error(env, ret, SQL_HANDLE_DBC, hDbc, "Failed to connect to DSN");

    return hDbc;
}

// Función para inicializar una consulta ODBC
SQLHSTMT init_statement(JNIEnv *env, SQLHDBC hDbc) {
    SQLHSTMT hStmt;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hDbc, &hStmt);
    check_error(env, ret, SQL_HANDLE_STMT, hStmt, "Failed to allocate ODBC statement handle");

    // Set query timeout
    SQLSetStmtAttr(hStmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)5, 0); // 5 seconds timeout

    return hStmt;
}

// Función para conectar
JNIEXPORT jlong JNICALL Java_odbcbridge_ODBCBridge_connect(
    JNIEnv *env, jobject obj, jstring jdsn
) {
    const char *dsn = (*env)->GetStringUTFChars(env, jdsn, 0);

    SQLHENV hEnv = setup_environment(env);
    SQLHDBC hDbc = connect_to_dsn(env, hEnv, dsn);

    ConnectionState *connectionState = (ConnectionState *)malloc(sizeof(ConnectionState));
    connectionState->hEnv = hEnv;
    connectionState->hDbc = hDbc;

    (*env)->ReleaseStringUTFChars(env, jdsn, dsn);

    return (jlong)(intptr_t)connectionState;
}

// Función para cerrar la conexión y liberar recursos
JNIEXPORT void JNICALL Java_odbcbridge_ODBCBridge_close(
    JNIEnv *env, jobject obj, jlong connectionPtr
) {
    ConnectionState *connectionState = (ConnectionState *)(intptr_t)connectionPtr;

    if (connectionState != NULL) {
        if (connectionState->hDbc != SQL_NULL_HDBC) {
            SQLDisconnect(connectionState->hDbc);
            SQLFreeHandle(SQL_HANDLE_DBC, connectionState->hDbc);
        }
        if (connectionState->hEnv != SQL_NULL_HENV) {
            SQLFreeHandle(SQL_HANDLE_ENV, connectionState->hEnv);
        }
        free(connectionState);
    }
}

// Optiene la información de la base de datos.
JNIEXPORT jobject JNICALL Java_odbcbridge_ODBCBridge_getDatabaseInfo(
    JNIEnv *env, jobject obj, jlong connectionPtr
) {
    ConnectionState *conn = (ConnectionState *)(intptr_t)connectionPtr;

    SQLCHAR dbmsName[256], dbmsVer[256], drvName[256], drvVer[256], srvName[256], userName[256];

    SQLGetInfo(conn->hDbc, SQL_DBMS_NAME, dbmsName, sizeof(dbmsName), NULL);
    SQLGetInfo(conn->hDbc, SQL_DBMS_VER, dbmsVer, sizeof(dbmsVer), NULL);
    SQLGetInfo(conn->hDbc, SQL_DRIVER_NAME, drvName, sizeof(drvName), NULL);
    SQLGetInfo(conn->hDbc, SQL_DRIVER_VER, drvVer, sizeof(drvVer), NULL);
    SQLGetInfo(conn->hDbc, SQL_SERVER_NAME, srvName, sizeof(srvName), NULL);
    SQLGetInfo(conn->hDbc, SQL_USER_NAME, userName, sizeof(userName), NULL);

    jclass cls = (*env)->FindClass(env, "odbcbridge/ODBCInfo");
    jmethodID constructor = (*env)->GetMethodID(env, cls, "<init>",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V");

    jstring jdbmsName = (*env)->NewStringUTF(env, (char *)dbmsName);
    jstring jdbmsVer  = (*env)->NewStringUTF(env, (char *)dbmsVer);
    jstring jdrvName  = (*env)->NewStringUTF(env, (char *)drvName);
    jstring jdrvVer   = (*env)->NewStringUTF(env, (char *)drvVer);
    jstring jsrvName  = (*env)->NewStringUTF(env, (char *)srvName);
    jstring juser     = (*env)->NewStringUTF(env, (char *)userName);

    return (*env)->NewObject(env, cls, constructor,
        jdbmsName, jdbmsVer, jdrvName, jdrvVer, jsrvName, juser);
}

// Función para listar bases de datos
JNIEXPORT jobjectArray JNICALL Java_odbcbridge_ODBCBridge_listDatabases
  (JNIEnv *env, jobject obj) {

    SQLHENV hEnv = setup_environment(env);
    SQLCHAR dsnName[256];
    SQLCHAR description[256];
    SQLSMALLINT dsnNameLen, descriptionLen;
    jobjectArray result;
    int rowCount = 0;

    // Contar el número de DSNs
    SQLRETURN ret = SQLDataSources(hEnv, SQL_FETCH_FIRST, dsnName, sizeof(dsnName), &dsnNameLen, description, sizeof(description), &descriptionLen);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        rowCount++;
        while (SQLDataSources(hEnv, SQL_FETCH_NEXT, dsnName, sizeof(dsnName), &dsnNameLen, description, sizeof(description), &descriptionLen) != SQL_NO_DATA) {
            rowCount++;
        }
    }

    result = (*env)->NewObjectArray(env, rowCount, (*env)->FindClass(env, "java/lang/String"), NULL);

    // Obtener los nombres de DSN
    ret = SQLDataSources(hEnv, SQL_FETCH_FIRST, dsnName, sizeof(dsnName), &dsnNameLen, description, sizeof(description), &descriptionLen);
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        int i = 0;
        (*env)->SetObjectArrayElement(env, result, i, (*env)->NewStringUTF(env, (char *)dsnName));
        while (SQLDataSources(hEnv, SQL_FETCH_NEXT, dsnName, sizeof(dsnName), &dsnNameLen, description, sizeof(description), &descriptionLen) != SQL_NO_DATA) {
            i++;
            (*env)->SetObjectArrayElement(env, result, i, (*env)->NewStringUTF(env, (char *)dsnName));
        }
    }

    SQLFreeHandle(SQL_HANDLE_ENV, hEnv);

    return result;
}

// Función para listar tablas
JNIEXPORT jobjectArray JNICALL Java_odbcbridge_ODBCBridge_listTables(
    JNIEnv *env, jobject obj, jlong connectionPtr
) {
    ConnectionState *connectionState = (ConnectionState *)(intptr_t)connectionPtr;
    SQLHSTMT hStmt = init_statement(env, connectionState->hDbc);

    SQLRETURN ret = SQLTables(hStmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR *)"TABLE", SQL_NTS);
    check_error(env, ret, SQL_HANDLE_STMT, hStmt, "Failed to execute SQLTables");

    SQLCHAR tableName[256];
    SQLLEN nameLength;
    jobjectArray result;
    int rowCount = 0;

    while ((ret = SQLFetch(hStmt)) != SQL_NO_DATA) {
        rowCount++;
    }

    result = (*env)->NewObjectArray(env, rowCount, (*env)->FindClass(env, "java/lang/String"), NULL);

    SQLCloseCursor(hStmt);
    SQLTables(hStmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR *)"TABLE", SQL_NTS);

    int i = 0;
    while ((ret = SQLFetch(hStmt)) != SQL_NO_DATA) {
        ret = SQLGetData(hStmt, 3, SQL_C_CHAR, tableName, sizeof(tableName), &nameLength);
        check_error(env, ret, SQL_HANDLE_STMT, hStmt, "Failed to get table name");

        (*env)->SetObjectArrayElement(env, result, i, (*env)->NewStringUTF(env, (char *)tableName));
        i++;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);

    return result;
}

// Funcion par alistar los comapos de una tabla
JNIEXPORT jobjectArray JNICALL Java_odbcbridge_ODBCBridge_listColumns(
    JNIEnv *env, jobject obj, jlong connectionPtr, jstring jtableName
) {
    ConnectionState *connectionState = (ConnectionState *)(intptr_t)connectionPtr;
    const char *tableName = (*env)->GetStringUTFChars(env, jtableName, 0);

    SQLHSTMT hStmt = init_statement(env, connectionState->hDbc);
    SQLRETURN ret = SQLColumns(hStmt, NULL, 0, NULL, 0, (SQLCHAR *)tableName, SQL_NTS, NULL, 0);
    check_error(env, ret, SQL_HANDLE_STMT, hStmt, "Failed to retrieve columns");

    // Contar número de columnas
    int rowCount = 0;
    while ((ret = SQLFetch(hStmt)) != SQL_NO_DATA) {
        rowCount++;
    }

    // Preparar clase Java: odbcbridge.ODBCField
    jclass clsODBCField = (*env)->FindClass(env, "odbcbridge/ODBCField");
    if (clsODBCField == NULL) return NULL;

    jmethodID constructor = (*env)->GetMethodID(env, clsODBCField, "<init>", "(Ljava/lang/String;II)V");
    if (constructor == NULL) return NULL;

    jobjectArray result = (*env)->NewObjectArray(env, rowCount, clsODBCField, NULL);

    // Reiniciar cursor para leer de nuevo
    SQLCloseCursor(hStmt);
    SQLColumns(hStmt, NULL, 0, NULL, 0, (SQLCHAR *)tableName, SQL_NTS, NULL, 0);

    SQLCHAR colName[256];
    SQLSMALLINT dataType;
    SQLINTEGER colSize;
    SQLLEN indName, indType, indSize;

    int i = 0;
    while ((ret = SQLFetch(hStmt)) != SQL_NO_DATA) {
        SQLGetData(hStmt, 4, SQL_C_CHAR, colName, sizeof(colName), &indName);   // COLUMN_NAME
        SQLGetData(hStmt, 5, SQL_C_SSHORT, &dataType, 0, &indType);             // DATA_TYPE
        SQLGetData(hStmt, 7, SQL_C_SLONG, &colSize, 0, &indSize);               // COLUMN_SIZE

        jstring jname = (*env)->NewStringUTF(env, (char *)colName);
        jobject field = (*env)->NewObject(env, clsODBCField, constructor, jname, dataType, colSize);
        (*env)->SetObjectArrayElement(env, result, i++, field);
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    (*env)->ReleaseStringUTFChars(env, jtableName, tableName);
    return result;
}

// Función para inicializar una consulta SQL
JNIEXPORT jlong JNICALL Java_odbcbridge_ODBCBridge_query(
    JNIEnv *env, jobject obj, jlong connectionPtr, jstring jsql
) {
    ConnectionState *connectionState = (ConnectionState *)(intptr_t)connectionPtr;
    const char *sql = (*env)->GetStringUTFChars(env, jsql, 0);

    SQLHSTMT hStmt = init_statement(env, connectionState->hDbc);

    SQLRETURN ret = SQLExecDirect(hStmt, (SQLCHAR *)sql, SQL_NTS);
    check_error(env, ret, SQL_HANDLE_STMT, hStmt, "Failed to execute SQL query");

    QueryState *queryState = (QueryState *)malloc(sizeof(QueryState));
    queryState->hStmt = hStmt;
    queryState->connectionState = connectionState;

    (*env)->ReleaseStringUTFChars(env, jsql, sql);

    return (jlong)(intptr_t)queryState;
}

// Función para obtener datos de una fila de resultados
JNIEXPORT jobjectArray JNICALL Java_odbcbridge_ODBCBridge_fetchArray(
    JNIEnv *env, jobject obj, jlong queryPtr
) {
    QueryState *queryState = (QueryState *)(intptr_t)queryPtr;

    SQLRETURN ret;
    SQLSMALLINT columns;
    SQLNumResultCols(queryState->hStmt, &columns);

    SQLCHAR buffer[256];
    SQLLEN indicator;
    jobjectArray row = (*env)->NewObjectArray(env, columns, (*env)->FindClass(env, "java/lang/String"), NULL);

    ret = SQLFetch(queryState->hStmt);
    if (ret == SQL_NO_DATA) {
        return NULL;
    }

    check_error(env, ret, SQL_HANDLE_STMT, queryState->hStmt, "Failed to fetch data");

    for (int j = 0; j < columns; j++) {
        ret = SQLGetData(queryState->hStmt, j + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
        check_error(env, ret, SQL_HANDLE_STMT, queryState->hStmt, "Failed to get data");

        if (indicator == SQL_NULL_DATA) {
            (*env)->SetObjectArrayElement(env, row, j, NULL);
        } else {
            (*env)->SetObjectArrayElement(env, row, j, (*env)->NewStringUTF(env, (char *)buffer));
        }
    }

    return row;
}

// Función para obtener los nombres de las columnas
JNIEXPORT jobjectArray JNICALL Java_odbcbridge_ODBCBridge_fetchFields(
    JNIEnv *env, jobject obj, jlong queryPtr
) {
    QueryState *queryState = (QueryState *)(intptr_t)queryPtr;

    SQLSMALLINT columnCount;
    SQLRETURN ret = SQLNumResultCols(queryState->hStmt, &columnCount);
    check_error(env, ret, SQL_HANDLE_STMT, queryState->hStmt, "Failed to get column count");

    jclass clsODBCField = (*env)->FindClass(env, "odbcbridge/ODBCField");
    jmethodID constructor = (*env)->GetMethodID(env, clsODBCField, "<init>", 
                                                "(Ljava/lang/String;II)V");

    jobjectArray fieldArray = (*env)->NewObjectArray(env, columnCount, clsODBCField, NULL);

    for (int i = 1; i <= columnCount; i++) {
        SQLCHAR colName[256];
        SQLSMALLINT dataType;
        SQLULEN colSize;
        SQLSMALLINT nullable;

        ret = SQLDescribeCol(queryState->hStmt, i, colName, sizeof(colName), NULL, &dataType, &colSize, NULL, &nullable);
        check_error(env, ret, SQL_HANDLE_STMT, queryState->hStmt, "Failed to describe column");

        jstring jColName = (*env)->NewStringUTF(env, (char *)colName);
        jobject odbcField = (*env)->NewObject(env, clsODBCField, constructor, jColName, dataType, colSize);

        (*env)->SetObjectArrayElement(env, fieldArray, i - 1, odbcField);
    }

    return fieldArray;
}

// Función para cerrar la consulta y liberar recursos
JNIEXPORT void JNICALL Java_odbcbridge_ODBCBridge_free(
    JNIEnv *env, jobject obj, jlong queryPtr
) {
    QueryState *queryState = (QueryState *)(intptr_t)queryPtr;

    if (queryState != NULL) {
        if (queryState->hStmt != SQL_NULL_HSTMT) {
            SQLFreeHandle(SQL_HANDLE_STMT, queryState->hStmt);
        }
        free(queryState);
    }
}
