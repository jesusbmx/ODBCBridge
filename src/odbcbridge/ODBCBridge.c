// ODBCBridge.c
#include "ODBCBridge.h"
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <time.h>

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

/*
 * Class:     odbcbridge_ODBCBridge
 * Method:    connectWithString
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_odbcbridge_ODBCBridge_connectWithString(
    JNIEnv *env, jobject obj,
    jstring jconnStr
) {
    const char *connStr = (*env)->GetStringUTFChars(env, jconnStr, 0);

    // 1) Setup ODBC environment
    SQLHENV hEnv = setup_environment(env);

    // 2) Alloc handle de conexión y timeout
    SQLHDBC hDbc = SQL_NULL_HDBC;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);
    check_error(env, ret, SQL_HANDLE_ENV, hEnv, "Failed to alloc DBC handle");

    // timeout de login opcional
    SQLSetConnectAttr(hDbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

    // 3) Conectar usando cadena completa
    SQLCHAR outConnStr[1024];
    SQLSMALLINT outConnStrLen = 0;
    ret = SQLDriverConnect(
        hDbc,
        NULL,
        (SQLCHAR*)connStr, SQL_NTS,
        outConnStr, sizeof(outConnStr), &outConnStrLen,
        SQL_DRIVER_NOPROMPT
    );
    check_error(env, ret, SQL_HANDLE_DBC, hDbc, "Failed SQLDriverConnect");

    (*env)->ReleaseStringUTFChars(env, jconnStr, connStr);

    // 4) Guardar estado
    ConnectionState *state = (ConnectionState*)malloc(sizeof(ConnectionState));
    state->hEnv = hEnv;
    state->hDbc = hDbc;

    // 5) Devolver puntero
    return (jlong)(intptr_t)state;
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

    SQLSMALLINT columnCount;
    if (SQLNumResultCols(queryState->hStmt, &columnCount) != SQL_SUCCESS) {
        return NULL;
    }

    jobjectArray rowArray = (*env)->NewObjectArray(env, columnCount, (*env)->FindClass(env, "java/lang/Object"), NULL);
    if (rowArray == NULL) return NULL;

    SQLLEN indicator;
    SQLSMALLINT dataType;
    SQLRETURN ret = SQLFetch(queryState->hStmt);

    if (ret == SQL_NO_DATA) {
        return NULL;
    } else if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        return NULL;
    }

    for (int i = 1; i <= columnCount; i++) {
        SQLDescribeCol(queryState->hStmt, i, NULL, 0, NULL, &dataType, NULL, NULL, NULL);
        jobject value = NULL;

        switch (dataType) {

            // Integer Types
            case SQL_INTEGER:
            case SQL_SMALLINT:
            case SQL_TINYINT: {
                SQLINTEGER intValue;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_SLONG, &intValue, sizeof(intValue), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    value = (*env)->NewObject(env, (*env)->FindClass(env, "java/lang/Integer"), (*env)->GetMethodID(env, (*env)->FindClass(env, "java/lang/Integer"), "<init>", "(I)V"), intValue);
                }
                break;
            }

            case SQL_BIGINT: {
                SQLBIGINT bigIntValue;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_SBIGINT, &bigIntValue, sizeof(bigIntValue), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    value = (*env)->NewObject(env, (*env)->FindClass(env, "java/lang/Long"), (*env)->GetMethodID(env, (*env)->FindClass(env, "java/lang/Long"), "<init>", "(J)V"), bigIntValue);
                }
                break;
            }

            // Floating Point Types
            case SQL_REAL:
            case SQL_FLOAT: {
                SQLREAL floatValue;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_FLOAT, &floatValue, sizeof(floatValue), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    value = (*env)->NewObject(env, (*env)->FindClass(env, "java/lang/Float"), (*env)->GetMethodID(env, (*env)->FindClass(env, "java/lang/Float"), "<init>", "(F)V"), floatValue);
                }
                break;
            }

            case SQL_DOUBLE: {
                SQLDOUBLE doubleValue;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_DOUBLE, &doubleValue, sizeof(doubleValue), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    value = (*env)->NewObject(env, (*env)->FindClass(env, "java/lang/Double"), (*env)->GetMethodID(env, (*env)->FindClass(env, "java/lang/Double"), "<init>", "(D)V"), doubleValue);
                }
                break;
            }

            // Decimal and Numeric Types
            case SQL_NUMERIC:
            case SQL_DECIMAL: {
                SQLCHAR buffer[256];
                ret = SQLGetData(queryState->hStmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    jclass bigDecimalClass = (*env)->FindClass(env, "java/math/BigDecimal");
                    jmethodID constructor = (*env)->GetMethodID(env, bigDecimalClass, "<init>", "(Ljava/lang/String;)V");
                    jstring valueStr = (*env)->NewStringUTF(env, (char *)buffer);
                    value = (*env)->NewObject(env, bigDecimalClass, constructor, valueStr);
                }
                break;
            }

            // Binary Data Types
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY: {
                SQLLEN length = 0;
                SQLGetData(queryState->hStmt, i, SQL_C_BINARY, NULL, 0, &length);

                if (length > 0) {
                    jbyteArray byteArray = (*env)->NewByteArray(env, length);
                    if (byteArray != NULL) {
                        char *buffer = (char *)malloc(length);
                        SQLGetData(queryState->hStmt, i, SQL_C_BINARY, buffer, length, &indicator);
                        (*env)->SetByteArrayRegion(env, byteArray, 0, length, (jbyte *)buffer);
                        free(buffer);
                        value = byteArray;
                    }
                }
                break;
            }

            // String Types
            case SQL_CHAR:
            case SQL_VARCHAR:
            case SQL_LONGVARCHAR:
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR: {
                SQLCHAR buffer[256];
                ret = SQLGetData(queryState->hStmt, i, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    value = (*env)->NewStringUTF(env, (char *)buffer);
                }
                break;
            }

            // Date/Time Types
            case SQL_DATE:
            case SQL_TYPE_DATE: {
                DATE_STRUCT dateStruct;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_TYPE_DATE, &dateStruct, sizeof(dateStruct), &indicator);

                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    jclass dateClass = (*env)->FindClass(env, "java/sql/Date");
                    jmethodID dateConstructor = (*env)->GetMethodID(env, dateClass, "<init>", "(J)V");

                    // Crear el `java.sql.Date` utilizando `java.util.Calendar`
                    struct tm tmDate = { 0 };
                    tmDate.tm_year = dateStruct.year - 1900;
                    tmDate.tm_mon = dateStruct.month - 1;
                    tmDate.tm_mday = dateStruct.day;

                    time_t timeInSeconds = mktime(&tmDate);
                    jlong timeInMillis = (jlong)timeInSeconds * 1000;

                    value = (*env)->NewObject(env, dateClass, dateConstructor, timeInMillis);
                }
                break;
            }

            case SQL_TIME:
            case SQL_TYPE_TIME: {
                TIME_STRUCT timeStruct;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_TYPE_TIME, &timeStruct, sizeof(timeStruct), &indicator);
                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    jclass timeClass = (*env)->FindClass(env, "java/sql/Time");
                    jmethodID timeConstructor = (*env)->GetMethodID(env, timeClass, "<init>", "(J)V");

                    long timeInMillis = (timeStruct.hour * 3600 + timeStruct.minute * 60 + timeStruct.second) * 1000;
                    value = (*env)->NewObject(env, timeClass, timeConstructor, timeInMillis);
                }
                break;
            }

            case SQL_TIMESTAMP:
            case SQL_TYPE_TIMESTAMP: {
                TIMESTAMP_STRUCT timestampStruct;
                ret = SQLGetData(queryState->hStmt, i, SQL_C_TYPE_TIMESTAMP, &timestampStruct, sizeof(timestampStruct), &indicator);

                if (ret == SQL_SUCCESS && indicator != SQL_NULL_DATA) {
                    jclass timestampClass = (*env)->FindClass(env, "java/sql/Timestamp");
                    jmethodID timestampConstructor = (*env)->GetMethodID(env, timestampClass, "<init>", "(J)V");

                    struct tm tmTimestamp = { 0 };
                    tmTimestamp.tm_year = timestampStruct.year - 1900;
                    tmTimestamp.tm_mon = timestampStruct.month - 1;
                    tmTimestamp.tm_mday = timestampStruct.day;
                    tmTimestamp.tm_hour = timestampStruct.hour;
                    tmTimestamp.tm_min = timestampStruct.minute;
                    tmTimestamp.tm_sec = timestampStruct.second;

                    time_t timeInSeconds = mktime(&tmTimestamp);
                    jlong timeInMillis = ((jlong)timeInSeconds * 1000) + (timestampStruct.fraction / 1000000);

                    value = (*env)->NewObject(env, timestampClass, timestampConstructor, timeInMillis);
                }
                break;
            }

            default:
                // Default case para capturar cualquier tipo no manejado explícitamente
                char typeInfo[128];
                snprintf(typeInfo, sizeof(typeInfo), "Tipo no manejado: %d", dataType);

                // Retornamos el tipo como un String
                value = (*env)->NewStringUTF(env, typeInfo);
                break;
        }

        (*env)->SetObjectArrayElement(env, rowArray, i - 1, value);
    }

    return rowArray;
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
