package tech.powerjob.common.utils.db.dialect;

import lombok.extern.slf4j.Slf4j;
import tech.powerjob.common.exception.ErrorCode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static tech.powerjob.common.exception.ServiceExceptionUtil.exception;


/**
 * Guess Dialect Utils
 */
@Slf4j
@SuppressWarnings("all")
public class GuessDialectUtils {
    private static final Map<DataSource, Dialect> dataSourceDialectCache = new ConcurrentHashMap<DataSource, Dialect>();

    /**
     * Guess dialect based on given JDBC connection instance, Note: this method does
     * not close connection
     *
     * @param jdbcConnection The connection
     * @return dialect or null if can not guess out which dialect
     */
    public static Dialect guessDialect(Connection jdbcConnection) {
        String databaseName = "";
        String driverName = "";
        int majorVersion = 0;
        int minorVersion = 0;
        try {
            DatabaseMetaData meta = jdbcConnection.getMetaData();
            databaseName = meta.getDatabaseProductName();
            driverName = meta.getDriverName();
            majorVersion = meta.getDatabaseMajorVersion();
            minorVersion = meta.getDatabaseMinorVersion();
        } catch (SQLException e) {
            log.info("excption:" + e);
        }
        return guessDialect(driverName, databaseName, majorVersion, minorVersion);
    }

    /**
     * Guess dialect based on given dataSource
     *
     * @param datasource The dataSource
     * @return dialect or null if can not guess out which dialect
     */
    public static Dialect guessDialect(DataSource dataSource) {
        Dialect result = dataSourceDialectCache.get(dataSource);
        if (result != null) return result;
        Connection con = null;
        try {
            con = dataSource.getConnection();
            result = guessDialect(con);
            if (result == null) {
                throw exception(new ErrorCode(999, "Can not get dialect from DataSource, please submit this bug."));
            }
            dataSourceDialectCache.put(dataSource, result);
            return result;
        } catch (SQLException e) {
            throw exception(new ErrorCode(999, "Dialect guessDialect:" + e.getMessage()));
        } finally {
            try {
                if (con != null && !con.isClosed()) {
                    try {// NOSONAR
                        con.close();
                    } catch (SQLException e) {
                        throw exception(new ErrorCode(999, "Dialect guessDialect:" + e.getMessage()));
                    }
                }
            } catch (SQLException e) {
                throw exception(new ErrorCode(999, "Dialect guessDialect:" + e.getMessage()));
            }
        }
    }

    /**
     * @param databaseName             The database name
     * @param majorVersionMinorVersion The major version,The minor version, Optional optional
     * @return dialect or null if not found
     */
    public static Dialect guessDialect(String driverName, String databaseName, Object... majorVersionMinorVersion) {// NOSONAR
        int majorVersion = 0;
        int minorVersion = 0;
        //用于扩展
        for (int i = 0; i < majorVersionMinorVersion.length; i++) {
            if (i == 0) majorVersion = (Integer) majorVersionMinorVersion[i];
            if (i == 1) minorVersion = (Integer) majorVersionMinorVersion[i];
        }

        log.info("databaseName:{}", databaseName);

        if ("Impala".equals(databaseName)) {

            return Dialect.IMPALA;
        }

        if ("Apache Hive".equals(databaseName)) {

            return Dialect.HIVE;
        }

        if ("MySQL".equals(databaseName)) {

            return Dialect.MYSQL;
        }

        if ("Oracle".equals(databaseName)) {
            return Dialect.ORACLE;
        }

//		if ("PostgreSQL".equals(databaseName)) {
//			return Dialect.POSTGRE_SQL;
//		}
        //return Dialect.OTHER;
        return null;
    }
}
