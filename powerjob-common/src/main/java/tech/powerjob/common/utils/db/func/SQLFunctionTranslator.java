package tech.powerjob.common.utils.db.func;

import cn.hutool.core.exceptions.ExceptionUtil;
import tech.powerjob.common.utils.PlaceholderFrameworkUtils;
import tech.powerjob.common.utils.StriUtils;
import tech.powerjob.common.utils.db.dialect.Dialect;
import tech.powerjob.common.utils.db.dialect.GuessDialectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class SQLFunctionTranslator {


    private static Logger logger = LoggerFactory.getLogger(SQLFunctionTranslator.class);


    /**
     * Guess Dialect by given connection, note:this method does not close connection
     *
     * @param connection The JDBC Connection
     * @return Dialect The Dialect intance, if can not guess out, return null
     */
    public static Dialect guessDialect(Connection connection) {
        return GuessDialectUtils.guessDialect(connection);
    }

    /**
     * Guess Dialect by given data source
     *
     * @param datasource
     * @return Dialect
     */
    public static Dialect guessDialect(DataSource datasource) {
        return GuessDialectUtils.guessDialect(datasource);
    }

    /**
     * Paginate and Translate a SQL
     */
    public static String paginAndTrans(Dialect dialect, int pageNumber, int pageSize, String sqlTemplate, Map contextData) {
        return dialect.pagin(pageNumber, pageSize, trans(dialect, sqlTemplate, contextData));
    }


    public static String paginAndTrans(Connection connection, int pageNumber, int pageSize, String sqlTemplate, Map contextData) {

        Dialect dialect = guessDialect(connection);

        return dialect.pagin(pageNumber, pageSize, trans(dialect, sqlTemplate, contextData));
    }


    public static String paginAndTrans(String dbName, int pageNumber, int pageSize, String sqlTemplate, Map contextData) {

        Dialect dialect = Dialect.getDialect(dbName);

        return dialect.pagin(pageNumber, pageSize, trans(dialect, sqlTemplate, contextData));
    }


    public static String trans(Dialect dialect, String templateSQl, Map contextData) {
        try {
            if (contextData == null) {
                contextData = new HashMap();
            }
            if (templateSQl == null) {
                return "";
            }
            //替换sql语句中的--注释
            templateSQl = StriUtils.filterSqlComment(templateSQl);
            templateSQl = templateSQl.trim();//去掉前后空格
            if (templateSQl.startsWith("select") || templateSQl.startsWith("SELECT")) {
                templateSQl = "select " + templateSQl.substring(6);
            }
//            contextData.put("_dbType", dialect.toString());
            contextData.put(Dialect.class.getCanonicalName(), dialect);
            return PlaceholderFrameworkUtils.convert(templateSQl, contextData);
        } catch (Exception e) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ExceptionUtil.stacktraceToString(e));
            throw new RuntimeException(stringBuilder.toString());
        }
    }

    public static String trans(DataSource datasource, String sqlTemplate, Map contextData) {
        return trans(guessDialect(datasource), sqlTemplate, contextData);

    }

    public static String trans(DataSource datasource, String sqlTemplate) {
        return trans(guessDialect(datasource), sqlTemplate, null);

    }

    public static String trans(Connection connection, String sqlTemplate, Map contextData) {

        return trans(guessDialect(connection), sqlTemplate, contextData);

    }

    public static String trans(String dbName, String sqlTemplate, Map contextData) {

        return trans(Dialect.getDialect(dbName), sqlTemplate, contextData);

    }

    public static String trans(String dbName, String sqlTemplate) {

        return trans(Dialect.getDialect(dbName), sqlTemplate, null);

    }
}
