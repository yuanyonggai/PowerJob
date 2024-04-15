package tech.powerjob.common.utils.db.dialect;

import cn.hutool.core.util.StrUtil;
import tech.powerjob.common.exception.ErrorCode;
import tech.powerjob.common.utils.db.func.SQLFunction;
import tech.powerjob.common.utils.db.func.impl.HiveFunction;
import tech.powerjob.common.utils.db.func.impl.ImpalaFunction;
import tech.powerjob.common.utils.db.func.impl.MysqlFunction;
import tech.powerjob.common.utils.db.func.impl.OracleFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static tech.powerjob.common.exception.ServiceExceptionUtil.exception;

public enum Dialect {
    MYSQL("mysql", new MysqlFunction(), "MySql数据库"),
    ORACLE("oracle", new OracleFunction(), "Oracle数据库"),
    IMPALA("impala", new ImpalaFunction(), "Impala数据源"),
    HIVE("hive", new HiveFunction(), "Apache Hive数据源"),
    //POSTGRE_SQL("postgresql", new PostgreSQLFunction(), "Postgre数据库"),
    OTHER("other", null, "其他");

    public static final String NOT_SUPPORT = "NOT_SUPPORT";
    private static final Logger logger = LoggerFactory.getLogger(Dialect.class);
    private static final String SKIP_ROWS = "$SKIP_ROWS";
    private static final String PAGESIZE = "$PAGESIZE";
    private static final String TOTAL_ROWS = "$TOTAL_ROWS";
    private static final String DISTINCT_TAG = "($DISTINCT)";

    /**
     * If set true will output log for each paginate, translate, paginAndTranslate,
     * toCreateDDL, toDropAndCreateDDL, toDropDDL method call, default value is
     * false
     */
    private static Boolean globalAllowShowSql = false;

    static {
        for (Dialect d : Dialect.values()) {
            d.sqlTemplate = DialectPaginationTemplate.initializePaginSQLTemplate(d);
            d.topLimitTemplate = DialectPaginationTemplate.initializeTopLimitSqlTemplate(d);
        }
    }

    private final String db;
    private final String desc;
    private final SQLFunction func;
    private String sqlTemplate = null;
    private String topLimitTemplate = null;

    private Dialect(String db, SQLFunction func, String desc) {
        this.db = db;
        this.func = func;
        this.desc = desc;
    }

    public static Dialect getDialect(String dbName) {
        Dialect[] dts = values();
        Dialect[] tmp = dts;
        int len = dts.length;
        for (int i = 0; i < len; ++i) {
            Dialect dt = tmp[i];
            if (dt.getDb().equalsIgnoreCase(dbName)) {
                return dt;
            }
        }
        return OTHER;
    }


    public static Boolean getGlobalAllowShowSql() {
        return globalAllowShowSql;
    }

    /**
     * Note! this is a global method to set globalAllowShowSql
     */
    public static void setGlobalAllowShowSql(Boolean ifAllowShowSql) {
        Dialect.globalAllowShowSql = ifAllowShowSql;
    }

    /**
     * An example tell users how to use a top limit SQL for a dialect
     */
    private static String aTopLimitSqlExample(String template) {
        String result = StrUtil.replaceIgnoreCase(template, "$SQL", "select * from users order by userid");
        result = StrUtil.replaceIgnoreCase(result, "$BODY", "* from users order by userid");
        result = StrUtil.replaceIgnoreCase(result, " " + DISTINCT_TAG, "");
        result = StrUtil.replaceIgnoreCase(result, SKIP_ROWS, "0");
        result = StrUtil.replaceIgnoreCase(result, PAGESIZE, "10");
        result = StrUtil.replaceIgnoreCase(result, TOTAL_ROWS, "10");
        return result;
    }

    public String getDb() {
        return this.db;
    }

    public String getDesc() {
        return this.desc;
    }

    public SQLFunction getFunc() {
        return this.func;
    }

    public String pagin(int pageNumber, int pageSize, String sql) {// NOSONAR
        String result = null;
        if (sql == null) {
            throw exception(new ErrorCode(999, "Dialect pagin() SQL不能为空"));
        }
        String trimedSql = sql.trim();

        if (!StrUtil.startWithIgnoreCase(trimedSql, "select ")) {
            throw exception(new ErrorCode(999, "Dialect pagin() 请输入以select开头的 SQL 查询语句"));
        }
        String body = trimedSql.substring(7).trim();

        int skipRows = (pageNumber - 1) * pageSize;
        int skipRowsPlus1 = skipRows + 1;
        int totalRows = pageNumber * pageSize;
        int totalRowsPlus1 = totalRows + 1;
        String useTemplate;
        if (skipRows == 0) {
            useTemplate = topLimitTemplate;
        } else {
            useTemplate = sqlTemplate;
        }

        if (Dialect.NOT_SUPPORT.equals(useTemplate)) {
            throw exception(new ErrorCode(999, "Dialect does not support physical pagination"));
        }

        if (useTemplate.contains(DISTINCT_TAG)) {
            // if distinct template use non-distinct sql, delete distinct tag
            if (!StrUtil.startWithIgnoreCase(body, "distinct "))
                useTemplate = StrUtil.replace(useTemplate, DISTINCT_TAG, "");
            else {
                // if distinct template use distinct sql, use it
                useTemplate = StrUtil.replace(useTemplate, DISTINCT_TAG, "distinct");
                body = body.substring(9);
            }
        }

        // if have $XXX tag, replaced by real values
        result = StrUtil.replaceIgnoreCase(useTemplate, SKIP_ROWS, String.valueOf(skipRows));
        result = StrUtil.replaceIgnoreCase(result, PAGESIZE, String.valueOf(pageSize));
        result = StrUtil.replaceIgnoreCase(result, TOTAL_ROWS, String.valueOf(totalRows));

        // now insert the customer's real full SQL here
        result = StrUtil.replace(result, "$SQL", trimedSql);

        // or only insert the body without "select "
        result = StrUtil.replace(result, "$BODY", body);
        if (getGlobalAllowShowSql()) {
            logger.info("Paginated sql: " + result);
        }
        return result;
    }


}

