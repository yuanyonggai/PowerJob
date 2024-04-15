package tech.powerjob.common.utils.db.dialect;

/**
 * Initialize pagination templates
 */
@SuppressWarnings("all")
public class DialectPaginationTemplate {

	/**
	 * Return pagination template of this Dialect
	 */
	protected static String initializePaginSQLTemplate(Dialect d) {
		switch (d) {
		case GAUSS200:
			return "select * from ( select $BODY ) amlalias limit $PAGESIZE offset $SKIP_ROWS";
		case MYSQL:
			return "select * from ( select $BODY ) amlalias limit $SKIP_ROWS, $PAGESIZE";
        case IMPALA:
            return "select * from ( select $BODY ) amlalias limit $PAGESIZE offset $SKIP_ROWS";
        case HIVE:
            return "select * from ( select $BODY ) amlalias limit $SKIP_ROWS, $PAGESIZE";
		case ORACLE:
			return "select * from ( select row_.*, rownum rownum_ from ( select $BODY ) row_ where rownum <= $TOTAL_ROWS) where rownum_ > $SKIP_ROWS";
        case GBASE8A:
            return "select * from ( select $BODY ) amlalias limit $PAGESIZE offset $SKIP_ROWS";
        case DM:
            return "select * from ( select row_.*, rownum rownum_ from ( select $BODY ) row_ where rownum <= $TOTAL_ROWS) where rownum_ > $SKIP_ROWS";
		default:
			return Dialect.NOT_SUPPORT;
		}
	}

	/**
	 * Return top limit sql template of this Dialect
	 */
	protected static String initializeTopLimitSqlTemplate(Dialect d) {
		switch (d) {
//		case DB2:
//			return "select $BODY fetch first $pagesize rows only";
		case MYSQL:
			return "select * from ( select $BODY ) amlalias limit $PAGESIZE";
		case GAUSS200:
			return "select * from ( select $BODY ) amlalias limit $PAGESIZE";
        case IMPALA:
			return "select * from ( select $BODY ) amlalias limit $PAGESIZE";
        case HIVE:
			return "select * from ( select $BODY ) amlalias limit $PAGESIZE";
		case ORACLE:
			return "select * from ( select $BODY ) where rownum <= $PAGESIZE";
        case GBASE8A:
            return "select * from ( select $BODY ) amlalias limit $PAGESIZE";
        case DM:
            return "select * from ( select $BODY ) where rownum <= $PAGESIZE";
        default:
			return Dialect.NOT_SUPPORT;
		}
	}

}
