package tech.powerjob.common.utils.db.func.impl;

import org.apache.commons.lang3.StringUtils;

import tech.powerjob.common.utils.JobDateUtil;
import tech.powerjob.common.utils.db.func.SQLFunction;

import java.util.Map;

public abstract class AbstractSQLFunction implements SQLFunction {


    private Map context;

    @Override
    public Map getContext() {
        return context;
    }

    @Override
    public void setContext(Map context) {
        this.context = context;
    }

    @Override
    public String now() {
        return UNIMPLEMENTED_FUNCTION;
    }

    @Override
    public String str_to_date(String dateStr,String dateFormatterStr){

        return UNIMPLEMENTED_FUNCTION;
    }
    @Override
    public String field_to_date_func(String field,String dateFormatterStr){

        return UNIMPLEMENTED_FUNCTION;
    }
    @Override
    public String field_date_to_char_func(String field,String dateFormatterStr){

        return UNIMPLEMENTED_FUNCTION;
    }

    /////////    可以使用的方法      ///////////////////////////////////
    @Override
    public String concat(String... vars) {
        String concatSql = "concat(";
        for (int i = 0; i < vars.length; i++) {
            String var = vars[i];
            if (var.startsWith(COLUMN_FIELD_FLAG) && var.length() > 1) {
                if (var.substring(1).startsWith("ifnull")) {
                    concatSql += var.substring(1);
                } else {
                    concatSql += "ifnull(" + var.substring(1) + ",'')";
                }
            } else {
                concatSql += "'" + var + "'";
            }
            if (i < vars.length - 1) concatSql += ",";
        }
        return concatSql + ")";
    }

    @Override
    public String nvl_str(String column, String value) {
        column = column.trim();
        String flagcolumn = "";
        if (column.startsWith(COLUMN_FIELD_FLAG)) {
            flagcolumn = COLUMN_FIELD_FLAG;
            column = column.substring(1);
        }
        return flagcolumn + "ifnull(" + column + ",'" + value + "')";
    }

    @Override
    public String nvl_number(String column, String value) {
        column = column.trim();
        String flagcolumn = "";
        if (column.startsWith(COLUMN_FIELD_FLAG)) {
            flagcolumn = COLUMN_FIELD_FLAG;
            column = column.substring(1);
        }
        return flagcolumn + "ifnull(" + column + "," + value + ")";
    }

    @Override
    public String interval_second(String second) {
        return UNIMPLEMENTED_FUNCTION;
    }

    /**
     * 分组排序取数sql
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    @Override
    public String pre_partition_by(String groupFields, String orderFields) {
        String sql = "ROW_NUMBER() OVER(";
        if (StringUtils.isEmpty(groupFields)) {
            sql += "ORDER BY " + orderFields + ")";
        } else {
            sql += "PARTITION BY " + groupFields + " ORDER BY " + orderFields + ")";
        }
        sql += " as PK";
        return sql;
    }

    @Override
    public String delete_by_table_name(String tablename) {
        return "delete from " + tablename;
    }

    @Override
    public String del_table_by_condition(String tableName, String arg1, String arg2) {
        return del_table_by_condition(tableName, arg1, arg2);
    }

    @Override
    public String handle_insert_table(String tablename) {
        String sql = " into " + tablename;
        return sql;
    }

    @Override
    public String del_table_by_condition(String tableName, String arg1, String arg2, String arg3) {
        String sql = "delete from " + tableName + " where " + arg2 + "='" + arg1 + "'";
        return sql;
    }

    /**
     * 分组排序后部分
     *
     * @param groupFields 分组字段
     * @param orderFields 排序字段
     */
    @Override
    public String sub_partition_by(String groupFields, String orderFields) {
        return "";
    }

    @Override
    public String uuid() {
        return "uuid()";
    }

//    @Override
//    public String fdttime2dt(String dtdate, String dateFormatterStr) {
//        return "to_date(to_char(" + dtdate + ",'" + dateFormatterStr + "'),'" + dateFormatterStr + "')";
//    }

    @Override
    public String db_date() {
        return "";
    }

    @Override
    public String db_time_stamp() {
        return "";
    }


    public String find_in_set(String findStr, String setColumnName) {

        return "find_in_set('"+findStr+"',"+setColumnName+")";
    }
    @Override
    public String from_db_table() {
        return "";
    }

    @Override
    public String use_hash(String table1, String table2) {
        return "";
    }

    @Override
    public String escape_for_mybatis(String str) {
        return str.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&apos;");
    }

    @Override
    public String get_next_month_last_day(String date) {
        return JobDateUtil.getNextMonthLastDay(date);
    }


    /**
     * 转字符对象
     *
     * @param fieldName   字段名
     * @return
     */
    @Override
    public String to_char(String fieldName){
        return fieldName;
    }


    ////////    以下方法不再使用     /////////////////////////////////////

//
//    @Override
//    public String get_start_num_sql(String sql) throws Exception {
//        if (sql.indexOf("where") > 0) {
//            return String.valueOf(sql) + " AND 1<0";
//        }
//        return String.valueOf(sql) + " where 1<0";
//    }
//
//    @Override
//    public String fsub_str(String str, int start, int index) {
//        String restr = "";
//        restr = "substr(" + str + "," + start + "," + index + ")";
//        return restr;
//    }
//
//    @Override
//    public String vsub_str(String str, int start, int index) {
//        String restr = "";
//        restr = "substr('" + str + "'," + start + "," + index + ")";
//        return restr;
//    }
//
//    @Override
//    public String escape(String str) {
//        String restr = "";
//        restr = StrUtil.replace(str, "'", "''");
//        return restr;
//    }
}

