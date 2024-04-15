package tech.powerjob.common.utils.db.func.impl;

import tech.powerjob.common.utils.JobDateUtil;

/**
 * @author shkstart
 * @create 2023-07-17 18:44
 */
public class GBase8AFunction extends AbstractSQLFunction  {
    @Override
    public String now() {
        return "sysdate()";
    }

    /////////    可以使用的方法      ///////////////////////////////////
    @Override
    public String concat(String... vars) {
        String concatSql = "";
        for (int i = 0; i < vars.length; i++) {
            String var = vars[i];
            if (var.startsWith(COLUMN_FIELD_FLAG)&&var.length()>1) {
                concatSql += var.substring(1);
            } else {
                concatSql += "'" + var + "'";
            }
            if (i < vars.length-1) concatSql += "||";
        }
        return concatSql;
    }


    @Override
    public String group_concat(String str, String split) {
        return "replace(concat(" + str + "','),',','" + split + "')";
    }

    @Override
    public String db_date() {
        return "trunc(sysdate())";
    }

    @Override
    public String db_time_stamp() {
        return "sysdate()";
    }

    @Override
    public String str_to_date(String dateStr, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date('" + dateStr + "','" + dateFormatterStr + "')" : "to_date('" + dateStr + "','yyyy-mm-dd')";
    }


    @Override
    public String field_to_date_func(String field, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date(" + field + ",'" + dateFormatterStr + "')" : "to_date(" + field + ",'yyyy-mm-dd')";
    }

    @Override
    public String field_date_to_char_func(String dtdate, String dttype) {
        String str = "";
        str = dttype != null ? "to_char(" + dtdate + ",'" + dttype + "')" : "to_char(" + dtdate + ",'yyyy-mm-dd')";
        return str;
    }

    public String to_char(String fieldName){
        return "to_char(" + fieldName + ")";
    }

    @Override
    public String find_in_set(String findStr, String setColumnName) {

        return "instr(','||"+setColumnName+"||',',',"+findStr+",')<>0";
    }

    @Override
    public String table(String tableAlias) {
        return "";
    }


    //gbase8a沒有這個方法
   /* @Override
    public String from_db_table() {
        return "from dual";
    }*/

    @Override
    public String row_num() {
        return " LIMIT 1 ";
    }

    @Override
    public String interval_second(String second) {
        return "sysdate() + INTERVAL" + second + ",'second')";
        //https://www.cnblogs.com/xyz0601/archive/2015/04/11/4417165.html
    }

    @Override
    public String truncate_by_table_name(String tablename) {
        return "truncate table " + tablename;
    }

    @Override
    public String match_trade_ip() {
        return "T.TRADE_IP RLIKE '([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})'";
    }

    @Override
    public String handle_insert_table(String tablename) {
        String sql = " /*+append*/  INTO " + tablename;
        return sql;
    }

    @Override
    public String get_next_month_last_day(String date) {
        String str = "";
        return str = "to_date('" + JobDateUtil.getNextMonthLastDay(date) + "','yyyy-mm-dd')";
    }

    @Override
    public String use_hash(String table1, String table2) {
        return "/*+ use_hash(" + table1 + ", " + table2 + ") */";
    }

    @Override
    public String get_begin_date(String granularity, String gradingDate) {
        String dateStr = JobDateUtil.getBeginDate(granularity, gradingDate);
        dateStr = "to_date('" + dateStr + "','yyyy-mm-dd')";
        return dateStr;
    }

    @Override
    public String date_add(String dateStr, String dateFormatterStr, String days) {
        String sep = "'";
        if (dateStr.startsWith(COLUMN_FIELD_FLAG)) {
            dateStr = dateStr.substring(1);
            sep = "";
        }
        return dateFormatterStr != null ? "to_date(" + sep + dateStr + sep + ",'" + dateFormatterStr + "') + " + days  : "to_date(" + sep + dateStr + sep + ",'yyyy-mm-dd') + " + days ;
    }

    @Override
    public String date_format(String dateStr, String dateFormatterStr) {
        String sep = "'";
        if (dateStr.startsWith(COLUMN_FIELD_FLAG)) {
            dateStr = dateStr.substring(1);
            sep = "";
        }
        return dateFormatterStr != null ? "to_date(" + sep + dateStr + sep + ",'" + dateFormatterStr + "')" : "to_date(" + sep + dateStr + sep + ",'yyyy-mm-dd')";
    }

    @Override
    public String uuid() {
        return "uuid()";
    }

    @Override
    public String get_month_begin(String dateStr) {
        String date = JobDateUtil.getMonthBegin(dateStr);
        date = "to_date('" + date + "','yyyy-mm-dd') ";
        return date;
    }

    @Override
    public String get_partition_by(String groupFields, String orderFields) {
        String restr = "ROW_NUMBER() OVER(PARTITION BY " + groupFields + " ORDER BY " + orderFields + ")";
        return restr;
    }

    @Override
    public String nvl_str(String column, String value) {
        return " ifnull(" + column + ",'" + value + "')";
    }

    @Override
    public String nvl_number(String column, String value) {
        return " ifnull(" + column + "," + value + ")";
    }

    @Override
    public String get_connect() {
        return "||";
    }


    ////////    以下方法不再使用     /////////////////////////////////////

    public String diffDate(String str1, String str2) {
        return "(" + str1 + " - " + str2 + ")";
    }


    public String firstrow() {
        return "and rownum=1";
    }

    public String lockrow(String sqlStr) {
        return String.valueOf(sqlStr) + " FOR UPDATE";
    }

    public String getSequenceNextVal(String seqname) {
        String sql = "select " + seqname + ".Nextval  from dual";
        return sql;
    }


    @Override
    public String rownum() {
        return "ROWNUM";
    }
}
