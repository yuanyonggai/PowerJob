package tech.powerjob.common.utils.db.func.impl;


import java.sql.Connection;

import tech.powerjob.common.utils.JobDateUtil;

public class Gauss200Function extends AbstractSQLFunction {

    /////////    可以使用的方法      ///////////////////////////////////

    @Override
    public String now() {
        return "now()";
    }

    @Override
    public String concat(String... vars) {
        String concatSql = "concat(";
        for (int i = 0; i < vars.length; i++) {
            String var = vars[i];
            if (var.startsWith(COLUMN_FIELD_FLAG)&&var.length()>1) {
                concatSql += "nvl("+var.substring(1)+",'')";
            } else {
                concatSql += "'" + var + "'";
            }
            if (i < vars.length-1) concatSql += ",";
        }
        return concatSql + ")";
    }

    @Override
    public String find_in_set(String findStr, String setColumnName) {
        return "instr(','||"+setColumnName+"||',',',"+findStr+",')<>0";
    }

    @Override
    public String group_concat(String str, String split) {
        return "replace(concat(" + str + "),',','" + split + "')";
    }


    @Override
    public String db_date() {
        return "trunc(sysdate)";
    }

    @Override
    public String db_time_stamp() {
        return "sysdate";
    }

    @Override
    public String table(String tableAlias) {
        return "";
    }

    @Override
    public String from_db_table() {
        return "from dual";
    }

    @Override
    public String row_num() {
        return " and rownum =1 ";
    }

    @Override
    public String interval_second(String second) {
        return "sysdate + numtodsinterval(" + second + ",'second')";
        //https://www.cnblogs.com/xyz0601/archive/2015/04/11/4417165.html
    }

    @Override
    public String truncate_by_table_name(String tablename) {
        return "truncate table " + tablename;
    }

    @Override
    public String match_trade_ip() {
        return "REGEXP_LIKE(T.TRADE_IP,'([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})')";
    }

    @Override
    public String get_next_month_last_day(String date) {
        String str = "";
        return str = "to_date('" + JobDateUtil.getNextMonthLastDay(date) + "','yyyy-mm-dd')";
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
    public String str_to_date(String dateStr, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date('" + dateStr + "','" + dateFormatterStr + "')" : "to_date('" + dateStr + "','yyyy-mm-dd')";
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
        return dateFormatterStr != null ? "to_date(" + sep + dateStr + sep + ",'" + dateFormatterStr + "') + " + days : "to_date(" + sep + dateStr + sep + ",'yyyy-mm-dd') + " + days;

    }

    @Override
    public String uuid() {
        return "sys_guid()";
    }

    @Override
    public String get_month_begin(String dateStr) {
        String date = JobDateUtil.getMonthBegin(dateStr);
        date = "to_date('" + date + "','yyyy-mm-dd') ";
        return date;
    }

//    @Override
//    public String date_format_for_mybatis(String dateStrAndDateFormatterStr)
//    {
//        String dateStr,  dateFormatterStr;
//        dateStr=dateStrAndDateFormatterStr.split("\\|")[0];
//        dateFormatterStr=dateStrAndDateFormatterStr.split("\\|")[1];
//        return dateFormatterStr != null ? "to_date(" + dateStr + ",'" + dateFormatterStr + "')" : "to_date(" + dateStr + ",'yyyy-mm-dd')";
//    }

    @Override
    public String get_partition_by(String groupFields, String orderFields) {
        String restr = "ROW_NUMBER() OVER(PARTITION BY " + groupFields + " ORDER BY " + orderFields + ")";
        return restr;
    }

    public String to_char(String fieldName){
        return "to_char(" + fieldName + ")";
    }

    @Override
    public String nvl_str(String column, String value) {
        return " nvl(" + column + ",'" + value + "')";
    }

    @Override
    public String nvl_number(String column, String value) {
        return " nvl(" + column + "," + value + ")";
    }

    @Override
    public String get_connect() {
        return "||";
    }

//    @Override
//    public String f_ch_dt(String dtstr, String dttype) {
//        String str = "";
//        str = dttype != null ? "to_date(" + dtstr + ",'" + dttype + "')" : "to_date(" + dtstr + ",'yyyy-mm-dd')";
//        return str;
//    }
//
//    @Override
//    public String fdt2ch(String dtdate, String dttype) {
//        String str = "";
//        str = dttype != null ? "to_char(" + dtdate + ",'" + dttype + "')" : "to_char(" + dtdate + ",'yyyy-mm-dd')";
//        return str;
//    }
//
//    @Override
//    public String fdttime2dt(String dtdate, String dateFormatterStr) {
//        return "to_date(to_char(" + dtdate + ",'" + dateFormatterStr + "'),'" + dateFormatterStr + "')";
//    }
//
//    @Override
//    public String vch2dt(String dtstr, String dateFormatterStr) {
//        return dateFormatterStr != null ? "to_date('" + dtstr + "','" + dateFormatterStr + "')" : "to_date('" + dtstr + "','yyyy-mm-dd')";
//    }
//
//    @Override
//    public String vdt2ch(String dtdate, String dttype) {
//        String str = "";
//        str = dttype != null ? "to_char('" + dtdate + "','" + dttype + "')" : "to_char('" + dtdate + "','yyyy-mm-dd')";
//        return str;
//    }

//    @Override
//    @Deprecated
//    public String date_format(String dateStr, String dateFormatterStr) {
//        return vch2dt(dateStr, dateFormatterStr);
//    }

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

    public String deleteTable(Connection conn, String tablename) {
        String sql = "truncate table " + tablename;
        return sql;
    }

    @Override
    public String rownum() {
        return "ROWNUM";
    }

    @Override
    public String field_to_date_func(String field, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date(" + field + ",'" + dateFormatterStr + "')" : "to_date('" + field + "','yyyy-mm-dd')";
    }

    @Override
    public String field_date_to_char_func(String dtdate, String dttype) {
        String str = "";
        str = dttype != null ? "to_char(" + dtdate + ",'" + dttype + "')" : "to_char(" + dtdate + ",'yyyy-mm-dd')";
        return str;
    }
}

