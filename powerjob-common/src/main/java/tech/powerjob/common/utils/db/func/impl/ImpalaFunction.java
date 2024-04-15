package tech.powerjob.common.utils.db.func.impl;

import tech.powerjob.common.utils.JobDateUtil;

public class ImpalaFunction extends AbstractSQLFunction {

    /////////    可以使用的方法  待实现    ///////////////////////////////////
    @Override
    public String db_date() {
        return "substr(cast(now() as string),1,10)";
    }

    @Override
    public String db_time_stamp() {
        return "now()";
    }

    @Override
    public String from_db_table() {
        return "";
    }

    @Override
    public String row_num() {
        return " FETCH FIRST 1 ROWS ONLY ";
    }

    @Override
    public String truncate_by_table_name(String tablename) {
        return "truncate " + tablename;
    }

    @Override
    public String get_next_month_last_day(String date) {
        return JobDateUtil.getNextMonthLastDay(date);
    }

    @Override
    public String match_trade_ip() {
        return "REGEXP_LIKE(T.TRADE_IP,'([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})\\.([[:digit:]]{1,3})')";
    }

//    @Override
//    public String date_format_for_mybatis(String dateStrAndDateFormatterStr) {
//        return null;
//    }

    @Override
    public String date_format(String dateStr, String dateFormatterStr) {
        return "'"+dateStr+"'";
    }

//    @Override
//    public String diff_date(String var1, String var2) {
//        return null;
//    }

    @Override
    public String table(String tableAlias) {
        return tableAlias;
    }

    @Override
    public String interval_second(String second) {
        return "";
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
    public String nvl_number(String column, int value) {
        return " ifnull(" + column + "," + value + ")";
    }

    @Override
    public String del_table_by_condition(String tableName, String arg1, String arg2, String arg3) {
        String sql = "";
        if ("true".equals(arg3.toLowerCase())) {
            sql = "alter table " + tableName + " drop if exists partition(" + arg2 + " = '" + arg1 + "')";
        } else {
            sql = "delete from " + tableName + " where " + arg2 + "='" + arg1 + "'";
        }

        return sql;
    }

    @Override
    public String get_connect() {
        return "||";
    }

    @Override
    public String group_concat(String str, String split) {
        return "group_concat(" + str + ",'" + split + "')";
    }
//
//    @Override
//    public String fdttime2dt(String dtdate, String dateFormatterStr) {
//        return "to_date(" + dtdate + ")";
//    }
//
//    @Override
//    public String f_ch_dt(String dtstr, String dttype) {
//        return "to_date(" + dtstr + ")";
//    }
//
//    @Override
//    public String fdt2ch(String dtdate, String dttype) {
//        return "cast(" + dtdate + " as string)";
//    }
//
//    @Override
//    public String vch2dt(String dtstr, String dttype) {
//        return dtstr;
//    }
//
//    @Override
//    public String vdt2ch(String dtdate, String dttype) {
//        return dtdate;
//    }

    ////////    以下方法不再使用     /////////////////////////////////////

    public String diffDate(String str1, String str2) {
        String str = "";
        str = "days(" + str1 + ")-days(" + str2 + ")";
        return str;
    }

//    @Override
//    public String ch2_num(String chstr) {
//        return "integer(" + chstr + ")";
//    }
//
//    @Override
//    public String first_row() {
//        return null;
//    }
//
//    @Override
//    public String lock_row(String var1) {
//        return null;
//    }
//
//    @Override
//    public String get_sequence_next_val(String var1) {
//        return null;
//    }
//
//    @Override
//    public String deal_inster_table(String var1) {
//        return null;
//    }

    public String firstrow() {
        return "fetch first 1 row only";
    }

    public String lockrow(String sqlStr) {
        return String.valueOf(sqlStr) + " FOR UPDATE WITH RR";
    }

    public String getSequenceNextVal(String seqname) {
        String sql = " select NEXTVAL FOR " + seqname + " from sysibm.sysdummy1 ";
        return sql;
    }

    public String deleteTable(String tablename) {
        String sql = "delete from " + tablename;
        return sql;
    }

    public String dealInsterTable(String tablename) {
        String sql = " into " + tablename;
        return sql;
    }

    @Override
    public String rownum() {
        return "ROW_NUMBER() over()";
    }

    @Override
    public String get_begin_date(String granularity, String gradingDate) {
        return null;
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
    public String get_month_begin(String dateStr) {
        return null;
    }

    public String getMonthBegin(String dateStr) {
        return null;
    }

//    @Override
//    public String length_without_space(String str) {
//        String restr = "";
//        restr = "length(ltrim(rtrim(" + str + ")))";
//        return restr;
//    }
//
//    @Override
//    public String trim_last_chr(String str) {
//        String restr = "";
//        restr = "substr(" + str + ",1,length(" + str + ")-1)";
//        return restr;
//    }
//
//    @Override
//    public String trim(String str) {
//        String restr = "";
//        restr = "ltrim(rtrim(" + str + "))";
//        return restr;
//    }
//
//    @Override
//    public String num2char(String str) {
//        String restr = "";
//        restr = "replace(ltrim(replace(rtrim(char(" + str + ")),'0','   ')),'   ','0')";
//        return restr;
//    }
//
//    @Override
//    public String short_date_to8_char(String str) {
//        String restr = "";
//        restr = "replace(char(" + str + "),'-','')";
//        return restr;
//    }
//
//    @Override
//    public String to_number(String str) {
//        String sql = "decimal(" + str + ")";
//        return sql;
//    }
//
//    @Override
//    public String delete_repeat_record(String tablename, String primarykey) {
//        StringBuffer sql = new StringBuffer();
//        sql.append("delete from (select row_number() over (partition by ");
//        sql.append(primarykey);
//        sql.append(" order by ");
//        sql.append(primarykey);
//        sql.append(" ) as rn ,a.* from ");
//        sql.append(String.valueOf(tablename) + " a )");
//        sql.append(" where rn<>1 ");
//        return sql.toString();
//    }
//
//    @Override
//    public String add_day(String date, String days) {
//        return date + days + " days ";
//    }
//
//    @Override
//    public String replace(String str, String org, String des) {
//        String restr = "";
//        restr = "replace(" + str + ",'" + org + "','" + des + "')";
//        return restr;
//    }

//    public String to_char(String str) {
//        String restr = "";
//        restr = "char(" + str + ")";
//        return this.trim(restr);
//    }


}

