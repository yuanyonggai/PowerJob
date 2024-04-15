package tech.powerjob.common.utils.db.func.impl;

import tech.powerjob.common.utils.JobDateUtil;

public class OracleFunction extends AbstractSQLFunction {

    @Override
    public String now() {
        return "sysdate";
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
            if (i < vars.length-1) {
                concatSql += "||";
            }
        }
        return concatSql;
    }


    @Override
    public String group_concat(String str, String split) {
        return "replace(concat(" + str + "','),',','" + split + "')";
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
    public String str_to_date(String dateStr, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date('" + dateStr + "','" + dateFormatterStr + "')" : "to_date('" + dateStr + "','yyyy-mm-dd')";
    }


    @Override
    public String field_to_date_func(String field, String dateFormatterStr) {
        return dateFormatterStr != null ? "to_date(" + field + ",'" + dateFormatterStr + "')" : "to_date('" + field + "','yyyy-mm-dd')";
    }

    @Override
    public String find_in_set(String findStr, String setColumnName) {

        return "instr(','||"+setColumnName+"||',',',"+findStr+",')<>0";
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
    public String handle_insert_table(String tablename) {
        String sql = " /*+append*/  into " + tablename + "  nologging ";
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

    @Override
    public String nvl_str(String column, String value) {
        return " nvl(" + column + ",'" + value + "')";
    }

    @Override
    public String nvl_number(String column, int value) {
        return " nvl(" + column + "," + value + ")";
    }

    @Override
    public String get_connect() {
        return "||";
    }


//    @Override
//    public String diff_date(String var1, String var2) {
//        return null;
//    }

    ////////    以下方法不再使用     /////////////////////////////////////

    public String diffDate(String str1, String str2) {
        return "(" + str1 + " - " + str2 + ")";
    }

//    @Override
//    public String ch2_num(String chstr) {
//        return "to_number(" + chstr + ")";
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

//    @Override
//    public String deal_inster_table(String var1) {
//        return "";
//    }


    @Override
    public String rownum() {
        return "ROWNUM";
    }

//    @Override
//    public String length_without_space(String str) {
//        String restr = "";
//        restr = "length(trim(" + str + "))";
//        return restr;
//    }

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
//        restr = "trim(" + str + ")";
//        return restr;
//    }
//
//    @Override
//    public String num2char(String str) {
//        String restr = "";
//        restr = "to_char(" + str + ")";
//        return restr;
//    }

    public String to_char(String fieldName){
        return "to_char(" + fieldName + ")";
    }
//    @Override
//    public String short_date_to8_char(String str) {
//        String restr = "";
//        restr = "to_char(" + str + ",'yyyymmdd')";
//        return restr;
//    }
//
//    @Override
//    public String to_number(String str) {
//        String sql = "to_number(" + str + ")";
//        return sql;
//    }
//
//    @Override
//    public String delete_repeat_record(String tablename, String primarykey) {
//        StringBuffer sql = new StringBuffer();
//        sql.append("Delete from ");
//        sql.append(String.valueOf(tablename) + " a ");
//        sql.append(" Where a.rowid > (Select min(rowid) from ");
//        sql.append(String.valueOf(tablename) + " b where ");
//        if (primarykey.indexOf(",") > 0) {
//            String[] primarykeys = primarykey.split(",");
//            int i = 0;
//            while (i < primarykeys.length) {
//                if (i == 0) {
//                    sql.append(" a." + primarykeys[i] + "=b." + primarykeys[i]);
//                } else {
//                    sql.append(" and a." + primarykeys[i] + "=b." + primarykeys[i]);
//                }
//                ++i;
//            }
//        } else {
//            sql.append("  a." + primarykey + "=b." + primarykey);
//        }
//        sql.append(" )");
//        return sql.toString();
//    }
//
//    @Override
//    public String add_day(String date, String days) {
//        return String.valueOf(this.date_format(date, null)) + days;
//    }
//
//    @Override
//    public String replace(String str, String org, String des) {
//        String restr = "";
//        restr = "replace(" + str + ",'" + org + "','" + des + "')";
//        return restr;
//    }
}

