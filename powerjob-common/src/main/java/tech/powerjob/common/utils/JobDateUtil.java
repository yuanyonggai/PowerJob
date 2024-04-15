package tech.powerjob.common.utils;

import cn.hutool.core.date.format.FastDateFormat;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;

public class JobDateUtil {


    public static void main(String[] args) {

        String strDate = "2008-02-29";
        Date aa = JobDateUtil.stringToDateShort(strDate);

        System.out.println(JobDateUtil.getLastYearSameDate(strDate));
        System.out.println(JobDateUtil.getBeforeBeginDate("5", strDate));
        System.out.println(JobDateUtil.getBeforeEndDate("5", strDate));
        System.out.println(JobDateUtil.getLastSameMonthBeginDate(strDate));
        System.out.println(JobDateUtil.getLastSameMonthEndDate(strDate));
        System.out.println(JobDateUtil.getLastSameSeasonBeginDate(strDate));
        System.out.println(JobDateUtil.getLastSameSeasonEndDate(strDate));
        System.out.println(getMonthBegin("2017-08-07"));
        System.out.println(getNextMonthLastDay("2017-08-07"));
    }

    /**
     * 检查粒度
     * @param granularity
     * @param jobDate
     * @return
     */
    public boolean checkGranularity(String granularity, String jobDate) {
        return getGranularityList(jobDate).contains(granularity);
    }

    /**
     * 返回粒度列表
     *
     * @param jobDate
     * @return
     */
    public static LinkedList<String> getGranularityList(String jobDate) {
        LinkedList<String> list = new LinkedList<>();

        Date date = JobDateUtil.stringToDateShort(jobDate);
        list.add("1");
        if (JobDateUtil.getWeekEnd(date).equals(date)) {
            list.add("2");
        }
        if (JobDateUtil.getPeriodEnd(date).equals(date)) {
            list.add("3");
        }
        if (JobDateUtil.getMonthEnd(date).compareTo(date) == 0) {
            list.add("4");
        }
        if (JobDateUtil.getSeasonEnd(date).equals(date)) {
            list.add("5");
        }
        if (JobDateUtil.getHalfYearEnd(date).equals(date)) {
            list.add("6");
        }
        if (JobDateUtil.getYearEnd(date).equals(date)) {
            list.add("7");
        }
        return list;
    }

    public static Date getCurrDateTime() {
        return new Date(System.currentTimeMillis());
    }

    //Date转换为LocalDateTime
    public static LocalDateTime convertDateToLDT(Date date) {
        return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }

    public static boolean isValidDate(String str, String format) {
        try {
            DateFormat formatter = new SimpleDateFormat(format);
            Date date = (Date) formatter.parse(str);
            return str.equals(formatter.format(date));
        } catch (Exception e) {
            return false;
        }
    }

    public static String getCurrTime() {
        Date date_time = new Date();
        return JobDateUtil.FormatDate(date_time, "yyyy-MM-dd HH:mm:ss");
    }

    public static String getCurrDate() {
        Date date_time = new Date();
        return JobDateUtil.FormatDate(date_time, "yyyy-MM-dd");
    }

    public static String getCurrDtFormat(String formatStr) {
        FastDateFormat fdt = FastDateFormat.getInstance((String) formatStr);
        return fdt.format(new Date(System.currentTimeMillis()));
    }

    public static String getYear(Date date) {
        return JobDateUtil.FormatDate(date, "yyyy");
    }

    public static String getMonth(Date date) {
        return JobDateUtil.FormatDate(date, "MM");
    }

    public static String getDay(Date date) {
        return JobDateUtil.FormatDate(date, "dd");
    }

    public static String getHour(Date date) {
        return JobDateUtil.FormatDate(date, "HH");
    }

    public static String getMinute(Date date) {
        return JobDateUtil.FormatDate(date, "mm");
    }

    public static String getSecond(Date date) {
        return JobDateUtil.FormatDate(date, "ss");
    }


    public static String getBeginDate(String granularity, String jobDate) {
        String beginDate = "";
        Date date = JobDateUtil.stringToDateShort(jobDate);
        Date beginDateTemp = null;
        if (granularity.equals("1")) {
            beginDateTemp = date;
        }
        if (granularity.equals("2")) {
            beginDateTemp = JobDateUtil.getWeekBegin(date);
        }
        if (granularity.equals("3")) {
            beginDateTemp = JobDateUtil.getPeriodBegin(date);
        } else if (granularity.equals("4")) {
            beginDateTemp = JobDateUtil.getMonthBegin(date);
        } else if (granularity.equals("5")) {
            beginDateTemp = JobDateUtil.getSeasonBegin(date);
        } else if (granularity.equals("6")) {
            beginDateTemp = JobDateUtil.getHalfYearBegin(date);
        } else if (granularity.equals("7")) {
            beginDateTemp = JobDateUtil.getYearBegin(date);
        }
        beginDate = JobDateUtil.dateToStringShort(beginDateTemp);
        return beginDate;
    }

    public static String getEndDate(String granularity, String jobDate) {
        String beginDate = "";
        Date date = JobDateUtil.stringToDateShort(jobDate);
        Date beginDateTemp = null;
        if (granularity.equals("1")) {
            beginDateTemp = date;
        }
        if (granularity.equals("2")) {
            beginDateTemp = JobDateUtil.getWeekEnd(date);
        }
        if (granularity.equals("3")) {
            beginDateTemp = JobDateUtil.getPeriodEnd(date);
        } else if (granularity.equals("4")) {
            beginDateTemp = JobDateUtil.getMonthEnd(date);
        } else if (granularity.equals("5")) {
            beginDateTemp = JobDateUtil.getSeasonEnd(date);
        } else if (granularity.equals("6")) {
            beginDateTemp = JobDateUtil.getHalfYearEnd(date);
        } else if (granularity.equals("7")) {
            beginDateTemp = JobDateUtil.getYearEnd(date);
        }
        beginDate = JobDateUtil.dateToStringShort(beginDateTemp);
        return beginDate;
    }

    public static String getTimedes(String granularity, String jobDate) {
        String timedes = "";
        Date date = JobDateUtil.stringToDateShort(jobDate);
        String year = "";
        String month = "01";
        String day = "01";
        year = JobDateUtil.getYear(date);
        month = JobDateUtil.getMonth(date);
        day = JobDateUtil.getDay(date);
        if (granularity.equals("1")) {
            timedes = jobDate;
        } else if (granularity.equals("4")) {
            timedes = String.valueOf(year) + "\u5e74" + month + "\u6708";
        } else if (granularity.equals("8")) {
            String quarter = String.valueOf(month) + "-" + day;
            if (quarter.equals("03-31")) {
                timedes = String.valueOf(year) + "\u5e74 \u7b2c1\u5b63\u5ea6";
            } else if (quarter.equals("06-30")) {
                timedes = String.valueOf(year) + "\u5e74 \u7b2c2\u5b63\u5ea6";
            } else if (quarter.equals("09-30")) {
                timedes = String.valueOf(year) + "\u5e74 \u7b2c3\u5b63\u5ea6";
            } else if (quarter.equals("12-31")) {
                timedes = String.valueOf(year) + "\u5e74 \u7b2c4\u5b63\u5ea6";
            }
        } else if (granularity.equals("32")) {
            timedes = String.valueOf(year) + "\u5e74";
        }
        return timedes;
    }

    public static String getDateBeforeAWeek(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(iDate);
        calendar.add(3, -1);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s = df.format(calendar.getTime());
        return s;
    }

    public static String dateToString(Date date) {
        if (date == null) {
            return "";
        }
        return JobDateUtil.FormatDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    public static String dateToStringShort(Date date) {
        if (date == null) {
            return "";
        }
        return JobDateUtil.FormatDate(date, "yyyy-MM-dd");
    }

    public static Date stringToDate(String dateString) {
        String sf = "yyyy-MM-dd HH:mm:ss";
        Date dt = JobDateUtil.stringToDate(dateString, sf);
        return dt;
    }

    public static Date stringToDateShort(String dateString) {
        String sf = "yyyy-MM-dd";
        Date dt = JobDateUtil.stringToDate(dateString, sf);
        return dt;
    }

    public static String FormatDate(Date date, String sf) {
        if (date == null) {
            return "";
        }
        SimpleDateFormat dateformat = new SimpleDateFormat(sf);
        return dateformat.format(date);
    }

    public static Date stringToDate(String dateString, String sf) {
        ParsePosition pos = new ParsePosition(0);
        SimpleDateFormat sdf = new SimpleDateFormat(sf);
        Date dt = sdf.parse(dateString, pos);
        return dt;
    }

    public static long diffTwoDate(Date date1, Date date2) {
        long l1 = date1.getTime();
        long l2 = date2.getTime();
        return l1 - l2;
    }

    public static long diffTwoTimestamp(Timestamp timestamp1, Timestamp timestamp2) {
        long l1 = timestamp1.getTime();
        long l2 = timestamp2.getTime();
        return l1 - l2;
    }

    public static int diffTwoDateDay(Date date1, Date date2) {
        long l1 = date1.getTime();
        long l2 = date2.getTime();
        int diff = Integer.parseInt("" + (l1 - l2) / 3600L / 24L / 1000L);
        return diff;
    }

    public static String getDateChangeTime(String currentTime, String type, int iQuantity) {
        Date curr = JobDateUtil.stringToDate(currentTime);
        curr = JobDateUtil.getDateChangeTime(curr, type, iQuantity);
        return JobDateUtil.dateToString(curr);
    }

    public static Date getDateChangeTime(Date currentTime, String type, int iQuantity) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "MM"));
        int day = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "dd"));
        int hour = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "HH"));
        int mi = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "mm"));
        int ss = Integer.parseInt(JobDateUtil.FormatDate(currentTime, "ss"));
        GregorianCalendar gc = new GregorianCalendar(year, --month, day, hour, mi, ss);
        if (type.equalsIgnoreCase("y")) {
            gc.add(1, iQuantity);
        } else if (type.equalsIgnoreCase("m")) {
            gc.add(2, iQuantity);
        } else if (type.equalsIgnoreCase("d")) {
            gc.add(5, iQuantity);
        } else if (type.equalsIgnoreCase("h")) {
            gc.add(10, iQuantity);
        } else if (type.equalsIgnoreCase("mi")) {
            gc.add(12, iQuantity);
        } else if (type.equalsIgnoreCase("s")) {
            gc.add(13, iQuantity);
        }
        return gc.getTime();
    }

    public static String getDateChangeALL(String currentTime, String type, int iQuantity) {
        Date curr = null;
        String newtype = "";
        if (currentTime.length() == 10) {
            curr = JobDateUtil.stringToDateShort(currentTime);
        }
        if (currentTime.length() > 10) {
            curr = JobDateUtil.stringToDate(currentTime);
        }
        if (type.equals("1")) {
            newtype = "d";
        } else if (type.equals("2")) {
            iQuantity *= 7;
            newtype = "d";
        } else if (type.equals("3")) {
            String day;
            newtype = "d";
            String month = JobDateUtil.getMonth(curr);
            if ((month.equals("02") || month.equals("2")) && (day = JobDateUtil.getDay(curr)).equals("20") && (iQuantity *= 10) > 0) {
                iQuantity = 8;
            }
        } else if (type.equals("4")) {
            newtype = "m";
        } else if (type.equals("5")) {
            iQuantity *= 3;
            newtype = "m";
        } else if (type.equals("6")) {
            iQuantity *= 6;
            newtype = "m";
        } else {
            newtype = type.equals("7") ? "y" : "d";
        }
        Date change = JobDateUtil.getDateChangeTime(curr, newtype, iQuantity);
        return JobDateUtil.dateToStringShort(change);
    }


    public static String getNextMonthLastDay(String date) {
        String dateStr = getDateAfterAMonth(date);
        Date monthEnd = getMonthEnd(JobDateUtil.stringToDateShort(dateStr));
        return dateToStringShort(monthEnd);
    }

    public static String getTime(String year, String month) {
        String time = "";
        int len = 31;
        int iYear = Integer.parseInt(year);
        int iMonth = Integer.parseInt(month);
        if (iMonth == 4 || iMonth == 6 || iMonth == 9 || iMonth == 11) {
            len = 30;
        }
        if (iMonth == 2) {
            len = 28;
            if (iYear % 4 == 0 && iYear % 100 == 0 && iYear % 400 == 0 || iYear % 4 == 0 && iYear % 100 != 0) {
                len = 29;
            }
        }
        time = String.valueOf(year) + "-" + month + "-" + String.valueOf(len);
        return time;
    }

    public static Date getMonthBegin(Date date) {
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy-MM")) + "-01";
        return JobDateUtil.stringToDate(newDateStr);
    }

    public static String getMonthBegin(String dateStr) {
        Date date = JobDateUtil.stringToDate(dateStr, "yyyy-MM");
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy-MM")) + "-01";
        return newDateStr;
    }

    public static Date getMonthEnd(Date date) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        int day = Integer.parseInt(JobDateUtil.FormatDate(date, "dd"));
        GregorianCalendar calendar = new GregorianCalendar(year, month - 1, day, 0, 0, 0);
        int monthLength = calendar.getActualMaximum(5);
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy")) + "-" + JobDateUtil.FormatDate(date, "MM") + "-";
        newDateStr = monthLength < 10 ? String.valueOf(newDateStr) + "0" + monthLength : String.valueOf(newDateStr) + monthLength;
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getSeasonBegin(Date date) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy")) + "-";
        if (month >= 1 && month <= 3) {
            newDateStr = String.valueOf(newDateStr) + "01-01";
        } else if (month >= 4 && month <= 6) {
            newDateStr = String.valueOf(newDateStr) + "04-01";
        } else if (month >= 7 && month <= 9) {
            newDateStr = String.valueOf(newDateStr) + "07-01";
        } else if (month >= 10 && month <= 12) {
            newDateStr = String.valueOf(newDateStr) + "10-01";
        }
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getHalfYearBegin(Date date) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy")) + "-";
        newDateStr = month <= 6 ? String.valueOf(newDateStr) + "01-01" : String.valueOf(newDateStr) + "07-01";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getPeriodBegin(Date date) {
        int days = Integer.parseInt(JobDateUtil.FormatDate(date, "dd"));
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy-MM")) + "-";
        newDateStr = days <= 10 ? String.valueOf(newDateStr) + "01" : (days <= 20 ? String.valueOf(newDateStr) + "11" : String.valueOf(newDateStr) + "21");
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getWeekBegin(Date date) {
        int week;
        GregorianCalendar gc;
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        int day = Integer.parseInt(JobDateUtil.FormatDate(date, "dd"));
        if ((week = (gc = new GregorianCalendar(year, --month, day)).get(7) - 1) == 0) {
            week = 7;
        }
        gc.add(5, 0 - week + 1);
        return gc.getTime();
    }

    public static Date getWeekEnd(Date date) {
        int week;
        GregorianCalendar gc;
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        int day = Integer.parseInt(JobDateUtil.FormatDate(date, "dd"));
        if ((week = (gc = new GregorianCalendar(year, --month, day)).get(7) - 1) == 0) {
            week = 7;
        }
        gc.add(5, 7 - week);
        return gc.getTime();
    }

    public static Date getPeriodEnd(Date date) {
        int days = Integer.parseInt(JobDateUtil.FormatDate(date, "dd"));
        String newDateStr = JobDateUtil.FormatDate(date, "yyyy-MM") + "-";
        newDateStr = days <= 10 ? String.valueOf(newDateStr) + "10" : (days <= 20 ? String.valueOf(newDateStr) + "20" : JobDateUtil.FormatDate(JobDateUtil.getMonthEnd(date), "yyyy-MM-dd"));
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getHalfYearEnd(Date date) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        String newDateStr =JobDateUtil.FormatDate(date, "yyyy") + "-";
        newDateStr = month <= 6 ? newDateStr + "06-30" : newDateStr + "12-31";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getSeasonEnd(Date date) {
        int year = Integer.parseInt(JobDateUtil.FormatDate(date, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(date, "MM"));
        String newDateStr = JobDateUtil.FormatDate(date, "yyyy") + "-";
        if (month >= 1 && month <= 3) {
            newDateStr = String.valueOf(newDateStr) + "03-31";
        } else if (month >= 4 && month <= 6) {
            newDateStr = String.valueOf(newDateStr) + "06-30";
        } else if (month >= 7 && month <= 9) {
            newDateStr = String.valueOf(newDateStr) + "09-30";
        } else if (month >= 10 && month <= 12) {
            newDateStr = String.valueOf(newDateStr) + "12-31";
        }
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getYearBegin(Date date) {
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy")) + "-01-01";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getYearEnd(Date date) {
        String newDateStr = String.valueOf(JobDateUtil.FormatDate(date, "yyyy")) + "-12-31";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public boolean IsXperiodEnd(Date date) {
        boolean flag = false;
        String day = JobDateUtil.getDay(date);
        String month = JobDateUtil.getMonth(date);
        if (day.equalsIgnoreCase("10")) {
            flag = true;
        } else if (day.equalsIgnoreCase("20")) {
            flag = true;
        }
        return flag;
    }

    public static java.sql.Date stringToSqlDateShort(String dateStr) {
        Date javaDate = JobDateUtil.stringToDateShort(dateStr);
        java.sql.Date d = new java.sql.Date(javaDate.getTime());
        return d;
    }

    public static java.sql.Date javaDateTosqlDate(Date jDate) {
        java.sql.Date sDate = new java.sql.Date(jDate.getTime());
        return sDate;
    }

    public static Date sqlDateTojavaDate(java.sql.Date sDate) {
        Date jDate = new Date(sDate.getTime());
        return jDate;
    }

    public static String formatDateTimeStr(String dateStr, String timeStr) {
        String dateTimeStr = "";
        if (dateStr.length() == 8) {
            dateStr = String.valueOf(dateStr.substring(0, 4)) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
        }
        if (timeStr.length() == 5) {
            timeStr = "0" + timeStr;
        }
        if (timeStr.length() == 6) {
            timeStr = String.valueOf(timeStr.substring(0, 2)) + ":" + timeStr.substring(2, 4) + ":" + timeStr.subSequence(4, 6);
        }
        dateTimeStr = String.valueOf(dateStr) + " " + timeStr;
        return dateTimeStr;
    }

    public static String getDateBeforeAMonth(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(iDate);
        calendar.add(2, -1);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s = df.format(calendar.getTime());
        return s;
    }

    public static String getDateAfterAMonth(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(iDate);
        calendar.add(2, 1);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s = df.format(calendar.getTime());
        return s;
    }

    public static Date getDateBMonthBegin(String date) {
        Date iDate = JobDateUtil.stringToDateShort(JobDateUtil.getDateBeforeAMonth(date));
        return JobDateUtil.getMonthBegin(iDate);
    }

    public static Date getDateBMonthEnd(String date) {
        Date iDate = JobDateUtil.stringToDateShort(JobDateUtil.getDateBeforeAMonth(date));
        return JobDateUtil.getMonthEnd(iDate);
    }

    public static Date getDateAMonthBegin(String date) {
        Date iDate = JobDateUtil.stringToDateShort(JobDateUtil.getDateAfterAMonth(date));
        return JobDateUtil.getMonthBegin(iDate);
    }

    public static Date getDateAMonthEnd(String date) {
        Date iDate = JobDateUtil.stringToDateShort(JobDateUtil.getDateAfterAMonth(date));
        return JobDateUtil.getMonthEnd(iDate);
    }

    public static Date getBSeasonBegin(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        int year = Integer.parseInt(JobDateUtil.FormatDate(iDate, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(iDate, "MM"));
        String yearStr = "";
        String monthStr = "";
        if (month >= 1 && month <= 3) {
            --year;
            monthStr = "10-01";
        } else if (month >= 4 && month <= 6) {
            monthStr = "01-01";
        } else if (month >= 7 && month <= 9) {
            monthStr = "04-01";
        } else if (month >= 10 && month <= 12) {
            monthStr = "07-01";
        }
        yearStr = String.valueOf(new Integer(year).toString()) + "-";
        return JobDateUtil.stringToDateShort(String.valueOf(yearStr) + monthStr);
    }

    public static Date getBSeasonEnd(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        int year = Integer.parseInt(JobDateUtil.FormatDate(iDate, "yyyy"));
        int month = Integer.parseInt(JobDateUtil.FormatDate(iDate, "MM"));
        String yearStr = "";
        String monthStr = "";
        if (month >= 1 && month <= 3) {
            --year;
            monthStr = "12-31";
        } else if (month >= 4 && month <= 6) {
            monthStr = "03-31";
        } else if (month >= 7 && month <= 9) {
            monthStr = "06-30";
        } else if (month >= 10 && month <= 12) {
            monthStr = "09-30";
        }
        yearStr = String.valueOf(new Integer(year).toString()) + "-";
        return JobDateUtil.stringToDateShort(String.valueOf(yearStr) + monthStr);
    }

    public static Date getBYearBegin(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        int year = Integer.parseInt(JobDateUtil.FormatDate(iDate, "yyyy"));
        String newDateStr = String.valueOf(new Integer(year - 1).toString()) + "-01-01";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static Date getBYearEnd(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        int year = Integer.parseInt(JobDateUtil.FormatDate(iDate, "yyyy"));
        String newDateStr = String.valueOf(new Integer(year - 1).toString()) + "-12-31";
        return JobDateUtil.stringToDateShort(newDateStr);
    }

    public static String getBeforeBeginDate(String granularity, String jobDate) {
        String beginDate = "";
        Date beginDateTemp = null;
        if (granularity.equals("4")) {
            beginDateTemp = JobDateUtil.getDateBMonthBegin(jobDate);
        } else if (granularity.equals("5")) {
            beginDateTemp = JobDateUtil.getBSeasonBegin(jobDate);
        } else if (granularity.equals("7")) {
            beginDateTemp = JobDateUtil.getBYearBegin(jobDate);
        }
        beginDate = JobDateUtil.dateToStringShort(beginDateTemp);
        return beginDate;
    }

    public static String getBeforeEndDate(String granularity, String jobDate) {
        String beginDate = "";
        Date beginDateTemp = null;
        if (granularity.equals("4")) {
            beginDateTemp = JobDateUtil.getDateBMonthEnd(jobDate);
        } else if (granularity.equals("5")) {
            beginDateTemp = JobDateUtil.getBSeasonEnd(jobDate);
        } else if (granularity.equals("7")) {
            beginDateTemp = JobDateUtil.getBYearEnd(jobDate);
        }
        beginDate = JobDateUtil.dateToStringShort(beginDateTemp);
        return beginDate;
    }

    public static String getLastYearSameDate(String date) {
        Date iDate = JobDateUtil.stringToDateShort(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(iDate);
        calendar.add(1, -1);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String s = df.format(calendar.getTime());
        return s;
    }

    public static String getLastSameMonthBeginDate(String date) {
        Date dt = JobDateUtil.stringToDateShort(JobDateUtil.getLastYearSameDate(date));
        return JobDateUtil.dateToStringShort(JobDateUtil.getMonthBegin(dt));
    }

    public static String getLastSameMonthEndDate(String date) {
        Date dt = JobDateUtil.stringToDateShort(JobDateUtil.getLastYearSameDate(date));
        return JobDateUtil.dateToStringShort(JobDateUtil.getMonthEnd(dt));
    }

    public static String getLastSameSeasonBeginDate(String date) {
        Date dt = JobDateUtil.stringToDateShort(JobDateUtil.getLastYearSameDate(date));
        return JobDateUtil.dateToStringShort(JobDateUtil.getSeasonBegin(dt));
    }

    public static String getLastSameSeasonEndDate(String date) {
        Date dt = JobDateUtil.stringToDateShort(JobDateUtil.getLastYearSameDate(date));
        return JobDateUtil.dateToStringShort(JobDateUtil.getSeasonEnd(dt));
    }

    public static String getDate8to10(String str) {
        String y = str.substring(0, 4);
        String m = str.substring(4, 6);
        String d = str.substring(6, 8);
        return String.valueOf(y) + "-" + m + "-" + d;
    }

    public static String getDate10to8(String str) {
        String str8 = str.replaceAll("-", "");
        return str8;
    }

    public static String getDateChangeTimeShort(String currentTime, String type, int iQuantity) {
        Date curr = JobDateUtil.stringToDateShort(currentTime);
        curr = JobDateUtil.getDateChangeTime(curr, type, iQuantity);
        return JobDateUtil.dateToStringShort(curr);
    }

    public static String getPreEndDate(String granularity, String jobDate) {
        Date date = JobDateUtil.stringToDateShort(jobDate);
        String year = "";
        String month = "01";
        String day = "01";
        year = JobDateUtil.getYear(date);
        month = JobDateUtil.getMonth(date);
        day = JobDateUtil.getDay(date);
        int preM = Integer.parseInt(month) - 1;
        if (preM == 0) {
            preM = 12;
        }
        String newdate = JobDateUtil.getDateChangeALL(jobDate, granularity, -1);
        return JobDateUtil.getEndDate(granularity, newdate);
    }

    public static String getNextEndDate(String granularity, String jobDate, int index) {
        String newdate = JobDateUtil.getDateChangeALL(jobDate, granularity, index);
        return JobDateUtil.getEndDate(granularity, newdate);
    }

    public static String getDateChangeTime2(String currentTime, String type, int iQuantity) {
        java.sql.Date curr = JobDateUtil.stringToSqlDateShort(currentTime);
        Date curr2 = JobDateUtil.getDateChangeTime(curr, type, iQuantity);
        return JobDateUtil.dateToStringShort(curr2);
    }

    public static String getCurrShortDateStr() {
        Date date_time = new Date();
        return JobDateUtil.FormatDate(date_time, "yyyy-MM-dd");
    }

    public static Date getIncDateTimeYear(Date d, int years) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(1, years);
        return c.getTime();
    }

    public static String getCurrShortDate8Char() {
        String str = JobDateUtil.getCurrShortDateStr();
        String[] pieces = str.split("-");
        return String.valueOf(pieces[0]) + pieces[1] + pieces[2];
    }

    public static Date getIncDateTimeMonth(Date d, int months) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(2, months);
        return c.getTime();
    }

    public static Date getIncDateTime(Date d, int days) {
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(5, days);
        return c.getTime();
    }

    public static String replaceString(String mainString, String oldString, String newString) {
        int i;
        if (mainString == null) {
            return null;
        }
        if (oldString == null || oldString.length() == 0) {
            return mainString;
        }
        if (newString == null) {
            newString = "";
        }
        if ((i = mainString.lastIndexOf(oldString)) < 0) {
            return mainString;
        }
        StringBuffer mainSb = new StringBuffer(mainString);
        while (i >= 0) {
            mainSb.replace(i, i + oldString.length(), newString);
            i = mainString.lastIndexOf(oldString, i - 1);
        }
        return mainSb.toString();
    }
}
