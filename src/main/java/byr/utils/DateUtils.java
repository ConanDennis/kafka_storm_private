package byr.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * Created by Administrator on 2017/8/1.
 */
public class DateUtils {

    public static String formatDate(String date){
        if(StringUtils.isBlank(date)){
            return date;
        }
        String result = date.replaceAll("/", ":");
        return result;
    }

    public static String splitYearMonthAndDay(String date){
        if(StringUtils.isBlank(date)){
            return date;
        }
        String[] split = date.split(":");
        if(split != null && split.length > 0){
            return split[0];
        }
        return date;
    }

    /**
     * 生成时间序列
     * @return
     */
    public static List<String> buildTimeList(){
        List<String> times = new ArrayList<>();
        for (int i = 0;i < 24;i++){
            if(i < 10){
                times.add("0"+i+":00:00");
            }else{
                times.add(i+":00:00");
            }
        }
        return times;
    }

    /**
     * 生成每天时间序列
     * @param date
     * @return
     */
    public static List<String> buildTimeList(String date){
        List<String> times = new ArrayList<>();
        for (int i = 0;i < 24;i++){
            if(i < 10){
                times.add(date+" 0"+i+":00:00");
            }else{
                times.add(date+" "+i+":00:00");
            }
        }
        System.out.println(times);
        return times;
    }

    /**
     * 根据时间确定分区
     * 01:00:00 会落入到 01:00:00
     * 01:00:01 会落入到 02:00:00
     * 23:00:01 会落入到 00:00:00
     * @param time
     * @return
     */
    public static String getCurrentTimeRange(String time){
        String[] date = time.split(":");
        if(date == null || date.length != 3){
            return null;
        }
        if(Integer.parseInt(date[1]) == 0 && Integer.parseInt(date[2]) == 0){
            return date[0]+":00:00";
        }else if(Integer.parseInt(date[1]) > 0 || (Integer.parseInt(date[1]) == 0 && Integer.parseInt(date[2]) > 0)){
            if(date[0].equals("23")){
                return "00:00:00";
            }else{
                int i = Integer.parseInt(date[0]) + 1;
                return (i < 10 ? "0"+i : i)+":00:00";
            }
        }
        return null;
    }

    public static final String C_DATE_PATTON_DEFAULT = "yyyy-MM-dd";

    public static boolean isValidDate(String createDate,String startDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat(C_DATE_PATTON_DEFAULT);
            Date cdate = format.parse(createDate);
            Date sdate = format.parse(startDate);
            if(cdate.getTime()>=sdate.getTime()) {
                return true;
            }else {
                return false;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(getCurrentTimeRange("22:42:19"));
        System.out.println(getCurrentTimeRange("00:00:01"));
        System.out.println(getCurrentTimeRange("12:00:00"));
        System.out.println(getCurrentTimeRange("12:00:01"));
        System.out.println(getCurrentTimeRange("23:00:02"));
        System.out.println(getCurrentTimeRange("23:00:02"));

        if(isValidDate("2015-01-01", "2014-04-01")) {
            System.out.println("true");
        }else {
            System.out.println("false");
        }
    }
}
