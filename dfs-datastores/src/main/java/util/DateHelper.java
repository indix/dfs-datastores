package util;

import org.joda.time.DateTime;

public class DateHelper {
    public DateHelper() {
    }

    public static String weekInterval(DateTime date) {
        int day = date.getDayOfWeek();
        String startDate = date.minusDays(day - 1).toString("YYYYMMdd");
        String endDate = date.plusDays(7 - day).toString("YYYYMMdd");
        return String.format(startDate + "_" + endDate, new Object[0]);
    }

    public static String getQuarter(DateTime dateTime) {
        int monthOfYear = dateTime.getMonthOfYear();
        int year = dateTime.getYear();
        int currentQuarter = ((monthOfYear - 1) / 3) + 1;
        return year + "/Q" + String.valueOf(currentQuarter);
    }
}

