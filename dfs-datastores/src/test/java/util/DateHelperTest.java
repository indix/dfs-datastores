package util;

import junit.framework.TestCase;
import org.joda.time.DateTime;

public class DateHelperTest extends TestCase {

    public void testWeekInterval() {
        DateTime date1 = new DateTime(2017, 9, 18, 0, 0);
        DateTime date2 = new DateTime(2017, 9, 30, 0, 0);

        String weekInterval1 = DateHelper.weekInterval(date1);
        String weekInterval2 = DateHelper.weekInterval(date2);

        assertEquals("20170918_20170924", weekInterval1);
        assertEquals("20170925_20171001", weekInterval2);
    }

    public void testGetQuarter() {
        DateTime date1 = new DateTime(2017, 9, 18, 0, 0);
        DateTime date2 = new DateTime(2016, 12, 31, 0, 0);

        String quarterResult1 = DateHelper.getQuarter(date1);
        String quarterResult2 = DateHelper.getQuarter(date2);

        assertEquals("2017/Q3", quarterResult1);
        assertEquals("2016/Q4", quarterResult2);
    }

}