package org.opendatakit.services.utilities;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtil {

    public static String getDisplayDate(long time){
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_TIME_FORMAT);
        return sdf.format(new Date(time));
    }

}
