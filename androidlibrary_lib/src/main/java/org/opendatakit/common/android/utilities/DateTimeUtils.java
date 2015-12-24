/*
 * Copyright (C) 2011-2013 University of Washington
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.opendatakit.common.android.utilities;

import android.annotation.SuppressLint;
import org.opendatakit.common.android.utilities.StaticStateManipulator.IStaticFieldManipulator;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Common utility methods for datetime conversions.
 *
 * @author mitchellsundt@gmail.com
 */
public final class DateTimeUtils {
  private static final String t = "DateTimeUtils";

  /**
   * Date format pattern used to parse HTTP date headers in RFC 1123 format.
   * copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in RFC 1036 format.
   * copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_RFC1036 = "EEEE, dd-MMM-yy HH:mm:ss zzz";

  /**
   * Date format pattern used to parse HTTP date headers in ANSI C
   * <code>asctime()</code> format. copied from apache.commons.lang.DateUtils
   */
  private static final String PATTERN_ASCTIME = "EEE MMM d HH:mm:ss yyyy";
  private static final String PATTERN_DATE_TOSTRING = "EEE MMM dd HH:mm:ss zzz yyyy";
  private static final String PATTERN_ISO8601_JAVAROSA = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  private static final String PATTERN_DATE_ONLY_JAVAROSA = "yyyy-MM-dd";
  private static final String PATTERN_TIME_ONLY_JAVAROSA = "HH:mm:ss.SSS'Z'";
  private static final String PATTERN_ISO8601 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
  private static final String PATTERN_ISO8601_WITHOUT_ZONE = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  private static final String PATTERN_ISO8601_DATE = "yyyy-MM-ddZ";
  private static final String PATTERN_ISO8601_TIME = "HH:mm:ss.SSSZ";
  private static final String PATTERN_YYYY_MM_DD_DATE_ONLY_NO_TIME_DASH = "yyyy-MM-dd";
  private static final String PATTERN_NO_DATE_TIME_ONLY = "HH:mm:ss.SSS";
  private static final String PATTERN_GOOGLE_DOCS = "MM/dd/yyyy HH:mm:ss.SSS";
  private static final String PATTERN_GOOGLE_DOCS_DATE_ONLY = "MM/dd/yyyy";

  private static final GregorianCalendar g = new GregorianCalendar(TimeZone.getTimeZone("GMT"));

  private static DateTimeUtils dateTimeUtils = new DateTimeUtils();

  static {
    // register a state-reset manipulator for 'webUtils' field.
    StaticStateManipulator.get().register(50, new IStaticFieldManipulator() {

      @Override
      public void reset() {
        dateTimeUtils = new DateTimeUtils();
      }

    });
  }

  public static DateTimeUtils get() {
    return dateTimeUtils;
  }

  /**
   * For mocking -- supply a mocked object.
   *
   * @param utils
   */
  public static void set(DateTimeUtils utils) {
    dateTimeUtils = utils;
  }

  protected DateTimeUtils() {
  };

  @SuppressLint("SimpleDateFormat")
  private Date parseDateSubset(String value, String[] parsePatterns, Locale l, TimeZone tz) {
    // borrowed from apache.commons.lang.DateUtils...
    Date d = null;
    SimpleDateFormat parser = null;
    ParsePosition pos = new ParsePosition(0);
    for (int i = 0; i < parsePatterns.length; i++) {
      if (i == 0) {
        if (l == null) {
          parser = new SimpleDateFormat(parsePatterns[0]);
        } else {
          parser = new SimpleDateFormat(parsePatterns[0], l);
        }
      } else {
        parser.applyPattern(parsePatterns[i]);
      }
      parser.setTimeZone(tz); // enforce UTC for formats without timezones
      pos.setIndex(0);
      d = parser.parse(value, pos);
      if (d != null && pos.getIndex() == value.length()) {
        return d;
      }
    }
    return d;
  }

  /**
   * Parse a string into a datetime value. Tries the common Http formats, the
   * iso8601 format (used by Javarosa), the default formatting from
   * Date.toString(), and a time-only format.
   *
   * @param value
   * @return
   */
  public Date parseDate(String value) {
    if (value == null || value.length() == 0)
      return null;

    String[] javaRosaPattern = new String[] { PATTERN_ISO8601_JAVAROSA, PATTERN_DATE_ONLY_JAVAROSA,
        PATTERN_TIME_ONLY_JAVAROSA };

    String[] iso8601Pattern = new String[] { PATTERN_ISO8601 };

    String[] localizedParsePatterns = new String[] {
        // try the common HTTP date formats that have time zones
        PATTERN_RFC1123, PATTERN_RFC1036, PATTERN_DATE_TOSTRING };

    String[] localizedNoTzParsePatterns = new String[] {
    // ones without timezones... (will assume UTC)
    PATTERN_ASCTIME };

    String[] tzParsePatterns = new String[] { PATTERN_ISO8601, PATTERN_ISO8601_DATE,
        PATTERN_ISO8601_TIME };

    String[] noTzParsePatterns = new String[] {
        // ones without timezones... (will assume UTC)
        PATTERN_ISO8601_WITHOUT_ZONE, PATTERN_NO_DATE_TIME_ONLY,
        PATTERN_YYYY_MM_DD_DATE_ONLY_NO_TIME_DASH, PATTERN_GOOGLE_DOCS };

    Date d = null;
    // iso8601 parsing is sometimes off-by-one when JR does it...
    d = parseDateSubset(value, iso8601Pattern, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    // try to parse with the JavaRosa parsers (these are approximate -- timezone
    // must be GMT)
    d = parseDateSubset(value, javaRosaPattern, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    // try localized and english text parsers (for Web headers and interactive
    // filter spec.)
    d = parseDateSubset(value, localizedParsePatterns, Locale.ENGLISH, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    d = parseDateSubset(value, localizedParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    d = parseDateSubset(value, localizedNoTzParsePatterns, Locale.ENGLISH,
        TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    d = parseDateSubset(value, localizedNoTzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    // try other common patterns that might not quite match JavaRosa parsers
    d = parseDateSubset(value, tzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    d = parseDateSubset(value, noTzParsePatterns, null, TimeZone.getTimeZone("GMT"));
    if (d != null)
      return d;
    throw new IllegalArgumentException("Unable to parse the date: " + value);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionDateTimeString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaDateTime = new SimpleDateFormat(PATTERN_ISO8601_JAVAROSA);
    asJavarosaDateTime.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaDateTime.format(d);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionDateOnlyString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaDate = new SimpleDateFormat(PATTERN_DATE_ONLY_JAVAROSA);
    asJavarosaDate.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaDate.format(d);
  }

  @SuppressLint("SimpleDateFormat")
  public String asSubmissionTimeOnlyString(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asJavarosaTime = new SimpleDateFormat(PATTERN_TIME_ONLY_JAVAROSA);
    asJavarosaTime.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asJavarosaTime.format(d);
  }

  /**
   * Return the GoogleDocs datetime string representation of a datetime.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String googleDocsDateTime(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asGoogleDoc = new SimpleDateFormat(PATTERN_GOOGLE_DOCS);
    asGoogleDoc.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGoogleDoc.format(d);
  }

  /**
   * Return the GoogleDocs date string representation of a date-only datetime.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String googleDocsDateOnly(Date d) {
    if (d == null)
      return null;
    SimpleDateFormat asGoogleDocDateOnly = new SimpleDateFormat(PATTERN_GOOGLE_DOCS_DATE_ONLY);
    asGoogleDocDateOnly.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGoogleDocDateOnly.format(d);
  }

  /**
   * Return the ISO8601 string representation of a date.
   *
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String iso8601Date(Date d) {
    if (d == null)
      return null;
    // SDF is not thread-safe
    // this pattern has a time zone
    SimpleDateFormat asGMTiso8601 = new SimpleDateFormat(PATTERN_ISO8601); 
    asGMTiso8601.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGMTiso8601.format(d);
  }

  /**
   * Return the RFC1123 string representation of a date.
   * 
   * @param d
   * @return
   */
  @SuppressLint("SimpleDateFormat")
  public String rfc1123Date(Date d) {
    if (d == null)
      return null;
    // SDF is not thread-safe
    // this pattern has a time zone
    SimpleDateFormat asGMTrfc1123 = new SimpleDateFormat(PATTERN_RFC1123);
    asGMTrfc1123.setTimeZone(TimeZone.getTimeZone("GMT"));
    return asGMTrfc1123.format(d);
  }
}
