package com.bigdata.hive.udf.impl;

import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.joda.time.DateTime;

/*
 * @Author: Manoj Kumar Vohra
 * @Date: 02-April-2016
 */

@Description(name = "last_day_of", value = "_FUNC_(unit, input_date, input_format, output_format, include_interval, interval) - Returns the last date for unit (day/week/month/quarter/year) "
		+ "based on input_date.", extended = "unit accepts value DAY, WEEK, MONTH, QUARTER, YEAR.\n "
				+ "input_date is a string with default assumed format being 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n "
				+ "input_format is a string which can be specified if input date is in format other than default 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n "
				+ "output_format is a string which can be specified if output date is expected in format other than default 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n "
				+ "include_interval is a boolean to control whether output date would contain the timestamp or not.\n "
				+ "interval is a string accepted in format 'HH:mm:ss' which can be added to return value.\n "
				+ "Example:\n " + "  > SELECT _FUNC_('QUARTER','22-01-2011','dd-MM-yyyy', true, '23:45:45');\n"
				+ "  '2011-06-30 23:45:45'" + ") " + "  > SELECT _FUNC_('YEAR','02-08-2011','dd-MM-yyyy', false);\n"
				+ "  '2011-12-31'")
public class LastDayOfTimeUnitUDF extends AbstractDayOfTimeUnitUDF {

	@Override
	public String getDisplayString(String[] children) {
		return "Gets last day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added.";
	}

	@Override
	public void calculateDayWithInterval(Date date, String output_format, boolean includeInterval, Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date));
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfWeekWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).dayOfWeek().withMaximumValue());
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfMonthWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).dayOfMonth().withMaximumValue());
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfQuarterWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = new DateTime(date);
		int month = dateTime.getMonthOfYear() - 1;
		int quarter = month / 3 + 1;
		DateTime lastDayOfQuarter = new DateTime(dateTime.getYear(), 3 * quarter + 1, 1, 0, 0, 0).minusDays(1);
		DateTime lastDayOfQuarterWithInterval = addInterval(includeInterval, interval, lastDayOfQuarter);
		setOutputDate(includeInterval, lastDayOfQuarterWithInterval, output_format);
	}

	@Override
	public void calculateDayOfYearWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).dayOfYear().withMaximumValue());
		setOutputDate(includeInterval, dateTime, output_format);
	}

}