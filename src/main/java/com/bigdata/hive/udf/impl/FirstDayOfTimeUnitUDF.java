package com.bigdata.hive.udf.impl;

import java.util.Date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.joda.time.DateTime;

@Description(name = "first_day_of", value = "_FUNC_(unit, input_date, format, include_interval, interval) - Returns the first date for unit (day/week/month/quarter/year) "
		+ "based on input_date.", extended = "unit accepts value DAY, WEEK, MONTH, QUARTER, YEAR.\n "
				+ "input_date is a string with default assumed format being 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n "
				+ "format is a string is input date is in format other than default 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'.\n "
				+ "include_interval is a boolean to control whether output date would contain the timestamp or not.\n "
				+ "interval is a string accepted in format 'HH:mm:ss' which can be added to return value.\n "
				+ "Example:\n " + "  > SELECT _FUNC_('QUARTER','22-01-2011','dd-MM-yyyy', true, '23:45:45');\n"
				+ "  '2011-01-01 23:45:45'" + ") " + "  > SELECT _FUNC_('YEAR','02-08-2011','dd-MM-yyyy', false);\n"
				+ "  '2011-01-01'")
public class FirstDayOfTimeUnitUDF extends AbstractDayOfTimeUnitUDF {

	@Override
	public String getDisplayString(String[] children) {
		return "Gets first day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added.";
	}

	@Override
	public void calculateDayWithInterval(Date date, String output_format, boolean includeInterval, Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date));
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfWeekWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).withDayOfWeek(1));
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfMonthWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).withDayOfMonth(1));
		setOutputDate(includeInterval, dateTime, output_format);
	}

	@Override
	public void calculateDayOfQuarterWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = new DateTime(date);
		int month = dateTime.getMonthOfYear() - 1;
		int quarter = month / 3 + 1;
		DateTime firstDayOfQuarter = new DateTime(dateTime.getYear(), 3 * quarter - 2, 1, 0, 0, 0);
		DateTime firstDayOfQuarterWithInterval = addInterval(includeInterval, interval, firstDayOfQuarter);
		setOutputDate(includeInterval, firstDayOfQuarterWithInterval, output_format);
	}

	@Override
	public void calculateDayOfYearWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval) {
		DateTime dateTime = addInterval(includeInterval, interval, new DateTime(date).withDayOfYear(1));
		setOutputDate(includeInterval, dateTime, output_format);
	}

}