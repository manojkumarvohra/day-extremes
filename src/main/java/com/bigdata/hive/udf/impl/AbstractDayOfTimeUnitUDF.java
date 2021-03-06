package com.bigdata.hive.udf.impl;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

/*
 * @Author: Manoj Kumar Vohra
 * @Date: 02-April-2016
 */
public abstract class AbstractDayOfTimeUnitUDF extends GenericUDF {

	private static final String FUNCTION_USAGE = "Invalid function usage: Correct Usage => FunctionName(<String> unit, <String/Timestamp/Date> date, <String> input_format[optional], <String> output_format[optional], <boolean> include_interval [optional], <String> interval[optional])";
	private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	private static final String DEFAULT_INTERVAL_FORMAT = "HH:mm:ss";
	private transient Converter dateConverter;
	private final Calendar calendar = Calendar.getInstance();
	private transient PrimitiveCategory dateType;
	private final Text outputDate = new Text();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		switch (arguments.length) {

		case 1:
			throw new UDFArgumentLengthException(FUNCTION_USAGE);
		case 2:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			break;

		case 3:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyInputFormatInspector(arguments);
			break;

		case 4:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyInputFormatInspector(arguments);
			verifyOutputFormatInspector(arguments);
			break;

		case 5:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyInputFormatInspector(arguments);
			verifyOutputFormatInspector(arguments);
			verifyIncludeIntervalInspector(arguments);
			break;

		case 6:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyInputFormatInspector(arguments);
			verifyOutputFormatInspector(arguments);
			verifyIncludeIntervalInspector(arguments);
			verifyIntervalInspector(arguments);
			break;
		default:
			throw new UDFArgumentLengthException(FUNCTION_USAGE);
		}

		dateType = ((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory();
		PrimitiveObjectInspector dateObjectInspector = (PrimitiveObjectInspector) arguments[1];
		switch (dateType) {
		case STRING:
		case VARCHAR:
		case CHAR:
			dateType = PrimitiveCategory.STRING;
			dateConverter = ObjectInspectorConverters.getConverter(dateObjectInspector,
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
			break;
		case TIMESTAMP:
			dateConverter = new TimestampConverter(dateObjectInspector,
					PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
			break;
		case DATE:
			dateConverter = ObjectInspectorConverters.getConverter(dateObjectInspector,
					PrimitiveObjectInspectorFactory.writableDateObjectInspector);
			break;
		default:
			throw new UDFArgumentException(
					" FIRST_DAY_OF() only takes STRING/TIMESTAMP/DATEWRITABLE types as second argument, got "
							+ dateType);
		}

		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {

		TimeUnit unit = null;
		Date date = null;
		String input_format = null;
		String output_format = null;
		boolean includeInterval = false;
		Integer[] interval = null;

		switch (arguments.length) {

		case 2:
			unit = checkAndGetUnit(arguments);
			date = checkAndGetDate(arguments, DEFAULT_DATE_FORMAT);
			break;
		case 3:
			unit = checkAndGetUnit(arguments);
			input_format = checkAndGetInputFormat(arguments);
			date = checkAndGetDate(arguments, input_format);
			break;
		case 4:
			unit = checkAndGetUnit(arguments);
			input_format = checkAndGetInputFormat(arguments);
			output_format = checkAndGetOutputFormat(arguments);
			date = checkAndGetDate(arguments, input_format);
			break;
		case 5:
			unit = checkAndGetUnit(arguments);
			input_format = checkAndGetInputFormat(arguments);
			output_format = checkAndGetOutputFormat(arguments);
			date = checkAndGetDate(arguments, input_format);
			includeInterval = checkAndGetIncludeInterval(arguments);
			break;
		case 6:
			unit = checkAndGetUnit(arguments);
			input_format = checkAndGetInputFormat(arguments);
			output_format = checkAndGetOutputFormat(arguments);
			date = checkAndGetDate(arguments, input_format);
			includeInterval = checkAndGetIncludeInterval(arguments);
			interval = checkAndGetInterval(arguments);
			break;
		}

		switch (unit) {

		case DAY:
			calculateDayWithInterval(date, output_format, includeInterval, interval);
			break;

		case WEEK:
			calculateDayOfWeekWithInterval(date, output_format, includeInterval, interval);
			break;

		case MONTH:
			calculateDayOfMonthWithInterval(date, output_format, includeInterval, interval);
			break;

		case QUARTER:
			calculateDayOfQuarterWithInterval(date, output_format, includeInterval, interval);
			break;

		case YEAR:
			calculateDayOfYearWithInterval(date, output_format, includeInterval, interval);
			break;
		}

		return outputDate;
	}

	private TimeUnit checkAndGetUnit(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object unit = arguments[0].get();

		if (unit == null) {
			throw new UDFArgumentException("unit cannot be null");
		}

		TimeUnit timeUnit = null;

		try {
			timeUnit = TimeUnit.valueOf(PrimitiveObjectInspectorFactory.javaStringObjectInspector
					.getPrimitiveJavaObject(unit).trim().toUpperCase());
		} catch (Exception exception) {
			throw new UDFArgumentException("unit can only be one of DAY, WEEK, MONTH, QUARTER, YEAR");
		}

		return timeUnit;
	}

	private Date checkAndGetDate(DeferredObject[] arguments, String format) throws HiveException {

		Object dateArgument = arguments[1].get();

		if (dateArgument == null) {
			throw new UDFArgumentException("date cannot be null");
		}

		SimpleDateFormat formatter = new SimpleDateFormat(format);
		switch (dateType) {
		case STRING:
			String dateString = dateConverter.convert(dateArgument).toString();
			try {
				calendar.setTime(formatter.parse(dateString.toString()));
			} catch (ParseException e) {
				return null;
			}
			break;
		case TIMESTAMP:
			Timestamp ts = ((TimestampWritable) dateConverter.convert(dateArgument)).getTimestamp();
			calendar.setTime(ts);
			break;
		case DATE:
			DateWritable dw = (DateWritable) dateConverter.convert(dateArgument);
			calendar.setTime(dw.get());
			break;
		default:
			throw new UDFArgumentException(
					" FIRST_DAY_OF() only takes STRING/TIMESTAMP/DATEWRITABLE types as second argument, got "
							+ dateType);
		}

		return calendar.getTime();

	}

	private String checkAndGetInputFormat(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object formatValue = arguments[2].get();

		if (formatValue == null) {
			throw new UDFArgumentException("input_format cannot be null");
		}

		String format = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(formatValue);

		return format;
	}

	private String checkAndGetOutputFormat(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object formatValue = arguments[3].get();

		if (formatValue == null) {
			throw new UDFArgumentException("output_format cannot be null");
		}

		String format = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(formatValue);

		return format;
	}

	private boolean checkAndGetIncludeInterval(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object includeIntervalValue = arguments[4].get();

		if (includeIntervalValue == null) {
			throw new UDFArgumentException("include_interval cannot be null");
		}

		boolean includeInterval = (Boolean) PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
				.getPrimitiveJavaObject(includeIntervalValue);

		return includeInterval;
	}

	private Integer[] checkAndGetInterval(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object intervalValue = arguments[5].get();

		if (intervalValue == null) {
			throw new UDFArgumentException("interval cannot be null");
		}

		String interval = PrimitiveObjectInspectorFactory.javaStringObjectInspector
				.getPrimitiveJavaObject(intervalValue);

		String[] intervalChunks = interval.split(":");

		if (intervalChunks.length != 3) {
			throw new UDFArgumentException("Invalid interval value. Supported format is HH:MM:SS");
		}

		Integer[] intervalParsedChunks = new Integer[intervalChunks.length];

		for (int i = 0; i < intervalChunks.length; i++) {
			try {
				intervalParsedChunks[i] = Integer.parseInt(intervalChunks[i]);
				if (i == 0) {
					int hour = intervalParsedChunks[i];
					if (hour < 0 || hour > 23) {
						throw new UDFArgumentException(
								"Invalid hour value in interval. It should be in between 0 and 23");
					}
				} else if (i == 1) {
					int minutes = intervalParsedChunks[i];
					if (minutes < 0 || minutes > 59) {
						throw new UDFArgumentException(
								"Invalid minutes value in interval. It should be in between 0 and 59");
					}
				} else if (i == 2) {
					int seconds = intervalParsedChunks[i];
					if (seconds < 0 || seconds > 59) {
						throw new UDFArgumentException(
								"Invalid seconds value in interval. It should be in between 0 and 59");
					}
				}
			} catch (NumberFormatException numberFormatException) {
				throw new UDFArgumentException("Unparsable interval value. Supported format is HH:MM:SS");
			}
		}
		return intervalParsedChunks;
	}

	private void verifyUnitInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector unitInspector = arguments[0];
		if (!(unitInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(0, "Only String is accepted for unit parameter but "
					+ unitInspector.getTypeName() + " is passed as first argument");
		}
	}

	private void verifyDateInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector dateInspector = arguments[1];

		if (!(dateInspector instanceof StringObjectInspector || dateInspector instanceof WritableDateObjectInspector
				|| dateInspector instanceof WritableTimestampObjectInspector)) {
			throw new UDFArgumentTypeException(1,
					"Only STRING/TIMESTAMP/DATEWRITABLE are accepted for date parameter but "
							+ dateInspector.getTypeName() + " is passed as second argument");
		}
	}

	private void verifyInputFormatInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector formatInspector = arguments[2];
		if (!(formatInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(2, "Only String is accepted for input_format parameter but "
					+ formatInspector.getTypeName() + " is passed as third argument");
		}
	}

	private void verifyOutputFormatInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector formatInspector = arguments[3];
		if (!(formatInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(2, "Only String is accepted for output_format parameter but "
					+ formatInspector.getTypeName() + " is passed as fourth argument");
		}
	}

	private void verifyIncludeIntervalInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector includeIntervalInspector = arguments[4];
		if (!(includeIntervalInspector instanceof BooleanObjectInspector)) {
			throw new UDFArgumentTypeException(4, "Only boolean is accepted for include_interval parameter but "
					+ includeIntervalInspector.getTypeName() + " is passed as fifth argument");
		}
	}

	private void verifyIntervalInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector intervalInspector = arguments[5];
		if (!(intervalInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(4, "Only String is accepted for interval parameter but "
					+ intervalInspector.getTypeName() + " is passed as sixth argument");
		}
	}

	protected void setOutputDate(boolean includeInterval, DateTime dateTime, String output_format_argument_passed) {
		String outputFormat = output_format_argument_passed != null ? output_format_argument_passed
				: DEFAULT_DATE_FORMAT;

		if (includeInterval
				&& !(outputFormat.contains("HH") || outputFormat.contains("mm") || outputFormat.contains("ss"))) {
			outputFormat = outputFormat + " " + DEFAULT_INTERVAL_FORMAT;
		}

		outputDate.set(dateTime.toString(outputFormat));
	}

	protected DateTime addInterval(boolean includeInterval, Integer[] interval, DateTime dateTime) {

		if (includeInterval) {
			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(),
					interval == null ? dateTime.getHourOfDay() : interval[0],
					interval == null ? dateTime.getMinuteOfDay() : interval[1],
					interval == null ? dateTime.getSecondOfDay() : interval[2]);
		}
		return dateTime;
	}

	protected abstract void calculateDayOfYearWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval);

	protected abstract void calculateDayOfQuarterWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval);

	protected abstract void calculateDayOfMonthWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval);

	protected abstract void calculateDayOfWeekWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval);

	protected abstract void calculateDayWithInterval(Date date, String output_format, boolean includeInterval,
			Integer[] interval);

}
