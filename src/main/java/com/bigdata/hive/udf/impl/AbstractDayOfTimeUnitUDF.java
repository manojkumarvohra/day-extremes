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
import org.apache.hadoop.io.Text;
import org.joda.time.DateTime;

public abstract class AbstractDayOfTimeUnitUDF extends GenericUDF {

	private static final String DATE_WITHOUT_STAMP = "yyyy-MM-dd";
	private static final String FORMAT_DATE_WITH_STAMP = "yyyy-MM-dd HH:mm:ss";
	private transient Converter dateConverter;
	private final Calendar calendar = Calendar.getInstance();
	private transient PrimitiveCategory dateType;
	private final Text outputDate = new Text();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

		switch (arguments.length) {

		case 1:
			throw new UDFArgumentLengthException(
					"Invalid function usage: Correct Usage => FunctionName(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <boolean> include_interval [optional], <String> interval[optional])");
		case 2:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			break;

		case 3:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyFormatInspector(arguments);
			break;

		case 4:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyFormatInspector(arguments);
			verifyIncludeIntervalInspector(arguments);
			break;

		case 5:
			verifyUnitInspector(arguments);
			verifyDateInspector(arguments);
			verifyFormatInspector(arguments);
			verifyIncludeIntervalInspector(arguments);
			verifyIntervalInspector(arguments);
			break;
		default:
			throw new UDFArgumentLengthException(
					"Invalid function usage: Correct Usage => FunctionName(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <String> interval[optional])");
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
		String format = null;
		boolean includeInterval = false;
		Integer[] interval = null;

		switch (arguments.length) {

		case 2:
			unit = checkAndGetUnit(arguments);
			date = checkAndGetDate(arguments, DATE_WITHOUT_STAMP);
			break;
		case 3:
			unit = checkAndGetUnit(arguments);
			format = checkAndGetFormat(arguments);
			date = checkAndGetDate(arguments, format);
			break;
		case 4:
			unit = checkAndGetUnit(arguments);
			format = checkAndGetFormat(arguments);
			date = checkAndGetDate(arguments, format);
			includeInterval = checkAndGetIncludeInterval(arguments);
			break;
		case 5:
			unit = checkAndGetUnit(arguments);
			format = checkAndGetFormat(arguments);
			date = checkAndGetDate(arguments, format);
			includeInterval = checkAndGetIncludeInterval(arguments);
			interval = checkAndGetInterval(arguments);
			break;
		}

		switch (unit) {

		case DAY:
			calculateDayWithInterval(date, includeInterval, interval);
			break;

		case WEEK:
			calculateDayOfWeekWithInterval(date, includeInterval, interval);
			break;

		case MONTH:
			calculateDayOfMonthWithInterval(date, includeInterval, interval);
			break;

		case QUARTER:
			calculateDayOfQuarterWithInterval(date, includeInterval, interval);
			break;

		case YEAR:
			calculateDayOfYearWithInterval(date, includeInterval, interval);
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

	private String checkAndGetFormat(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object formatValue = arguments[2].get();

		if (formatValue == null) {
			throw new UDFArgumentException("format cannot be null");
		}

		String format = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveJavaObject(formatValue);

		return format;
	}

	private boolean checkAndGetIncludeInterval(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object includeIntervalValue = arguments[3].get();

		if (includeIntervalValue == null) {
			throw new UDFArgumentException("include interval cannot be null");
		}

		boolean includeInterval = (Boolean) PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
				.getPrimitiveJavaObject(includeIntervalValue);

		return includeInterval;
	}

	private Integer[] checkAndGetInterval(DeferredObject[] arguments) throws HiveException, UDFArgumentException {
		Object intervalValue = arguments[4].get();

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

		if (dateInspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentTypeException(1,
					"Only STRING/TIMESTAMP/DATEWRITABLE are accepted for date parameter but "
							+ dateInspector.getTypeName() + " is passed as second argument");
		}
	}

	private void verifyFormatInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector formatInspector = arguments[2];
		if (!(formatInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(2, "Only String is accepted for format parameter but "
					+ formatInspector.getTypeName() + " is passed as third argument");
		}
	}

	private void verifyIncludeIntervalInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector includeIntervalInspector = arguments[3];
		if (!(includeIntervalInspector instanceof BooleanObjectInspector)) {
			throw new UDFArgumentTypeException(4, "Only boolean is accepted for include_interval parameter but "
					+ includeIntervalInspector.getTypeName() + " is passed as fourth argument");
		}
	}

	private void verifyIntervalInspector(ObjectInspector[] arguments) throws UDFArgumentException {
		ObjectInspector intervalInspector = arguments[4];
		if (!(intervalInspector instanceof StringObjectInspector)) {
			throw new UDFArgumentTypeException(4, "Only String is accepted for interval parameter but "
					+ intervalInspector.getTypeName() + " is passed as fifth argument");
		}
	}

	protected void setOutputDate(boolean includeInterval, DateTime dateTime) {
		String outPutFormat = includeInterval ? FORMAT_DATE_WITH_STAMP : DATE_WITHOUT_STAMP;
		outputDate.set(dateTime.toString(outPutFormat));
		System.out.println(outputDate);
	}

	protected DateTime addInterval(boolean includeInterval, Integer[] interval, DateTime dateTime) {

		if (includeInterval) {
			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), interval[0],
					interval[1], interval[2]);
		}
		return dateTime;
	}

	protected abstract void calculateDayOfYearWithInterval(Date date, boolean includeInterval, Integer[] interval);

	protected abstract void calculateDayOfQuarterWithInterval(Date date, boolean includeInterval, Integer[] interval);

	protected abstract void calculateDayOfMonthWithInterval(Date date, boolean includeInterval, Integer[] interval);

	protected abstract void calculateDayOfWeekWithInterval(Date date, boolean includeInterval, Integer[] interval);

	protected abstract void calculateDayWithInterval(Date date, boolean includeInterval, Integer[] interval);

}
