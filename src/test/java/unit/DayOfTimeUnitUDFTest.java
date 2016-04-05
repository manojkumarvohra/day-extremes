package unit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.bigdata.hive.udf.impl.FirstDayOfTimeUnitUDF;
import com.bigdata.hive.udf.impl.LastDayOfTimeUnitUDF;

import model.DeferredArgument;

/*
 * @Author: Manoj Kumar Vohra
 * @Date: 02-April-2016
 */

public class DayOfTimeUnitUDFTest {

	private static final String FUNCTION_USAGE = "Invalid function usage: Correct Usage => FunctionName(<String> unit, <String/Timestamp/Date> date, <String> input_format[optional], <String> output_format[optional], <boolean> include_interval [optional], <String> interval[optional])";
	private FirstDayOfTimeUnitUDF firstDayOfTimeUnitUDF = new FirstDayOfTimeUnitUDF();
	private LastDayOfTimeUnitUDF lastDayOfTimeUnitUDF = new LastDayOfTimeUnitUDF();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void shouldThrowExceptionForNoArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(FUNCTION_USAGE);

		firstDayOfTimeUnitUDF.initialize(new ObjectInspector[0]);
	}

	@Test
	public void shouldThrowExceptionForLessThan2ArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(FUNCTION_USAGE);

		firstDayOfTimeUnitUDF.initialize(new ObjectInspector[1]);
	}

	@Test
	public void shouldThrowExceptionForMoreThan6ArgumentsPassed() throws Exception {
		expectedException.expect(UDFArgumentLengthException.class);
		expectedException.expectMessage(FUNCTION_USAGE);

		firstDayOfTimeUnitUDF.initialize(new ObjectInspector[7]);
	}

	@Test
	public void shouldThrowExceptionIfUnitIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("unit cannot be null");

		DeferredObject[] arguments = new DeferredObject[2];
		arguments[0] = new DeferredArgument<String>(null);

		firstDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSequenceNameIsNotString() throws Exception {
		expectedException.expect(UDFArgumentTypeException.class);
		expectedException
				.expectMessage("Only String is accepted for unit parameter but bigint is passed as first argument");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfDateValueIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);

		expectedException.expectMessage("date cannot be null");
		DeferredObject[] arguments = new DeferredObject[2];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>(null);

		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfDateValueIsNotOfCorrectType() throws Exception {
		expectedException.expect(UDFArgumentTypeException.class);
		expectedException.expectMessage(
				"Only STRING/TIMESTAMP/DATEWRITABLE are accepted for date parameter but bigint is passed as second argument");

		ObjectInspector[] objectInspector = new ObjectInspector[2];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldNotThrowExceptionIfDateValueIsOfTypeString() throws Exception {
		ObjectInspector[] objectInspector = new ObjectInspector[2];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldNotThrowExceptionIfDateValueIsOfTypeDate() throws Exception {
		ObjectInspector[] objectInspector = new ObjectInspector[2];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.writableDateObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldNotThrowExceptionIfDateValueIsOfTypeTimestamp() throws Exception {
		ObjectInspector[] objectInspector = new ObjectInspector[2];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfInputFormatIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);

		expectedException.expectMessage("input_format cannot be null");
		DeferredObject[] arguments = new DeferredObject[3];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>(null);

		firstDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfInputFormatIsNotString() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage(
				"Only String is accepted for input_format parameter but bigint is passed as third argument");

		ObjectInspector[] objectInspector = new ObjectInspector[3];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfOutputFormatIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);

		expectedException.expectMessage("output_format cannot be null");
		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>(null);

		firstDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfOutputFormatIsNotString() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage(
				"Only String is accepted for output_format parameter but bigint is passed as fourth argument");

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfIncludeIntervalIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("include_interval cannot be null");

		ObjectInspector[] objectInspector = new ObjectInspector[5];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[5];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<Boolean>(null);

		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfIncludeIntervalIsNotBoolean() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage(
				"Only boolean is accepted for include_interval parameter but string is passed as fifth argument");

		ObjectInspector[] objectInspector = new ObjectInspector[5];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionIfIntervalIsNull() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("interval cannot be null");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>(null);
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfIntervalIsNotString() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException
				.expectMessage("Only String is accepted for interval parameter but int is passed as sixth argument");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaIntObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);
	}

	@Test
	public void shouldThrowExceptionForInvalidInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid interval value. Supported format is HH:MM:SS");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfHourIsGreaterThan23InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid hour value in interval. It should be in between 0 and 23");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("72:22:22");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfHourIsLessThan0InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid hour value in interval. It should be in between 0 and 23");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("-2:22:34");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfMinutesIsGreaterThan59InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid minutes value in interval. It should be in between 0 and 59");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:72:22");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfMinutesIsLessThan0InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid minutes value in interval. It should be in between 0 and 59");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:-2:22");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSecondsIsGreaterThan59InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid seconds value in interval. It should be in between 0 and 59");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:72");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldThrowExceptionIfSecondsIsLessThan0InInterval() throws Exception {
		expectedException.expect(UDFArgumentException.class);
		expectedException.expectMessage("Invalid seconds value in interval. It should be in between 0 and 59");

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:-2");
		lastDayOfTimeUnitUDF.evaluate(arguments);
	}

	@Test
	public void shouldGetCorrectDisplayStringForFirstDayOfTimeUnitUDF() throws Exception {

		assertThat(firstDayOfTimeUnitUDF.getDisplayString(null), is(
				"Gets first day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added."));
	}

	@Test
	public void shouldGetFirstDayWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22"));
	}

	@Test
	public void shouldGetFirstDayWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22 23:22:22"));
	}

	@Test
	public void shouldGetFirstDayWithIntervalIfBothIntervalFormatSpecifiedAndIncludeIntervalIsTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd HH:mm");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22 23:22"));
	}

	@Test
	public void shouldGetFirstDayOfWeekWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("WEEK");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-17"));
	}

	@Test
	public void shouldGetFirstDayOfWeekWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("WEEk");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-17 23:22:22"));
	}

	@Test
	public void shouldGetFirstDayOfMonthWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-01"));
	}

	@Test
	public void shouldGetFirstDayOfMonthWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-01 23:22:22"));
	}

	@Test
	public void shouldGetFirstDayOfQuarterWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("QUARTER");
		arguments[1] = new DeferredArgument<String>("1986-08-02");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1986-07-01"));
	}

	@Test
	public void shouldGetFirstDayOfQuarterWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("QUARTER");
		arguments[1] = new DeferredArgument<String>("1986-08-02");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1986-07-01 23:22:22"));
	}

	@Test
	public void shouldGetFirstDayOfYearWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("YEAR");
		arguments[1] = new DeferredArgument<String>("1983-07-18");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1983-01-01"));
	}

	@Test
	public void shouldGetFirstDayOfYearWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		firstDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("YEAR");
		arguments[1] = new DeferredArgument<String>("1983-07-18");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) firstDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1983-01-01 23:22:22"));
	}

	@Test
	public void shouldGetCorrectDisplayStringForLastDayOfTimeUnitUDF() throws Exception {

		assertThat(lastDayOfTimeUnitUDF.getDisplayString(null), is(
				"Gets last day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added."));
	}

	@Test
	public void shouldGetLastDayWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22"));
	}

	@Test
	public void shouldGetLastDayWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22 23:22:22"));
	}

	@Test
	public void shouldGetLastDayWithIntervalIfBothIntervalFormatSpecifiedAndIncludeIntervalIsTrue() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("DAY");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd HH:mm");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-22 23:22"));
	}

	@Test
	public void shouldGetLastDayOfWeekWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("WEEK");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-23"));
	}

	@Test
	public void shouldGetLastDayOfWeekWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("WEEk");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-23 23:22:22"));
	}

	@Test
	public void shouldGetLastDayOfMonthWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-31"));
	}

	@Test
	public void shouldGetLastDayOfMonthWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("MONTH");
		arguments[1] = new DeferredArgument<String>("2011-01-22");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("2011-01-31 23:22:22"));
	}

	@Test
	public void shouldGetLastDayOfQuarterWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("QUARTER");
		arguments[1] = new DeferredArgument<String>("1986-08-02");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1986-09-30"));
	}

	@Test
	public void shouldGetLastDayOfQuarterWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("QUARTER");
		arguments[1] = new DeferredArgument<String>("1986-08-02");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1986-09-30 23:22:22"));
	}

	@Test
	public void shouldGetLastDayOfYearWithoutInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[4];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[4];
		arguments[0] = new DeferredArgument<String>("YEAR");
		arguments[1] = new DeferredArgument<String>("1983-07-18");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");

		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1983-12-31"));
	}

	@Test
	public void shouldGetLastDayOfYearWithInterval() throws Exception {

		ObjectInspector[] objectInspector = new ObjectInspector[6];
		objectInspector[0] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[1] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[2] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[3] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
		objectInspector[4] = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		objectInspector[5] = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

		lastDayOfTimeUnitUDF.initialize(objectInspector);

		DeferredObject[] arguments = new DeferredObject[6];
		arguments[0] = new DeferredArgument<String>("YEAR");
		arguments[1] = new DeferredArgument<String>("1983-07-18");
		arguments[2] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[3] = new DeferredArgument<String>("yyyy-MM-dd");
		arguments[4] = new DeferredArgument<BooleanWritable>(new BooleanWritable(true));
		arguments[5] = new DeferredArgument<String>("23:22:22");
		Text outputDate = (Text) lastDayOfTimeUnitUDF.evaluate(arguments);

		assertThat(outputDate.toString(), is("1983-12-31 23:22:22"));
	}

}
