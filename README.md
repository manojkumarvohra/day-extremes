# Day Extremes
The project supports HIVE UDFs to enable fetching first/last day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added.

-----
Usage
-----
*FIRST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> input_format[optional], <String> output_format[optional], <boolean> include_interval [optional], <String> interval[optional])*

*LAST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> input_format[optional], <String> output_format[optional], <boolean> include_interval [optional], <String> interval[optional])*


---------
Examples
--------
hive> SELECT FIRST_DAY_OF('QUARTER','22-01-2011','dd-MM-yyyy', 'yyyy-MM-dd HH:mm:ss', true, '23:45:45');

*2011-01-01 23:45:45*

hive> SELECT FIRST_DAY_OF('YEAR','02-08-2011','dd-MM-yyyy', 'yyyy-MM-dd',);

*2011-01-01*

hive> SELECT LAST_DAY_OF('QUARTER','22-01-2011','dd-MM-yyyy', 'yyyy-MM-dd HH:mm:ss', true, '23:45:45');

*2011-03-31 23:45:45*

hive> SELECT LAST_DAY_OF('YEAR','02-08-2011','dd-MM-yyyy', 'yyyy-MM-dd');

*2011-12-31*

hive> select FIRST_DAY_OF('Year', current_date,'dd-MM-yyyy', 'dd-MMM-yyyy');

*01-Jan-2016*

hive> select FIRST_DAY_OF('month', current_timestamp,'dd-MM-yyyy', 'dd-MMM-yyyy');

*01-Apr-2016*


------------
Installation
------------

- checkout the repository
- make the package
- add the jar (without dependencies) to hive
- create temporary/permanent function first_day_of as 'com.bigdata.hive.udf.impl.FirstDayOfTimeUnitUDF'
- create a temporary/permanent function last_day_of as 'com.bigdata.hive.udf.impl.LastDayOfTimeUnitUDF'
