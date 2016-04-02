# Day Extremes
The project supports two HIVE UDFs to enable fetching first/last day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added.

-----
Usage
-----
*FIRST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <boolean> include_interval [optional], <String> interval[optional])*

*LAST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <boolean> include_interval [optional], <String> interval[optional])*


--------
Examples
--------
hive> SELECT FIRST_DAY_OF('QUARTER','22-01-2011','dd-MM-yyyy', true, '23:45:45');
		> 2011-01-01 23:45:45
hive> SELECT FIRST_DAY_OF('YEAR','02-08-2011','dd-MM-yyyy', false);
		> 2011-01-01
hive> SELECT LAST_DAY_OF('QUARTER','22-01-2011','dd-MM-yyyy', true, '23:45:45');
		> 2011-06-30 23:45:45
hive> SELECT LAST_DAY_OF('YEAR','02-08-2011','dd-MM-yyyy', false);\n"
		> 2011-12-31


------------
Installation
------------

- checkout the repository
- make the package
- add the jar (without dependencies) to hive
- create a temporary/permanent function 'first_day_of' using the class 'com.bigdata.hive.udf.impl.FirstDayOfTimeUnitUDF'
- create a temporary/permanent function 'last_day_of' using the class 'com.bigdata.hive.udf.impl.LastDayOfTimeUnitUDF'
