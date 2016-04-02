# Day Extremes
The project supports two HIVE UDFs to enable fetching first/last day of day/week/month/quarter/year for a provided date with optional interval timestamp value can be added.

-----
Usage
-----
*FIRST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <boolean> include_interval [optional], <String> interval[optional])*

*LAST_DAY_OF(<String> unit, <String/Timestamp/Date> date, <String> format[optional], <boolean> include_interval [optional], <String> interval[optional])*

--------------
Installation
------------

- checkout the repository
- make the package
- add the jar (without dependencies) to hive
- create a temporary/permanent function 'first_day_of' using the class 'com.bigdata.hive.udf.impl.FirstDayOfTimeUnitUDF'
- create a temporary/permanent function 'last_day_of' using the class 'com.bigdata.hive.udf.impl.LastDayOfTimeUnitUDF'
