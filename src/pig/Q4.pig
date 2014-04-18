REGISTER /home/004/p/px/pxn121530/BigData/HW3/pig_udf.jar;

A = LOAD '/Spring2014_HW-3-Pig/movies_new.dat' using PigStorage(';') as (MOVIEID:chararray, TITLE:chararray, GENRE:chararray);

B = FOREACH A GENERATE MOVIEID, TITLE, FORMAT_GENRE(GENRE);

C = LIMIT B 10;

DUMP C;