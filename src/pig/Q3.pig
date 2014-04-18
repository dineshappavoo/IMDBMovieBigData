
rating = load '/Spring2014_HW-3-Pig/ratings_new.dat' using PigStorage(';') as (userID:chararray,movieID:chararray, rating:chararray, timestamp:chararray);
movies  = load '/Spring2014_HW-3-Pig/movies_new.dat' using PigStorage(';') as (movieID:chararray, Title:chararray, Genre:chararray);

Result1 = COGROUP movies by movieID, rating by movieID;
Result2 = foreach Result1 GENERATE $0,FLATTEN($1),FLATTEN($2);
dump Result2;