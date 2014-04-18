users = load '/Spring2014_HW-3-Pig/users_new.dat' using PigStorage(';') as (userID:chararray, gender:chararray, age:int, occupation:chararray, zip:chararray);
rating = load '/Spring2014_HW-3-Pig/ratings_new.dat' using PigStorage(';') as (userID:chararray,movieID:chararray, rating:float, Timestamp:chararray);

CG = COGROUP users by userID, rating by userID;
dump CG;
