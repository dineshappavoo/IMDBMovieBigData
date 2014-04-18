movies = load '/Spring2014_HW-3-Pig/movies_new.dat' using PigStorage(';') AS (movieid:chararray, moviename:chararray, genre:chararray);
ratings = load '/Spring2014_HW-3-Pig/ratings_new.dat' using PigStorage(';') AS (userid:chararray, movieid:chararray, rating:float, time:chararray);
users = load '/Spring2014_HW-3-Pig/users_new.dat' using PigStorage(';') AS (userid:chararray, gender:chararray, age:int,occupation:chararray, zip:chararray);

out = foreach movies GENERATE movieid as mid, FLATTEN(TOKENIZE(genre,'|')) as g;
out2 = filter out by g matches 'Action' or g matches 'War';
gd = group out2 by mid;
counts = foreach gd generate group as mid,COUNT(out2) as nummovieid;
mlist = filter counts by $1==2;
mlisting = foreach mlist generate mid;
joined_mr = join mlisting by mid, ratings by movieid;
grouping = group joined_mr by mid;
counting = foreach grouping generate group as mid,AVG(joined_mr.rating) as avg_rating;
maxcount = ORDER counting by avg_rating DESC;
toprating = limit maxcount 1;
movietop = filter counting by $1==toprating.$1;
ratingtablelist = filter ratings by movieid matches movietop.$0;
ratingtable = foreach ratingtablelist generate $0 as userid;
ratUser = join users by userid,ratingtable by userid;
females = filter ratUser by $1 matches 'F' and ($2 > 20 and $2 < 30);
justFemales = foreach females generate $0 as fuser_id;
distinct_female = distinct justFemales;
dump distinct_female
