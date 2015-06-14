
// read the artist id to name mapping
val artist_data = sc.textFile("artist_data.txt")
val artist_id = artist_data.flatMap(line => 
    {val (id, name) = line.span(_ != '\t')
    if (name.isEmpty) {
         None
    }
    else{
       try {
          Some ((id.toInt, name.trim))
          } catch { case e: NumberFormatException => None}
        }
    })
print (artist_id.count())


// read the implicit ratings data
case class UserData(userID: Int, artistID:Int, numPlays:Int)

val user_artist_data = sc.textFile("user_artist_data.txt").
                          map(line => line.split(' ').map(_.toInt))
                         

print (user_artist_data.count())

// use Spark SQL for data exploration
val sql_cxt = new org.apache.spark.sql.SQLContext(sc)
import sql_cxt.implicits._
val ua_data = user_artist_data.map(t=> UserData(t(0),t(1),t(2)))
print ( ua_data.count())

ua_data.toDF().registerTempTable("ua_data")

// rank artists by number of plays in descending order 
val rank_artist_ID = sql_cxt.sql("SELECT artistID, SUM (numPlays) AS total_plays FROM ua_data GROUP BY artistID ORDER BY total_plays DESC")

// rank artists by number of distinct listeners in descending order
val rank_artist_by_user = sql_cxt.sql("SELECT artistID, COUNT (DISTINCT(userID)) AS num_users FROM ua_data GROUP BY artistID ORDER BY num_users DESC")

print(rank_artist_ID.count())

rank_artist_ID.take(5).foreach(println)

// number of plays ranking
val top = rank_artist_ID.take(5)
val names = top.map(t=> (artist_id.lookup(t(0).toString.toInt).head, t(1)))
names.foreach(println)

// number of distinct listeners ranking
val top_artist_by_user = rank_artist_by_user.take(5)
val art_names = top_artist_by_user.map(t=> (artist_id.lookup(t(0).toString.toInt).head, t(1)))
art_names.foreach(println)

// rank users by number of unique artists they listen to
val user_stats = sql_cxt.sql("SELECT userID, COUNT (DISTINCT(artistID)) AS num_arts FROM ua_data GROUP BY userID ORDER BY num_arts DESC")
user_stats.map(t=> t(1).toString.toDouble).stats())

// get the users with few ratings. - use these for recommendation
val low_users = user_stats.map(t=> (t(0), t(1))).filter(t => t._2.toString.toInt < 5)
print (low_users.count())

low_users.take(5).foreach(println)

import org.apache.spark.mllib.recommendation._
val ratings = user_artist_data.map{t => 
     //val Array(user_id, artist_id, count) = line.split(' ').map(_.toInt)
     //val a_id = bc_aalias.value.getOrElse(artist_id, artist_id)
     Rating(t(0), t(1), t(2))
     }.cache()

// split into training and test - optional 
val Array(train_data,test_data) = ratings.randomSplit(Array(0.9,0.1)) 
train_data.cache()
test_data.cache()

// (data, rank, numIter, lambda (reg param), alpha (learning rate)
//val als_mod = ALS.trainImplicit(train_data, 20, 5,0.0001,40.0)

// build ALS model on the ratings data
val als_mod = ALS.trainImplicit(ratings, 20, 5,0.0001,40.0)


//val target = test_data.match( case Rating(u, _,_) => u)
//test_data.take(5).foreach(println)

// recommend artists
var u = 2091831
val recos = als_mod.recommendProducts(u,5)


recos.foreach(println)

// get names of recommended artists
val reco_prod_ids =  recos.map(_.product).toSet
artist_id.filter{case (id, name) => reco_prod_ids.contains(id)}.values.collect().
             foreach(println)

val artists_for_user = user_artist_data. //map(_.split(' '))
                         filter(t=> t(0).toInt==u)
                             //case Array(user,_,_) => user.toInt == u}
 
val prod_rated = artists_for_user.map{ 
                   case Array(_, artist,_) => artist.toInt
                 }.collect().toSet
                 


// validation : compare with  artists the user has listened to 
artist_id.filter{case (id, name) => prod_rated.contains(id)}.values.collect().
             foreach(println)


