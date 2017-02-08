/**
 * Created by vincegonzalez on 2/8/17.
 */

/*
{"namespace": "example.hdfs2cass",
 "type": "record",
 "name": "SongStream",
 "fields": [
     {"name": "user_id", "type": "string"},
     {"name": "timestamp", "type": "int"},
     {"name": "song_id", "type": "int"}
 ]
}
 */

public class SongStream {
 String userId;
  long timestamp;
 long songId;

 public void setUserId(String id) {
   userId = id;
 }

 public String getUserId() {
   return userId;
 }

 public void setTimestamp(long t) {
   timestamp = t;
 }

 public long getTimestamp() {
   return timestamp;
 }

 public void setSongId(long id) {
   songId = id;
 }

 public long songId() {
   return songId();
 }

}
