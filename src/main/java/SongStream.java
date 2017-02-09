/** Created by vincegonzalez on 2/8/17. */

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
  int timestamp;
  int songId;

  public void setUserId(String id) {
    userId = id;
  }

  public String getUserId() {
    return userId;
  }

  public void setTimestamp(int t) {
    timestamp = t;
  }

  public int getTimestamp() {
    return timestamp;
  }

  public void setSongId(int id) {
    songId = id;
  }

  public int getSongId() {
    return songId;
  }
}
