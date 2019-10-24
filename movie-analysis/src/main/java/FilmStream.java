import java.util.List;

/**
 * Created by shea on 2019/10/23.
 */
public class FilmStream {
    String time;
    String movieName;
    long totalBox;
    long onlineBox;
    String location;
    List<String> actors;
    List<String> type;

    public FilmStream() {
    }

    public FilmStream(String time, String movieName, long totalBox, long onlineBox, String location, List<String> actors, List<String> type) {
        this.time = time;
        this.movieName = movieName;
        this.totalBox = totalBox;
        this.onlineBox = onlineBox;
        this.location = location;
        this.actors = actors;
        this.type = type;
    }

    public FilmStream(FilmStream filmStream) {
        this.time = filmStream.time;
        this.movieName = filmStream.movieName;
        this.totalBox = filmStream.totalBox;
        this.onlineBox = filmStream.onlineBox;
        this.location = filmStream.location;
        this.actors = filmStream.actors;
        this.type = filmStream.type;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public long getTotalBox() {
        return totalBox;
    }

    public void setTotalBox(long totalBox) {
        this.totalBox = totalBox;
    }

    public long getOnlineBox() {
        return onlineBox;
    }

    public void setOnlineBox(long onlineBox) {
        this.onlineBox = onlineBox;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public List<String> getActors() {
        return actors;
    }

    public void setActors(List<String> actors) {
        this.actors = actors;
    }

    public List<String> getType() {
        return type;
    }

    public void setType(List<String> type) {
        this.type = type;
    }
}
