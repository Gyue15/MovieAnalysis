package bean;

import java.io.Serializable;
import java.util.List;

/**
 * Created by shea on 2019/10/23.
 */
public class FilmStream implements Serializable {
    String time;
    String movieName;
    Long totalBox;
    Long onlineBox;
    String location;
    List<String> actors;
    List<String> type;
    String director;


    public FilmStream() {
    }

    public FilmStream(String time, String movieName, long totalBox, long onlineBox, String location, List<String> actors, List<String> type, String director) {
        this.time = time;
        this.movieName = movieName;
        this.totalBox = totalBox;
        this.onlineBox = onlineBox;
        this.location = location;
        this.actors = actors;
        this.type = type;
        this.director = director;
    }

    public FilmStream(FilmStream filmStream) {
        this.time = filmStream.time;
        this.movieName = filmStream.movieName;
        this.totalBox = filmStream.totalBox;
        this.onlineBox = filmStream.onlineBox;
        this.location = filmStream.location;
        this.actors = filmStream.actors;
        this.type = filmStream.type;
        this.director = filmStream.director;
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

    public Long getTotalBox() {
        return totalBox;
    }

    public void setTotalBox(Long totalBox) {
        this.totalBox = totalBox;
    }

    public Long getOnlineBox() {
        return onlineBox;
    }

    public void setOnlineBox(Long onlineBox) {
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

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }
}
