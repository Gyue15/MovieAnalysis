import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shea on 2019/10/23.
 */
public class FilmStreamHandler {
    private static void dataProcess(JavaSparkContext sc) {
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1));
        JavaDStream<String> contentList = jsc.textFileStream("streamInput/");
        jsc.checkpoint("checkPoint/");
        JavaDStream<FilmStream> films = contentList.flatMap(content -> {
            JSONArray jsonArray = JSONArray.parseArray(content);
            List<FilmStream> res = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                res.add(JSONObject.parseObject(jsonArray.getString(i), FilmStream.class));
            }
            return res.iterator();
        });
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = films
                .filter(filmStream -> filmStream.getTotalBox()!=null)
                .mapToPair(filmStream -> new Tuple2<>(filmStream.getMovieName(), filmStream))
                .reduceByKey(
                        (accumulator1, accumulator2) -> {
                            FilmStream filmStream = new FilmStream(accumulator1);
                            filmStream.setOnlineBox(accumulator2.getTotalBox() + accumulator1.getTotalBox());
                            filmStream.setTotalBox(accumulator2.getTotalBox() + accumulator1.getTotalBox());
                            return filmStream;
                        }
                ).cache();
        boxPerFilmPerMonth.print();
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerFilm = boxPerFilmPerMonth
                .mapValues(v -> new Tuple2<>(v.getOnlineBox(), v.getTotalBox()))
                .updateStateByKey(
                        (currentValues, state) -> {
                            long online = 0l;
                            long total = 0l;
                            if (state.isPresent()) {
                                online = state.get()._1();
                                total = state.get()._2();
                            }
                            for (Tuple2<Long, Long> value : currentValues) {
                                online += value._1();
                                total += value._2();
                            }
                            return Optional.of(new Tuple2<>(online, total));
                        }
                );
        boxPerFilm.print();
        LongAccumulator onlineBoxAccum = sc.sc().longAccumulator();
        LongAccumulator totalBoxAccum = sc.sc().longAccumulator();
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerTypePerMonth = boxPerFilmPerMonth.flatMapToPair(
                element -> {
                    List<Tuple2<String, Tuple2<Long, Long>>> res = new ArrayList<>();
                    long onlineBox = element._2().getOnlineBox();
                    long totalBox = element._2().getTotalBox();
                    for (int i = 0; i < element._2().getType().size(); i++) {
                        res.add(new Tuple2<>(element._2().getType().get(i), new Tuple2<>(onlineBox, totalBox)));
                    }
                    return res.iterator();
                }
        ).reduceByKey(
                (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())
        ).cache();
//        boxPerTypePerMonth.print();
        boxPerTypePerMonth.map(x -> {
            onlineBoxAccum.add(x._2()._1());
            totalBoxAccum.add(x._2()._2());
            return x;
        });
        Double onlineBox = Double.valueOf(onlineBoxAccum.sum());
        Double totalBox = Double.valueOf(totalBoxAccum.sum());
        JavaPairDStream<String, Tuple2<Double, Double>> boxRatePerLocationPerMonth = boxPerFilmPerMonth.mapToPair(
                element -> new Tuple2<>(element._2().getLocation(), new Tuple2<>(element._2().getOnlineBox(), element._2().getTotalBox())))
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                .mapValues(x -> new Tuple2<>(x._1() / onlineBox, x._2() / totalBox));
//        boxRatePerLocationPerMonth.print();
        jsc.start();              // Start the computation
        try {
            jsc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        jsc.stop();
//        JavaPairDStream


    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("myStreaming");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        dataProcess(sparkContext);
    }
}
