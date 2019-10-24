import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.bson.Document;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                            filmStream.setOnlineBox(accumulator2.getOnlineBox() + accumulator1.getOnlineBox());
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
        JavaPairDStream<String,Tuple2<Long,Long>> boxPerActorPerYear = boxPerFilmPerMonth.flatMapToPair(
                element->{
                    List<Tuple2<String,Tuple2<Long,Long>>> res = new ArrayList<>();
                    long onlineBox = element._2().getOnlineBox();
                    long totalBox = element._2().getTotalBox();
                    for(int i=0;i<element._2().getActors().size();i++){
                        res.add(new Tuple2<>(element._2().getActors().get(i),new Tuple2<>(onlineBox, totalBox)));
                    }
                    return res.iterator();
                }
        ).window(Durations.seconds(12)).reduceByKey(
                (a,b)->new Tuple2<>(a._1()+b._1(),a._2()+b._2())
        );
//        boxPerActorPerYear.print();
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sc).withOptions(writeOverrides);
        boxPerActorPerYear.foreachRDD(rdd->{
            JavaRDD<Document> documents = rdd.map(t->{
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("name",t._1());
                jsonObject.put("total_box",t._2()._2());
                jsonObject.put("online_box",t._2()._1());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents,writeConfig);
        });
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
        } finally {
            jsc.close();
        }
//        jsc.stop();
//        JavaPairDStream


    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingProcess")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .config("spark.mongodb.output.database", "sparkpractise")
                .config("spark.mongodb.output.collection", "actorPerYear")
//                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .getOrCreate();
//        SparkConf sparkConf = new SparkConf().setAppName("myStreaming");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        dataProcess(sparkContext);
    }
}
