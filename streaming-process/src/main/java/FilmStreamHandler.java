import bean.FilmBox;
import bean.FilmStream;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shea on 2019/10/23.
 */
public class FilmStreamHandler {

    private static void computeActorPerYear(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerActorPerYear");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<Tuple2<String, String>, Tuple3<Long, Long,String>> boxPerActorPerYear = boxPerFilmPerMonth
                .flatMapToPair(
                        element -> {
                            List<Tuple2<Tuple2<String, String>, Tuple3<Long, Long,String>>> res = new ArrayList<>();
                            long onlineBox = element._2().getOnlineBox();
                            long totalBox = element._2().getTotalBox();
                            String month = element._2().getTime();
                            String year = element._2().getTime().substring(0, 4);
                            for (int i = 0; i < element._2().getActors().size(); i++) {
                                res.add(new Tuple2<>(new Tuple2<>(element._2().getActors().get(i), year), new Tuple3<>(onlineBox, totalBox,month)));
                            }
                            return res.iterator();
                        }
                ).reduceByKeyAndWindow(
                        (a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(),b._3()),
                        Durations.seconds(12), Durations.seconds(12)
                ).updateStateByKey(
                        (currentValues, state) -> {
                            long online = 0;
                            long total = 0;
                            String month = currentValues.size() > 0 ? currentValues.get(0)._3() : "";
                            if (state.isPresent()) {
                                online = state.get()._1();
                                total = state.get()._2();
                            }
                            for (Tuple3<Long, Long, String> value : currentValues) {
                                online += value._1();
                                total += value._2();
                            }
                            return Optional.of(new Tuple3<>(online, total, month));
                        }
                );
        boxPerActorPerYear.print();
        boxPerActorPerYear.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("actor", t._1()._1());
                jsonObject.put("time", t._1()._2());
                jsonObject.put("month", t._2()._3());
                jsonObject.put("total_year_box", t._2()._2());
                jsonObject.put("online_year_box", t._2()._1());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents, writeConfig);
        });

    }

    private static JavaPairDStream<String, Tuple2<Long, Long>> computeBoxPerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerMonth");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth = boxPerFilmPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2().getTime(), new Tuple2<>(element._2().getOnlineBox(), element._2().getTotalBox()))

                ).reduceByKey(
                        (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())
                ).cache();
        boxPerMonth.print();
        boxPerMonth.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("time", t._1());
                jsonObject.put("online_month_box", t._2()._1());
                jsonObject.put("total_month_box", t._2()._2());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents, writeConfig);
        });
        return boxPerMonth;

    }

    private static void computeLocationRatePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext, JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "locationRatePerMonth");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String, Tuple3<Long, Long, String>> perLocationPerMonth = boxPerFilmPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2().getLocation(), new Tuple3<>(element._2().getOnlineBox(), element._2().getTotalBox(), element._2().getTime()))
                ).reduceByKey((a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3()));
        JavaPairDStream<String, Tuple2<Tuple3<Long, Long, String>, Tuple2<Long, Long>>> temp = perLocationPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2()._3(), new Tuple3<>(element._2()._1(), element._2()._2(), element._1()))
                ).join(boxPerMonth);
        JavaPairDStream<String, Tuple3<Double, Double, String>> locationRatePerMonth = temp
                .mapToPair(
                        element->{
                            String filmName = element._2()._1()._3();
                            String month = element._1();
                            Long onlineOfFilm = element._2()._1()._1();
                            Long totalOfFilm = element._2()._1()._2();
                            Double onlineOfMonth = (double)element._2()._2()._1();
                            Double totalOfMonth = (double)element._2()._2()._2();
                            return new Tuple2<>(filmName,new Tuple3<>(onlineOfFilm/onlineOfMonth,totalOfFilm/totalOfMonth,month));
                        }
                );
        locationRatePerMonth.print();
        locationRatePerMonth.foreachRDD(
                pairRdd -> {
                    JavaRDD<Document> documents = pairRdd.map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("location", t._1());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("box_percent", t._2()._2());
                        Document document = Document.parse(jsonObject.toJSONString());
                        return document;
                    });
                    MongoSpark.save(documents, writeConfig);
                }
        );
    }

    private static void computeBoxPerTypePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerTypePerMonth");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
//        writeConfig.withOption("collection", "boxPerTypePerMonth");
        JavaPairDStream<String, Tuple3<Long, Long, String>> boxPerTypePerMonth = boxPerFilmPerMonth
                .flatMapToPair(
                        element -> {
                            List<Tuple2<String, Tuple3<Long, Long, String>>> res = new ArrayList<>();
                            long onlineBox = element._2().getOnlineBox();
                            long totalBox = element._2().getTotalBox();
                            String month = element._2().getTime();
                            for (int i = 0; i < element._2().getType().size(); i++) {
                                res.add(new Tuple2<>(element._2().getType().get(i), new Tuple3<>(onlineBox, totalBox, month)));
                            }
                            return res.iterator();
                        }
                ).reduceByKey(
                        (a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3())
                );
        boxPerTypePerMonth.print(1);
        boxPerTypePerMonth.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("type", t._1());
//                jsonObject.put("time", month.value());
                jsonObject.put("time", t._2()._3());
                jsonObject.put("total_month_box", t._2()._2());
                jsonObject.put("online_month_box", t._2()._1());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents, writeConfig);
        });
    }

    private static void computeBoxPerFilm(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerFilm");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
//        writeConfig.withOption("collection", "boxPerFilm");
        JavaPairDStream<String, Tuple3<Long, Long, String>> boxPerFilm = boxPerFilmPerMonth
                .mapValues(v -> {
                    return new Tuple3<>(v.getOnlineBox(), v.getTotalBox(), v.getTime());
                }).updateStateByKey(
                        (currentValues, state) -> {
                            long online = 0l;
                            long total = 0l;
                            String month = currentValues.size() > 0 ? currentValues.get(0)._3() : "";
                            if (state.isPresent()) {
                                online = state.get()._1();
                                total = state.get()._2();
                            }
                            for (Tuple3<Long, Long, String> value : currentValues) {
                                online += value._1();
                                total += value._2();
                            }
                            return Optional.of(new Tuple3<>(online, total, month));
                        }
                );
        boxPerFilm.print(1);
        boxPerFilm.foreachRDD(
                pairRdd -> {
                    JavaRDD<Document> documents = pairRdd.map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("name", t._1());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("total_box", t._2()._2());
                        jsonObject.put("online_box", t._2()._1());
                        Document document = Document.parse(jsonObject.toJSONString());
                        return document;
                    });
                    MongoSpark.save(documents, writeConfig);
                }
        );


    }

    private static JavaPairDStream<String, FilmStream> computeBoxPerFilmPerMonth(JavaPairDStream<String, FilmStream> perFilmPerMonth) {
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = perFilmPerMonth
                .reduceByKey(
                        (a, b) -> {
                            FilmStream filmStream = new FilmStream(a);
                            filmStream.setOnlineBox(a.getOnlineBox() + b.getOnlineBox());
                            filmStream.setTotalBox(a.getTotalBox() + b.getTotalBox());
                            return filmStream;
                        }
                ).cache();
        return boxPerFilmPerMonth;
    }

    private static void receiveStream(JavaSparkContext sparkContext) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));
        JavaDStream<String> contentList = streamingContext.textFileStream("streamInput/");
        streamingContext.checkpoint("checkPoint/");
        JavaPairDStream<String, FilmStream> perFilmPerMonth = contentList
                .flatMap(content -> {
                    JSONArray jsonArray = JSONArray.parseArray(content);
                    List<FilmStream> res = new ArrayList<>();
                    for (int i = 0; i < jsonArray.size(); i++) {
                        res.add(JSONObject.parseObject(jsonArray.getString(i), FilmStream.class));
                    }
                    return res.iterator();
                }).filter(
                        filmStream -> {
                            return filmStream.getTotalBox() != null && filmStream.getOnlineBox() != null
                                    && filmStream.getOnlineBox() <= filmStream.getTotalBox();
                        }
                ).mapToPair(
                        filmStream -> {
                            filmStream.setTime(filmStream.getTime().substring(0, 7));
                            return new Tuple2<>(filmStream.getMovieName(), filmStream);
                        }
                );
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = computeBoxPerFilmPerMonth(perFilmPerMonth);
        computeBoxPerFilm(boxPerFilmPerMonth, sparkContext);
        computeBoxPerTypePerMonth(boxPerFilmPerMonth, sparkContext);
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth = computeBoxPerMonth(boxPerFilmPerMonth,sparkContext);
        computeLocationRatePerMonth(boxPerFilmPerMonth, sparkContext,boxPerMonth);
        computeActorPerYear(boxPerFilmPerMonth,sparkContext);
        streamingContext.start();              // Start the computation
        try {
            streamingContext.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
//            sparkContext.close();
        }

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingProcess")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .config("spark.mongodb.output.database", "sparkpractise")
                .config("spark.mongodb.output.collection", "testCollection")
                .getOrCreate();
//        SparkConf sparkConf = new SparkConf().setAppName("myStreaming");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        receiveStream(sparkContext);
    }
}
