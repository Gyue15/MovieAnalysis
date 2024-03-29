import bean.FilmStream;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.*;

/**
 *
 * Created by shea on 2019/10/23.
 */
public class FilmStreamHandler {

    //更新全局变量
    private static Optional<Tuple3<Long, Long, String>> updateState(List<Tuple3<Long, Long, String>> currentValues,
    Optional<Tuple3<Long, Long, String>> state) {
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

    private static void computeActorPerYear(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "actor_box");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<Tuple2<String, String>, Tuple3<Long, Long, String>> boxPerActorPerYear = boxPerFilmPerMonth
                .filter(
                        element -> "中国大陆".equals(element._2().getLocation())
                )
                .flatMapToPair(
                        element -> {
                            List<Tuple2<Tuple2<String, String>, Tuple3<Long, Long, String>>> res = new ArrayList<>();
                            long onlineBox = element._2().getOnlineBox();
                            long totalBox = element._2().getTotalBox();
                            String month = element._2().getTime();
                            String year = element._2().getTime().substring(0, 4);
                            for (int i = 0; i < element._2().getActors().size(); i++) {
                                res.add(new Tuple2<>(new Tuple2<>(element._2().getActors().get(i), year), new Tuple3<>(onlineBox, totalBox, month)));
                            }
                            return res.iterator();
                        }
                ).updateStateByKey(FilmStreamHandler::updateState);
        boxPerActorPerYear.print();
        boxPerActorPerYear.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd
                    .filter(t -> t._2()._3().length() > 0)
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("actor", t._1()._1());
                        jsonObject.put("time", t._1()._2());
                        jsonObject.put("month", t._2()._3());
                        jsonObject.put("total_year_box", t._2()._2());
                        jsonObject.put("online_year_box", t._2()._1());
                        return Document.parse(jsonObject.toJSONString());
                    });
            MongoSpark.save(documents, writeConfig);
        });

    }

    //计算每月电影票房，并做缓存
    private static JavaPairDStream<String, Tuple2<Long, Long>> computeBoxPerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "total_box");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth = boxPerFilmPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2().getTime(), new Tuple2<>(element._2().getOnlineBox(), element._2().getTotalBox()))

                ).reduceByKey(
                        (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2())
                ).cache();
        boxPerMonth.print(1);
        boxPerMonth.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("time", t._1());
                        jsonObject.put("online_month_box", t._2()._1());
                        jsonObject.put("total_month_box", t._2()._2());
                        return Document.parse(jsonObject.toJSONString());
                    });
            MongoSpark.save(documents, writeConfig);
        });
        return boxPerMonth;

    }

    //计算每月每个产地的电影票房占比
    private static void computeLocationRatePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext, JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "location_box");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String, Tuple3<Long, Long, String>> perLocationPerMonth = boxPerFilmPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2().getLocation(),
                                new Tuple3<>(element._2().getOnlineBox(), element._2().getTotalBox(), element._2().getTime()))
                ).reduceByKey((a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3()));
        JavaPairDStream<String, Tuple2<Tuple3<Long, Long, String>, Tuple2<Long, Long>>> temp = perLocationPerMonth
                .mapToPair(
                        element -> new Tuple2<>(element._2()._3(), new Tuple3<>(element._2()._1(), element._2()._2(), element._1()))
                ).join(boxPerMonth);
        JavaPairDStream<String, Tuple3<Double, Double, String>> locationRatePerMonth = temp
                .mapToPair(
                        element -> {
                            String filmName = element._2()._1()._3();
                            String month = element._1();
                            Long onlineOfFilm = element._2()._1()._1();
                            Long totalOfFilm = element._2()._1()._2();
                            Double onlineOfMonth = (double) element._2()._2()._1();
                            Double totalOfMonth = (double) element._2()._2()._2();
                            return new Tuple2<>(filmName, new Tuple3<>(onlineOfFilm / onlineOfMonth, totalOfFilm / totalOfMonth, month));
                        }
                );
        locationRatePerMonth.print();
        locationRatePerMonth.foreachRDD(
                pairRdd -> {
                    JavaRDD<Document> documents = pairRdd.map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("region", t._1());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("box_percent", t._2()._2());
                        return Document.parse(jsonObject.toJSONString());
                    });
                    MongoSpark.save(documents, writeConfig);
                }
        );
    }

    //计算每月每种类型的电影票房
    private static void computeBoxPerTypePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "type_box");
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
                jsonObject.put("time", t._2()._3());
                jsonObject.put("total_month_box", t._2()._2());
                jsonObject.put("online_month_box", t._2()._1());
                return Document.parse(jsonObject.toJSONString());
            });
            MongoSpark.save(documents, writeConfig);
        });
    }

    private static void computeBoxPerFilm(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "film_box");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
//        writeConfig.withOption("collection", "boxPerFilm");
        JavaPairDStream<String, Tuple3<Long, Long, String>> boxPerFilm = boxPerFilmPerMonth
                .mapValues(v -> new Tuple3<>(v.getOnlineBox(), v.getTotalBox(), v.getTime())
                ).updateStateByKey(FilmStreamHandler::updateState);
        boxPerFilm.print(1);
        boxPerFilm.foreachRDD(
                pairRdd -> {
                    JavaRDD<Document> documents = pairRdd
                            .filter(t -> t._2()._3().length() > 0)
                            .map(t -> {
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("name", t._1());
                                jsonObject.put("time", t._2()._3());
                                jsonObject.put("total_box", t._2()._2());
                                jsonObject.put("online_box", t._2()._1());
                                return Document.parse(jsonObject.toJSONString());
                            });
                    MongoSpark.save(documents, writeConfig);
                }
        );


    }

    //计算每部电影每月的票房，并做缓存
    private static JavaPairDStream<String, FilmStream> computeBoxPerFilmPerMonth(JavaPairDStream<String, FilmStream> perFilmPerMonth,JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "film_box_per_month");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String,FilmStream> boxPerFilmPerMonth = perFilmPerMonth.reduceByKey(
                        (a, b) -> {
                            FilmStream filmStream = new FilmStream(a);
                            filmStream.setOnlineBox(a.getOnlineBox() + b.getOnlineBox());
                            filmStream.setTotalBox(a.getTotalBox() + b.getTotalBox());
                            return filmStream;
                        }
                ).cache();
        boxPerFilmPerMonth.print(1);
        boxPerFilmPerMonth.foreachRDD(
                pairRdd->{
                    JavaRDD<Document> documents = pairRdd.map(t->{
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("name", t._1());
                        jsonObject.put("time", t._2().getTime());
                        jsonObject.put("total_box", t._2().getTotalBox());
                        jsonObject.put("online_box", t._2().getTotalBox());
                        return Document.parse(jsonObject.toJSONString());
                    });
                    MongoSpark.save(documents,writeConfig);
                }
        );
        return boxPerFilmPerMonth;
    }

    //计算每个导演每种类型电影的每月票房
    private static void computeBoxPerDirectorPerType(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext){
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "uptonow_box_per_director_per_type");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<Tuple2<String,String>,Tuple3<Long,Long,String>> upToNowPerDirectorPerType = boxPerFilmPerMonth
                .filter(element-> element._2().getDirector()!=null&&element._2().getDirector().length()>0
                        &&element._2().getLocation().contains("中国"))
                .flatMapToPair(element->{
                    List<Tuple2<Tuple2<String,String>,Tuple3<Long,Long,String>>> res = new ArrayList<>();
                    for(int i=0;i<element._2().getType().size();i++){
                        res.add(new Tuple2<>(
                                new Tuple2<>(element._2().getDirector(),element._2().getType().get(i)),
                                new Tuple3<>(element._2().getOnlineBox(),element._2().getTotalBox(),element._2().getTime())
                        ));
                    }
                    return res.iterator();
                }).updateStateByKey(FilmStreamHandler::updateState);
        upToNowPerDirectorPerType.print(1);
        upToNowPerDirectorPerType.foreachRDD(pairRdd->{
            JavaRDD<Document> documents = pairRdd
                    .filter(t -> t._2()._3().length() > 0)
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("director", t._1()._1());
                        jsonObject.put("type", t._1()._2());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("total_box", t._2()._2());
                        jsonObject.put("online_box", t._2()._1());
                        return Document.parse(jsonObject.toJSONString());
                    });
            MongoSpark.save(documents, writeConfig);
        });
    }

    //计算每月前三票房和和其他排名票房和
    private static void computeTop3PerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth,JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth,JavaSparkContext sparkContext){
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "box_top3_per_month");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<String, Tuple5<Long,Long,Long,Long,Long>> top3PerMonth = boxPerFilmPerMonth
                .mapToPair(element->new Tuple2<>(element._2().getTotalBox(),element._2().getTime()))
                .transformToPair(rdd->{
                    List<Tuple2<Long,String>> top3List = rdd.sortByKey(false).take(3);
                    Long top1 = top3List.size()>=1?top3List.get(0)._1():0l;
                    Long top2 = top3List.size()>=2?top3List.get(1)._1():0l;
                    Long top3 = top3List.size()>=3?top3List.get(2)._1():0l;
                    Long sum = top1+top2+top3;
                    return rdd.mapToPair(r->new Tuple2<>(r._2(),new Tuple4<>(top1,top2,top3,sum))).distinct();
                }).join(boxPerMonth).mapValues(v->
                    new Tuple5<>(v._1()._1(),v._1()._2(),v._1()._3(),v._1()._4(),v._2()._2()-v._1()._4())
                );
        top3PerMonth.print(1);
        top3PerMonth.foreachRDD(pairRdd->{
            JavaRDD<Document> documents = pairRdd
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("time", t._1());
                        jsonObject.put("top1", t._2()._1());
                        jsonObject.put("top2", t._2()._2());
                        jsonObject.put("top3", t._2()._3());
                        jsonObject.put("top3_sum", t._2()._4());
                        jsonObject.put("other_sum", t._2()._5());
                        return Document.parse(jsonObject.toJSONString());
                    });
            MongoSpark.save(documents, writeConfig);
        });

    }

    //计算每一个演员每种类型电影的每月票房
    private static void computeBoxPerActorPerType(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext){
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "uptonow_box_per_actor_per_type");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
        JavaPairDStream<Tuple2<String,String>,Tuple3<Long,Long,String>> upToNowPerActorPerType = boxPerFilmPerMonth
                .filter(element-> element._2().getLocation().contains("中国"))
                .flatMapToPair(element->{
                    List<Tuple2<Tuple2<String,String>,Tuple3<Long,Long,String>>> res = new ArrayList<>();
                    for(int i=0;i<element._2().getType().size();i++){
                        for(int j=0;j<element._2().getActors().size();j++){
                            res.add(new Tuple2<>(
                                    new Tuple2<>(element._2().getActors().get(j),element._2().getType().get(i)),
                                    new Tuple3<>(element._2().getOnlineBox(),element._2().getTotalBox(),element._2().getTime())
                            ));
                        }
                    }
                    return res.iterator();
                }).updateStateByKey(FilmStreamHandler::updateState);
        upToNowPerActorPerType.print(1);
        upToNowPerActorPerType.foreachRDD(pairRdd->{
            JavaRDD<Document> documents = pairRdd
                    .filter(t -> t._2()._3().length() > 0)
                    .map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("actor", t._1()._1());
                        jsonObject.put("type", t._1()._2());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("total_box", t._2()._2());
                        jsonObject.put("online_box", t._2()._1());
                        return Document.parse(jsonObject.toJSONString());
                    });
            MongoSpark.save(documents, writeConfig);
        });
    }

    private static void receiveStream(JavaSparkContext sparkContext) {
        //监听流输入，并解析hdfs文件，做数据预处理
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));
        JavaDStream<String> contentList = streamingContext.textFileStream("streamInput/film");
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
                        filmStream -> filmStream.getTotalBox() != null && filmStream.getOnlineBox() != null
                                && filmStream.getOnlineBox() <= filmStream.getTotalBox() && filmStream.getOnlineBox()
                                >= 0

                ).mapToPair(
                        filmStream -> {
                            filmStream.setTime(filmStream.getTime().substring(0, 7));
                            return new Tuple2<>(filmStream.getMovieName(), filmStream);
                        }
                );
        //计算每部电影每月的票房，并做缓存
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = computeBoxPerFilmPerMonth(perFilmPerMonth,sparkContext);
        //无用
        computeBoxPerFilm(boxPerFilmPerMonth, sparkContext);
        //计算每月每种类型的电影票房
        computeBoxPerTypePerMonth(boxPerFilmPerMonth, sparkContext);
        //计算每月电影票房，并做缓存
        JavaPairDStream<String, Tuple2<Long, Long>> boxPerMonth = computeBoxPerMonth(boxPerFilmPerMonth, sparkContext);
        //计算每月每个产地的电影票房占比
        computeLocationRatePerMonth(boxPerFilmPerMonth, sparkContext, boxPerMonth);
        //无用
        computeActorPerYear(boxPerFilmPerMonth, sparkContext);
        //计算每个导演每种类型电影的每月票房
        computeBoxPerDirectorPerType(boxPerFilmPerMonth,sparkContext);
        //计算每一个演员每种类型电影的每月票房
        computeBoxPerActorPerType(boxPerFilmPerMonth,sparkContext);
        //计算每月前三票房和和其他排名票房和
        computeTop3PerMonth(boxPerFilmPerMonth,boxPerMonth,sparkContext);
        streamingContext.start();              // Start the computation
        try {
            streamingContext.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
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
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        receiveStream(sparkContext);
    }
}
