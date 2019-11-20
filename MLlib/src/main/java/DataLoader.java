import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;


public class DataLoader {

    static JavaRDD<Vector> loadVector(JavaSparkContext jsc, String dataPath) {
        // Load and parse the data file.
        return MLUtils.loadLibSVMFile(jsc.sc(), dataPath).toJavaRDD().map(LabeledPoint::features);
    }

    static JavaPairRDD<Long, Vector> loadIndexVector(JavaSparkContext jsc, String dataPath) {
        JavaRDD<LabeledPoint> trainingData = MLUtils.loadLibSVMFile(jsc.sc(), dataPath).toJavaRDD();
        // Index documents with unique IDs
        return JavaPairRDD.fromJavaRDD(trainingData.map(labeledPoint -> new Tuple2<>((long)labeledPoint.label(),
                        labeledPoint.features())));
    }

    static JavaRDD<LabeledPoint> loadLabeledPoint(JavaSparkContext jsc, String dataPath) {
        return MLUtils.loadLibSVMFile(jsc.sc(), dataPath).toJavaRDD();
    }
}
