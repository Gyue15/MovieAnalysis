package cluster;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class KMeansCluster {

    private static String modelPath = "target/model/KMeans";

    public static void train(JavaRDD<Vector> trainingData, JavaSparkContext jsc, int numClusters, int numIterations) {
        KMeansModel clusters = KMeans.train(trainingData.rdd(), numClusters, numIterations);

        System.out.println("Cluster centers:");
        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(trainingData.rdd());
        System.out.println("Cost: " + cost);

        // Save and load model
        clusters.save(jsc.sc(), modelPath);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(trainingData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }

    public static JavaRDD<Integer> predict(JavaSparkContext jsc, JavaRDD<Vector> testData) {
        KMeansModel model = KMeansModel.load(jsc.sc(), modelPath);
        return model.predict(testData);
    }
}
