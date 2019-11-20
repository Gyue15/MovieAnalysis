package cluster;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.BisectingKMeans;
import org.apache.spark.mllib.clustering.BisectingKMeansModel;
import org.apache.spark.mllib.linalg.Vector;

public class BisectingKMeansCluster {

    private static String modelPath = "target/model/BisectingKMeans";

    public static void train(JavaRDD<Vector> trainingData, JavaSparkContext jsc, int numCluster) {
        BisectingKMeans bkm = new BisectingKMeans().setK(numCluster);
        BisectingKMeansModel model = bkm.run(trainingData);

        System.out.println("Compute Cost: " + model.computeCost(trainingData));

        Vector[] clusterCenters = model.clusterCenters();
        for (int i = 0; i < clusterCenters.length; i++) {
            Vector clusterCenter = clusterCenters[i];
            System.out.println("Cluster Center " + i + ": " + clusterCenter);
        }
        model.save(jsc.sc(), modelPath);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = model.computeCost(trainingData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
    }

    public static JavaRDD<Integer> predict(JavaSparkContext jsc, JavaRDD<Vector> testData) {
        BisectingKMeansModel model = BisectingKMeansModel.load(jsc.sc(), modelPath);
        return model.predict(testData);
    }
}
