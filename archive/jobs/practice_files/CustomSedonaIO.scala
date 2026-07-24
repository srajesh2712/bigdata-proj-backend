import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.sedona.spark.raster.RasterFileRDD
import org.apache.sedona.spark.operation.ImageMosaicking
import org.geotools.coverage.grid.GridCoverage2D

/**
 * Custom Scala object to house the I/O logic for Sedona Rasters.
 * This bypasses the broken PySpark bindings by calling the Java/Scala 
 * classes directly, which is required for the Mosaicking and RasterFileRDD 
 * functionality in this environment.
 */
object CustomSedonaIO {

    /**
     * Reads GeoTIFF files from HDFS and creates a Raster RDD (JavaRDD).
     * This method bypasses the broken Python RasterFileRDD binding.
     * @param sc The JavaSparkContext.
     * @param path The HDFS path to the directory or file.
     * @param numPartitions The desired number of partitions.
     * @return JavaRDD<GridCoverage2D> containing the raster tiles.
     */
    def readGeoTiff(sc: JavaSparkContext, path: String, numPartitions: Int): JavaRDD[GridCoverage2D] = {
        System.out.println(s"Scala: Reading GeoTIFFs from path: $path with $numPartitions partitions.")
        val rasterRDD = RasterFileRDD.create(sc, path, numPartitions)
        // Convert the Scala RDD to JavaRDD for PySpark bridge consumption
        rasterRDD.toJavaRDD()
    }

    /**
     * Mosaics the tiles in the RDD and writes the result to HDFS as a GeoTIFF.
     * This method bypasses the broken Python ImageMosaicking binding.
     * @param rdd The RDD of GridCoverage2D tiles.
     * @param outputPath The HDFS path to write the output file.
     * @param overwrite Boolean flag to overwrite existing files.
     */
    def writeMosaic(rdd: JavaRDD[GridCoverage2D], outputPath: String, overwrite: Boolean): Unit = {
        System.out.println(s"Scala: Writing mosaicked GeoTIFF to path: $outputPath. Overwrite: $overwrite")
        // The Java Bridge will pass the RDD in, we pass it to the Sedona Mosaicking utility
        ImageMosaicking.mosaickToGeoTiff(rdd.rdd, outputPath, overwrite)
        System.out.println("Scala: Mosaicking write complete.")
    }
}

