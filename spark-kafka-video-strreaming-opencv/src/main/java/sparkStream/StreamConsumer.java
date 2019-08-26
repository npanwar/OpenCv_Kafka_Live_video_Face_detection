package sparkStream;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.util.*;
import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.opencv.videoio.VideoCapture;

// implement from Apache Spark user docs

public class StreamConsumer extends JPanel

{
	private static final long serialVersionUID = 1L;
	static JFrame frame0 = new JFrame();
	private BufferedImage image;

	public StreamConsumer() {

	}

	public StreamConsumer(BufferedImage img) {
		image = img;
	}

	static {
		String opencvpath = "C:\\opencv\\build\\java\\x64\\";
		System.out.println(opencvpath);
		System.load(opencvpath + Core.NATIVE_LIBRARY_NAME + ".dll");
		// System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	public static String AdaBoostName = "C:\\haarcascade_frontalface_alt.xml";

	public static CascadeClassifier AdaBoost;

	// static int partitions;

	@Override
	public void paint(Graphics g) {
		g.drawImage(image, 0, 0, this);
	}

	public static void main(String[] agrs) throws InterruptedException {

		// Mat frame2;

		AdaBoost = new CascadeClassifier();

		if (!AdaBoost.load(AdaBoostName)) {
			System.out.print("Could not load AdaBoost model\n");

		}

		Map<String, Object> kafkaParams = new HashMap<String, Object>();

		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("max.partition.fetch.bytes", 2097152);
		kafkaParams.put("auto.commit.interval.ms", "10000");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "jbm");

		Collection<String> topics = Arrays.asList("jbm");

//SparkConf conf =new SparkConf().setMaster("local").setAppName("S3 Example");
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaInput").set("spark.driver.host",
				"localhost");
// 1 sec batch sizede
		System.out.println("initializong JavaStreamingContext");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1));

		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		JavaDStream<String> lines = stream.map(ConsumerRecord::value);

		/*
		 * Scanner scan = new Scanner(System.in);
		 * System.out.println("Enter the number of partitions required");
		 * partitions=scan.nextInt();
		 */

//scan.close();
		System.out.println("going for foreachRDD 110");
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) {
				System.out.println("inside call method : rdd");
				// System.out.println("size of rdd :"+rdd.count());
				JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(String msg) {
						System.out.println("inside second call: " + msg);
						Encoder<SnapStream> encoder = Encoders.bean(SnapStream.class);
						SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
						System.out.println("Dataset from json:" + msg);
						Dataset<SnapStream> ds = spark.read().json(msg).as(encoder);

						// ds.repartition(partitions);

						ds.foreach(values -> {

							SnapStream encoded;
							Mat image;
							Mat frame2;
							encoded = values;

							image = new Mat(encoded.getRows(), encoded.getCols(), encoded.getType());
							image.put(0, 0, Base64.getDecoder().decode(encoded.getData()));

							String imagePath = "D:\\VideoImage\\" + "WebCam-" + encoded.getTimestamp().getTime()
									+ ".png";
							System.out.println("saving image snapshots");
							Imgcodecs.imwrite(imagePath, image);

							frame2 = detect(image);
							System.out.println("Showing UI:");
							showUI(frame2);

						});

						return msg;
					}

					private void showUI(Mat frame2) {
						// TODO Auto-generated method stub

						BufferedImage img;

						img = bufferedImage(frame2);

						window(img, "Detected", 100, 50);

					}

					public void window(BufferedImage img, String text, int x, int y) {

						frame0.getContentPane().add(new StreamConsumer(img));
						frame0.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
						frame0.setTitle(text);
						frame0.setSize(img.getWidth() + 300, img.getHeight() + 300);
						frame0.setLocation(x, y);
						frame0.setVisible(true);
					}

					public BufferedImage bufferedImage(Mat m) {
						int type = BufferedImage.TYPE_BYTE_GRAY;
						if (m.channels() > 1) {
							type = BufferedImage.TYPE_3BYTE_BGR;
						}
						BufferedImage image = new BufferedImage(m.cols(), m.rows(), type);
						m.get(0, 0, ((DataBufferByte) image.getRaster().getDataBuffer()).getData()); // get all the
																										// pixels
						return image;
					}

					public Mat detect(Mat frame) {
						Mat frame_gray = new Mat();
						MatOfRect face = new MatOfRect();

						// Rect[] facesArray = face.toArray();

						Imgproc.cvtColor(frame, frame_gray, Imgproc.COLOR_BGRA2GRAY);
						Imgproc.equalizeHist(frame_gray, frame_gray);

						AdaBoost.detectMultiScale(frame_gray, face, 1.1, 2, 0, new Size(30, 30), new Size());

						Rect[] facesArray = face.toArray();

						System.out.println(facesArray.length);

						for (int i = 0; i < facesArray.length; i++) {

							Point center = new Point(facesArray[i].x + facesArray[i].width * 0.5,
									facesArray[i].y + facesArray[i].height * 0.5);
							Imgproc.ellipse(frame, center,
									new Size(facesArray[i].width * 0.5, facesArray[i].height * 0.5), 0, 0, 360,
									new Scalar(255, 0, 255), 4, 8, 0);

							// Mat faceROI = frame_gray.submat(facesArray[i]);
						}

						return frame;

					}

				});

			}
		});

		jssc.start();

		jssc.awaitTermination();

	}
}

class JavaSparkSessionSingleton {
	private static transient SparkSession instance = null;

	public static SparkSession getInstance(SparkConf sparkConf) {
		if (instance == null) {
			instance = SparkSession.builder().config(sparkConf).getOrCreate();
		}
		return instance;
	}

}
