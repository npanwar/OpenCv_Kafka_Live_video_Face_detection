package sparkStream;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
// this is the main consumer
import java.util.Base64;
import java.util.Scanner;

import javax.swing.JFrame;
import javax.swing.JPanel;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;

public class Stream2 extends JPanel {

//static int partitions;

	static {
		String opencvpath = "C:\\opencv\\build\\java\\x64\\";
		System.out.println(opencvpath);
		System.load(opencvpath + Core.NATIVE_LIBRARY_NAME + ".dll");
		// System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	static JFrame frame0 = new JFrame();
	private BufferedImage image;

	public static String AdaBoostName = "C:\\haarcascade_frontalface_alt.xml";

	public static CascadeClassifier AdaBoost;

	public Stream2() {

	}

	public Stream2(BufferedImage img) {
		image = img;
	}

	@Override
	public void paint(Graphics g) {
		g.drawImage(image, 0, 0, this);
	}

	public static void main(String[] args) throws Exception {

		AdaBoost = new CascadeClassifier();

		if (!AdaBoost.load(AdaBoostName)) {
			System.out.print("Could not load AdaBoost model\n");

		}

		/*
		 * Scanner scan = new Scanner(System.in);
		 * System.out.println("Enter the number of partitions required");
		 * partitions=scan.nextInt();
		 */

		// scan.close();

		SparkSession spark = SparkSession.builder().appName("VideoStreamProcessor").master("local[2]")
				.config("spark.driver.host", "localhost").getOrCreate();
		// SparkConf conf = new
		// SparkConf().setMaster("local[2]").setAppName("KafkaInput").set("spark.driver.host",
		// "localhost");

		StructType schema = DataTypes.createStructType(new StructField[] {

				DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
				DataTypes.createStructField("rows", DataTypes.IntegerType, true),
				DataTypes.createStructField("cols", DataTypes.IntegerType, true),
				DataTypes.createStructField("type", DataTypes.IntegerType, true),
				DataTypes.createStructField("data", DataTypes.StringType, true) });

		// whole kafka consumer config
		Dataset<SnapStream> ds = spark.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "jbm").option("kafka.max.partition.fetch.bytes", 2097152)
				.option("kafka.max.poll.records", 350).load().selectExpr("CAST(value AS STRING) as message")
				.select(functions.from_json(functions.col("message"), schema).as("json")).select("json.*")
				.as(Encoders.bean(SnapStream.class));

		// ds.repartition(partitions);

		ds.foreach(values -> {

			SnapStream encoded;
			Mat image;
			Mat frame2;
			encoded = values;
			image = new Mat(encoded.getRows(), encoded.getCols(), encoded.getType());
			image.put(0, 0, Base64.getDecoder().decode(encoded.getData()));

			frame2 = detect(image);
			showUI(frame2);

			String imagePath = "D:\\VideoImage\\" + "WebCam-" + encoded.getTimestamp().getTime() + ".png";

			// swing(image);
			Imgcodecs.imwrite(imagePath, image);

		});

		// starting stream
		StreamingQuery query = ds.writeStream().outputMode("update").format("console").start();

		query.awaitTermination();

	}

	private static void showUI(Mat frame2) {
		// TODO Auto-generated method stub

		BufferedImage img;

		img = bufferedImage(frame2);

		window(img, "Detected", 100, 50);

	}

	public static void window(BufferedImage img, String text, int x, int y) {

		frame0.getContentPane().add(new StreamConsumer(img));
		frame0.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame0.setTitle(text);
		frame0.setSize(img.getWidth() + 300, img.getHeight() + 300);
		frame0.setLocation(x, y);
		frame0.setVisible(true);
	}

	public static BufferedImage bufferedImage(Mat m) {
		int type = BufferedImage.TYPE_BYTE_GRAY;
		if (m.channels() > 1) {
			type = BufferedImage.TYPE_3BYTE_BGR;
		}
		BufferedImage image = new BufferedImage(m.cols(), m.rows(), type);
		m.get(0, 0, ((DataBufferByte) image.getRaster().getDataBuffer()).getData()); // get all the pixels
		return image;
	}

	public static Mat detect(Mat frame) {
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
			Imgproc.ellipse(frame, center, new Size(facesArray[i].width * 0.5, facesArray[i].height * 0.5), 0, 0, 360,
					new Scalar(255, 0, 255), 4, 8, 0);

		}

		return frame;

	}

	public void swing(Mat img) {
		//
	}

}
