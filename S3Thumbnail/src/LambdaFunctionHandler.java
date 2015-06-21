import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import javax.imageio.ImageIO;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONObject;





public class LambdaFunctionHandler<E> implements RequestHandler<S3Event, Object> {

    @SuppressWarnings("unchecked")
	@Override
    public Object handleRequest(S3Event input, Context context) {
        context.getLogger().log("Input: " + input.toJson().toString() + "\n");
        JSONArray recordArray;
        JSONObject s3, object, bucket, result, headers;
        String bucketName = null, objectKey = null;
        
        final String DESTINATION_BUCKET = "<BUCKET>";
        
        AmazonS3Client s3client = new AmazonS3Client();
        try {
			recordArray = new JSONObject(input.toJson()).getJSONArray("Records");
			result = recordArray.getJSONObject(0);
			s3 = result.getJSONObject("s3");
			object = s3.getJSONObject("object");
			bucket = s3.getJSONObject("bucket");
			headers = result.getJSONObject("responseElements");
			System.out.println(headers.toString());
			
			bucketName = bucket.getString("name");
			objectKey = object.getString("key");
			
			System.out.println("PUT Object: s3://"+bucketName+"/"+objectKey);
			S3Object pic = s3client.getObject(bucketName, objectKey);
			S3ObjectInputStream io = pic.getObjectContent();
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ObjectMetadata data = pic.getObjectMetadata();

			System.out.println("Original Content-Length: "+data.getContentLength());

			
			BufferedImage buf = ImageIO.read(io);

			int beforeH = buf.getHeight();
			int beforeW = buf.getWidth();
			
			System.out.println("Original resolution: "+beforeH+"x"+beforeW);
			
			int afterH = beforeH/2;
			int afterW = beforeW/2;
			
			Image scaled = buf.getScaledInstance(afterW, afterH, Image.SCALE_SMOOTH);
			io.close();
			BufferedImage bufImage = new BufferedImage(afterW, afterH, BufferedImage.TYPE_INT_RGB);
			Graphics2D bImageGraphics = bufImage.createGraphics();
			bImageGraphics.drawImage(scaled, null, null);
			RenderedImage rImage = (RenderedImage)bufImage;

			ImageIO.write(rImage, "jpg", os);			
			data.setContentLength(os.size());
			System.out.println("Thumbnail Content-Length: "+os.size());
			System.out.println("Thumbnail resolution: "+afterH+"x"+afterW);
			InputStream output = new ByteArrayInputStream(os.toByteArray());

			s3client.putObject(DESTINATION_BUCKET, "thumbs/"+objectKey, output, data);
			System.out.println("Uploaded Thumnail: s3://"+DESTINATION_BUCKET+"/thumbs/"+objectKey);
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println(bucketName + " " + objectKey);

        return null;
    }

}
