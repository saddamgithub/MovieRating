package MainPackage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public static class Map extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {// Mapper class

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {// Mapper method with output datatype as
								// Text, Text
								// This method maps the input dataset to key
								// value pair, with Genre
		// as the key and MovieName--Rating pattern as the value

		Text keyOutput = new Text();// Mapper's Output Key that stores Genre
		Text valueOutput = new Text();// Mapper's Output Value in the form
										// of "MovieName--Rating"
		String line = value.toString();
		List<String> content = Arrays.asList(line.split("::"));
		List<String> genres = Arrays.asList(content.get(2).split(","));
		String valueString;
		for (int i = 0; i < genres.size(); i++) {
			valueString = content.get(1) + "--" + content.get(6);
			System.out.println(valueString);
			if (genres.get(i).equals("")) { // validating if genre is not
											// empty
				keyOutput.set(genres.get(i));
				valueOutput.set(valueString);
				output.collect(keyOutput, valueOutput);// passing the output
														// as genre key and
														// moviename-rating
														// value
			}
		}
	}
}