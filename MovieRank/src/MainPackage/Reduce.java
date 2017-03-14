package MainPackage;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public static class Reduce extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {// Reducer method with output datatype as
								// Text,Text
								// Reducer identifies the average rating of each
								// movie for every
		// genre key and outputs the movie name with highest average rating

		Text outputValue = new Text();
		// outputValue contains the output value ie. movie name and it's average
		// rating
		String element;
		List<String> movieList = new ArrayList<String>();
		List<String> ratingList = new ArrayList<String>();

		// below arraylists contain the moviename, sum of it's ratings, number
		// of ratings and average of ratings. this is mapped
		// based on index value
		List<String> movieAvgList = new ArrayList<String>();
		List<Integer> ratingSumList = new ArrayList<Integer>();
		List<Integer> ratingCountList = new ArrayList<Integer>();
		List<Double> ratingAvgList = new ArrayList<Double>();
		while (values.hasNext()) {
			Object valueObject = values.next();
			element = valueObject + "";
			List<String> templist = Arrays.asList(element.split("--"));
			movieList.add(templist.get(0));
			ratingList.add(templist.get(1));
		}
		DecimalFormat df = new DecimalFormat("#.00");

		for (int i = 0; i < movieList.size(); i++) {
			if (!movieAvgList.contains(movieList.get(i))) {
				movieAvgList.add(movieList.get(i));
				ratingCountList.add(1);
				ratingSumList.add(Integer.parseInt(ratingList.get(i)));
				System.out.println();
			} else {
				// making entry for movie, sum and count
				int index = movieAvgList.indexOf(movieList.get(i));
				ratingCountList.set(index, ratingCountList.get(index) + 1);
				ratingSumList.set(
						index,
						ratingSumList.get(index)
								+ Integer.parseInt(ratingList.get(i)));
			}
		}
		// finding the average of rating
		for (int i = 0; i < movieAvgList.size(); i++) {
			double avgs = (double) ratingSumList.get(i)
					/ (double) ratingCountList.get(i);
			String formattedAvgs = df.format(avgs);
			ratingAvgList.add(Double.parseDouble(formattedAvgs));
		}
		Integer m = 0, maxIndex = -1;
		Double max = null;
		// finding the max of average of rating
		for (Double x : ratingAvgList) {
			if ((x != null) && ((max == null) || (x > max))) {
				max = x;
				maxIndex = m;
			}
			m++;
		}

		String outputString = movieAvgList.get(maxIndex) + "\t"
				+ ratingAvgList.get(maxIndex);
		outputValue.set(outputString);
		output.collect(key, outputValue);// sending as output genre as key and
											// (moviename and rating) of movie
											// with max avg. rating as value

	}
}
