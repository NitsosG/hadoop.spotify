package aueb.fnitsos;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostDancableSong {

    private static Log log = LogFactory.getLog(MostDancableSong.class);
    public static class CountMapper extends Mapper<LongWritable, Text, Text, DancableWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split a line into words

            if (key.get()==0){
                System.out.println(value);
                return;
            }

            String[] words = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            String country = removeQuotes(words[6]);
            if (country == null) {
                return;
            }
            String month = removeQuotes(words[7].substring(0,8));
            String myKey = country.concat("_").concat(month);
            String danceability = removeQuotes(words[13]);
            String song = removeQuotes(words[1]);
            context.write(new Text(myKey), new DancableWritable(song, danceability));



        }
    }

    private static String removeQuotes(String str){
        return str.replaceAll("\"", "");
    }
    public static class CountReducer extends Reducer<Text, DancableWritable, Text, DancableWritable> {
        @Override
        public void reduce(Text key, Iterable<DancableWritable> values, Context context) throws IOException, InterruptedException {
            // sum up counts for the key
            float maxD = 0.0f;
            String song = "";


            for (DancableWritable value : values) {
                log.info("Reducer : key "+ key + " | value " + value.toString());
                float danceability = value.getDanceabilityAsFloat();
                if (danceability > maxD) {
                    maxD = danceability;
                    song = value.getSong();
                }
            }
            // output (word, count)
            context.write(key, new DancableWritable(song, String.valueOf(maxD)));
        }
    }
}
