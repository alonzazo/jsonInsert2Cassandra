import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import static java.time.temporal.ChronoUnit.SECONDS;

public class Main {
    public static void main(String[] args){
        Cluster cluster = null;
        try {
            String tablename;
            String path;

            if (args.length == 1){
                tablename = "review";
                path = args[0];
            }
            else if (args.length == 2){
                tablename = args[1];
                path = args[0];
            }
            else
                throw new Exception();

            File fileInput1 = new File(path);
            ProgressCounter progressCounter = new ProgressCounter(fileInput1.length());

            InputStream inputStream1 = new FileInputStream(path);

            Scanner scanner1 = new Scanner(inputStream1);

            //File file = new File("out.sql");
            //file.createNewFile();

            //FileWriter fileWriter = new FileWriter("out.sql",true);

            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("18.232.133.103")
                    .build();
            Session session = cluster.connect("scyllaproject");                                           // (2)

            JSONParser jsonParser = new JSONParser();
            progressCounter.start();
            int i = 0;
            while (scanner1.hasNextLine()){

                String lineLeft = scanner1.nextLine();
                if(i > 5336){
                    JSONObject rowLeft = (JSONObject) jsonParser.parse(lineLeft);

                    String query = convertJsonToQuery(rowLeft, tablename);

                    session.execute(query);
                    System.out.println(i);
                    progressCounter.advance(lineLeft.length());
                }
                i++;
            }

            progressCounter.finish();

        }catch (Exception e){
            e.printStackTrace();

        } finally {
            if (cluster != null) cluster.close();
        }
    }


    private static String convertJsonToQuery(JSONObject jsonObject, String tableName){

//        if (jsonObject.containsKey("helpful"))
//            jsonObject.put("helpful","");
       String reviewTime = jsonObject.get("reviewTime").toString();

        String year = reviewTime.substring(reviewTime.length()-4);

        jsonObject.put("year", year);

//        jsonObject.put("related","");

//
//        JSONArray categoriesArray = (JSONArray) jsonObject.get("categories");
//        JSONArray categoriesExtended = new JSONArray();
//        for (int i = 0; i < categoriesArray.size(); i++){
//            if (categoriesArray.get(i) instanceof JSONArray) {
//                for (Object element : (JSONArray) categoriesArray.get(i))
//                    categoriesExtended.add(element);
//            }else {
//                categoriesExtended.add(categoriesArray.get(i));
//            }
//        }
//        jsonObject.put("categories",categoriesExtended);

        return "INSERT INTO " + tableName + " JSON '" + jsonObject.toJSONString().replace("\'","\\\"") + "';";
    }

    private static class ProgressCounter {
        private long lastProgress;
        private Instant lastTimestamp;
        private long currentProgress;
        private long total;
        private PrintStream printStream;

        public ProgressCounter(long total){
            currentProgress = 0;
            this.total = total;
            printStream = System.out;
        }

        public ProgressCounter(long total, PrintStream printStream){
            currentProgress = 0;
            this.total = total;
            this.printStream = printStream;
        }

        public void start(){
            lastProgress = 0;
            lastTimestamp = Instant.now();
            printStream.println("This process has been started!");
        }

        public void advance(long quantity){
            lastProgress = currentProgress;
            currentProgress += quantity;
            report();
        }

        public void setProgress(long progressPoint){
            lastProgress = currentProgress;
            currentProgress = progressPoint;
            report();
        }

        public void setTotal(long total){
            this.total = total;
            report();
        }

        public void finish(){
            printStream.println("The process has been finished!");
        }

        private void report(){
            double porcentageCurrentProgress = (currentProgress * 100.0) / total;
            double estimatedTimeAproximation =  (100.0-porcentageCurrentProgress) / porcentagePerSecond();
            Duration duration = Duration.ofSeconds(Math.round(estimatedTimeAproximation));
            printStream.println("Current progress: " + (currentProgress * 100) / total + "% " + currentProgress + " B /" + total + " B ETA: " + formatDuration(duration));
        }

        private double porcentagePerSecond(){
            Instant currentTimestamp = Instant.now();
            double deltaProgress = (currentProgress  - lastProgress) * 100.0 / total;
            double deltaTime = lastTimestamp.until(currentTimestamp, SECONDS);
            lastTimestamp = currentTimestamp;
            return deltaProgress / deltaTime;
        }

        private String formatDuration(Duration duration) {
            long seconds = duration.getSeconds();
            long absSeconds = Math.abs(seconds);
            String positive = String.format(
                    "%d:%02d:%02d",
                    absSeconds / 3600,
                    (absSeconds % 3600) / 60,
                    absSeconds % 60);
            return seconds < 0 ? "-" + positive : positive;
        }
    }


}
