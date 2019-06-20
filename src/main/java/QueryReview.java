import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Scanner;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class QueryReview {

    private Cluster cluster;
    private static String keyspace = "scyllaproject";
    private Session session;
    QueryReview() {
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("18.232.133.103")
                    .build();
            connectSession();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    QueryReview(String keyspace) {
        this.keyspace = keyspace;
        try {
            cluster = Cluster.builder()                                                    // (1)
                    .addContactPoint("18.232.133.103")
                    .build();
            connectSession();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connectSession(){
        session = cluster.connect(keyspace);
    }

    public String getAvgProductYear(){
        BuiltStatement select = QueryBuilder.select().column("year").distinct().from(keyspace, "review");
        ResultSet rs =  session.execute(select);
        ArrayList<String> years = new ArrayList();
        for(Row row: rs){
            years.add(
                    row.toString().substring(4, 8));
        }
        for(String year: years){
            select = QueryBuilder.select().avg("overall")
                    .from(keyspace, "review")
                    .where();
        }
        return "";
    }

    public void closeConnection(){
        cluster.close();
    }

    public static void main(String[] args){
        QueryReview queryReview = new QueryReview();
        System.out.println(queryReview.getAvgProductYear());
        queryReview.closeConnection();
    }
}
