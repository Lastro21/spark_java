package com.max;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import spark.Request;
import spark.Response;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import static spark.Spark.*;

final public class Main {

    private static final SparkConf CONF = new SparkConf().setMaster("local").setAppName("SparkTest");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(CONF);

    private static final String PASS_TRING = "http://localhost:8090/testEnter";
    private static final RestTemplate REST_TEMPLATE = new RestTemplate();

    private static int check_control_count;

    private static final SparkSession sparkSession = SparkSession.builder()
            .master("local")
            .appName("SparkTest")
            .getOrCreate();
    private static final Dataset<Row> jdbcDS = sparkSession.read()
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", "jdbc:postgresql://localhost:5432/test_spark")
            .option("dbtable", "spark_table")
            .option("user", "postgres")
            .option("password", "root")
            .load();

    public static void main(String[] args) throws IOException {

        port(8090);
        get("/hello", (req, res) -> "Hello World");
        get("/doit", Main::mydoIt);
        get("/testEnter", Main::testEnter);
        get("/showbroadcastvar", Main::showCheckControlCount);
        get("/changebroadcastvar", Main::changeCheckControlCount);
        get("/insert", Main::insertRowInDB);
        get("/stop", Main::stopWebApp);

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                String result = "";
                try {
                    result = REST_TEMPLATE.getForObject(PASS_TRING, String.class);
                } catch (RestClientException e) {
                    //System.err.println("");
                }
            }
        }, 2000, 5000);

        /////////////////////////////////////////////////////////////////////////////////////////
        /////////////////////////////////////////////////////////////////////////////////////////

//        jdbcDS.show();
        try {
            jdbcDS.createOrReplaceTempView("people");
            Dataset<Row> sql = sparkSession.sql("select id, name, age from people where age = 567");
            sql.foreach(x -> System.out.println(x.get(1)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Object mydoIt(Request req, Response res) {

        JavaRDD<String> list1 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list2 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list3 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list4 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list5 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list6 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list7 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list8 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list9 = CONTEXT.textFile("D:/readme.txt");
        JavaRDD<String> list10 = CONTEXT.textFile("D:/readme.txt");

        list1.count();
        list2.count();
        list3.count();
        list4.count();
        list5.count();
        list6.count();
        list7.count();
        list8.count();
        list9.count();
        list10.count();

        //list1.saveAsTextFile("D:/readme23.txt", (Class<? extends CompressionCodec>) BZip2Codec.class);
        //list1.saveAsTextFile("D:/readme23.txt");

        return "Do it was complete";
    }

    private static Object testEnter(Request req, Response res) {
        return "Response body";
    }

    private static Object showCheckControlCount(Request req, Response res) {
        return "Check_control_count variable = " + check_control_count;
    }

    private static Object changeCheckControlCount(Request req, Response res) {
        check_control_count++;
        return "Check_control_count variable = " + check_control_count;
    }

    private static Object insertRowInDB(Request req, Response res) {

        return "Row was insert into DB!";
    }

    private static Object stopWebApp(Request req, Response res) {
        stop();
        return "WebApp was stop";
    }
}