package com.gizwits.bean;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by feel on 16/6/2.
 */
public class GenData {

    private final static Logger LOGGER = LoggerFactory.getLogger(GenData.class);
    private final static String fileName = "product.json";
    private final static String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private final static String DATE_FORMAT4 = "yyyy-MM-dd'T'00:00:00Z";

    private final static String DATE_FORMAT2 = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    private final static String DATE_FORMAT3 = "yyyy-MM-dd HH:mm:ssZ";
    private final static String DATE_FORMAT5 = "yyyy/MM/dd";


    private final static ObjectMapper mapper = new ObjectMapper();


    private static Long genRandom(int num) {


        Random random = new Random();
        return (long) random.nextInt(num);

    }

    private static void gen(String path) {


        DateTime dateTime2 = new DateTime().plusMinutes(genRandom(1000).intValue());
        DateTime dateTime = new DateTime();

        String timetamp = dateTime2.toString(DATE_FORMAT);

        String product_key = "a5e319e1062111e3a250a82066295b22";
        String mac = "acff0000" + genRandom(5);
        Long count = genRandom(1000);

        Product product = new Product(timetamp, product_key, mac, count);

        // Convert object to JSON string
        String product2Json = null;
        try {

            product2Json = mapper.writeValueAsString(product);

            FileUtils.writeStringToFile(new File(path), product2Json, true);
            FileUtils.writeStringToFile(new File(path), "\n", true);

        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info(product2Json);

    }


    public static void main(String[] args) throws Exception {


        final String path = "/Users/feel/githome/gizwits/spark-es-analyzed/target/" + fileName;
        //FileUtils.forceDelete(new File(path));

        ExecutorService service = Executors.newFixedThreadPool(100);


        service.execute(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 100; i++) {
                    gen(path);
                }

            }
        });


        service.shutdown();
        service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);


    }

}
