package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.Tuple4;
import com.hazelcast.jet.datamodel.Tuple5;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.org.json.JSONObject;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.hazelcast.jet.python.PythonTransforms.mapUsingPython;

public class Main {
    private static final String TRANSACTION_MAP = "transactions";
    private static final String JOB_NAME="fraud-detection-ml-2";
    private static final String MERCHANT_MAP="merchants";
    private static final String CUSTOMER_MAP="customers";

    public static void main(String[] args) throws Exception {

        // get a client connection to Hazelcast
        Map<String, String> env = System.getenv();
        String HZ_ENDPOINT = env.get("HZ_ENDPOINT");
        System.out.println("Connected to Hazelcast at " + HZ_ENDPOINT);
        HazelcastInstance client = getHazelClient(HZ_ENDPOINT);

        //create real-time transaction fraud detection pipeline
        Pipeline p = createPythonMLPipeline();

        //Submit Pipeline Job -  Cancelling any existing run of the job with the same name
        JobConfig cfg = getConfig();
        Job existingJob = client.getJet().getJob(JOB_NAME);
        if (existingJob!=null) {
            try {
                existingJob.cancel();
                Thread.sleep(2000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        //Submit Pipeline Job
        client.getJet().newJob(p, cfg);

        //Gracefully shutdown client connection
        client.shutdown();

    }
    private static JobConfig getConfig() {
        JobConfig cfg = new JobConfig().setName(JOB_NAME);
        cfg.addClass(Main.class);
        return cfg;
    }

    private static Pipeline createPythonMLPipeline() throws Exception {

        Pipeline pipeline = Pipeline.create();

        //pipeline starts as soon as a "transaction" is put on the "transactions" MAP
        StreamStage<Tuple2<String, JSONObject>> transactions =  pipeline.readFrom(Sources.<String, String>mapJournal(TRANSACTION_MAP,
                        JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .setLocalParallelism(8)
                .setName("Start Fraud Detection ML Pipeline")

                //Convert Transaction String into JSONObject
                .map(tup -> Tuple2.tuple2(tup.getKey(),new JSONObject(tup.getValue())))
                .setName("INGEST Transaction in JSON format");

        //Look up Merchant for this transaction
        StreamStage<Tuple3<String, JSONObject, Object>> enrichMerchantFeatures = transactions
                .mapUsingReplicatedMap(MERCHANT_MAP,
                        tup -> tup.getValue().getString("merchant"),
                        (tup, merchant) -> Tuple3.tuple3(tup.f0(), tup.f1(), merchant))
                .setName("ENRICH - Retrieve Merchant Features");

        //Look up Customer features for this transaction
        StreamStage<Tuple4<String, JSONObject, Object, Object>> enrichCustomerFeatures = enrichMerchantFeatures
                .mapUsingIMap(CUSTOMER_MAP,
                        tup -> tup.f1().getString("cc_num"),
                        (tup, customer) -> Tuple4.tuple4(tup.f0(), tup.f1(), tup.f2(), customer))
                .setName("ENRICH - Retrieve Customer Features");

        //Calculate Real-Time Features
        StreamStage<Tuple5<String, JSONObject, Object, Object, Double>> calculateRealtimeFeatures = enrichCustomerFeatures
                .map(tup -> {
                    double transactionLat = tup.f1().getFloat("lat");
                    double transactionLon = tup.f1().getFloat("long");
                    float customerLatitude = ((GenericRecord) tup.f3()).getFloat32("latitude");
                    float customerLongitude = ((GenericRecord) tup.f3()).getFloat32("longitude");
                    double distanceKms = calculateDistanceKms(transactionLat, transactionLon, customerLatitude, customerLongitude);
                    return Tuple5.tuple5(tup.f0(), tup.f1(), tup.f2(), tup.f3(), distanceKms);
                })
                .setName("ENRICH - Real-Time Features");

        //Prepare fraud request (JSON String) to be sent to Python
        StreamStage<String> getFraudPredictions = (StreamStage<String>) calculateRealtimeFeatures
                .map(tup -> {
                    //date related codes
                    LocalDateTime transactionDate = LocalDateTime.parse(tup.f1().getString("transaction_date"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    int weekday = transactionDate.getDayOfWeek().getValue()-1;
                    int month = transactionDate.getMonthValue();
                    int hour = transactionDate.getHour();
                    JsonObject jsonFraudDetectionRequest = new JsonObject()
                            .add("transaction_number", tup.f1().getString("trans_num"))
                            .add("transaction_date", tup.f1().getString("transaction_date"))
                            .add("amount", tup.f1().getFloat("amount"))
                            .add("merchant", tup.f1().getString("merchant"))
                            .add("merchant_lat", tup.f1().getDouble("lat"))
                            .add("merchant_lon", tup.f1().getDouble("long"))
                            .add("credit_card_number", tup.f1().getLong("cc_num"))
                            .add("customer_name", ((GenericRecord) tup.f3()).getString("first") + " " + ((GenericRecord) tup.f3()).getString("last"))
                            .add("customer_city", ((GenericRecord) tup.f3()).getString("city"))
                            .add("customer_age_group", ((GenericRecord) tup.f3()).getString("age_group"))
                            .add("customer_gender", ((GenericRecord) tup.f3()).getString("gender"))
                            .add("customer_lat", ((GenericRecord) tup.f3()).getFloat32("latitude"))
                            .add("customer_lon", ((GenericRecord) tup.f3()).getFloat32("longitude"))
                            .add("distance_from_home", tup.f4())
                            .add("category_code", ((GenericRecord) tup.f2()).getInt32("category_code"))
                            .add("transaction_weekday_code",weekday)
                            .add("transaction_hour_code",hour)
                            .add("transaction_month_code",month)
                            .add("gender_code", ((GenericRecord) tup.f3()).getInt32("gender_code"))
                            .add("customer_zip_code", ((GenericRecord) tup.f3()).getInt32("zip_code"))
                            .add("customer_city_population", ((GenericRecord) tup.f3()).getInt32("city_pop"))
                            .add("customer_job_code", ((GenericRecord) tup.f3()).getInt32("job_code"))
                            .add("customer_age", ((GenericRecord) tup.f3()).getInt32("age"))
                            .add("customer_setting_code", ((GenericRecord) tup.f3()).getInt32("setting_code"))
                            .add("customer_age_group_code", ((GenericRecord) tup.f3()).getInt32("age_group_code"))
                            .add("transaction_processing_total_time", 0);
                    return  jsonFraudDetectionRequest.toString();});

        //Time to Call the Python Fraud Detection Model and get a prediction!
        // Store the returned prediction in "predictionResult" MAP

        PythonServiceConfig pythonServiceConfig = getPythonServiceConfig("fraud_handler");
        SinkStage predictFraud = getFraudPredictions
                // Run Python Model
                .apply(mapUsingPython(pythonServiceConfig))
                .setLocalParallelism(8)
                .setName("PREDICT (Python)- Fraud Probability")
                //serialize the prediction returned by Python a (String) back into a JSONObject
                .map(prediction -> new JSONObject(prediction))
                // Sink JSONObject to Hazelcast fast data store (MAP)
                .map (predictionJSON -> {
                   String key = predictionJSON.getString("transaction_number") + "@" + String.valueOf(predictionJSON.getLong("credit_card_number"));
                    HazelcastJsonValue jv = new HazelcastJsonValue(predictionJSON.toString());
                    return Tuple2.tuple2(key,jv);
                })
                .writeTo(Sinks.map("predictionResult"));
        return pipeline;

    }

    private static HazelcastInstance getHazelClient(String hazelcastClusterMemberAddresses)  {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig()
                .addAddress(hazelcastClusterMemberAddresses);

        //Start the client
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("Connected to Hazelcast Cluster");
        return client;
    }

    private static Double calculateDistanceKms(double lat1, double long1, double lat2, double long2) {
        return org.apache.lucene.util.SloppyMath.haversinMeters(lat1, long1, lat2, long2) / 1_000;
    }
    protected static PythonServiceConfig getPythonServiceConfig(String name) throws Exception {
        File temporaryDir = getTemporaryDir(name);

        PythonServiceConfig pythonServiceConfig = new PythonServiceConfig();
        pythonServiceConfig.setBaseDir(temporaryDir.toString());
        pythonServiceConfig.setHandlerFunction("transform_list");
        pythonServiceConfig.setHandlerModule(name);

        return pythonServiceConfig;
    }
    private static File getTemporaryDir(String name) throws Exception {
        Path targetDirectory = Files.createTempDirectory(name);
        targetDirectory.toFile().deleteOnExit();

        // These files will be copied over to the python environment created  by hazelcast
        String[] resourcesToCopy = { name + ".py", "lgbm_model_no_merchant_cc_num"};
        for (String resourceToCopy : resourcesToCopy) {
            try (InputStream inputStream = Main.class.getResourceAsStream(resourceToCopy)) {
                if (inputStream == null) {
                    System.out.println(resourceToCopy + ": NOT FOUND in Jar's src/main/resources");
                } else {
                    System.out.println(resourceToCopy + ": FOUND  in Jar");
                    Path targetFile =
                            Paths.get(targetDirectory + File.separator + resourceToCopy);
                    Files.copy(inputStream, targetFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }

        return targetDirectory.toFile();
    }
}
