package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.JsonObject;

import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.*;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.jet.python.PythonServiceConfig;
import com.hazelcast.internal.json.Json;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.python.PythonTransforms.mapUsingPython;
import static java.util.concurrent.TimeUnit.*;
import static java.util.concurrent.TimeUnit.HOURS;

public class Main {
    private static final String TRANSACTION_MAP = "transactions";
    private static final String STREAMING_FEATURE_CALCULATION_JOB_NAME="fraud-detection-streaming-features-calculation";
    private static final String TRANSACTION_PROCESSING_JOB_NAME="fraud-detection-transaction-processing-ml";
    private static final String MERCHANT_MAP="merchants";
    private static final String CUSTOMER_MAP="customers";
    private static final String STREAMING_FEATURE_MAP = "streaming-features";

    public static void main(String[] args) throws Exception {

        // get a client connection to Hazelcast
        Map<String, String> env = System.getenv();
        String HZ_ENDPOINT = env.get("HZ_ENDPOINT");
        String KAFKA_CLUSTER_KEY = env.get("KAFKA_CLUSTER_KEY");
        String KAFKA_CLUSTER_SECRET = env.get("KAFKA_CLUSTER_SECRET");
        String KAFKA_CLUSTER_ENDPOINT = env.get("KAFKA_ENDPOINT");
        HazelcastInstance client = getHazelClient(HZ_ENDPOINT);
        System.out.println("Connected to Hazelcast at " + HZ_ENDPOINT);
        Properties kafkaConsumerProperties = getKafkaBrokerProperties(KAFKA_CLUSTER_ENDPOINT,KAFKA_CLUSTER_KEY,KAFKA_CLUSTER_SECRET);

        //Job to calculate streaming features
        Pipeline streamingFeaturePipeline = createStreamingFeaturesPipeline(kafkaConsumerProperties);

        //Job to process transactions
        Pipeline transactionProcessingPipeline = createTransactionProcessingPipeline(kafkaConsumerProperties);

        //Submit Jobs to Hazelcast for execution
        //submitJob(STREAMING_FEATURE_CALCULATION_JOB_NAME,streamingFeaturePipeline,client);
        submitJob(TRANSACTION_PROCESSING_JOB_NAME,transactionProcessingPipeline,client);



    }
    private static void submitJob(String jobName, Pipeline pipeline,HazelcastInstance client) {
        JobConfig jobConfig = getConfig(jobName);

        Job existingJob = client.getJet().getJob(jobName);
        if (existingJob!=null) {
            try {
                existingJob.cancel();
                Thread.sleep(5000);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        //now submit the jobs
        client.getJet().newJob(pipeline,jobConfig);

    }
    private static Pipeline createTransactionProcessingPipeline(Properties kafkaConsumerProperties) {
        Pipeline p = Pipeline.create();
        // Get stream of Transactions from Kafka
        StreamStage<Map.Entry<Object, Object>> source = sourceTransactionsFromKafka(p, kafkaConsumerProperties);

        //log original transaction received
        SinkStage sendToLogger = source
                .writeTo((Sinks.logger()));

        //Retrieve Current Streaming Feature values from map
        /*
        StreamStage<Tuple2<HazelcastJsonValue, Map.Entry<Object, Object>>> retrieveStreamingFeatures = source
                .mapUsingIMap(
                        STREAMING_FEATURE_MAP,
                        //key is credit card number
                        entry -> entry.getKey().toString(),
                        // read streaming features from map
                        (transactionTuple, streamingFeatureValues) -> {
                            //current transaction
                            String transactionJson = transactionTuple.getValue().toString();
                            //existing values in streaming-feature map (as JsonObject)
                            JsonObject streamingFeaturesAsJsonObject = getStreamingFeaturesAsJsonObject((HazelcastJsonValue) streamingFeatureValues);

                            //determine transaction count in last 24 hours
                            double transactionCountLastDay = retrieveFeatureByName((HazelcastJsonValue) streamingFeatureValues, transactionJson, "transactions_last_24_hours", DAYS.toMillis(1));
                            //determine transaction in last 7 days
                            double transactionCountLast7Days = retrieveFeatureByName((HazelcastJsonValue) streamingFeatureValues, transactionJson, "transactions_last_week", DAYS.toMillis(7));
                            //determine amount spent in the last 24 hours
                            double amountSpentLast24Hours = retrieveFeatureByName((HazelcastJsonValue) streamingFeatureValues, transactionJson, "amount_spent_last_24_hours", HOURS.toMillis(24));

                            //Set streaming feature values to use with this transaction
                            streamingFeaturesAsJsonObject.set("transactions_last_24_hours", transactionCountLastDay);
                            streamingFeaturesAsJsonObject.set("transactions_last_week", transactionCountLast7Days);
                            streamingFeaturesAsJsonObject.set("amount_spent_last_24_hours", amountSpentLast24Hours);

                            //return streaming features + incoming transaction tuple
                            HazelcastJsonValue streamingFeaturesJsonValue = new HazelcastJsonValue(streamingFeaturesAsJsonObject.toString());
                            return Tuple2.tuple2(streamingFeaturesJsonValue, transactionTuple);
                        });



        // Last Step -> Update streaming feature map with last transaction processed timestamp
        /*SinkStage updateLTPDate = retrieveStreamingFeatures
                .writeTo(Sinks.mapWithUpdating(STREAMING_FEATURE_MAP,
                        tup -> tup.getValue().getKey().toString(),
                        (old, tup) -> {
                            //get transaction date from transaction json
                            String transactionJsonString = tup.getValue().getValue().toString();
                            HazelcastJsonValue existingStreamingValues = (HazelcastJsonValue) old;
                            return updateLastTransactionDate(transactionJsonString,existingStreamingValues);
                        }));

         */
        return p;
    }
    private static HazelcastJsonValue updateLastTransactionDate(String transactionJsonString,HazelcastJsonValue oldFeatureValues ) {

        JsonObject transactionJsonObject = new JsonObject(Json.parse(transactionJsonString).asObject());
        long current_transaction_date_ms = transactionJsonObject.getLong("timestamp_ms", -1l);

        //use this value to set last transaction date in map
        JsonObject newJsonObject = getStreamingFeaturesAsJsonObject(oldFeatureValues);
        newJsonObject.set("last_transaction_date", current_transaction_date_ms);
        return new HazelcastJsonValue(newJsonObject.toString());

    }

    private static Pipeline createStreamingFeaturesPipeline(Properties kafkaConsumerProperties) {
        //Create new Pipeline
        Pipeline p = Pipeline.create();
        // Get stream of Transactions from Kafka (Map CC_NUM, transactionJsonString)
        StreamStage<Map.Entry<Object, Object>> source = sourceTransactionsFromKafka(p, kafkaConsumerProperties);
        //Count transactions in the last 1 Day. Emit count every 1 hour
        addCountStreamingFeature(source,"transactions_last_24_hours", sliding(HOURS.toMillis(24), HOURS.toMillis(1)),counting(),STREAMING_FEATURE_MAP);

        //Count transactions in the last Week. Emit count every half hour
        addCountStreamingFeature(source,"transactions_last_week", sliding(DAYS.toMillis(7), MINUTES.toMillis(30)), counting(), STREAMING_FEATURE_MAP);

        //Sum Amount Spent in the last 24 hours. Emit sum every 1 hour
        addSumStreamingFeature(source, "amount_spent_last_24_hours", sliding(HOURS.toMillis(24), HOURS.toMillis(1)), STREAMING_FEATURE_MAP, "amt");
        return p;
    }


    private static StreamStage<Map.Entry<Object,Object>> sourceTransactionsFromKafka(Pipeline p, Properties kafkaConsumerProperties) {
        return p.readFrom(KafkaSources.kafka(kafkaConsumerProperties,"transactions"))
                //Use transaction timestamp
                .withTimestamps(tup -> {
                    JsonObject event = new JsonObject(Json.parse(tup.getValue().toString()).asObject());
                    return event.getLong("timestamp_ms",0L);
                },0L)
                .setLocalParallelism(6);

    }
    private static void addSumStreamingFeature(StreamStage<Map.Entry<Object, Object>> streamSource, String featureName, WindowDefinition windowDefinition, String streamingFeatureMapName, String fieldToSum) {

        StreamStage<KeyedWindowResult<Object, Double>> featureAggregation = streamSource
                .groupingKey(Map.Entry::getKey)
                .window(windowDefinition)
                .aggregate(summingDouble(input -> {
                    return (new JsonObject(Json.parse(input.getValue().toString()).asObject()).getDouble(fieldToSum, 0));
                }));

        // Update "streaming-features" map with sum amt at every window
        featureAggregation
                .writeTo(Sinks.mapWithUpdating(streamingFeatureMapName,
                        kwr -> kwr.getKey().toString(),
                        (old, entry) -> {
                            JsonObject streamingFeaturesAsJsonObject = getStreamingFeaturesAsJsonObject((HazelcastJsonValue) old);
                            streamingFeaturesAsJsonObject.set(featureName, entry.getValue().doubleValue());
                            return new HazelcastJsonValue(streamingFeaturesAsJsonObject.toString());
                        }));
    }

    private static void addCountStreamingFeature (StreamStage<Map.Entry<Object, Object>> streamSource, String featureName, WindowDefinition windowDefinition, AggregateOperation1 aggregateOperation, String streamingFeatureMapName) {

        //Count events over a window of time
        StreamStage<KeyedWindowResult<Object, Long>> featureAggregation = streamSource
                .groupingKey(Map.Entry::getKey)
                .window(windowDefinition)
                .aggregate(aggregateOperation)
                .setLocalParallelism(6);

        // Update "streaming-features" map with count at every window
        featureAggregation
                .writeTo(Sinks.mapWithUpdating(streamingFeatureMapName,
                        kwr -> kwr.getKey().toString(),
                        (old, entry) -> {
                            JsonObject streamingFeaturesAsJsonObject = getStreamingFeaturesAsJsonObject((HazelcastJsonValue) old);
                            streamingFeaturesAsJsonObject.set(featureName, entry.getValue().doubleValue());
                            return new HazelcastJsonValue(streamingFeaturesAsJsonObject.toString());
                        }));
    }
    private static Properties getKafkaBrokerProperties (String bootstrapServers, String clusterKey, String clusterSecret) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootstrapServers);
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        //Cloud
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + clusterKey + "' password='" + clusterSecret + "';");
        props.setProperty("sasl.mechanism", "PLAIN");
        props.setProperty("session.timeout.ms", "45000");
        props.setProperty("client.dns.lookup", "use_all_dns_ips");
        props.setProperty("acks", "all");
        props.setProperty("auto.offset.reset","earliest");

        return props;
    }
    private static JsonObject getStreamingFeaturesAsJsonObject(HazelcastJsonValue existingValue) {
        return (existingValue==null) ? new JsonObject() : new JsonObject(Json.parse(existingValue.toString()).asObject()) ;
    }
    private static JobConfig getConfig(String jobName) {
        JobConfig cfg = new JobConfig().setName(jobName);
        cfg.addClass(Main.class);
        return cfg;
    }
    private static Pipeline createPythonMLPipeline() throws Exception {

        Pipeline pipeline = Pipeline.create();

        //pipeline starts as soon as a "transaction" is put on the "transactions" MAP
        StreamStage<Tuple2<String, JsonObject>> transactions =  pipeline.readFrom(Sources.<String, String>mapJournal(TRANSACTION_MAP,
                        JournalInitialPosition.START_FROM_CURRENT))
                .withIngestionTimestamps()
                .setLocalParallelism(8)
                .setName("Start Fraud Detection ML Pipeline")

                //Convert Transaction String into JSONObject
                .map(tup -> Tuple2.tuple2(tup.getKey(),new JsonObject(Json.parse(tup.getValue()).asObject())))
                .setName("INGEST Transaction in JSON format");

        //Look up Merchant for this transaction
        StreamStage<Tuple3<String, JsonObject, JsonObject>> enrichMerchantFeatures = transactions
                .mapUsingReplicatedMap(MERCHANT_MAP,
                        tup -> tup.getValue().getString("merchant","none"),
                        (tup, merchant) -> {
                                    HazelcastJsonValue m = (HazelcastJsonValue) merchant;
                                    JsonObject merchantJSON = new JsonObject(Json.parse(m.toString()).asObject());
                                    return Tuple3.tuple3(tup.f0(), tup.f1(),merchantJSON);
                                })
                .setName("ENRICH - Retrieve Merchant Features");


        //Look up Customer features for this transaction
        StreamStage<Tuple4<String, JsonObject, JsonObject, JsonObject>> enrichCustomerFeatures = enrichMerchantFeatures
                .mapUsingIMap(CUSTOMER_MAP,
                        tup -> tup.f1().getString("cc_num","none"),
                        (tup, customer) -> {
                                HazelcastJsonValue c = (HazelcastJsonValue) customer;
                                JsonObject customerJSON = new JsonObject(Json.parse(c.toString()).asObject());
                                return Tuple4.tuple4(tup.f0(), tup.f1(), tup.f2(),customerJSON);
                            })

                .setName("ENRICH - Retrieve Customer Features");

        //Calculate Real-Time Features
        StreamStage<Tuple5<String, JsonObject, JsonObject, JsonObject, Double>> calculateRealtimeFeatures = enrichCustomerFeatures
                .map(tup -> {
                    double transactionLat = tup.f1().getDouble("lat",0);
                    double transactionLon = tup.f1().getDouble("long",0);

                    JsonObject customerJsonObject = tup.f3();
                    float customerLatitude = customerJsonObject.getFloat("latitude",0);
                    float customerLongitude = customerJsonObject.getFloat("longitude",0);

                    //Calculate Distance between Transaction Location and Customer Billing address
                    double distanceKms = calculateDistanceKms(transactionLat, transactionLon, customerLatitude, customerLongitude);
                    return Tuple5.tuple5(tup.f0(), tup.f1(), tup.f2(), tup.f3(), distanceKms);
                })
                .setName("ENRICH - Real-Time Features");

        //Prepare fraud request (JSON String) to be sent to Python
        StreamStage<JsonObject> getFraudPredictions = calculateRealtimeFeatures
                .map(tup -> {
                    //Customer and Merchant JsonObjects
                    JsonObject customerJsonObject = tup.f3();
                    JsonObject merchantJsonObject = tup.f2();
                    JsonObject transactionJsonObject = tup.f1();
                    Double distanceKms = tup.f4();

                    //date related codes
                    LocalDateTime transactionDate = LocalDateTime.parse(tup.f1().getString("transaction_date",""), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    int weekday = transactionDate.getDayOfWeek().getValue()-1;
                    int month = transactionDate.getMonthValue();
                    int hour = transactionDate.getHour();
                    JsonObject jsonFraudDetectionRequest = new JsonObject()
                            .add("transaction_number", transactionJsonObject.getString("trans_num",""))
                            .add("transaction_date", transactionJsonObject.getString("transaction_date",""))
                            .add("amount", transactionJsonObject.getFloat("amount",0))
                            .add("merchant", transactionJsonObject.getString("merchant",""))
                            .add("merchant_lat", transactionJsonObject.getDouble("lat",0))
                            .add("merchant_lon", transactionJsonObject.getDouble("long",0))
                            .add("credit_card_number", Long.parseLong( transactionJsonObject.getString("cc_num","")))
                            .add("customer_name", customerJsonObject.getString("first","") +
                                                        " " + customerJsonObject.getString("last",""))
                            .add("customer_city", customerJsonObject.getString("city",""))
                            .add("customer_age_group", customerJsonObject.getString("age_group",""))
                            .add("customer_gender", customerJsonObject.getString("gender",""))
                            .add("customer_lat", customerJsonObject.getFloat("latitude",0))
                            .add("customer_lon", customerJsonObject.getFloat("longitude",0))
                            .add("distance_from_home", distanceKms)
                            .add("category_code", merchantJsonObject.getInt("category_code",0))
                            .add("transaction_weekday_code",weekday)
                            .add("transaction_hour_code",hour)
                            .add("transaction_month_code",month)
                            .add("gender_code", customerJsonObject.getInt("gender_code",0))
                            .add("customer_zip_code", customerJsonObject.getInt("zip_code",0))
                            .add("customer_city_population", customerJsonObject.getInt("city_pop",0))
                            .add("customer_job_code", customerJsonObject.getInt("job_code",0))
                            .add("customer_age", customerJsonObject.getInt("age",0))
                            .add("customer_setting_code", customerJsonObject.getInt("setting_code",0))
                            .add("customer_age_group_code", customerJsonObject.getInt("age_group_code",0))
                            .add("transaction_processing_total_time", 0);
                    return  jsonFraudDetectionRequest;});


        //Time to Call the Python Fraud Detection Model and get a prediction!
        // Store the returned prediction in "predictionResult" MAP
        PythonServiceConfig pythonServiceConfig = getPythonServiceConfig("fraud_handler");

        SinkStage predictFraud = getFraudPredictions
                //from JsonObject to string for mapUsingPython
                .map(predictionRequest -> predictionRequest.toString())
                // Run Python Model
                .apply(mapUsingPython(pythonServiceConfig))
                .setLocalParallelism(8)
                .setName("PREDICT (Python)- Fraud Probability")
                //From String back into a JSONObject
                .map(predictionRequest -> new JsonObject(Json.parse(predictionRequest).asObject()))
                // Sink JSONObject to Hazelcast fast data store (MAP)
                .map (predictionJSON -> {
                   String key = predictionJSON.getString("transaction_number","") + "@" + String.valueOf(predictionJSON.getLong("credit_card_number",0));
                    HazelcastJsonValue jv = new HazelcastJsonValue(predictionJSON.toString());
                    return Tuple2.tuple2(key,jv);
                })
                .writeTo(Sinks.map("predictionResult"));
        return pipeline;

        //.writeTo(Sinks.logger());

    }

    private static HazelcastInstance getHazelClient(String hazelcastClusterMemberAddresses)  {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig().addAddress(hazelcastClusterMemberAddresses);

        //Start the client
        //HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        HazelcastInstance client = Hazelcast.bootstrappedInstance();

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

        // These files will be copied over to the python environment created by hazelcast
        String[] resourcesToCopy = { name + ".py", "lgbm_model_no_merchant_cc_num","requirements.txt"};
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
    private static double retrieveFeatureByName(HazelcastJsonValue existingStreamingFeatureValue, String currentTransactionString, String featureName, long aggregationPeriodMillis) {

        double result = 0;

        // if there is no previous value on the map
        if (existingStreamingFeatureValue == null) {
            result = 0l;
        }
        else {

            //Get JsonObject from the existing existingStreamingFeatureValue (HazelcastJsonValue)
            JsonObject existingStreamingFeaturesObject = new JsonObject(Json.parse(existingStreamingFeatureValue.toString()).asObject());
            //retrieve  result for a given feature
            result = existingStreamingFeaturesObject.getDouble(featureName, -20l);
            if (result==-20l)
                result=0;

            //round to 2 decimals
            result  = Math.round(result*100)/100.00;

            //Now last transaction processed timestamp
            long last_transaction_date_ms = existingStreamingFeaturesObject.getLong("last_transaction_date", -3l);

            //retrieve current transaction date (from current transaction)
            JsonObject transactionJsonObject = new JsonObject(Json.parse(currentTransactionString).asObject());
            long current_transaction_date_ms = transactionJsonObject.getLong("timestamp_ms", -4l);

            //Compare current transaction vs last transaction processed timestamps. If wider than Window
            // Ignore existing value and return 0
            if (current_transaction_date_ms - last_transaction_date_ms >= aggregationPeriodMillis) {
                result = 0;
            }
        }

        return result;
    }

}
