package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
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

public class StreamingFeatures {
    private static final String TRANSACTION_PROCESSING_JOB_NAME="fraud-detection-transaction-processing-ml";
    private static final String MERCHANT_MAP="merchants";
    private static final String CUSTOMER_MAP="customers";
    private static final String STREAMING_FEATURE_MAP = "streaming-features";
    private static final String PREDICTION_RESULT_MAP = "predictionResult";

    public static void main(String[] args) throws Exception {

        // get a client connection to Hazelcast
        Map<String, String> env = System.getenv();
        String HZ_ENDPOINT = env.get("HZ_ENDPOINT");
        //HazelcastInstance client = Hazelcast.bootstrappedInstance();
        HazelcastInstance client = getHazelClient(HZ_ENDPOINT);

        //Kafka Environment variables to source transactions from
        String KAFKA_CLUSTER_KEY = env.get("KAFKA_CLUSTER_KEY");
        String KAFKA_CLUSTER_SECRET = env.get("KAFKA_CLUSTER_SECRET");
        String KAFKA_CLUSTER_ENDPOINT = env.get("KAFKA_ENDPOINT");
        Properties kafkaConsumerProperties = getKafkaBrokerProperties(KAFKA_CLUSTER_ENDPOINT,KAFKA_CLUSTER_KEY,KAFKA_CLUSTER_SECRET);

        //Job to process transactions and calculate streaming features at the same time
        Pipeline transactionProcessingPipeline = createTransactionProcessingPipeline(kafkaConsumerProperties);

        //Submit Jobs to Hazelcast for execution
        submitJob(TRANSACTION_PROCESSING_JOB_NAME,transactionProcessingPipeline,client);

        client.shutdown();

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
        //now submit the job
        client.getJet().newJob(pipeline,jobConfig);
    }
    private static Pipeline createTransactionProcessingPipeline(Properties kafkaConsumerProperties)  throws Exception {
        Pipeline p = Pipeline.create();
        // Get stream of Transactions from Kafka Topic
        StreamStage<Map.Entry<Object, Object>> source = sourceTransactionsFromKafka(p, kafkaConsumerProperties);

        //1. Retrieve Current Streaming Feature values from map
        StreamStage<Tuple2<HazelcastJsonValue, JsonObject>> retrieveStreamingFeatures = source
                .mapUsingIMap(
                        STREAMING_FEATURE_MAP,
                        //Kafka message key is credit card number
                        entry -> entry.getKey().toString(),
                        // read streaming features from map
                        (transactionTuple, streamingFeatureValues) -> {
                            //current transaction
                            String transactionJson = transactionTuple.getValue().toString();
                            JsonObject transactionJsonObject = getJsonObject(transactionJson);
                            //existing values in streaming-feature map (as JsonObject)
                            JsonObject streamingFeaturesAsJsonObject = getJsonObject((HazelcastJsonValue) streamingFeatureValues);

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

                            HazelcastJsonValue streamingFeaturesJsonValue = new HazelcastJsonValue(streamingFeaturesAsJsonObject.toString());
                            return Tuple2.tuple2(streamingFeaturesJsonValue, transactionJsonObject);
                        })
                .setLocalParallelism(10)
                .setName("ENRICH - Retrieve STREAMING Feature values");

        //2.a Add Branches to calculate 3 streaming features
        //Count transactions in previous 24 hours. Emit count every HOUR
        addCountStreamingFeature(source,"transactions_last_24_hours", sliding(HOURS.toMillis(24), HOURS.toMillis(1)),counting(),STREAMING_FEATURE_MAP);
        //Count transactions in the previous 7 days. Emit count every HOUR
        addCountStreamingFeature(source,"transactions_last_week", sliding(DAYS.toMillis(7), HOURS.toMillis(1)), counting(), STREAMING_FEATURE_MAP);
        //Sum Amount Spent in the last 24 hours. Emit sum every HOUR
        addSumStreamingFeature(source, "amount_spent_last_24_hours", sliding(HOURS.toMillis(24), HOURS.toMillis(1)), STREAMING_FEATURE_MAP, "amt");

        //2.b Look up Merchant for this transaction (input Tuple <StreamingFeatures, OriginalTransaction)
        StreamStage<Tuple3<HazelcastJsonValue, JsonObject, JsonObject>> retrieveMerchantFeatures = retrieveStreamingFeatures
                .mapUsingReplicatedMap(MERCHANT_MAP,
                        tup -> tup.getValue().getString("merchant", "none"),
                        (tup, merchant) -> {
                            JsonObject merchantJSON = getJsonObject((HazelcastJsonValue) merchant);
                            return Tuple3.tuple3(tup.f0(), tup.f1(), merchantJSON);
                        })
                .setName("ENRICH - Retrieve MERCHANT Features");


        //3. Look up Customer Info for this transaction (input Tuple <StreamingFeatures, OriginalTransaction, Merchant>)
        StreamStage<Tuple4<HazelcastJsonValue, JsonObject, JsonObject, JsonObject>> retrieveCustomerFeatures = retrieveMerchantFeatures
                .mapUsingIMap(CUSTOMER_MAP,
                        tup -> tup.f1().getString("cc_num", "none"),
                        (tup, customer) -> {
                            JsonObject customerJson = getJsonObject((HazelcastJsonValue) customer);
                            return Tuple4.tuple4(tup.f0(), tup.f1(), tup.f2(), customerJson);
                        })
                .setName("ENRICH - Retrieve CUSTOMER Features");

        //4. Prepare JSON String to be sent to Python Fraud Model
        //(input Tuple <StreamingFeatures, OriginalTransaction, Merchant, Customer>)
        StreamStage<JsonObject> preparePredictionRequest = retrieveCustomerFeatures
                .map( tup -> {
                    //StreamingFeatures, Transaction, Customer and Merchant JsonObjects
                    JsonObject streamingFeaturesJsonObject = getJsonObject(tup.f0());
                    JsonObject transactionJsonObject = tup.f1();
                    JsonObject merchantJsonObject = tup.f2();
                    JsonObject customerJsonObject = tup.f3();
                    return prepareFraudPredictionRequest( streamingFeaturesJsonObject, transactionJsonObject, merchantJsonObject, customerJsonObject);
                })
                .setName("PREPARE - Fraud Prediction Request");

        //5. Time to Call the Python Fraud Detection Model and get a fraud prediction!
        PythonServiceConfig pythonServiceConfig = getPythonServiceConfig("fraud_handler");
        StreamStage<JsonObject> fraudPrediction = preparePredictionRequest
                //from JsonObject to string for mapUsingPython
                .map(predictionRequest -> predictionRequest.toString())
                // Run Python Model
                .apply(mapUsingPython(pythonServiceConfig))
                .setLocalParallelism(10)
                .setName("PREDICT (Python)- Fraud Probability")
                //From String back into a JSONObject
                .map(predictionRequest -> getJsonObject(predictionRequest));

        //6. Store fraud prediction in Hazelcast Map
        StreamStage<Tuple2<String, HazelcastJsonValue>> storePredictionToMap = fraudPrediction
                .map(predictionJsonObject -> {
                    String transactionPlusCreditCardNumber = predictionJsonObject.getString("transaction_number", "") + "@" + predictionJsonObject.getString("credit_card_number", "N/A");
                    HazelcastJsonValue transactionAndFraudPrediction = new HazelcastJsonValue(predictionJsonObject.toString());
                    return Tuple2.tuple2(transactionPlusCreditCardNumber, transactionAndFraudPrediction);
                });

        //7.1 Sink to PredictionResult Map
        storePredictionToMap
                .writeTo(Sinks.map(PREDICTION_RESULT_MAP))
                .setLocalParallelism(10)
                .setName("STORE - Fraud Prediction");

        //7.2 Update streaming feature map with last transaction processed timestamp

        storePredictionToMap
                .writeTo(Sinks.mapWithEntryProcessor(STREAMING_FEATURE_MAP,
                    entry -> getJsonObject( entry.getValue()).getString("credit_card_number",""),
                    entry -> new UpdateStreamingFeatureValueProcessor("last_transaction_date", getJsonObject( entry.getValue()).getLong("timestamp_ms",-1L))))
                .setLocalParallelism(10)
                .setName("UPDATE - Last Transaction Processed date");


        return p;
    }
    private static JsonObject prepareFraudPredictionRequest(JsonObject streamingFeatures, JsonObject transaction, JsonObject merchant, JsonObject customer) {

        //transaction date
        LocalDateTime transactionDate = LocalDateTime.parse(transaction.getString("transaction_date",""), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        //calculate distance from merchant to customer billing address
        double merchantLat = transaction.getFloat("merch_lat",0);
        double merchantLon = transaction.getFloat("merch_long",0);
        double customerLat = customer.getFloat("latitude",0);
        double customerLon = customer.getFloat("longitude",0);

        //distance to 6-decimal places
        double distanceKms = Math.round(calculateDistanceKms(merchantLat, merchantLon, customerLat, customerLon) * 1000000d) / 1000000d;

        //create fraud prediction request
        JsonObject fraudPredictionRequest = new JsonObject()
                .add("transaction_number", transaction.getString("transaction_id","N/A"))
                .add("transaction_date", transaction.getString("transaction_date",""))
                .add("timestamp_ms", transaction.getLong("timestamp_ms",0))
                .add("amount", transaction.getFloat("amt",0))
                .add("merchant_lat", merchantLat)
                .add("merchant_lon", merchantLon)
                .add("merchant", transaction.getString("merchant",""))
                .add("credit_card_number", transaction.getString("cc_num",""))
                .add("customer_name", customer.getString("first","") + " " + customer.getString("last",""))
                .add("customer_city", customer.getString("city",""))
                .add("customer_age_group", customer.getString("age_group",""))
                .add("customer_gender", customer.getString("gender",""))
                .add("customer_lat", customerLat)
                .add("customer_lon", customerLon)
                .add("distance_from_home", distanceKms)
                .add("category_code", merchant.getInt("category_code",0))
                .add("transaction_weekday_code",transactionDate.getDayOfWeek().getValue()-1)
                .add("transaction_hour_code",transactionDate.getHour())
                .add("transaction_month_code",transactionDate.getMonthValue())
                .add("gender_code", customer.getInt("gender_code",0))
                .add("customer_zip_code", customer.getInt("zip_code",0))
                .add("customer_city_population", customer.getInt("city_pop",0))
                .add("customer_job_code", customer.getInt("job_code",0))
                .add("customer_age", customer.getInt("age",0))
                .add("customer_setting_code", customer.getInt("setting_code",0))
                .add("customer_age_group_code", customer.getInt("age_group_code",0))
                .add("transaction_processing_total_time", 0)
                .add("last_transaction_date",streamingFeatures.getLong("last_transaction_date",0))
                .add("transactions_last_24_hours",streamingFeatures.getInt("transactions_last_24_hours",0))
                .add("transactions_last_week",streamingFeatures.getInt("transactions_last_week",0))
                .add("amount_spent_last_24_hours",streamingFeatures.getDouble("amount_spent_last_24_hours",0));

        return  fraudPredictionRequest;
    }

    private static StreamStage<Map.Entry<Object,Object>> sourceTransactionsFromKafka(Pipeline p, Properties kafkaConsumerProperties) {
        return p.readFrom(KafkaSources.kafka(kafkaConsumerProperties,"transactions"))
                //Use transaction timestamp
                .withTimestamps(tup -> {
                    JsonObject event = new JsonObject(Json.parse(tup.getValue().toString()).asObject());
                    return event.getLong("timestamp_ms",0L);
                    }, MINUTES.toMillis(30))
                .setLocalParallelism(10);

    }
    private static void addSumStreamingFeature(StreamStage<Map.Entry<Object, Object>> streamSource, String featureName, WindowDefinition windowDefinition, String streamingFeatureMapName, String fieldToSum) {

        StreamStage<KeyedWindowResult<Object, Double>> featureAggregation = streamSource
                .groupingKey(Map.Entry::getKey)
                .window(windowDefinition)
                .aggregate(summingDouble(input -> {
                    return (new JsonObject(Json.parse(input.getValue().toString()).asObject()).getDouble(fieldToSum, 0));
                }))
                .setLocalParallelism(10);;

        //Update "streaming-features" map with sum amt at every window -- AFTER (with EntryProcessor)
        featureAggregation
                .writeTo(Sinks.mapWithEntryProcessor(streamingFeatureMapName,
                    entry -> entry.getKey().toString(),
                    entry -> new UpdateStreamingFeatureValueProcessor(featureName, entry.getValue().doubleValue())))
                .setLocalParallelism(10)
                .setName("Update " + featureName);
    }
    private static void addCountStreamingFeature (StreamStage<Map.Entry<Object, Object>> streamSource, String featureName, WindowDefinition windowDefinition, AggregateOperation1 aggregateOperation, String streamingFeatureMapName) {
        //Count events over a window of time
        StreamStage<KeyedWindowResult<Object, Long>> featureAggregation = streamSource
                .groupingKey(Map.Entry::getKey)
                .window(windowDefinition)
                .aggregate(aggregateOperation)
                .setLocalParallelism(10);


        // Update "streaming-features" map with count at every window - (AFTER With Entry Processor)
        featureAggregation
                .writeTo(Sinks.mapWithEntryProcessor(streamingFeatureMapName,
                        entry -> entry.getKey().toString(),
                        entry -> new UpdateStreamingFeatureValueProcessor(featureName, entry.getValue().longValue())))
                .setLocalParallelism(10)
                .setName("Update: " + featureName);
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
        props.setProperty("auto.offset.reset","earliest");
        return props;
    }
    private static JsonObject getJsonObject(HazelcastJsonValue existingValue) {
        return (existingValue==null) ? new JsonObject() : new JsonObject(Json.parse(existingValue.toString()).asObject()) ;
    }
    private static JsonObject getJsonObject(String jsonString) {
        return (jsonString==null) ? new JsonObject() : new JsonObject(Json.parse(jsonString).asObject()) ;
    }
    private static JobConfig getConfig(String jobName) {
        JobConfig cfg = new JobConfig().setName(jobName);
        cfg.addClass(StreamingFeatures.class);
        cfg.addClass(UpdateStreamingFeatureValueProcessor.class);
        return cfg;
    }
    private static Double calculateDistanceKms(double lat1, double long1, double lat2, double long2) {
        return org.apache.lucene.util.SloppyMath.haversinMeters(lat1, long1, lat2, long2) / 1_000;
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
    protected static PythonServiceConfig getPythonServiceConfig(String name) throws Exception {
        File temporaryDir = getTemporaryDir(name);

        PythonServiceConfig pythonServiceConfig = new PythonServiceConfig();
        pythonServiceConfig.setBaseDir(temporaryDir.toString());
        pythonServiceConfig.setHandlerModule(name);
        pythonServiceConfig.setHandlerFunction("transform_list");
        return pythonServiceConfig;
    }
    private static File getTemporaryDir(String name) throws Exception {
        Path targetDirectory = Files.createTempDirectory(name);
        targetDirectory.toFile().deleteOnExit();

        // These files will be copied over to the python environment created by hazelcast
        String[] resourcesToCopy = { name + ".py", "sf_fraud_prediction_hazelcast.model","requirements.txt"};
        for (String resourceToCopy : resourcesToCopy) {
            try (InputStream inputStream = StreamingFeatures.class.getResourceAsStream(resourceToCopy)) {
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
    private static HazelcastInstance getHazelClient(String hazelcastClusterMemberAddresses)  {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("dev");
        clientConfig.getNetworkConfig()
                .addAddress(hazelcastClusterMemberAddresses);
                //.setSmartRouting(false);
        //Start the client
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        System.out.println("Connected to Hazelcast Cluster");
        return client;
    }
}