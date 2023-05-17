import json
import datetime
import lightgbm as lgbm

#load model from disk
fraud_model = lgbm.Booster(model_file='lgbm_model_no_merchant_cc_num')

FEATURE_NAMES = ['category_code','amount','transaction_weekday_code','transaction_month_code','transaction_hour_code','gender_code',
    'customer_zip_code','customer_city_population','customer_job_code','customer_age','customer_setting_code','customer_age_group_code','distance_from_home']

def request_to_model_input(fr):
    result = []
    for feature in FEATURE_NAMES:
        result.append(fr[feature])
    return result


def transform_list(input_list):
    start = datetime.datetime.now().microsecond
    #Deserialize incoming requests as JSON Objects
    fraud_requests = [ json.loads(request) for request in input_list]

    #Extract from each request the inputs required by the model
    model_inputs=[]
    for fr in fraud_requests:
        model_input = request_to_model_input(fr)
        model_inputs.append(model_input)

    #Generate predictions with previously trained LightGBM Model
    fraud_predictions =  fraud_model.predict(model_inputs)

    #Get Final Results - Zip requests and predictions
    final_results = []
    for (req, prediction) in zip (fraud_requests,fraud_predictions):
        req['fraud_probability'] = prediction
        req['fraud_model_prediction'] = 1 if prediction > 0.5 else 0
        req['inference_time_ns'] = (datetime.datetime.now().microsecond - start) * 1000
        final_results.append(req)

    #Return predictions as Strings (containing a JSON Object)
    fraud_responses = [json.dumps(r) for r in final_results]
    return fraud_responses



