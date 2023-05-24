# The Fraud Analytics Dashboard

The Fraud Analytics Dashboard was written entirely in Python using:
* Streamlit - Data Visualization
* Hazelcast Python client - to run SQL queries against the data stored in Hazelcast in the "PredictionResult" map

![Fraud dashboard](../images/fraud-dashboard.png)

The Streamlit app is written in [app.py](./app.py). The code is relatively straighforward. 

The key parts are:
* Connecting to a Hazelcast server - See `def get_hazelcast_client(cluster_members=['127.0.0.1'])`
* Creating a SQL Schema (aka as a Hazelcast Mapping) so that the client is able to run SQL Queries against data inside a Hazelcast map. See the same method  `def get_hazelcast_client(cluster_members=['127.0.0.1'])`
* Runnning SQL queries against Hazelcast and transforming the results into a Pandas dataframe format
    * See the  `def get_df(_client, sql_statement, date_cols):` method
* Plotting SQL data returned from Hazelcast using Streamlit

## The Fraud Analytics Dashboard image

The [`Dockerfile`](./Dockerfile) contains the instructions to package the Streamlit app into a Docker image

The image can be (re-)built by running
```
docker-compose -f fraud-dashboard-image.yml build
```

Please note that [fraud-dashbaord-image.yml](./fraud-dashboard-image.yml) specifies the `linux/amd64` architecture as target.
You should be able to build and run this image even if you are developing on a different architecture. This image was built using an M1-powered MacBook

Next, you will need to tag and push your newly rebuilt image to a docker image repository (like Dockerhub)
```
docker tag fraud-dashboard-fraud-dashboard <yourdockerhubaccount>/python-data-loader
docker push<yourdockerhubaccount>/python-data-loader
```

Finally, You will need to update the data-loader deployment manifest file [hz-pod deployment file](../hz-pods.yaml)

Replace `image: docker.io/edsandovalhz/python-sql-fraud-dashboard` with your newly built image 

Redeploy/Update the Fraud Dashboard image by applying its Kubernetes deployment manifest with
```
cd ..
kubectl apply -f hz-pods.yaml
```





