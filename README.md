# Before you start
Make sure you have
* A Google Cloud Kubernetes Engine (GKE) cluster with 4 nodes. Each with:
    * 16 VCPU cores and 
    * 16 GB memory
* Your Google Cloud user has:
    * The `Owner` Role in the Google Cloud Project your GKE cluster is running on OR
    * A role containing the `container.clusterRoleBindings.create` permission.  This perms is included in `Kubernetes Engine Admin`


* [Kubectl](https://kubernetes.io/docs/tasks/tools/)
* [gcloud](https://cloud.google.com/sdk/docs/install)
* [Helm](https://helm.sh/docs/intro/install/) 
* [Hazelcast CLI tool 5.3.1](https://docs.hazelcast.com/hazelcast/5.2/getting-started/install-hazelcast#using-the-binary)


# Fraud Detection With Hazelcast 
In this demo, you will deploy a Real-time Fraud Detection Solution to Hazelcast. 

![Real-time Fraud Detection Demo Set up](./images/set-up.png)

The main components are:
* A 3-node Hazelcast cluster storing fake customer and merchant data (in memory) and running a fraud detection model (written in Python using the LightGBM framework)
* A customer and merchant data loader program (written in Python) showing how to load data into Hazelcast (distributed in-memory data store)
* A client Java program to define and submit the real-time inference pipeline to Hazelcast. This pipeline defines a sequence of steps to determine if an incoming transaction is potentially fraudulent. 
* A Python transaction loader program similuating transactions being sent to Hazelcast
* Hazelcast Management Center
* A Fraud Analytics dashboard showing transactions and fraud predictions

## Fraud Analytics Dashboard
By the end of this demo, you will be able to visualize fraud predictions. This dashboard was built in Python and uses SQL to retrieve data from Hazelcast

![Fraud dashboard](./images/fraud-dashboard.png)

# Let's Get Started - Clone this Repo
```
git clone https://github.com/hazelcast/fraud-detection-python.git
cd fraud-detection-python
```

NOTE: If your clone command fails, set up a Github Personal Access token as [described here](https://www.shanebart.com/clone-repo-using-token/). 

Ensure you set a reasonable expiry date and set scope to: repo, user, admin:public_key



# STEP 1: Set up Your Kubernetes Cluster 


Make sure you have a Kubernetes cluster and your `kubectl` command pointing to it. 

We will use Google Kubernetes Engine (GKE) in this setup but you could use another Kubernetes provider. 

In GKE, create a cluster named `hz-fraud-detection-python`. Ensure your cluster has 5 nodes with at least 8 VCPUs and 8GB memory each.
You can use this [video as guide to create your GKE Cluster](https://youtu.be/hxpGC19PzwI?t=395)

Once created, you can point `kubectl` to it by running
```
gcloud container clusters get-credentials hz-fraud-detection-python --zone europe-west2-a --project <your-gke-project>
```

## Deploy a 3-node Hazelcast Cluster, Management Center, Fraud Dashboard & Data loader PODS
First, prepare your Kubernetes cluster with
```
kubectl apply -f https://raw.githubusercontent.com/hazelcast/hazelcast-kubernetes/master/rbac.yaml
helm repo add hazelcast https://hazelcast-charts.s3.amazonaws.com/
helm repo update
```

Finally, deploy all components with
```
helm install -f values.yaml hz-python hazelcast/hazelcast && kubectl apply -f hz-pods.yaml 
```

Wait 3-5 minutes and **ALL 6 PODS** should be **RUNNING**
```
kubectl get pods
```
The output should be similar to
![kubectl get pods](./images/pods.png)


# STEP 2: Load Customer and Merchant data to Hazelcast (In-Memory data store)
Open a second Terminal window

Identify your "data-loader" pod name. Check the output of `kubectl get pods`
In the above case, it is `data-loader-6ccbb8b88b-7sjvq`

Open a Terminal to this pod 
```
kubectl exec --stdin --tty data-loader-6ccbb8b88b-7sjvq -- /bin/bash
```
Within that terminal run
```
python feature-data-loader.py
```
The output should confirm that Customer and Merchant data is now stored in Hazelcast
![kubectl get pods](./images/data-load.png)

# STEP 3: Submit Real-time Inference Pipeline to Hazelcast
Go back to your initial Terminal window. Make sure to keep the Data loader Terminal in a separate window/tab

Let's grab the Hazelcast endpoint to our cluster, run
```
kubectl get services
```
You should see the following SERVICES available
![kubectl get services](./images/services.png)

Make a note of the EXTERNAL-IP for your Hazelcast cluster. Look for the `hz-python-hazelcast` service. In this example, it is `34.89.10.163`

## 3.1 MAC & Linux Users
Set an environment variable
```
export HZ_ENDPOINT=<your-hz-python--service-external-ip>:5701
```

Now you can deploy the real-time inference pipeline by running
```
cd deploy-jobs
```
Folowed by
```
hz-cli submit -t $HZ_ENDPOINT -v -c org.example.Main target/deploy-jobs-1.0-SNAPSHOT.jar 
```

## 3.2  Windows Users!
> :warning: **Windows users, run this hz-cli command instead**: Replace `your-hz-python--service-external-ip` with the above EXTERNAL-IP 
> address for the `hz-python-hazelcast` service

First cd to the deploy-jobs directory
```
cd deploy-jobs
```

```
docker run \
    -v "$(pwd)"/target/deploy-jobs-1.0-SNAPSHOT.jar:/usr/lib/hazelcast/deploy-jobs-1.0-SNAPSHOT.jar \
    -e HZ_ENDPOINT=<your-hz-python--service-external-ip>:5701 
    --rm edsandovalhz/hz-531-python-310 \
    /usr/lib/hazelcast/bin/hz-cli submit -t <your-hz-python--service-external-ip>:5701 -c org.example.Main /usr/lib/hazelcast/deploy-jobs-1.0-SNAPSHOT.jar
```


## What is the Real-time Inference pipeline doing?
Your inference pipeline has now been deployed to Hazelcast. But wait, what is this pipeline doing? 

The picture below illustrates what this real-time pipeline is automating
![Realtime fraud detection pipeline: behind the scenes](./images/pipeline.png)


Broadly speaking, the pipeline is processing incoming transactions. The steps:
* **Ingest** - placing new transactions in the "transaction" map (in-memory distributed data structure in Hazelcast) triggers the execution of this pipeline
* **Enrich** - Using credit card number and merchant code on the incoming transaction, it looks up data in the "customer" and "merchant" maps. This information was previosuly loaded to Hazelcast in-memory data store (in step 2)
* **Transform** - Calculates the 'Distance from home' feature using location reported in the transaction and customer billing address stored (which is available on the "customer" map)
* **Predict** - Runs the LightGBM model passing the required input data (transformed in the format required by the model)
* **Act** - Stores the fraud probability returned by the model, along with the transaction data in the `predictionResult` MAP (Hazelcast in-memory) for real-time analytics


You can find a full description of the [inference pipeline here](./inference-pipeline.md)



# STEP 4: Time to fire some transactions into your Hazelcast inference pipeline!
Go back to your Data loader Terminal window
```
python transaction-data-loader.py data/transaction-stream-full.csv 4 5000
```
Note: This command will split the load into 4 processes. Each process will print out a message for every 5000 transactions loaded

# STEP 5: Monitor your Inference Pipeline in Management Center
Go back to your main Terminal Window

Let's grab your management center IP address
```
kubectl get services
```
You should see the following SERVICES available
![kubectl get services](./images/services.png)

Make a note of the EXTERNAL-IP for your management center. Look for the `hz-python-hazelcast-mancenter` service. 
In this example, it is `34.89.44.29:8080`

Open a Browser to this location

Navigate to Streaming->Jobs. 

Your Inference pipeline should be processsing transactions
![Man center showing real-time inference pipeline job running](./images/man-center.png)

# STEP 6: Let's Visualize transactions and their fraud predictions
Go back your main terminal window

Make a note of the EXTERNAL-IP for your fraud dashboard service. Look for the `fraud-dashboard` service. 
In this example, it is `34.105.167.221:8501`

Open a Browser to this location
![Fraud dashboard](./images/fraud-dashboard.png)

Play around with the Analyst - SQL Playground
You can enter a SQL Statement like
```
SELECT * 
FROM predictionResult 
where fraud_probability > 0.7 and customer_name='Carol Serrano'
LIMIT 200
```
Press CTRL+ENTER to execute the SQL

Enjoy!

# Gracefully Terminate your Kubernetes Deployment
Go back to you main Terminal
```
helm delete hz-python && kubectl delete -f hz-pods.yaml
```

Finally, 
# Don't forget to DELETE Your Kubernetes cluster
to avoid unnecesary GKE/Cloud bills!


# WANT TO LEARN MORE?

## How was the Feature and Transaction Data Loaded into Hazelcast?

Using Hazelcast Python client. [More details here](./data-loader/README.md)

## How was the Fraud Dashboard built?

With Streamlit as data visualization and Hazelcast Python issuing SQL queries. [More details here](./fraud-dashboard/README.md)

## How was the LightGBM model trained? 

Using a fictional credit card transaction dataset and the LightGBM framework. [Check out this Google Colab Notebook](https://colab.research.google.com/drive/1x_j_9tZGwH__ZsdO7ECMWEY3niBuvQUG?usp=sharing)
The notebook describes most of the training process. You will notice that 
* It only uses 50% of the original dataset for training and 
* it drops a few features 

This was done so you can train a similar model for free on Google Colab. 

When you execute all Cells in the notebook, you can download the trained model
![Download trained Model from Colab](./images/download-model.png)

## How was the LightGBM model used to score transactions in a Hazelcast?

Using Hazelcast's Pipeline API and the `MapUsingPython` function.

The `MapUsingPython` function allows to run Python code on a Hazelcast cluster

This function can only be used in a Hazelcast Pipeline. See [more details here](./inference-pipeline.md)

## Can I run this demo on my local machine (eg. No Kubernetes)?

Yes, You will need Docker and a machine at least 10 CPU cores and 32GB RAM

See step-by-step [instructions here](./run-demo-locally.md)








