# Loading Customer, Merchant and Transaction Data into Hazelcast

The data-loader image in the main demo ships with 2 Python programs and data sent to Hazelcast

The Python data loader programs are:
* [feature-data-loader.py](./feature-data-loader.py) - Uses the Hazelcast Python client to load fake Customer and Merchant data into Hazelcast Maps. The Customer data is loaded into a standard Hazelcast Map ("customers") while the merchant data is loaded into a Hazelcast replicated map ("merchants").

A Hazelcast map is a distributed data structure where data is partitioned across members of the Hazelcast cluster. A single Hazelcast cluster member stores only a fraction (partition) of the entire data.

A Hazelcast replicated map is a distributed data structure that allows the same data to be kept in ALL Hazelcast cluster members.

* [transaction-data-loader.py](./transaction-data-loader.py) - Uses the Hazelcast Python client to load fake transaction details into a Hazelcast map named "transactions"

Both Python data loading programs use data in csv & JSON files within the **data** folder

Both Python programs expect an environment variable `HZ_ENDPOINT` to be set. This variable should be something like 54.3.22.111:5701. (<hazelcastIPAddress:port>)


## The Feature Data Loader image

The [`Dockerfile`](./Dockerfile) contains the instructions to package the data loading programs and their CSV/JSON data into a Docker image

The image can be (re-)built by running
```
docker-compose -f data-loader-image.yml build
```

Please note that [data-loader-image.yml](./data-loader-image.yml) specifies the `linux/amd64` architecture as target.
You should be able to build and run this image even if you are developing on a different architecture. This image was built using an M1-powered MacBook

Next, you will need to tag and push your newly rebuilt image to a docker image repository (like Dockerhub)
```
docker tag data-loader-data-loader <yourdockerhubaccount>/python-data-loader
docker push<yourdockerhubaccount>/python-data-loader
```

Finally, You will need to update the data-loader deployment manifest file [hz-pod deployment file](../hz-pods.yaml)

Replace `image: docker.io/edsandovalhz/python-data-loader` with your newly built image 

Redeploy/Update the manifest with
```
cd ..
kubectl apply -f hz-pods.yaml
```

## The Data Files
All customer, merchant and transaction files are stored under the [data](./data/) folder

EXCEPT the `transaction-stream-full.csv` file as it is over 300MB in size

For your convenience, this file can be [downloaded here](https://storage.googleapis.com/fraud-demo-data/data/transaction-stream-full.csv)



