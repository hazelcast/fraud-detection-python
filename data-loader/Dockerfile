FROM python:3.11.4-slim

WORKDIR /app

#install python dependencies and main programs
COPY feature-data-loader.py ./feature-data-loader.py
COPY transaction-data-loader.py ./transaction-data-loader.py
COPY requirements.txt ./requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

#Copy Customer and Merchant Python Classes
RUN mkdir /app/entities
COPY entities/__init__.py ./entities/__init__.py
COPY entities/Customer.py ./entities/Customer.py
COPY entities/Merchant.py ./entities/Merchant.py

#skip Serializers
#COPY entities/CustomerSerializer.py ./entities/CustomerSerializer.py
#COPY entities/MerchantSerializer.py ./entities/MerchantSerializer.py

#Copy Customer and Merchant data files
RUN mkdir /app/data
COPY data/customer_data.csv ./data/customer_data.csv
COPY data/merchant_data.csv ./data/merchant_data.csv


#Files mapping Customer and Merchant categorical features to integer values
COPY data/age_group_dict.json ./data/age_group_dict.json
COPY data/category_dict.json ./data/category_dict.json
COPY data/cc_num_dict.json ./data/cc_num_dict.json
COPY data/gender_dict.json ./data/gender_dict.json
COPY data/job_dict.json ./data/job_dict.json
COPY data/merchant_dict.json ./data/merchant_dict.json
COPY data/setting_dict.json ./data/setting_dict.json
COPY data/zip_dict.json ./data/zip_dict.json

#COPY transaction data
COPY data/test.csv ./data/test.csv
COPY data/transaction-stream-full.csv ./data/transaction-stream-full.csv



CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"