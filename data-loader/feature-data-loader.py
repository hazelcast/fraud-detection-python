import hazelcast
import csv
import os
import json
from hazelcast.serialization.api import Portable
from hazelcast import HazelcastClient
from hazelcast.discovery import HazelcastCloudDiscovery
from entities.Customer import Customer
from entities.Merchant import Merchant

def load_dictionary(json_file):
    with open (json_file) as f:
        dictionary = json.load(f)
        return dictionary

def load_merchants(merchant_file,merchant_codes_file,merchant_category_codes_file): 

    merchant_codes = load_dictionary(merchant_codes_file)
    merchant_category_codes = load_dictionary(merchant_category_codes_file)
    
    merchants = []
    with open(merchant_file,mode= 'r',encoding='utf-8') as f:
        # Create a CSV reader object
        reader = csv.reader(f,quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True,doublequote=True)
        # Skip the header 
        next(reader)
        for row in reader:
            ##row - category(0),merchant_name(1)
            merchant_code = merchant_codes[row[1]]
            merchant_category_code = merchant_category_codes[row[0]]
            merchant = Merchant(row[1],row[0],merchant_code,merchant_category_code)
            merchants.append(merchant)
    return merchants

def load_customers(customer_file, category_mapping_files):

    age_group_code_file =  category_mapping_files['age_group_codes']
    age_group_codes = load_dictionary(age_group_code_file)

    cc_code_file = category_mapping_files['cc_codes']
    cc_codes = load_dictionary(cc_code_file)

    gender_code_file =  category_mapping_files['gender_codes']
    gender_codes = load_dictionary(gender_code_file)

    job_code_file =  category_mapping_files['job_codes']
    job_codes = load_dictionary(job_code_file)

    setting_code_file =  category_mapping_files['setting_codes']
    setting_codes = load_dictionary(setting_code_file)

    zip_code_file =  category_mapping_files['zip_codes']
    zip_codes = load_dictionary(zip_code_file)

    #read file and augment with codes for categorical vars
    customers = []
    with open(customer_file,mode= 'r',encoding='utf-8') as f:
        # Create a CSV reader object
        reader = csv.reader(f,quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL, skipinitialspace=True,doublequote=True)
        # Skip the header row
        #ssn(0),cc_num(1),first(2),last(3),gender(4),street,city(6),state(7),zip(8),latitude(9),longitude(10),city_pop(11),job(12),dob,acct_num,profile(15),age(16),setting(17),age_group(18)
        next(reader)
        for row in reader:
            #get codes for categorical vars
            age_group_code = age_group_codes[row[18]]
            cc_code = cc_codes[row[1]]
            gender_code = gender_codes[row[4]]
            job_code = job_codes[row[12]]
            setting_code = setting_codes[row[17]]
            zip_code = zip_codes[row[8]]
            #Create Customer instannce
            customer = Customer(row[1],row[2],row[3],row[4],row[6],row[7],row[8],float(row[9]),float(row[10]),int(row[11]),row[12],row[15],int(row[16]),row[17],row[18],
                                age_group_code,cc_code,gender_code,job_code,setting_code,zip_code)
            customers.append(customer)
    return customers

def get_client():
    cluster_member_string = os.environ.get('HZ_ENDPOINT', '127.0.0.1')
    cluster_members = cluster_member_string.split(",")
    print(cluster_members)
    client = hazelcast.HazelcastClient(cluster_members=cluster_members,
        use_public_ip=True,
        portable_factories={
            Merchant.FACTORY_ID: {Merchant.CLASS_ID: Merchant},
            Customer.FACTORY_ID: {Customer.CLASS_ID: Customer}
            }
    )
    return client

print("starting hazelcast client")
client = get_client()
print("Connection Successful!")

#load merchants
merchant_map = client.get_replicated_map('merchants')
merchants = load_merchants('./data/merchant_data.csv','./data/merchant_dict.json','./data/category_dict.json')
for m in merchants:
    merchant_map.put(m.name, m)

#load customers
customer_map = client.get_map("customers")
category_mapping_files = dict()
category_mapping_files['age_group_codes'] = './data/age_group_dict.json'
category_mapping_files['cc_codes'] = './data/cc_num_dict.json'
category_mapping_files['gender_codes'] = './data/gender_dict.json'
category_mapping_files['job_codes'] = './data/job_dict.json'
category_mapping_files['setting_codes'] = './data/setting_dict.json'
category_mapping_files['zip_codes'] = './data/zip_dict.json'
customers = load_customers('./data/customer_data.csv',category_mapping_files)
for customer in customers:
    customer_map.put(str(customer.cc_num),customer)
#Doble check customer and merchant data is on Hazelcast Maps
merchant_map = client.get_replicated_map("merchants").blocking()
num_merchants = merchant_map.size()
print(f' Loaded {num_merchants} Merchants')
customer_map = client.get_map("customers").blocking()
num_customers = customer_map.size()
print(f' Loaded {num_customers} Customers')

client.shutdown()