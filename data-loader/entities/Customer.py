import hazelcast

from hazelcast.serialization.api import Portable

class Customer(Portable):
    CLASS_ID = 2
    FACTORY_ID = 2

    def __init__(self, cc_num=None,first=None,last=None,gender=None,city=None,state=None,zip=None,latitude=None,longitude=None,
                 city_pop=None,job=None,profile=None,age=None,setting=None,age_group=None,
                 age_group_code=None,cc_code=None,gender_code=None,job_code=None,setting_code=None,zip_code=None):
        self.cc_num = int(cc_num)
        self.first = first
        self.last = last
        self.gender = gender
        self.city = city
        self.state = state
        self.zip = zip
        self.latitude = latitude
        self.longitude = longitude
        self.city_pop = city_pop
        self.job = job
        self.profile = profile
        self.age = age
        self.setting = setting
        self.age_group = age_group
        self.age_group_code = age_group_code
        self.cc_code = cc_code
        self.gender_code = gender_code
        self.job_code = job_code
        self.setting_code = setting_code
        self.zip_code = zip_code


        
    def read_portable(self, reader):
        self.cc_num = reader.read_long("cc_num")
        self.first = reader.read_string("first")
        self.last = reader.read_string("last")
        self.gender = reader.read_string("gender")
        self.city = reader.read_string("city")
        self.state = reader.read_string("state")
        self.zip = reader.read_string("zip")
        self.latitude = reader.read_float("latitude")
        self.longitude = reader.read_float("longitude")
        self.city_pop = reader.read_int("city_pop")
        self.job = reader.read_string("job")
        self.profile = reader.read_string("profile")
        self.age = reader.read_int("age")
        self.setting = reader.read_string("setting")
        self.age_group = reader.read_string("age_group")
        self.age_group_code = reader.read_int("age_group_code")
        self.cc_code = reader.read_int("cc_code")
        self.gender_code = reader.read_int("gender_code")
        self.job_code = reader.read_int("job_code")
        self.setting_code = reader.read_int("setting_code")
        self.zip_code = reader.read_int("zip_code")

    def write_portable(self, writer):
        writer.write_long("cc_num", self.cc_num)
        writer.write_string("first", self.first)
        writer.write_string("last", self.last)
        writer.write_string("gender", self.gender)
        writer.write_string("city", self.city)
        writer.write_string("state", self.state)
        writer.write_string("zip", self.zip)
        writer.write_float("latitude", self.latitude)
        writer.write_float("longitude", self.longitude)
        writer.write_int("city_pop", self.city_pop)
        writer.write_string("job", self.job)
        writer.write_string("profile", self.profile)
        writer.write_int("age", self.age)
        writer.write_string("setting", self.setting)
        writer.write_string("age_group", self.age_group)
        writer.write_int("age_group_code", self.age_group_code)
        writer.write_int("cc_code", self.cc_code)
        writer.write_int("gender_code", self.gender_code)
        writer.write_int("job_code", self.job_code)
        writer.write_int("setting_code", self.setting_code)
        writer.write_int("zip_code", self.zip_code)
        


    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return "Customer(cc_num=%s, category=%s %s)" % (self.cc_num, self.first,self.last)
