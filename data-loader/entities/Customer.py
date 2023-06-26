import hazelcast

class Customer():

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
        