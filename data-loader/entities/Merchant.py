import hazelcast

from hazelcast.serialization.api import Portable

class Merchant(Portable):
    CLASS_ID = 1
    FACTORY_ID = 1

    def __init__(self, name=None, category=None, merchant_code=None, category_code=None):
        self.name = name
        self.category = category
        self.code = merchant_code
        self.category_code = category_code
        
    def read_portable(self, reader):
        self.name = reader.read_string("name")
        self.category = reader.read_string("category")
        self.code = reader.read_int("code")
        self.category_code = reader.read_int("category_code")

    def write_portable(self, writer):
        writer.write_string("name", self.name)
        writer.write_string("category", self.category)
        writer.write_int("code",self.code)
        writer.write_int("category_code",self.category_code)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return "Merchant(name=%s, category=%s, code=%i, category_code=%i)" % (self.name, self.category, self.code, self.category_code)
