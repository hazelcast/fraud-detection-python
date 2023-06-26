import hazelcast

class Merchant():
    

    def __init__(self, name=None, category=None, merchant_code=None, category_code=None):
        self.name = name
        self.category = category
        self.code = merchant_code
        self.category_code = category_code
        
    
    def __repr__(self):
        return "Merchant(name=%s, category=%s, code=%i, category_code=%i)" % (self.name, self.category, self.code, self.category_code)
