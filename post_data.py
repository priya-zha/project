import requests
import random
import string
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
# 1from pykafka import KafkaClient

from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
from json import JSONEncoder
# 2client = KafkaClient(hosts="127.0.0.1:9092")
#t  # 3opic = client.topics['hospital']
# 4producer = topic.get_sync_producer()
driver = webdriver.Firefox()

url = "http://127.0.0.1:8000"
driver.get(url)


producer = KafkaProducer(bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))


# 5class setEncoder(JSONEncoder):
# 6      def default(self, obj):
# 7        return list(obj)

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


for i in range(300):
    name = randomString()  # + " " + randomString()
    age = round(random.random()*100)
    sex_condition = round(random.random()*2)
    RBC_Count = round(random.random()*5000)
    Platelets = round(random.random()*2000)
    Neutrofils = round(random.random()*9000)
    Basofils = round(random.random()*1200)
    glucose = round(random.random()*200)
    bloodpressure = round(random.random()*100)
    skinthickness = round(random.random()*50)
    insulin = round(random.random()*100)
    bmi = round(random.random()*100)
    max_heart_rate = round(random.random()*120)
    marital_status_condition = round(random.random()*1)
    cholestrol = round(random.random()*100)

    sex = 'male'
    marital_status = 'yes'

    if sex_condition == 1:
        sex = 'female'

    elif sex_condition == 2:
        sex = 'other'

    if marital_status_condition:
        marital_status = 'no'

    # values = {'name':name,
    #         'age':age,
    #         'sex':sex,
    #         'RBC_Count':RBC_Count,
    #         'Platelets':Platelets,
    #         'Neutrofils':Neutrofils,
    #         'Basofils':Basofils,
    #         'glucose':glucose,
    #         'bloodpressure':bloodpressure,
    #         'skinthickness':skinthickness,
    #         'insulin':insulin,
    #         'bmi':bmi,
    #         'max_heart_rate':max_heart_rate,
    #         'marital_status':marital_status,
    #         'cholestrol':cholestrol,
    #         'submitButton':'Submit',
    #         }

    name_box = driver.find_element_by_id('name')
    age_box = driver.find_element_by_id('age')
    male_box = driver.find_element_by_id('male')
    female_box = driver.find_element_by_id('female')
    other_box = driver.find_element_by_id('other')
    RBC_Count_box = driver.find_element_by_id('RBC_Count')
    Platelets_box = driver.find_element_by_id('Platelets')
    Neutrofils_box = driver.find_element_by_id('Neutrofils')
    Basofils_box = driver.find_element_by_id('Basofils')
    glucose_box = driver.find_element_by_id('glucose')
    bloodpressure_box = driver.find_element_by_id('bloodpressure')
    skinthickness_box = driver.find_element_by_id('skinthickness')
    insulin_box = driver.find_element_by_id('insulin')
    bmi_box = driver.find_element_by_id('bmi')
    max_heart_rate_box = driver.find_element_by_id('max_heart_rate')
    # marital_status_box = driver.find_element_by_id('marital_status')
    marital_status_yes_box = driver.find_element_by_id('yes')
    marital_status_no_box = driver.find_element_by_id('no')
    cholestrol_box = driver.find_element_by_id('cholestrol')

    # name_box = driver.find_element_by_id('name')
    # age_box = driver.find_element_by_id('age')
    # male_box = driver.find_element_by_id('male')
    # female_box = driver.find_element_by_id('female')
    # other_box = driver.find_element_by_id('other')
    # RBC_Count_box = driver.find_element_by_id('RBC_Count')
    # Platelets_box = driver.find_element_by_id('Platelets')
    # Neutrofils_box = driver.find_element_by_id('Neutrofils')
    # Basofils_box = driver.find_element_by_id('Basofils')
    # glucose_box = driver.find_element_by_id('glucose')
    # bloodpressure_box = driver.find_element_by_id('bloodpressure')
    # skinthickness_box = driver.find_element_by_id('skinthickness')
    # insulin_box = driver.find_element_by_id('insulin')
    # bmi_box = driver.find_element_by_id('bmi')
    # max_heart_rate_box = driver.find_element_by_id('max_heart_rate')
    # # marital_status_box = driver.find_element_by_id('marital_status')
    # marital_status_yes_box = driver.find_element_by_id('yes')
    # marital_status_no_box = driver.find_element_by_id('no')
    # cholestrol_box = driver.find_element_by_id('cholestrol')

    # r = requests.post(url, params = values)

    button = driver.find_element_by_id('submitButton')

    # message = ('name:'+ name+'\tage:'+str(age)+'\tsex:'+str(sex_condition)+'\tRBC_Count:'+str(RBC_Count)+'\tPlatelets:'+str(Platelets)+'\tNeutrofils:'+str(Neutrofils)+'\tBasofils'+str(Basofils)
    #+'\tglucose:'+str(glucose)+'\tbloodpressure:'+str(bloodpressure)+'\tskinthickness'+str(skinthickness))

    # 8message = ('name:'+ name,'age:'+str(age),'sex:'+str(sex_condition),'RBC_Count:'+str(RBC_Count),'Platelets:'+str(Platelets),'Neutrofils:'+str(Neutrofils),'Basofils:'+str(Basofils)
    # 9	,'glucose:'+str(glucose),'bloodpressure:'+str(bloodpressure),'skinthickness'+str(skinthickness))
    # producer.produce(str.encode(message))
    # 10jsonData = json.dumps(message, indent=4, cls=setEncoder,sort_keys=True)
    #setObj = json.loads(message)
    # jj=json.dumps(setObj,indent=4,sort_keys=True)
    # 11producer.produce (str.encode(jsonData))


# for e in range(1000):
    #data = {'number' : e,'any' :1}
    data = {'name': name,'age':age,'sex':sex_condition,'RBC_Count':RBC_Count,'Platelets':Platelets,'Neutrofils':Neutrofils,'Basofils':Basofils,'glucose':glucose,'bloodpressure':bloodpressure,'skinthickness':skinthickness}
    producer.send('hospital', value=data)
    sleep(5)

    name_box.send_keys(name)
    age_box.send_keys(age)
    # sex_box.send_keys(sex)
    if sex_condition == 0:
        male_box.click()
    elif sex_condition == 1:
        female_box.click()
    else:
        other_box.click()

    RBC_Count_box.send_keys(RBC_Count)
    Platelets_box.send_keys(Platelets)
    Neutrofils_box.send_keys(Neutrofils)
    Basofils_box.send_keys(Basofils)
    glucose_box.send_keys(glucose)
    bloodpressure_box.send_keys(bloodpressure)
    skinthickness_box.send_keys(skinthickness)
    insulin_box.send_keys(insulin)
    bmi_box.send_keys(bmi)
    max_heart_rate_box.send_keys(max_heart_rate)
    # marital_status_box.send_keys(marital_status)
    if marital_status_condition == 0:
        marital_status_yes_box.click()
    else:
        marital_status_no_box.click()

    cholestrol_box.send_keys(cholestrol)

    button.click()

driver.close()


# In case you want to see the see the output html page use the code below.

html = r.content
