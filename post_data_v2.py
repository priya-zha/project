import requests
import random
import string
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.keys import Keys
from time import sleep


opts = FirefoxOptions()
opts.add_argument("--headless")
driver = webdriver.Firefox(firefox_options=opts)

url = "http://127.0.0.1:8000/collect/"
driver.get(url)

my_file = open("districts.csv", "r")
content_list = my_file.read().splitlines()[1:]

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


for i in range(300):
    name = randomString()  # + " " + randomString()
    age = 21 + round(random.random()*60)
    glucose = 44 + round(random.random()*155)
    bloodpressure = 24 + round(random.random()*98)
    skinthickness = 7 + round(random.random()*103)
    insulin = 14 + round(random.random()*730)
    bmi = 18 + round(random.random()*62)
    pregnancy = round(random.random()*17)
    diabetes_pdf = int(0.078 + random.random()*2.342)
    location = random.choice(content_list)

    name_box = driver.find_element_by_id('name')
    age_box = driver.find_element_by_id('age')
    glucose_box = driver.find_element_by_id('glucose')
    bloodpressure_box = driver.find_element_by_id('bloodpressure')
    skinthickness_box = driver.find_element_by_id('skinthickness')
    insulin_box = driver.find_element_by_id('insulin')
    bmi_box = driver.find_element_by_id('bmi')
    pregnancy_box = driver.find_element_by_id('pregnancy')
    diabetes_pdf_box = driver.find_element_by_id('diabetes_pdf')
    location_box = driver.find_element_by_id('location')

    button = driver.find_element_by_id('submitButton')

    sleep(2)

    name_box.send_keys(name)
    age_box.send_keys(age)
    glucose_box.send_keys(glucose)
    bloodpressure_box.send_keys(bloodpressure)
    skinthickness_box.send_keys(skinthickness)
    insulin_box.send_keys(insulin)
    bmi_box.send_keys(bmi)
    pregnancy_box.send_keys(pregnancy)
    diabetes_pdf_box.send_keys(diabetes_pdf)
    location_box.send_keys(location)
    button.click()

driver.close()


# In case you want to see the see the output html page use the code below.

#html = r.content
