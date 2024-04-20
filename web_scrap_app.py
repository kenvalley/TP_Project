# Import the Necessary Libraries


from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import uuid
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import pandas as pd
import uuid
import boto3
import configparser


options = Options()
options.add_argument('--headless') 
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920x1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166")

driver = webdriver.Chrome(options=options)

class scrapping_tp:

    def get_url(self, url):
        return driver.get(url)

    def accept_cookies(self, accept_cookies_xpath):
        time.sleep(7)
        return driver.find_element(By.XPATH, accept_cookies_xpath).click()


    def scrap_page_by_page(self, search_xpath, search_element, search_button_xpath, container_xpath, Next_page_xpath):

        search_bar = driver.find_element(By.XPATH, search_xpath)
        # send keys  
        search_bar.send_keys(search_element) 
        # click the search button
        search_buttn_xpth = driver.find_element(By.XPATH, search_button_xpath)
        search_buttn_xpth.click()

        link_list = []
        #for i in range(0,9):

        container = driver.find_element(By.XPATH, container_xpath)

        number_of_page_to_scrap = 5

        for i in range(0,number_of_page_to_scrap):
            bank_container = container.find_elements(By.XPATH, './div')
            for bank in bank_container:
                try:
                    link_list.append(bank.find_element(By.TAG_NAME, 'a').get_attribute('href')) 
                except NoSuchElementException:
                        print('Not Found')
                #link_list.append(bank.find_element(By.TAG_NAME, 'a').get_attribute('href')) 
                
            driver.find_element(By.XPATH, Next_page_xpath).click()
            time.sleep(3)
        #wait = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH,Next_page_xpath)))
        filtered_list= [x for x in link_list if x is not None]
        final_links = [link for link in filtered_list if "review" in link]

        bank_dict = {
                    'ID':[],
                    'Link': [],
                    'src': [],
                    'Bank_name': [],
                    'Website': [],
                    'Number_of_Reviews': [],
                    'Reviews': [],
                    'Rating_5_star': [],
                    'Rating_4_star': [],
                    'Rating_3_star': [],
                    'Rating_2_star': [],
                    'Rating_1_star': [],
                # 'Most_recent_reviews':["MRR1": [],"MRR2": [],"MRR3": [], "MRR4": [], "MRR5": []] 
                    }

        for link in final_links:
            id = str(uuid.uuid4())
            bank_dict['ID'].append(id) 
            driver.get(link) 
            time.sleep(5)
            bank_dict['Link'].append(link)
            try:
                src_path = driver.find_elements(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[3]/div[2]/div/div/div/section/div[1]/div[1]/a/picture')
                for path in src_path:
                    src = path.find_element(By.TAG_NAME, 'img').get_attribute('src')
                    bank_dict['src'].append(src)
            except NoSuchElementException:
                bank_dict['src'].append('N/A')
            try:
                bank_name = driver.find_element(By.XPATH, '//*[@id="business-unit-title"]/h1/span[1]')
                bank_dict['Bank_name'].append(bank_name.text)
            except NoSuchElementException:
                bank_dict['Bank_name'].append('N/A')
            try:
                website_path = driver.find_elements(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[3]/div[2]/div/div/div/section/div[2]/div/div')
                for path in website_path:
                    website= path.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    bank_dict['Website'].append(website)
            except NoSuchElementException:
                bank_dict['Website'].append('N/A')
            try:
                no_of_reviews = driver.find_element(By.XPATH, '//*[@id="business-unit-title"]/span[1]/span')
                bank_dict['Number_of_Reviews'].append(no_of_reviews.text)
            except NoSuchElementException:
                bank_dict['Number_of_Reviews'].append('N/A')
            try:
                reviews = driver.find_element(By.XPATH, '//*[@id="business-unit-title"]/div/div/p')
                bank_dict['Reviews'].append(reviews.text)
            except NoSuchElementException:
                bank_dict['Reviews'].append('N/A')
            try:
                rating_5_star = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[4]/section/div[2]/div[2]/label[1]/p[2]')
                bank_dict['Rating_5_star'].append(rating_5_star.text)
            except NoSuchElementException:
                bank_dict['Rating_5_star'].append('N/A')
            try:
                rating_4_star = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[4]/section/div[2]/div[2]/label[2]/p[2]')
                bank_dict['Rating_4_star'].append(rating_4_star.text)
            except NoSuchElementException:
                bank_dict['Rating_4_star'].append('N/A')
            try:
                rating_3_star = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[4]/section/div[2]/div[2]/label[3]/p[2]')
                bank_dict['Rating_3_star'].append(rating_3_star.text)
            except NoSuchElementException:
                bank_dict['Rating_3_star'].append('N/A')
            try:
                rating_2_star = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[4]/section/div[2]/div[2]/label[4]/p[2]')
                bank_dict['Rating_2_star'].append(rating_2_star.text)
            except NoSuchElementException:
                bank_dict['Rating_2_star'].append('N/A')
            try:
                rating_1_star = driver.find_element(By.XPATH, '//*[@id="__next"]/div/div/main/div/div[4]/section/div[2]/div[2]/label[5]/p[2]')
                bank_dict['Rating_1_star'].append(rating_1_star.text)
            except NoSuchElementException:
                bank_dict['Rating_1_star'].append('N/A')
            

        data_cols = ['ID','Link', 'src', 'Bank_name', 'Website','Number_of_Reviews', 'Reviews','Rating_5_star','Rating_4_star','Rating_3_star','Rating_2_star','Rating_1_star']
        scrapped_df= pd.DataFrame.from_dict(bank_dict, orient= 'index')
        scrapped_df= scrapped_df.transpose()
        scrapped_df = scrapped_df.to_csv("scrap_1103.csv", sep=',', encoding='utf-8')

        return scrapped_df

    def upload_to_s3(self, scrapped_df):
        config = configparser.ConfigParser()
        config.read_file(open('encrypt.cfg'))
        KEY                    = config.get('AWS','KEY')
        SECRET                 = config.get('AWS','SECRET')
        s3 = boto3.client('s3',aws_access_key_id= KEY, aws_secret_access_key= SECRET, region_name= "eu-west-2")
        return s3.upload_file(scrapped_df, 'BUCKET-NAME-project-01', scrapped_df)


url ='<URL>'
accept_cookies_xpath = '//*[@id="onetrust-accept-btn-handler"]'
search_xpath = '//*[@id="heroSearchContainer"]/div/div/div/form/div/input'
search_element = "Bank"
search_button_xpath = '//*[@id="heroSearchContainer"]/div/div/div/form/div/button[1]/span'
container_xpath = '/html/body/div[1]/div/div/main/div/div[2]/div[2]'
Next_page_xpath = '//*[@id="__next"]/div/div/main/div/div[2]/div[2]/div[11]/nav/a[5]'

scrapping_tp = scrapping_tp()

def scrapping_app():
    scrapping_tp.get_url(url)
    scrapping_tp.accept_cookies(accept_cookies_xpath)
    scrapped_df = scrapping_tp.scrap_page_by_page(search_xpath, search_element, search_button_xpath, container_xpath, Next_page_xpath)
    return scrapped_df

# scrapping_app()