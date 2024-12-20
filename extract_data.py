from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from sqlalchemy import create_engine
from urllib import request
import pandas as pd
import requests
import luigi
import time
import json


def db_source_sales_engine():
    db_username = 'postgres'
    db_password = 'password123'
    db_host = 'localhost:5433'
    db_name = 'etl_db'

    engine_str = f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(engine_str)

    return engine

class ExtractMarketingData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        #read data
        marketing_data = pd.read_csv('source-marketing_data/ElectronicsProductsPricingData.csv')

        marketing_data.to_csv(self.output().path, index = False)

    def output(self):
        return luigi.LocalTarget('raw-data/extracted_marketing_data.csv')

class ExtractDatabaseSalesData(luigi.Task):
    def requires(self):
        pass
    def run(self):
        engine = db_source_sales_engine()
        query = 'SELECT * FROM amazon_sales_data'

        db_data = pd.read_sql(query, engine)
        
        db_data.to_csv(self.output().path, index = False)
    def output(self):
        return luigi.LocalTarget('raw-data/extracted_sales_data.csv')

class ExtractTokpedTorchData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        base_url = "https://www.tokopedia.com/torch-id/product/page/{}"

        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        driver = webdriver.Chrome(options=options)

        product_data = []

        try:
            for page in range(1, 12):
                url = base_url.format(page)
                driver.get(url)

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

                for _ in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                    time.sleep(2)

                product_containers = driver.find_elements(By.CSS_SELECTOR, "[data-testid='divProductWrapper']")

                for container in product_containers:
                    try:
                        name = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductName']").text
                    except:
                        name = None

                    try:
                        link = container.find_element(By.CSS_SELECTOR, "a.pcv3__info-content").get_attribute('href')
                    except:
                        link = None

                    try:
                        price_sale_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductPrice']")
                        price_sale = price_sale_elem.text if price_sale_elem else None
                    except:
                        price_sale = None

                    try:
                        price_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductSlashPrice']")
                        price = price_elem.text if price_elem else None
                    except:
                        price = None

                    try:
                        discount_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductDiscount']")
                        discount = discount_elem.text if discount_elem else None
                    except:
                        discount = None

                    product_data.append({
                        'name_product': name,
                        'product_link': link,
                        'price_sale': price_sale,
                        'price_original': price,
                        'discount': discount
                    })

            torch_tokped_df = pd.DataFrame(product_data)

            torch_tokped_df.to_csv(self.output().path, index=False)

        except Exception as e:
            print(f"Terjadi kesalahan: {e}")
        
        finally:
            driver.quit()

    def output(self):
        return luigi.LocalTarget('raw-data/torch_tokped_raw.csv')

class ExtractLazadaTorchData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        base_url = "https://www.lazada.co.id/torch/?from=wangpu&langFlag=en&page={page}&pageTypeId=2&q=All-Products"

        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])

        driver = webdriver.Chrome(options=options)

        product_data = []

        try:
            for page in range(1, 7):
                url = base_url.format(page=page)
                driver.get(url)

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

                for _ in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                    time.sleep(2)

                product_containers = driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']")

                for container in product_containers:
                    try:
                        name = container.find_element(By.CSS_SELECTOR, ".RfADt a").text
                    except:
                        name = None

                    try:
                        link = container.find_element(By.CSS_SELECTOR, ".RfADt a").get_attribute('href')
                    except:
                        link = None

                    try:
                        price_sale = container.find_element(By.CSS_SELECTOR, ".aBrP0 .ooOxS").text
                    except:
                        price_sale = None

                    try:
                        price = container.find_element(By.CSS_SELECTOR, "._1m41m del.ooOxS").text
                    except:
                        price = None

                    try:
                        discount = container.find_element(By.CSS_SELECTOR, ".ic-dynamic-badge-text").text
                    except:
                        discount = None

                    try:
                        sold_elem = container.find_element(By.CSS_SELECTOR, "._1cEkb span")
                        sold = sold_elem.text.replace(' Terjual', '')
                    except:
                        sold = None

                    try:
                        rating_container = container.find_element(By.CSS_SELECTOR, ".mdmmT")
                        filled_stars = len(rating_container.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx"))

                        try:
                            rating_count_elem = rating_container.find_element(By.CSS_SELECTOR, ".qzqFw")
                            rating_count = rating_count_elem.text.strip('()')
                        except:
                            rating_count = None
                    except:
                        filled_stars = None
                        rating_count = None
                    try:
                        image = container.find_element(By.CSS_SELECTOR, ".picture-wrapper img[type='product']").get_attribute('src')
                    except:
                        image = None
                    

                    product_data.append({
                        'name_product': name,
                        'product_link': link,
                        'price_sale': price_sale,
                        'price_original': price,
                        'discount': discount,
                        'sold': sold,
                        'rating': filled_stars,
                        'rating_count': rating_count,
                        'image_link': image
                    })
            torch_lazada_df = pd.DataFrame(product_data)
            torch_lazada_df.to_csv(self.output().path, index=False)
        
        except Exception as e:
            print(f"Terjadi kesalahan: {e}")
        
        finally:
            driver.quit()

    def output(self):
        return luigi.LocalTarget('raw-data/torch_lazada_raw.csv')

class RunAllTasks(luigi.Task):
    def requires(self):
        return [
            ExtractMarketingData(),
            ExtractDatabaseSalesData(),
            ExtractTokpedTorchData(),
            ExtractLazadaTorchData()
        ]

    def output(self):
        return luigi.LocalTarget('raw-data/all_tasks_completed.txt')

    def run(self):
        with self.output().open('w') as f:
            f.write('All tasks completed successfully.')

if __name__ == '__main__':
    luigi.build([RunAllTasks()], local_scheduler=True)