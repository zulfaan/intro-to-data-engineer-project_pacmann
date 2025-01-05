from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from sqlalchemy import create_engine, text
from urllib import request
import pandas as pd
import requests
import luigi
import time
import json
import re

# Extract Data
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

                    try:
                        rating_elem = container.find_element(By.CSS_SELECTOR, ".prd_rating-average-text")
                        rating = rating_elem.text if rating_elem else None
                    except:
                        rating = None
                    
                    try:
                        sold_elem = container.find_element(By.CSS_SELECTOR, ".prd_label-integrity")
                        sold = sold_elem.text if sold_elem else None
                    except:
                        sold = None
                    
                    try:
                        image_elem = container.find_element(By.CSS_SELECTOR, ".css-1q90pod")
                        image = image_elem.get_attribute('src') if image_elem else None
                    except:
                        image = None

                    product_data.append({
                        'name_product': name,
                        'product_link': link,
                        'price_sale': price_sale,
                        'price_original': price,
                        'discount': discount,
                        'sold': sold,
                        'rating': rating,
                        'image_link': image
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

# Transform Data
def clean_product_name(name):
    # Mengubah menjadi huruf kapital
    name = name.upper()
    # Menghapus tanda baca dan karakter khusus
    name = re.sub(r'[-/\[\]"\']', ' ', name)
    # Menghapus spasi ekstra
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# Fungsi untuk membersihkan data Tokopedia
def clean_data_tokped(tokped):
    tokped['price_original'] = tokped['price_original'].fillna(tokped['price_sale'])
    tokped['discount'] = tokped['discount'].fillna('Tidak Ada Diskon')
    tokped['sold'] = tokped['sold'].fillna('0')
    tokped['rating'] = tokped['rating'].fillna('Belum Ada Penilaian')

    tokped['price_sale'] = tokped['price_sale'].str.replace('Rp', '').str.replace('.', '').astype(int)
    tokped['price_original'] = tokped['price_original'].str.replace('Rp', '').str.replace('.', '').astype(int)

    tokped['sold'] = tokped['sold'].str.extract(r'(\d+)').astype(int)

    drop_columns = ['image_link']
    tokped = tokped.drop(columns=drop_columns)

    tokped['name_product'] = tokped['name_product'].apply(clean_product_name)
    tokped = tokped.drop_duplicates(subset='name_product', keep='first')

    return tokped

# Fungsi untuk membersihkan data Lazada
def clean_data_lazada(lazada):
    lazada['price_original'] = lazada['price_original'].fillna(lazada['price_sale'])
    lazada['price_sale'] = lazada['price_sale'].str.replace('Rp', '').str.replace('.', '').astype(int)
    lazada['price_original'] = lazada['price_original'].str.replace('Rp', '').str.replace('.', '').astype(int)

    lazada['discount'] = lazada['discount'].fillna('Tidak Ada Diskon')
    lazada['discount'] = lazada['discount'].str.replace('Voucher save', '')
    
    lazada['sold'] = lazada['sold'].fillna('Belum Ada Penjualan')
    lazada['sold'] = lazada['sold'].str.replace(' sold', '')
    lazada['sold'] = lazada['sold'].str.replace(' K', '000').str.replace('K', '000').str.replace('.', '')

    lazada['rating'] = lazada['rating'].fillna('Belum Ada Penilaian')
    lazada['rating_count'] = lazada['rating_count'].fillna('0')
    lazada['rating_count'] = lazada['rating_count'].astype(int)
    lazada = lazada.drop(columns=['image_link'])

    lazada['name_product'] = lazada['name_product'].apply(clean_product_name)
    lazada = lazada.drop_duplicates(subset='name_product', keep='first')

    return lazada

def get_earliest_date(dates):
    date_list = [pd.to_datetime(date.strip(), format="%Y-%m-%dT%H:%M:%SZ") for date in dates.split(",")]
    earliest_date = min(date_list)
    
    return earliest_date.strftime("%Y-%m-%d")

def clean_weight(weight):
    weight = weight.strip().lower()
    weight_parts = re.findall(r'(\d+\.?\d*)\s*(lbs?|pounds?|ounces?|oz)', weight)
    total_weight = 0.0

    for value, unit in weight_parts:
        value = float(value) 
        if unit in ['lbs', 'lb', 'pounds']:
            total_weight += value * 0.453592 
        elif unit in ['ounces', 'ounce', 'oz']:
            total_weight += value * 0.0283495 
    
    return round(total_weight, 2)

def clean_shipping(value):
    if pd.isna(value):
        return 'Undefined'
    value = str(value).lower()
    if 'free' in value:
        return 'Free Shipping'
    elif 'expedited' in value:
        return 'Expedited Shipping'
    elif 'standard' in value:
        return 'Standard Shipping'
    elif 'freight' in value:
        return 'Freight Shipping'
    elif 'usd' in value or 'cad' in value:
        return 'Paid Shipping'
    else:
        return 'Undefined'

def clean_availability(value):
    if pd.isna(value):
        return 'Unavailable'
    value = str(value).lower()
    if 'in stock' or 'yes' or 'true' or ' availaible' in value:
        return 'Available'
    elif 'out of stock' or 'no' or 'retired' or 'sold' or 'false' in value:
        return 'Unavailable'
    elif 'special order' or 'more on the way' in value:
        return 'Pending'
    else:
        return 'Unavailable'

def clean_condition(condition):
    if pd.isna(condition):
        return 'Undefined'
    condition = str(condition).lower()
    if 'new' in condition:
        return 'New'
    else:
        return 'Used'

class TransformTorchData(luigi.Task):
    def requires(self):
        return [ExtractTokpedTorchData(), ExtractLazadaTorchData()]

    def output(self):
        return {
            'tokped': luigi.LocalTarget('transform-data/torch_tokped_clean.csv'),
            'lazada': luigi.LocalTarget('transform-data/torch_lazada_clean.csv')
        }

    def run(self):
        # Load the data
        tokped_data = pd.read_csv(self.input()[0].path)
        lazada_data = pd.read_csv(self.input()[1].path)

        # Clean the data
        tokped_data_clean = clean_data_tokped(tokped_data)
        lazada_data_clean = clean_data_lazada(lazada_data)
        
        # Save the data
        tokped_data_clean.to_csv(self.output()['tokped'].path, index=False)
        lazada_data_clean.to_csv(self.output()['lazada'].path, index=False)

class TransformMarketingData(luigi.Task):
    def requires(self):
        return ExtractMarketingData()

    def output(self):
        return luigi.LocalTarget("transform-data/marketing_clean.csv")

    def run(self):
        # Load the data
        marketing_data = pd.read_csv(self.input().path)
        
        # Clean the data
        marketing_data['prices.dateSeen'] = marketing_data['prices.dateSeen'].apply(get_earliest_date)

        marketing_data['weight'] = marketing_data['weight'].apply(clean_weight)

        marketing_data['prices.availability'] = marketing_data['prices.availability'].apply(clean_availability)

        marketing_data['prices.condition'] = marketing_data['prices.condition'].apply(clean_condition)

        marketing_data['prices.shipping'] = marketing_data['prices.shipping'].apply(clean_shipping)

        marketing_data['prices.amountMax'] = pd.to_numeric(marketing_data['prices.amountMax'], errors='coerce')
        marketing_data['prices.amountMin'] = pd.to_numeric(marketing_data['prices.amountMin'], errors='coerce')
        marketing_data['discount'] = round((marketing_data['prices.amountMax'] - marketing_data['prices.amountMin']) / marketing_data['prices.amountMax'] * 100, 0)
        marketing_data['discount'] = marketing_data['discount'].astype(int).astype(str) + '%'
        marketing_data['discount'] = marketing_data['discount'].replace('0%', 'No Discount')

        marketing_data['prices.merchant'] = marketing_data['prices.merchant'].str.replace('.com', '')

        marketing_data['name'] = marketing_data['name'].apply(clean_product_name)
        marketing_data['name'] = marketing_data['name'].str.replace(' " ', ' ')

        columns_to_drop = ['id', 'prices.currency', 'prices.sourceURLs', 'prices.sourceURLs','prices.isSale', 'prices.sourceURLs', 'asins', 'brand', 'categories', 'dateAdded', 'dateUpdated', 'ean', 'imageURLs', 'keys', 'manufacturer', 'manufacturerNumber', 'sourceURLs', 'upc', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30']
        rename_columns = {
            'prices.dateSeen': 'date_seen',
            'prices.availability': 'availability',
            'prices.amountMax': 'price_original',
            'prices.amountMin': 'price_sale',
            'primaryCategories': 'category',
            'name': 'name_product',
            'weight': 'weight_kg',
            'discount': 'discount',
            'prices.shipping': 'shipping',
            'prices.merchant': 'merchant',
            'prices.condition': 'condition'
        }

        marketing_data = marketing_data.drop(columns=columns_to_drop)
        marketing_data = marketing_data.rename(columns=rename_columns)
        marketing_data = marketing_data[['name_product', 'category', 'price_original', 'price_sale', 'discount', 'condition', 'availability', 'date_seen', 'weight_kg', 'shipping', 'merchant']]

        # Save the data
        marketing_data.to_csv("transform-data/marketing_clean.csv", index=False)

class TransformSalesData(luigi.Task):
    def requires(self):
        return ExtractDatabaseSalesData()

    def output(self):
        return luigi.LocalTarget("transform-data/sales_clean.csv")

    def run(self):
        # Load the data
        sales_data = pd.read_csv(self.input().path)
        
        # Clean the data
        sales_data['name'] = sales_data['name'].apply(clean_product_name)
        sales_data['name'] = sales_data['name'].str.replace('"', '')

        sales_data = sales_data.drop_duplicates(subset='name', keep='first')
        sales_data = sales_data.dropna(subset=['actual_price'])

        sales_data['discount_price'] = sales_data['discount_price'].fillna(sales_data['actual_price'])

        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '').str.replace(',', '').astype(float)
        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float)

        sales_data['discount'] = round((sales_data['actual_price'] - sales_data['discount_price']) / sales_data['actual_price'] * 100, 0)
        sales_data['discount'] = sales_data['discount'].astype(int).astype(str) + '%'
        sales_data['discount'] = sales_data['discount'].replace('0%', 'No Discount')

        sales_data['ratings'] = sales_data['ratings'].fillna('No Ratings')
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].fillna('No Ratings')

        columns_to_drop = ['sub_category', 'image', 'Unnamed: 0']
        sales_data = sales_data.drop(columns=columns_to_drop)

        category_mapping = {
            "women's clothing": "clothing",
            "men's clothing": "clothing",
            "kids' fashion": "kids and baby products",
            "toys & baby products": "kids and baby products",
            "home & kitchen": "home and kitchen",
            "home, kitchen, pets": "home and kitchen",
            "women's shoes": "footwear",
            "men's shoes": "footwear",
            "beauty & health": "beauty and health",
            "grocery & gourmet foods": "grocery",
            "tv, audio & cameras": "electronics",
            "car & motorbike": "automotive",
        }
        sales_data['main_category'] = sales_data['main_category'].replace(category_mapping)

        rename_columns = {
            'name': 'name_product',
            'main_category': 'category',
            'actual_price': 'price_original_rupee',
            'discount_price': 'price_sale_rupee',
            'discount': 'discount',
            'ratings': 'rating',
            'no_of_ratings': 'rating_count'
        }
        sales_data = sales_data.rename(columns=rename_columns)
        
        sales_data['price_original_rupee'] = sales_data['price_original_rupee'].astype(int)
        sales_data['price_sale_rupee'] = sales_data['price_sale_rupee'].astype(int)

        # Save the data
        sales_data.to_csv("transform-data/sales_clean.csv", index=False)

# Load Data
def postgres_engine():
    db_username = 'postgres'
    db_password = 'qwerty123'
    db_host = 'localhost:5432'
    db_name = 'de_project_pacmann'

    engine_str = f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(engine_str)

    return engine

class LoadData(luigi.Task):
    def requires(self):
        return [TransformSalesData(), TransformMarketingData(), TransformTorchData()]

    def output(self):
        return [luigi.LocalTarget('sales_db.csv'),
                luigi.LocalTarget('marketing_db.csv'),
                luigi.LocalTarget('torch_tokped_db.csv'),
                luigi.LocalTarget('torch_lazada_db.csv')]
    
    def run(self):
        dw_engine = postgres_engine()

        sales_data_db = pd.read_csv(self.input()[0].path)
        marketing_data_db = pd.read_csv(self.input()[1].path)
        torch_tokped_data_db = pd.read_csv(self.input()[2]['tokped'].path)
        torch_laza_data_db = pd.read_csv(self.input()[2]['lazada'].path)

        create_table_query = """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'condition_type') THEN
                CREATE TYPE condition_type AS ENUM ('New', 'Used', 'Undefined');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'availability_type') THEN
                CREATE TYPE availability_type AS ENUM ('Unavailable', 'Available', 'Pending');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'shipping_type') THEN
                CREATE TYPE shipping_type AS ENUM ('Free Shipping', 'Expedited Shipping', 'Standard Shipping', 'Freight Shipping', 'Paid Shipping', 'Undefined');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sold_info_type') THEN
                CREATE TYPE sold_info_type AS ENUM ('+ terjual', 'Belum Ada Penjualan');
            END IF;
        END $$;

        CREATE TABLE IF NOT EXISTS sales_clean (
            name_product TEXT NOT NULL,
            category VARCHAR(50) NOT NULL,
            link TEXT NOT NULL,
            rating TEXT NOT NULL,
            rating_count TEXT NOT NULL,
            price_original_rupee NUMERIC NOT NULL,
            price_sale_rupee NUMERIC NOT NULL,
            discount TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS marketing_clean (
            name_product TEXT NOT NULL,
            category VARCHAR(50) NOT NULL,
            price_original NUMERIC NOT NULL,
            price_sale NUMERIC NOT NULL,
            discount TEXT NOT NULL,
            condition condition_type NOT NULL,
            availability availability_type NOT NULL,
            shipping shipping_type NOT NULL,
            date_seen DATE NOT NULL,
            weight_kg NUMERIC NOT NULL, 
            merchant VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS torch_tokped_clean (
            name_product TEXT NOT NULL,
            product_link TEXT NOT NULL,
            price_original INT NOT NULL,
            price_sale INT NOT NULL,
            discount TEXT NOT NULL,
            rating TEXT NOT NULL,
            sold INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS torch_lazada_clean (
            name_product TEXT NOT NULL,
            product_link TEXT NOT NULL,
            price_original INT NOT NULL,
            price_sale INT NOT NULL,
            discount TEXT NOT NULL,
            sold TEXT NOT NULL,
            rating TEXT NOT NULL,
            rating_count TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );
        """
        with dw_engine.connect() as conn:
            conn.execute(text(create_table_query))
            conn.close()

        sales_data_db.to_sql(name='sales_clean', con=dw_engine, if_exists="append", index=False)
        marketing_data_db.to_sql(name='marketing_clean', con=dw_engine, if_exists="append", index=False)
        torch_tokped_data_db.to_sql(name='torch_tokped_clean', con=dw_engine, if_exists="append", index=False)
        torch_laza_data_db.to_sql(name='torch_lazada_clean', con=dw_engine, if_exists="append", index=False)



if __name__ == '__main__':
    luigi.build([ExtractMarketingData(),
                ExtractDatabaseSalesData(),
                ExtractTokpedTorchData(),
                ExtractTokpedTorchData(),
                TransformSalesData(),
                TransformMarketingData(),
                TransformTorchData(),
                LoadData()], local_scheduler=True)