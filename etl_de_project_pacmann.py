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

# Fungsi untuk menghubungkan ke database PostgreSQL
def db_source_sales_engine():
    db_username = 'postgres' # Nama pengguna database
    db_password = 'password123' # Kata sandi database
    db_host = 'localhost:5433' # Host database
    db_name = 'etl_db' # Nama database

    # Membuat string koneksi untuk database PostsgreSQL
    engine_str = f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(engine_str) # Membuat koneksi ke database

    return engine # Mengembalikan engine

# Mengekstrak data pemasaran dari file CSV
class ExtractMarketingData(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan

    def output(self):
        return luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/raw-data/extracted_marketing_data.csv') # Menyimpan data yang diekstrak ke file CSV

    def run(self):
        # Membaca data dari file CSV
        marketing_data = pd.read_csv('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/source-marketing_data/ElectronicsProductsPricingData.csv')

        # Menyimpan data yang diekstrak ke file CSV 
        marketing_data.to_csv(self.output().path, index = False)

# Mengekstrak data penjualan dari database PostgreSQL
class ExtractDatabaseSalesData(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan

    def output(self):
        return luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/raw-data/extracted_sales_data.csv') # Menyimpan data yang diekstrak ke file CSV

    def run(self):
        engine = db_source_sales_engine() # Menghubungkan ke database
        query = 'SELECT * FROM amazon_sales_data' # Query untuk mengambil data dari tabel amazon_sales_data

        # Mengambil data dari database menggunakan query SQL
        db_data = pd.read_sql(query, engine)

        # Menyimpan data yang diekstrak ke file CSV
        db_data.to_csv(self.output().path, index = False)

# Mengekstrak data produk Torch dari Tokopedia
class ExtractTokpedTorchData(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan
    
    def output(self):
        return luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/raw-data/torch_tokped_raw.csv') # MTempat penyimpanan data yang diekstrak


    def run(self):
        base_url = "https://www.tokopedia.com/torch-id/product/page/{}" # URL dasar untuk mengambil data produk Torch dari Tokopedia

        # Mengatur opsi untuk webdriver Chrome
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled') # Menonaktifkan fitur otomatisasi
        options.add_experimental_option('useAutomationExtension', False) # Menonaktifkan ekstensi otomatisasi
        options.add_experimental_option("excludeSwitches", ["enable-automation"]) # Mengecualikan switch otomatisasi
        driver = webdriver.Chrome(options=options) # Membuat instance dari webdriver Chrome

        product_data = [] # List untuk menyimpan data produk

        try:
            for page in range(1, 13): # Mengambil data dari halaman 1 hingga 12
                url = base_url.format(page) # Membuat URL untuk halaman saat ini
                driver.get(url) # Mengakses URL

                # Menunggu hingga elemen body muncul
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )

                # Menggulir halaman untuk memuat lebih banyak produk
                for _ in range(5):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") # Menggulir ke bawah
                    time.sleep(2) # Menunggu 2 detik
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);") # Menggulir ke atas
                    time.sleep(2) # Menunggu 2 detik

                # Mengambil elemen produk
                product_containers = driver.find_elements(By.CSS_SELECTOR, "[data-testid='divProductWrapper']")

                for container in product_containers: # Iterasi setiap elemen produk
                    try:
                        name = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductName']").text # Mengambil nama produk
                    except:
                        name = None # Jika gagal, set nama menjadi None
                    
                    # Mengambil link produk dari elemen
                    try:
                        link = container.find_element(By.CSS_SELECTOR, "a.pcv3__info-content").get_attribute('href') # Mencari elemen link produk dan mengambil atribut 'href'
                    except:
                        link = None # Jika gagal, set link menjadi None

                    # Mengambil harga jual produk dari elemen
                    try:
                        price_sale_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='linkProductPrice']") # Mencari elemen harga jual produk
                        price_sale = price_sale_elem.text if price_sale_elem else None # Mengambil teks dari elemen harga jual produk
                    except:
                        price_sale = None # Jika gagal, set harga jual menjadi None

                    # Mengambil harga asli produk dari elemen
                    try:
                        price_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductSlashPrice']") # Mencari elemen harga asli produk
                        price = price_elem.text if price_elem else None # Mengambil teks dari elemen harga asli produk
                    except:
                        price = None # Jika gagal, set harga asli menjadi None

                    try:
                        discount_elem = container.find_element(By.CSS_SELECTOR, "[data-testid='lblProductDiscount']") # Mencari elemen diskon produk
                        discount = discount_elem.text if discount_elem else None # Mengambil teks dari elemen diskon produk
                    except:
                        discount = None # Jika gagal, set diskon menjadi None

                    # Mengambil rating produk dari elemen
                    try:
                        rating_elem = container.find_element(By.CSS_SELECTOR, ".prd_rating-average-text") # Mencari elemen rating produk
                        rating = rating_elem.text if rating_elem else None # Mengambil teks dari elemen rating produk
                    except:
                        rating = None # Jika gagal, set rating menjadi None
                    
                    # Mengambil jumlah produk yang terjual dari elemen
                    try:
                        sold_elem = container.find_element(By.CSS_SELECTOR, ".prd_label-integrity") # Mencari elemen jumlah produk yang terjual
                        sold = sold_elem.text if sold_elem else None # Mengambil teks dari elemen jumlah produk yang terjual
                    except:
                        sold = None # Jika gagal, set jumlah produk yang terjual menjadi None

                    # Mengambil link gambar produk dari elemen
                    try:
                        image_elem = container.find_element(By.CSS_SELECTOR, ".css-1q90pod") # Mencari elemen gambar produk
                        image = image_elem.get_attribute('src') if image_elem else None # Mengambil atribut 'src' dari elemen gambar produk
                    except:
                        image = None # Jika gagal, set link gambar menjadi None

                    # Menambahkan data produk ke dalam list product_data
                    product_data.append({
                        'name_product': name, # Nama produk
                        'product_link': link, # Link produk
                        'price_sale': price_sale, # Harga jual
                        'price_original': price, # Harga asli
                        'discount': discount, # Diskon
                        'sold': sold, # Jumlah produk yang terjual
                        'rating': rating, # Rating produk
                        'image_link': image # Link gambar produk
                    })

            # Mengonversi list product_data ke dalam DataFrame
            torch_tokped_df = pd.DataFrame(product_data)

            # Menyimpan DataFrame ke dalam file CSV
            torch_tokped_df.to_csv(self.output().path, index=False)

        except Exception as e:
            print(f"Terjadi kesalahan: {e}") # Menampilkan pesan kesalahan jika terjadi kesalahan
        
        finally:
            driver.quit() # Menutup browser


# Mengekstrak data produk Torch dari Lazada
class ExtractLazadaTorchData(luigi.Task):
    def requires(self):
        pass # Tidak ada task yang diperlukan

    def output(self):
        return luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/raw-data/torch_lazada_raw.csv') # Tempat penyimpanan data yang diekstrak

    def run(self):
        base_url = "https://www.lazada.co.id/torch/?from=wangpu&langFlag=en&page={page}&pageTypeId=2&q=All-Products" # URL dasar untuk mengambil data produk Torch dari Lazada

        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled') # Menonaktifkan fitur otomatisasi
        options.add_experimental_option('useAutomationExtension', False) # Menonaktifkan ekstensi otomatisasi
        options.add_experimental_option("excludeSwitches", ["enable-automation"]) # Mengecualikan switch otomatisasi

        driver = webdriver.Chrome(options=options) # Membuat instance dari webdriver Chrome

        product_data = [] # List untuk menyimpan data produk

        try:
            for page in range(1, 8): # Mengambil data dari halaman 1 hingga 7
                url = base_url.format(page=page) # Membuat URL untuk halaman saat ini
                driver.get(url) # Mengakses URL

                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body')) # Menunggu hingga elemen body muncul
                )

                for _ in range(5): # Menggulir halaman untuk memuat lebih banyak produk
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") # Menggulir ke bawah
                    time.sleep(2) # Menunggu 2 detik
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);") # Menggulir ke atas
                    time.sleep(2) # Menunggu 2 detik

                product_containers = driver.find_elements(By.CSS_SELECTOR, "[data-qa-locator='product-item']") # Mengambil elemen produk

                for container in product_containers: # Iterasi setiap elemen produk
                    try:
                        name = container.find_element(By.CSS_SELECTOR, ".RfADt a").text # Mengambil nama produk
                    except:
                        name = None # Jika gagal, set nama menjadi None

                    try:
                        link = container.find_element(By.CSS_SELECTOR, ".RfADt a").get_attribute('href') # Mengambil link produk
                    except:
                        link = None # Jika gagal, set link menjadi None

                    try:
                        price_sale = container.find_element(By.CSS_SELECTOR, ".aBrP0 .ooOxS").text # Mengambil harga jual produk
                    except:
                        price_sale = None # Jika gagal, set harga jual menjadi None

                    try:
                        price = container.find_element(By.CSS_SELECTOR, "._1m41m del.ooOxS").text # Mengambil harga asli produk
                    except:
                        price = None # Jika gagal, set harga asli menjadi None

                    try:
                        discount = container.find_element(By.CSS_SELECTOR, ".ic-dynamic-badge-text").text # Mengambil diskon produk
                    except:
                        discount = None # Jika gagal, set diskon menjadi None

                    try:
                        sold_elem = container.find_element(By.CSS_SELECTOR, "._1cEkb span")
                        sold = sold_elem.text.replace(' Terjual', '') # Mengambil jumlah produk yang terjual
                    except:
                        sold = None # Jika gagal, set jumlah produk yang terjual menjadi None

                    try:
                        rating_container = container.find_element(By.CSS_SELECTOR, ".mdmmT") # Mengambil elemen rating produk
                        filled_stars = len(rating_container.find_elements(By.CSS_SELECTOR, "i._9-ogB.Dy1nx")) # Menghitung jumlah bintang rating

                        try:
                            rating_count_elem = rating_container.find_element(By.CSS_SELECTOR, ".qzqFw") # Mengambil elemen jumlah orang yang memberikan rating
                            rating_count = rating_count_elem.text.strip('()') # Mengambil teks jumlah orang yang memberikan rating
                        except:
                            rating_count = None # Jika gagal, set jumlah orang yang memberikan rating menjadi None
                    except:
                        filled_stars = None # Jika gagal, set jumlah bintang rating menjadi None
                        rating_count = None # Jika gagal, set jumlah orang yang memberikan rating menjadi None
                    try:
                        image = container.find_element(By.CSS_SELECTOR, ".picture-wrapper img[type='product']").get_attribute('src') # Mengambil link gambar produk
                    except:
                        image = None # Jika gagal, set link gambar menjadi None
                    

                    product_data.append({
                        'name_product': name, # Nama produk
                        'product_link': link, # Link produk
                        'price_sale': price_sale, # Harga jual
                        'price_original': price, # Harga asli
                        'discount': discount, # Diskon
                        'sold': sold, # Jumlah produk yang terjual
                        'rating': filled_stars, # Rating produk
                        'rating_count': rating_count, # Jumlah orang yang memberikan rating
                        'image_link': image # Link gambar produk
                    })
            torch_lazada_df = pd.DataFrame(product_data) # Mengonversi list product_data ke dalam DataFrame
            torch_lazada_df.to_csv(self.output().path, index=False) # Menyimpan DataFrame ke dalam file CSV
        
        except Exception as e:
            print(f"Terjadi kesalahan: {e}") # Menampilkan pesan kesalahan jika terjadi kesalahan
        
        finally:
            driver.quit() # Menutup browser

# Fungsi untuk membersihkan nama produk
def clean_product_name(name):
    # Mengubah menjadi huruf kapital
    name = name.upper()
    # Menghapus tanda baca dan karakter khusus dari nama produk
    name = re.sub(r'[-/\[\]"\']', ' ', name)
    # Menghapus spasi ekstra di sekitar nama produk
    name = re.sub(r'\s+', ' ', name).strip()
    return name # Mengembalikan nama produk yang telah dibersihkan

# Fungsi untuk membersihkan data Tokopedia
def clean_data_tokped(tokped):
    # Mengisi nilai NaN pada kolom 'price_original' dengan nilai pada kolom 'price_sale'
    tokped['price_original'] = tokped['price_original'].fillna(tokped['price_sale'])
    # Mengisi nilai NaN pada kolom 'discount' dengan 'Tidak Ada Diskon'
    tokped['discount'] = tokped['discount'].fillna('Tidak Ada Diskon')
    # Mengisi nilai NaN pada kolom 'sold' dengan '0'
    tokped['sold'] = tokped['sold'].fillna('0')
    # Mengisi nilai NaN pada kolom 'rating' dengan 'Belum Ada Penilaian'
    tokped['rating'] = tokped['rating'].fillna('Belum Ada Penilaian')
    # Menghapus karakter 'Rp' dan '.' dari kolom 'price_sale' dan mengonversi ke tipe data integer
    tokped['price_sale'] = tokped['price_sale'].str.replace('Rp', '').str.replace('.', '').astype(int)
    # Menghapus karakter 'Rp' dan '.' dari kolom 'price_original' dan mengonversi ke tipe data integer
    tokped['price_original'] = tokped['price_original'].str.replace('Rp', '').str.replace('.', '').astype(int)
    # Mengambil angka dari kolom 'sold' dan mengonversi ke tipe data integer
    tokped['sold'] = tokped['sold'].str.extract(r'(\d+)').astype(int)
    # Menghapus kolom 'image_link' dari DataFrame
    drop_columns = ['image_link']
    tokped = tokped.drop(columns=drop_columns)
    # Menerapkan fungsi clean_product_name pada kolom 'name_product'
    tokped['name_product'] = tokped['name_product'].apply(clean_product_name)
    # Menghapus data duplikat berdasarkan kolom 'name_product'
    tokped = tokped.drop_duplicates(subset='name_product', keep='first')

    return tokped # Mengembalikan DataFrame yang telah dibersihkan

# Fungsi untuk membersihkan data Lazada
def clean_data_lazada(lazada):
    # Mengisi nilai Nan pada kolom 'price_original' dengan nilai pada kolom 'price_sale'
    lazada['price_original'] = lazada['price_original'].fillna(lazada['price_sale'])
    # Menghapus karakter 'Rp' dan '.' dari kolom 'price_sale' dan mengonversi ke tipe data integer
    lazada['price_sale'] = lazada['price_sale'].str.replace('Rp', '').str.replace('.', '').astype(int)
    # Menghapus karakter 'Rp' dan '.' dari kolom 'price_original' dan mengonversi ke tipe data integer
    lazada['price_original'] = lazada['price_original'].str.replace('Rp', '').str.replace('.', '').astype(int)
    # Mengisi nilai Nan pada kolom 'discount' dengan 'Tidak Ada Diskon'
    lazada['discount'] = lazada['discount'].fillna('Tidak Ada Diskon')
    # Menghapus 'Voucher save' dari kolom 'discount'
    lazada['discount'] = lazada['discount'].str.replace('Voucher save', '')
    # Mengisi nilai Nan pada kolom 'rating' dengan 'Belum Ada Penilaian'
    lazada['rating'] = lazada['rating'].fillna('Belum Ada Penilaian')
    # Mengisi nilai Nan pada kolom 'rating_count' dengan '0' dan mengonversi ke tipe data integer
    lazada['rating_count'] = lazada['rating_count'].fillna('0').astype(int)
    # Mengisi nilai Nan pada kolom 'sold' dengan nilai pada kolom 'rating_count'
    lazada['sold'] = lazada['sold'].fillna(lazada['rating_count'])
    # Menghapus karakter ' sold' dari kolom sold
    lazada['sold'] = lazada['sold'].str.replace(' sold', '')
    # Mengonversi format 'K' dan titik pada kolom 'sold' menjadi angka
    lazada['sold'] = lazada['sold'].str.replace(' K', '000').str.replace('K', '000').str.replace('.', '')
    # Mengisi nilai NaN pada kolom 'sold' dengan nilai pada kolom 'rating_count' dan mengonversi ke tipe data integer
    lazada['sold'] = lazada['sold'].fillna(lazada['rating_count']).astype(int)
    # Menghapus kolom 'image_link' dari DataFrame
    lazada = lazada.drop(columns=['image_link'])
    # Menerapkan fungsi clean_product_name pada kolom 'name_product'
    lazada['name_product'] = lazada['name_product'].apply(clean_product_name)
    # Menghapus data duplikat berdasarkan kolom 'name_product'
    lazada = lazada.drop_duplicates(subset='name_product', keep='first')

    return lazada # Mengembalikan DataFrame yang telah dibersihkan

# Fungsi untuk mengambil tanggal terawal
def get_earliest_date(dates):
    # Mengonversi string tanggal yang dipisahkan koma menjadi list tanggal
    date_list = [pd.to_datetime(date.strip(), format="%Y-%m-%dT%H:%M:%SZ") for date in dates.split(",")]# Mengambil tanggal terawal dari list tanggal
    earliest_date = min(date_list)
    # Mengembalikan tanggal terawal dalam format 'YYYY-MM-DD'
    return earliest_date.strftime("%Y-%m-%d")

# Fungsi untuk mengonversi berat produk
def clean_weight(weight):
    # Menghapus spasi di awal dan akhir string dan mengubah menjadi huruf kecil
    weight = weight.strip().lower()
    # Mencari angka dan satuan berat (lbs, pounds, ounces, oz) dalam string
    weight_parts = re.findall(r'(\d+\.?\d*)\s*(lbs?|pounds?|ounces?|oz)', weight)
    total_weight = 0.0 # Inisialisasi total berat

    # Menghitung total berat berdasarkan satuannya
    for value, unit in weight_parts:
        value = float(value)  # Mengonversi nilai berat ke tipe data float
        if unit in ['lbs', 'lb', 'pounds']:
            total_weight += value * 0.453592  # Mengonversi lbs ke kg
        elif unit in ['ounces', 'ounce', 'oz']:
            total_weight += value * 0.0283495  # Mengonversi ounces ke kg
    # Mengembalikan total berat dalam kg hingga 2 desimal
    return round(total_weight, 2)

# Fungsi untuk membersihkan data pengiriman
def clean_shipping(value):
    # Memeriksa apakah nilah adalah NaN
    if pd.isna(value):
        return 'Undefined' # Mengembalikan 'Undefined' jika nilai adalah NaN
    value = str(value).lower() # Mengubah nilai menjadi string dan huruf kecil 
    # Mengidentifikasi jenis pengiriman berdasarkan nilai
    if 'free' in value:
        return 'Free Shipping' # Mengembalikan 'Free Shipping' jika 'free' ada dalam nilai
    elif 'expedited' in value:
        return 'Expedited Shipping' # Mengembalikan 'Expedited Shipping' jika 'expedited' ada dalam nilai
    elif 'standard' in value:
        return 'Standard Shipping' # Mengembalikan 'Standard Shipping' jika 'standard' ada dalam nilai
    elif 'freight' in value:
        return 'Freight Shipping' # Mengembalikan 'Freight Shipping' jika 'freight' ada dalam nilai
    elif 'usd' in value or 'cad' in value:
        return 'Paid Shipping' # Mengembalikan 'Paid Shipping' jika 'usd' atau 'cad' ada dalam nilai
    else:
        return 'Undefined' # Mengembalikan 'Undefined' jika tidak ada kondisi di atas

# Fungsi untuk membersihkan ketersediaan produk
def clean_availability(value):
    # Memeriksa apakah nilai adalah NaN
    if pd.isna(value):
        return 'Unavailable' # Mengembalikan 'Unavailable' jika nilai adalah NaN
    value = str(value).lower() # Mengubah nilai menjadi string dan huruf kecil
    # Mengidentifikasi ketersediaan produk berdasarkan nilai
    if 'in stock' in value or 'yes' in value or 'true' in value or 'available' in value:
        return 'Available' # Mengembalikan 'Available' jika 'in stock', 'yes', 'true', atau 'available' ada dalam nilai
    elif 'out of stock' in value or 'no' in value or 'retired' in value or 'sold' in value or 'false' in value:
        return 'Unavailable' # Mengembalikan 'Unavailable' jika 'out of stock', 'no', 'retired', 'sold', atau 'false' ada dalam nilai
    elif 'special order' in value or 'more on the way' in value:
        return 'Pending' # Mengembalikan 'Pending' jika 'special order' atau 'more on the way' ada dalam nilai
    else:
        return 'Unavailable' # Mengembalikan 'Unavailable' jika tidak ada kondisi di atas

# Fungsi untuk membersihkan kondisi produk
def clean_condition(condition):
    if pd.isna(condition):
        return 'Undefined' # Mengembalikan 'Undefined' jika nilai adalah NaN
    condition = str(condition).lower() # Mengubah nilai menjadi string dan huruf kecil
    if 'new' in condition:
        return 'New' # Mengembalikan 'New' jika 'new' ada dalam nilai
    else:
        return 'Used' # Mengembalikan 'Used jika kondisi tidak 'new'

# Kelas untuk mentransformasi data dari Tokopedia dan Lazada
class TransformTorchData(luigi.Task):
    def requires(self):
        # Mengembalikan instance dari task ExtractTokpedTorchData dan ExtractLazadaTorchData
        return [ExtractTokpedTorchData(), ExtractLazadaTorchData()]

    def output(self):
        # Menentukan lokasi output untuk data yang telah dibersihkan
        return {
            'tokped': luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/torch_tokped_clean.csv'),
            'lazada': luigi.LocalTarget('/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/torch_lazada_clean.csv')
        }

    def run(self):
        # Memuat data dari instance task ExtractTokpedTorchData dan ExtractLazadaTorchData
        tokped_data = pd.read_csv(self.input()[0].path)
        lazada_data = pd.read_csv(self.input()[1].path)
        # Membersihkan data 
        tokped_data_clean = clean_data_tokped(tokped_data)
        lazada_data_clean = clean_data_lazada(lazada_data)
        # Menyimpan data hasil ekstrak ke dalam format csv
        tokped_data_clean.to_csv(self.output()['tokped'].path, index=False)
        lazada_data_clean.to_csv(self.output()['lazada'].path, index=False)

# Kelas untuk mentranformasi Data Marketing
class TransformMarketingData(luigi.Task):
    def requires(self):
        # Menentukan bahwa tugas ini bergantung pada tugas ExtractMarketingData
        return ExtractMarketingData()

    def output(self):
        # Menentukan lokasi output untuk data yang telah dibersihkan
        return luigi.LocalTarget("/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/marketing_clean.csv")

    def run(self):
        # Memuat data dari file CSV yang dihasilkan oleh tugas sebelumnya
        marketing_data = pd.read_csv(self.input().path)
        # Membersihkan data dengan menerapkan fungsi yang sesuai
        marketing_data['prices.dateSeen'] = marketing_data['prices.dateSeen'].apply(get_earliest_date) # Mengambil tanggal terawal
        marketing_data['weight'] = marketing_data['weight'].apply(clean_weight) # Membersihkan dan mengonversi berat
        marketing_data['prices.availability'] = marketing_data['prices.availability'].apply(clean_availability) # Membersihkan ketersediaan
        marketing_data['prices.condition'] = marketing_data['prices.condition'].apply(clean_condition) # Membersihkan kondisi produk
        marketing_data['prices.shipping'] = marketing_data['prices.shipping'].apply(clean_shipping) # Membersihkan informasi pengiriman
        # Mengonversi kolom harga maksimum dan minimum menjadi tipe numerik
        marketing_data['prices.amountMax'] = pd.to_numeric(marketing_data['prices.amountMax'], errors='coerce')
        marketing_data['prices.amountMin'] = pd.to_numeric(marketing_data['prices.amountMin'], errors='coerce')
        # Menghitung diskon berdasarkan harga maksimum dan minimum
        marketing_data['discount'] = round((marketing_data['prices.amountMax'] - marketing_data['prices.amountMin']) / marketing_data['prices.amountMax'] * 100, 0)
        marketing_data['discount'] = marketing_data['discount'].astype(int).astype(str) + '%' # Mengonversi diskon menjadi string dengan simbol persen
        marketing_data['discount'] = marketing_data['discount'].replace('0%', 'No Discount') # Mengganti '0%' dengan 'No Discount'
        # Menghapus '.com' dari nama merchant
        marketing_data['prices.merchant'] = marketing_data['prices.merchant'].str.replace('.com', '')
        # Membersihkan nama produk
        marketing_data['name'] = marketing_data['name'].apply(clean_product_name)
        marketing_data['name'] = marketing_data['name'].str.replace(' " ', ' ') # Menghapus spasi yang tidak perlu
        # Menentukan kolom yang dihapus
        columns_to_drop = ['id', 'prices.currency', 'prices.sourceURLs', 'prices.sourceURLs','prices.isSale', 'prices.sourceURLs', 'asins', 'brand', 'categories', 'dateAdded', 'dateUpdated', 'ean', 'imageURLs', 'keys', 'manufacturer', 'manufacturerNumber', 'sourceURLs', 'upc', 'Unnamed: 26', 'Unnamed: 27', 'Unnamed: 28', 'Unnamed: 29', 'Unnamed: 30']
        # Menentukan nama kolom baru
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
        # Menghapus kolom yang tidak diperlukan
        marketing_data = marketing_data.drop(columns=columns_to_drop)
        # Mengganti nama kolom yang sesuai dengan mapping yang telah ditentukan
        marketing_data = marketing_data.rename(columns=rename_columns)
        # Mengatur urutan kolom yang akan disimpan
        marketing_data = marketing_data[['name_product', 'category', 'price_original', 'price_sale', 'discount', 'condition', 'availability', 'date_seen', 'weight_kg', 'shipping', 'merchant']]
        # Menyimpan data yang telah dibersihkan ke file CSV
        marketing_data.to_csv("/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/marketing_clean.csv", index=False)

# Kelas untuk mentranformasi Data Sales
class TransformSalesData(luigi.Task):
    def requires(self):
        # Menentukan bahwa tugas ini bergantung pada ExtractDatabaseSalesData
        return ExtractDatabaseSalesData()

    def output(self):
        # Menentukan lokasi output untuk data yang telah dibersihkan
        return luigi.LocalTarget("/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/sales_clean.csv")

    def run(self):
        # Memuat data dari file CSV yang dihasilkan oleh tugas sebelumnya
        sales_data = pd.read_csv(self.input().path)
        # Membersihkan data dengan menerapkan fungsi yang sesuai
        sales_data['name'] = sales_data['name'].apply(clean_product_name) # Membersihkan nama produk
        sales_data['name'] = sales_data['name'].str.replace('"', '') # Menghapus tanda kutip dari nama produk
        # Menhapus duplikat berdasarkan kolom 'name', menyimpan yang pertama
        sales_data = sales_data.drop_duplicates(subset='name', keep='first')
        # Menghapus baris yang memiliki NaN pada kolom 'actual_price'
        sales_data = sales_data.dropna(subset=['actual_price'])
        # Mengisi nilai NaN pada kolom 'discount_price' dengan nilai dari 'actual_price'
        sales_data['discount_price'] = sales_data['discount_price'].fillna(sales_data['actual_price'])
        # Menghapus simbol '₹' dan koma dari kolom 'discount_price' dan 'actual_price' serta mengonversinya menjadi float
        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '').str.replace(',', '').astype(float)
        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '').str.replace(',', '').astype(float)
        # Menghitung diskon berdasarkan 'actual_price' dan 'discount_price'
        sales_data['discount'] = round((sales_data['actual_price'] - sales_data['discount_price']) / sales_data['actual_price'] * 100, 0)
        sales_data['discount'] = sales_data['discount'].astype(int).astype(str) + '%' # Mengonversi diskon menjadi string dengan simbol persen
        sales_data['discount'] = sales_data['discount'].replace('0%', 'No Discount') # Mengganti '0%' dengan 'No Discount'
        # Mengisi nilai NaN pada kolom 'ratings' dan 'no_of_ratings'
        sales_data['ratings'] = sales_data['ratings'].fillna('No Ratings')
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].fillna('No Ratings')
        # Menentukan kolom yang akan dihapus
        columns_to_drop = ['sub_category', 'image', 'Unnamed: 0']
        # Menghapus kolom yang tidak diperlukan
        sales_data = sales_data.drop(columns=columns_to_drop)
        # Mapping kategori untuk menyederhanakan kategori produk
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
            "Testing Category": "testing"
        }
        # Mengganti kategori utama dengan kategori yang telah dipetakan
        sales_data['main_category'] = sales_data['main_category'].replace(category_mapping)
        # Menentukan nama kolom baru
        rename_columns = {
            'name': 'name_product',
            'main_category': 'category',
            'actual_price': 'price_original_rupee',
            'discount_price': 'price_sale_rupee',
            'discount': 'discount',
            'ratings': 'rating',
            'no_of_ratings': 'rating_count'
        }
        # Mengganti nama kolom sesuai dengan mapping yang telah ditentukan
        sales_data = sales_data.rename(columns=rename_columns)
        # Mengonversi kolom 'price' menjadi integer
        sales_data['price_original_rupee'] = sales_data['price_original_rupee'].astype(int)
        sales_data['price_sale_rupee'] = sales_data['price_sale_rupee'].astype(int)
        # Menyimpan data yang telah dibersihkan ke file CSV
        sales_data.to_csv("/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/transform-data/sales_clean.csv", index=False)

# Membuat koneksi kedatabase PostgreSQL
def postgres_engine():
    db_username = 'postgres' # Nama pengguna database
    db_password = 'qwerty123' # Kata sandi database
    db_host = 'localhost:5432' # Host database
    db_name = 'de_project_pacmann' # Nama database
    # Membuat string koneksi untuk database PostgreSQL
    engine_str = f"postgresql://{db_username}:{db_password}@{db_host}/{db_name}"
    engine = create_engine(engine_str) # Membuat engine database

    return engine # Mengembalikan engine database

class LoadData(luigi.Task):
    def requires(self):
        # Menentukan bahwa tugas ini bergantung pada beberapa tugas transformasi
        return [TransformSalesData(), TransformMarketingData(), TransformTorchData()]

    def output(self):
        # Menentukan lokasi output untuk data yang akan disimpan
        return [luigi.LocalTarget('sales_db.csv'),
                luigi.LocalTarget('marketing_db.csv'),
                luigi.LocalTarget('torch_tokped_db.csv'),
                luigi.LocalTarget('torch_lazada_db.csv')]
    
    def run(self):
        # Menghubungkan ke database PostgreSQL
        dw_engine = postgres_engine()
        # Memuat data dari file  CSV yang dihasilkan oleh kelas sebelumnya
        sales_data_db = pd.read_csv(self.input()[0].path)
        marketing_data_db = pd.read_csv(self.input()[1].path)
        torch_tokped_data_db = pd.read_csv(self.input()[2]['tokped'].path)
        torch_laza_data_db = pd.read_csv(self.input()[2]['lazada'].path)
        # Query untuk membuat tipe data dan tabel jika belum ada
        create_table_query = """
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'condition_type') THEN
                CREATE TYPE condition_type AS ENUM ('New', 'Used', 'Undefined'); -- Tipe untuk kondisi produk
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'availability_type') THEN
                CREATE TYPE availability_type AS ENUM ('Unavailable', 'Available', 'Pending'); -- Tipe untuk ketersediaan produk
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'shipping_type') THEN
                CREATE TYPE shipping_type AS ENUM ('Free Shipping', 'Expedited Shipping', 'Standard Shipping', 'Freight Shipping', 'Paid Shipping', 'Undefined'); -- Tipe untuk pengiriman
            END IF;
        END $$;

        -- Membuat tabel untuk data penjualan jika belum ada
        CREATE TABLE IF NOT EXISTS sales_clean (
            name_product TEXT NOT NULL,
            category VARCHAR(50) NOT NULL,
            link TEXT NOT NULL,
            rating TEXT NOT NULL,
            rating_count TEXT NOT NULL,
            price_original_rupee NUMERIC NOT NULL,
            price_sale_rupee NUMERIC NOT NULL,
            discount TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now() -- Waktu pembuatan
        );
        -- Membuat tabel untuk data pemasaran jika belum ada
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
        -- Membuat tabel untuk data dari Tokopedia jika belum ada
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
        -- Membuat tabel untuk data dari Lazada jika belum ada
        CREATE TABLE IF NOT EXISTS torch_lazada_clean (
            name_product TEXT NOT NULL,
            product_link TEXT NOT NULL,
            price_original INT NOT NULL,
            price_sale INT NOT NULL,
            discount TEXT NOT NULL,
            sold INT NOT NULL,
            rating TEXT NOT NULL,
            rating_count INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now()
        );
        """
        # Menggunakan koneksi ke database untu mengeksekusi query
        with dw_engine.connect() as conn:
            # Menjalankan query untuk membuat tipe data dan tabel jika belum ada
            conn.execute(text(create_table_query))
            conn.close() # Menutup koneksi setelah eksekusi selesai
        # Menyimpan data penjualan ke tabel 'sales_clean' di database
        sales_data_db.to_sql(name='sales_clean', con=dw_engine, if_exists="append", index=False)
        # Menyimpan data pemasaran e tabel 'marketing_clean' di database
        marketing_data_db.to_sql(name='marketing_clean', con=dw_engine, if_exists="append", index=False)
        # Menyimpan data dari Tokopedia ke tabel 'torch_tokped_clean' di database
        torch_tokped_data_db.to_sql(name='torch_tokped_clean', con=dw_engine, if_exists="append", index=False)
        # Menyimpan data dari Lazada ke tabel 'torch_lazada_clean' di databse
        torch_laza_data_db.to_sql(name='torch_lazada_clean', con=dw_engine, if_exists="append", index=False)


# Memulai eksekusi program
if __name__ == '__main__':
    # Membangun pipeline Luigi dengan urutan tugas yang telah ditentukan
    luigi.build([ExtractMarketingData(), # Tugas untuk mengekstrak data pemasaran
                ExtractDatabaseSalesData(), # Tugas untuk mengekstrak data penjualan dari database
                ExtractTokpedTorchData(), # Tugas untuk mengekstrak data dari Tokopedia
                ExtractTokpedTorchData(), # Tugas untuk mengekstrak data dari Lazada
                TransformSalesData(), # Tugas untuk mentransformasi data penjualan
                TransformMarketingData(), # Tugas untuk mentransformasi data pemasaran
                TransformTorchData(), # Tugas untuk mentransformasi data dari Tokopedia dan Lazada
                LoadData() # Tugas untuk memuat data ke dalam database
                ], local_scheduler=True) # Menjalankan scheduler lokal untuk mengatur eskusi tugas