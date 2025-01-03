import pandas as pd
import re

# Fungsi untuk membersihkan nama produk
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

    tokped['sold_numeric'] = tokped['sold'].str.extract(r'(\d+)').astype(int)
    tokped['sold_info'] = tokped['sold'].apply(lambda x: '+ terjual' if '+ terjual' in x else 'Belum Ada Penjualan')

    drop_columns = ['sold', 'image_link']
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
