# Dokumentasi Proyek ETL Pipeline

## Latar Belakang Proyek

Sebagai Data Engineer di Perusahaan XYZ, saya diberi tugas untuk membuat ETL (Extract, Transform, Load) Pipeline yang robust untuk mendukung kebutuhan data dari berbagai tim dalam perusahaan. Infrastruktur data perusahaan saat ini masih dalam tahap pengembangan, sehingga diperlukan pipeline yang dapat memproses data dari berbagai sumber secara end-to-end.

## Temuan dari Requirements Gathering

- **Tim Sales** memiliki data penjualan barang di Database PostgreSQL, namun terdapat banyak nilai yang hilang dan format data yang tidak konsisten.
- **Tim Product** memiliki data harga produk elektronik dalam bentuk file CSV, namun datanya berantakan dan banyak nilai yang hilang.
- **Tim Marketing Torch** membutuhkan data produk torch untuk riset, yang saya ambil melalui web scraping dari platform Lazada dan Tokopedia.

## Struktur Proyek
```plaintext
.
├── raw-data/                               # File data mentah dan hasil pemrosesan
│   ├── extracted_marketing_data.csv       
│   ├── extracted_sales_data.csv     
│   ├── torch_lazada_raw.csv         
│   └── torch_tokped_raw.csv                  
├── source-marketing_data/                  # File data mentah marketing
│   ├── ElectronicsProductsPricingData.csv
├── transform-data                          # File data hasil transformasi
│   ├── marketing_clean.csv       
│   ├── sales_clean.csv     
│   ├── torch_lazada_clean.csv         
│   └── torch_tokped_clean.csv
├── validate-data                           # File data hasil pengecekan
│   └── validate_data.txt 
├── etl_de_project_pacmann.py               # Pipeline luigi 
├── validate_data.py                        # Validasi data
└── README.md                               # Dokumentasi proyek
```


## Solusi yang Diusulkan

### Data dari Tim Sales:
- Saya melakukan proses ekstraksi langsung dari database PostgreSQL.
- Missing values diatasi dengan metode interpolasi atau imputasi untuk menjaga kualitas data.

### Data dari Tim Product:
- Saya mengimpor data dari file CSV ke pipeline.
- Selama proses transformasi, saya menangani data yang hilang dan memperbaiki format yang tidak konsisten.

### Data untuk Tim Marketing Torch:
- Saya melakukan web scraping untuk mengumpulkan data produk torch dari Lazada dan Tokopedia.
- Data yang diperoleh akan dibersihkan dan disesuaikan untuk keperluan analisis pemasaran.

## Implementasi Pipeline ETL

Pipeline ETL ini saya implementasikan menggunakan Luigi dengan langkah-langkah berikut:

### Extract:
- Ekstraksi data dilakukan dari tiga sumber:
  - **PostgreSQL Database** untuk data penjualan tim Sales.
  - **File CSV** untuk data harga produk tim Product.
  - **Web Scraping** untuk data produk torch dari Lazada dan Tokopedia untuk tim Marketing Torch.

### Transform:
- **Data Penjualan (Sales)**:
  - Memperbaiki format tanggal dan menambahkan kolom yang hilang seperti `discount_price`.
  - Mengatasi missing values dengan interpolasi untuk data numerik dan mode untuk data kategori.
  - Menghapus duplikasi dan memperbaiki kesalahan format.

- **Data Produk (Product)**:
  - Mengonversi data harga menjadi format yang konsisten.
  - Mengatasi missing values dengan imputasi berdasarkan harga rata-rata.
  - Menambahkan kolom baru seperti `discount` dan `category`.

- **Data Marketing Torch**:
  - Menyaring data produk torch berdasarkan kategori relevan dari Lazada dan Tokopedia.
  - Membersihkan data, seperti menghapus karakter khusus atau tanda baca yang tidak diperlukan.
  - Menyusun ulang data dalam format yang lebih terstruktur (misalnya, menggabungkan nama produk dengan harga dan rating).

### Load:
- Data yang telah diproses dimuat ke dalam database PostgreSQL untuk analisis lebih lanjut.

## Cara Menjalankan Pipeline

Pastikan semua dependensi telah diinstal. Jika Anda belum memilikinya, Anda dapat membuatnya dengan cara menginstal dependensi berikut secara manual:


Data yang telah diproses akan disimpan di direktori `transform-data/` dan/atau database PostgreSQL.

## Validasi Data

Untuk memastikan kualitas data yang dimuat ke database, saya membuat skrip `validate_data.py`. Skrip ini memeriksa:

- Konsistensi format data
- Missing values
- Duplikasi data

Untuk menjalankan validasi data, gunakan perintah berikut:


## Instalasi

1. Clone repositori:

    ```
    git clone https://github.com/zulfaan/intro-to-data-engineer-project_pacmann.git
    cd intro-to-data-engineer-project_pacmann
    ```

2. Install dependensi:

    ```
    pip install luigi psycopg2 pandas requests tabulate selenium sqlalchemy
    ```

3. Siapkan database PostgreSQL dan perbarui konfigurasi koneksi di file kode.

## Desain Pipeline ETL

Pipeline ini dirancang untuk mendukung proses berikut:

- Ekstraksi data dari berbagai sumber.
- Transformasi data untuk meningkatkan kualitas dan menambahkan informasi baru.
- Pemuatan data ke dalam database PostgreSQL untuk analisis lebih lanjut.

## Skenario Pengujian

- **Tes Ekstraksi Data**: Memastikan data berhasil diambil dari setiap sumber.
- **Tes Transformasi Data**: Memastikan data yang telah diproses sesuai dengan format yang diharapkan.
- **Tes Pemuatan Data**: Memastikan data yang telah diproses berhasil dimuat ke database tanpa error.

## Kontribusi

Kontribusi sangat dihargai! Silakan kirimkan issue atau pull request untuk meningkatkan pipeline atau menambahkan fitur baru.


## Penulis

[Zulfa Nurfajar](https://www.linkedin.com/in/zulfanurfajar/)

Jika Anda memiliki pertanyaan atau saran, silakan buka issue atau hubungi saya melalui repositori ini!

