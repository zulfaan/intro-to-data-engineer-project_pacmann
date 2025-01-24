# Dokumentasi Proyek ETL Pipeline

## Latar Belakang Proyek

Sebagai Data Engineer di Perusahaan XYZ, saya diberi tugas untuk membuat ETL (Extract, Transform, Load) Pipeline yang robust untuk mendukung kebutuhan data dari berbagai tim dalam perusahaan. Infrastruktur data perusahaan saat ini masih dalam tahap pengembangan, sehingga diperlukan pipeline yang dapat memproses data dari berbagai sumber secara end-to-end.

## Temuan dari Requirements Gathering

- **Tim Sales** memiliki data penjualan barang di Database PostgreSQL, namun terdapat banyak nilai yang hilang dan format data yang tidak konsisten.
- **Tim Marketing** memiliki data harga produk elektronik dalam bentuk file CSV, namun datanya berantakan dan banyak nilai yang hilang.
- **Tim Data Scientist Torch** membutuhkan data produk torch untuk riset, yang saya ambil melalui web scraping dari platform Lazada dan Tokopedia.

## Solusi yang Diusulkan

### 1. Data dari Tim Sales
- Data diambil langsung dari database PostgreSQL menggunakan pipeline otomatis.
- Missing values diatasi dengan teknik interpolasi atau imputasi untuk menjaga kualitas data dan konsistensi.

### 2. Data dari Tim Marketing
- Data diimpor dari file CSV yang kemudian diproses melalui pipeline.
- Selama transformasi, dilakukan:
  - Penanganan missing values.
  - Perbaikan format data yang tidak konsisten.

### 3. Data untuk Tim Data Scientist Torch
- Data produk Torch dikumpulkan melalui web scraping dari platform Lazada dan Tokopedia.
- Proses pembersihan dan penyesuaian data dilakukan untuk memastikan data siap digunakan dalam analisis pemasaran.

# Desain ETL Pipeline

## Extraction (Extract)

### Marketing Data:
- Data diambil dari file CSV lokal `ElectronicsProductsPricingData.csv` yang ada dalam folder `source-marketing_data`.
- Menggunakan class `ExtractMarketingData` untuk membaca dan menyimpan data ke file `extracted_marketing_data.csv`.

### Sales Data:
- Data diambil dari tabel database PostgreSQL bernama `amazon_sales_data`.
- Menggunakan class `ExtractDatabaseSalesData` untuk menjalankan query SQL dan menyimpan hasilnya ke file `extracted_sales_data.csv`.

### Product Data dari Tokopedia:
- Data diambil menggunakan scraping dengan Selenium dari halaman produk Torch di Tokopedia.
- Class `ExtractTokpedTorchData` menyimpan hasil ekstrak ke file `torch_tokped_raw.csv`.

### Product Data dari Lazada:
- Data diambil menggunakan scraping dengan Selenium dari halaman produk Torch di Lazada.
- Class `ExtractLazadaTorchData` menyimpan hasil ekstrak ke file `torch_lazada_raw.csv`.


## Transformation (Transform)

## Validasi Data

Untuk memastikan kualitas data yang dimuat ke database, saya membuat skrip `validate_data.py`. Skrip ini memeriksa:

- Konsistensi format data
- Missing values
- Duplikasi data

Untuk menjalankan validasi data, gunakan perintah berikut:

```bash
python validate_data.py
```

### Sales Data:
Transformasi dilakukan melalui class `TransformSalesData`, termasuk:
- Pembersihan nama produk.
- Konversi format harga (dalam rupee) dan kalkulasi diskon.
- Penyederhanaan kategori produk menggunakan mapping tertentu.
- Hasil disimpan ke file `sales_clean.csv`.

### Marketing Data:
Transformasi dilakukan melalui class `TransformMarketingData`, termasuk:
- Pembersihan data tanggal, berat produk, ketersediaan, kondisi produk, dan informasi pengiriman.
- Kalkulasi diskon berdasarkan harga maksimum dan minimum.
- Hasil disimpan ke file `marketing_clean.csv`.

### Product Data Tokopedia & Lazada:
Pembersihan dan transformasi data dilakukan melalui class `TransformTorchData`, seperti:
- Pembersihan harga, rating, dan diskon.
- Konversi format data untuk keseragaman.
- Pembuangan kolom yang tidak diperlukan.
- Hasil disimpan ke file `torch_tokped_clean.csv` dan `torch_lazada_clean.csv`.

---

## Loading (Load)

- Data dari tahap transformasi dimuat ke database PostgreSQL bernama `de_project_pacmann`.
- Kelas `LoadData` menangani proses pembuatan tabel database jika belum ada, termasuk:
  - Tabel untuk data penjualan (`sales_clean`).
  - Tabel untuk data pemasaran (`marketing_clean`).
  - Tabel untuk data Tokopedia (`torch_tokped_clean`).
  - Tabel untuk data Lazada (`torch_lazada_clean`).

- Data dimasukkan ke tabel menggunakan metode `.to_sql()` dari Pandas.


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
├── run_etl.sh                              # Script untuk menjalankan ETL secara manual
├── logfile                                 # Log file proses ETL
└── README.md                               # Dokumentasi proyek
```


## Cara Menjalankan Pipeline

### Secara Manual:
1. Pastikan semua dependensi telah diinstal.
2. Jalankan script `run_etl.sh` untuk memulai pipeline ETL.

   ```bash
   ./run_etl.sh
   ```

### Secara Terjadwal:
Pipeline ini dijalankan secara otomatis setiap 2 menit dengan menggunakan cron job. Berikut adalah konfigurasi crontab yang digunakan:

```plaintext
*/2 * * * * /path/to/project/run_etl.sh >> /path/to/project/logfile 2>&1
```

Untuk memastikan cron berjalan:
1. Tambahkan jadwal cron dengan menjalankan perintah:

   ```bash
   crontab -e
   ```

2. Masukkan konfigurasi di atas ke dalam file crontab.

3. Periksa status cron dengan perintah:

   ```bash
   sudo service cron status
   ```

4. Hasil eksekusi akan dicatat di file `logfile`.


## Instalasi

1. Clone repositori:

    ```bash
    git clone https://github.com/zulfaan/intro-to-data-engineer-project_pacmann.git
    cd intro-to-data-engineer-project_pacmann
    ```

2. Siapkan database PostgreSQL dan perbarui konfigurasi koneksi di file kode.

## Kontribusi

Kontribusi sangat dihargai! Silakan kirimkan issue atau pull request untuk meningkatkan pipeline atau menambahkan fitur baru.

## Penulis

[Zulfa Nurfajar](https://www.linkedin.com/in/zulfanurfajar/)

Jika Anda memiliki pertanyaan atau saran, silakan buka issue atau hubungi saya melalui repositori ini!