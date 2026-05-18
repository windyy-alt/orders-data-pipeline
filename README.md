# Anggota Kelompok:
| Nama | NRP | Jobdesk |
| --- | --- | --- |
| Muhammad Abid Baihaqi Al Faridzi | 5025241133 | - |
| Dilbina Windi Azahra | 5025241180 | - |

# Orders Pipeline

Proyek ini adalah pipeline data sederhana yang dibuat dengan Apache Airflow, Apache Spark, dan ClickHouse. Tujuan utamanya adalah mengambil data pesanan (orders) dari API eksternal, menyimpannya di data lake lokal, memprosesnya dengan Spark, lalu memasukkannya ke ClickHouse untuk analitik.

## Struktur Proyek

- `docker-compose.yml`: Konfigurasi Docker Compose untuk menjalankan Airflow, PostgreSQL, ClickHouse, dan Metabase.
- `Dockerfile`: Basis image Airflow + instalasi Java untuk mendukung Spark.
- `requirements.txt`: Dependensi Python yang dibutuhkan oleh Airflow dan skrip pipeline.
- `dags/orders_pipeline.py`: Definisi DAG Airflow yang mengatur urutan tugas pipeline.
- `dags/scripts/fetch_orders.py`: Skrip untuk menarik data dari API orders dan menyimpan hasilnya sebagai file Parquet di `data_lake`.
- `dags/scripts/process_orders_spark.py`: Skrip Spark untuk membaca data Parquet dari data lake, memproses data, dan menulis ke ClickHouse.
- `data_lake/orders/`: Folder penyimpanan file Parquet untuk data orders.
- `data_lake/products/`: Folder penyimpanan file Parquet untuk data produk yang terkait pesanan.

## Alur Kerja

1. `docker-compose up` menjalankan seluruh stack:
   - Airflow webserver dan scheduler
   - PostgreSQL sebagai metadata database Airflow
   - ClickHouse sebagai data warehouse analytic
   - Metabase untuk visualisasi

2. Airflow membaca DAG `orders_pipeline` dari `dags/orders_pipeline.py`.

3. DAG `orders_pipeline` memiliki dua task utama:
   - `fetch_orders`: menjalankan `fetch_orders.py`.
   - `process_orders_spark`: menjalankan `process_orders_spark.py`.

4. Task `fetch_orders`:
   - Memanggil API `http://96.9.212.102:8000/orders` dengan parameter `table_name=orders`.
   - Mengambil respons JSON dan memisahkan data menjadi dua tabel:
     - `orders`
     - `products`
   - Menyimpan hasil sebagai file Parquet ke:
     - `/opt/airflow/data_lake/orders/orders_<timestamp>.parquet`
     - `/opt/airflow/data_lake/products/products_<timestamp>.parquet`

5. Task `process_orders_spark`:
   - Memulai Spark session.
   - Membaca semua file Parquet di folder `data_lake/orders` dan `data_lake/products`.
   - Memilih kolom yang penting dan membersihkan nilai kosong.
   - Mengumpulkan baris Spark menjadi list tuple.
   - Membuat koneksi ClickHouse ke host `clickhouse-server` dengan user `admin` dan password `rahasia`.
   - Membuat database `analytics` jika belum ada.
   - Membuat tabel `analytics.orders` dan `analytics.orders_products` jika belum ada.
   - Mengosongkan tabel ClickHouse, lalu melakukan insert ulang data dari Parquet.
   - Menghapus file Parquet yang sudah diproses agar data lake tetap bersih.

## Tujuan Setiap File

### `docker-compose.yml`
- Menyediakan lingkungan terisolasi dengan Airflow, PostgreSQL, ClickHouse, dan Metabase.
- Airflow menggunakan `LocalExecutor`.
- `airflow-init` menyiapkan database dan membuat user admin Airflow.
- ClickHouse dan Metabase tersedia untuk analitik dan visualisasi.

### `Dockerfile`
- Menggunakan image resmi `apache/airflow:2.9.1-python3.11`.
- Menambahkan Java runtime karena `pyspark` membutuhkan Java.
- Meng-copy `requirements.txt` dan meng-install dependensi Python.

### `requirements.txt`
- `pyspark`: untuk pemrosesan data Spark.
- `clickhouse-driver`: untuk menulis hasil ke ClickHouse.
- `pandas`: untuk memproses JSON API dan menyimpan Parquet.
- `requests`: untuk mengambil data dari API.
- `pyarrow`: backend Parquet untuk Pandas.

### `dags/orders_pipeline.py`
- DAG Airflow dengan dua tugas.
- `schedule_interval='@once'` berarti pipeline dijalankan satu kali secara manual atau ketika docker-compose dijalankan.
- Menjamin eksekusi `fetch_orders` dulu, lalu `process_orders_spark`.

### `dags/scripts/fetch_orders.py`
- Menarik data sumber dari API orders.
- Melakukan flatten terhadap informasi pesanan dan produk.
- Menyimpan hasil sebagai Parquet di data lake lokal.

### `dags/scripts/process_orders_spark.py`
- Membaca data Parquet dengan Spark.
- Melakukan transformasi kolom dan membersihkan nilai kosong.
- Menyinkronkan data ke ClickHouse dengan model tabel analytics.
- Menghapus file Parquet usang setelah dimuat.

## Catatan Tambahan

- Path hardcoded di Airflow task menggunakan `/opt/airflow/dags/scripts/...` karena container mapping volume ke folder lokal.
- ClickHouse diakses pada `clickhouse-server` sesuai service name Docker Compose.
- Proses ini menggunakan pendekatan `truncate & insert` agar tabel analitik selalu fresh.
- Jika ingin menjalankan ulang pipeline, pastikan `docker-compose up --build` dijalankan dan DAG Airflow dieksekusi dari UI atau trigger manual.

## Cara Jalankan

1. Dari root proyek, jalankan:
   ```bash
   docker-compose up --build
   ```

2. Buka Airflow UI di `http://localhost:8080`.
3. Aktifkan dan jalankan DAG `orders_pipeline`.
4. Gunakan ClickHouse client atau Metabase untuk memeriksa data di database `analytics`.

## Output yang Dihasilkan

- File Parquet temporer di `data_lake/orders/` dan `data_lake/products/`.
- Tabel ClickHouse:
  - `analytics.orders`
  - `analytics.orders_products`

Semoga README ini membantu menjelaskan struktur dan alur kerja proyek orders pipeline Anda.