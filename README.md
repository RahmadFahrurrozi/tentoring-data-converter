# Tentoring Data Converter

Alat konversi data Excel/CSV/GeoJSON ke format **Parquet** untuk diupload ke sistem **TENTORING SE2026**.

---

## Daftar Isi

1. [Persyaratan](#persyaratan)
2. [Instalasi dari Nol](#instalasi-dari-nol)
3. [Struktur Folder](#struktur-folder)
4. [Cara Konversi Data](#cara-konversi-data)
5. [Cara Cek / Debug Data](#cara-cek--debug-data)
6. [Troubleshooting](#troubleshooting)

---

## Persyaratan

- Komputer dengan sistem operasi Windows 10/11, macOS, atau Linux
- Koneksi internet (untuk instalasi pertama kali)
- File data dalam format: `.xlsx`, `.xls`, `.csv`, `.geojson`, atau `.shp`

---

## Instalasi dari Nol

### Langkah 1 -- Install Python

> Jika sudah punya Python 3.10 ke atas, lewati langkah ini.
> Cek versi Python dengan membuka Terminal / Command Prompt lalu ketik:
> ```
> python --version
> ```

1. Buka [https://www.python.org/downloads/](https://www.python.org/downloads/)
2. Klik tombol **Download Python 3.x.x** (versi terbaru)
3. Jalankan installer
4. **PENTING:** Centang opsi **"Add Python to PATH"** sebelum klik Install
5. Klik **Install Now**
6. Setelah selesai, buka Terminal baru dan cek:
   ```
   python --version
   ```
   Harus muncul versi Python, contoh: `Python 3.12.3`

---

### Langkah 2 -- Download Repo Ini

**Opsi A -- Pakai Git:**
```bash
git clone https://github.com/USERNAME/tentoring-data-converter.git
cd tentoring-data-converter
```

**Opsi B -- Download ZIP:**
1. Klik tombol **Code** -> **Download ZIP** di halaman GitHub
2. Ekstrak ZIP ke folder yang anda inginkan
3. Buka Terminal / Command Prompt, masuk ke folder tersebut:
   ```bash
   cd path/ke/folder/tentoring-data-converter
   ```

---

### Langkah 3 -- Buat Virtual Environment

Virtual environment adalah ruang isolasi agar library yang diinstall tidak bentrok dengan aplikasi lain di komputer anda.

```bash
python -m venv venv
```

Aktifkan venv:

| Sistem Operasi | Perintah |
|---|---|
| Windows (CMD) | `venv\Scripts\activate` |
| Windows (PowerShell) | `venv\Scripts\Activate.ps1` |
| macOS / Linux | `source venv/bin/activate` |

Setelah aktif, di terminal akan muncul `(venv)` di depan baris perintah:
```
(venv) C:\Users\folder\tentoring-data-converter>
```

> Setiap kali membuka terminal baru, anda harus aktifkan venv lagi dengan perintah di atas.

---

### Langkah 4 -- Install Library

Pastikan venv sudah aktif (ada tulisan `(venv)`), lalu jalankan:

```bash
pip install -r requirements.txt
```

Proses ini membutuhkan koneksi internet dan mungkin memakan waktu 2-5 menit bisa lebih cepat tergantung koneksi internet anda.
Jika berhasil, akan muncul tulisan `Successfully installed ...` di akhir.

---

## Struktur Folder

```
tentoring-data-converter/
|
|-- konversi_data.py      <- Script utama konversi
|-- cek_data.py           <- Script untuk debug/lihat isi parquet
|-- requirements.txt      <- Daftar library yang dibutuhkan
|-- README.md             <- Panduan ini
|
|-- data_input/           <- TARUH FILE DATA DATA DI SINI
|   `-- (kosong)
|
`-- output_parquet/       <- HASIL KONVERSI AKAN MUNCUL DI SINI
    `-- (kosong)
```

---

## Cara Konversi Data

### Langkah 1 -- Siapkan File Data

Taruh file data data ke dalam folder `data_input/`:

```
data_input/
|-- data-rumah-tangga.xlsx
|-- data-usaha.xlsx
`-- data-bangunan.geojson
```

Format yang didukung:

| Format | Keterangan |
|---|---|
| `.xlsx` / `.xlsm` | Excel modern |
| `.xls` | Excel lama |
| `.csv` | Comma/semicolon separated |
| `.geojson` | Data spasial GeoJSON |
| `.shp` | Shapefile (sertakan .dbf dan .prj juga) |

---

### Langkah 2 -- Edit Konfigurasi

Buka file `konversi_data.py` dengan teks editor (Notepad, VS Code, dll).

Cari bagian **KONFIGURASI** dan ubah nama file sesuai file yang anda taruh:

```python
KONFIGURASI = {

    "rumah_tangga": {
        "input": "data-rumah-tangga.xlsx",  # sesuaikan nama file
        ...
    },

    "usaha": {
        "input": "data-usaha.xlsx",          # sesuaikan nama file
        ...
    },

    "bangunan": {
        "input": "data-bangunan.geojson",    # sesuaikan nama file
        "is_spatial": True,
    },
}
```

**Jika nama kolom lat/lon berbeda** dari standar, isi secara manual:
```python
"kolom_lat":   "LATITUDE",   # nama kolom latitude di file anda
"kolom_lon":   "LONGITUDE",  # nama kolom longitude di file anda
"kolom_idsls": "ID_SLS",     # nama kolom ID SLS di file anda
```

**Jika tidak punya salah satu data**, kosongkan saja:
```python
"bangunan": {
    "input": "",  # kosong = dilewati
    ...
},
```

---

### Langkah 3 -- Jalankan Konversi

Pastikan venv aktif, lalu:

```bash
python konversi_data.py
```

Contoh output:
```
══════════════════════════════════════════════════════════
  TENTORING DATA CONVERTER
  TENTORING SE2026 -- Kabupaten Banyuwangi
══════════════════════════════════════════════════════════
  -> Folder input  : /path/ke/data_input
  -> Folder output : /path/ke/output_parquet

──────────────────────────────────────────────────────────
  RUMAH TANGGA
──────────────────────────────────────────────────────────
  -> Ukuran input  : 45.2 MB
  -> Membaca Excel: data-rumah-tangga.xlsx
  -> Jumlah baris  : 124,532 | Kolom: 18
  -> Kolom lat     : lat final
  -> Kolom lon     : long final
  OK Kolom idsls_str berhasil dinormalisasi
  OK Tersimpan     : rumah-tangga.parquet
  OK Ukuran output : 8.3 MB
  OK Baris final   : 124,489 baris
  OK Waktu proses  : 12.4 detik

══════════════════════════════════════════════════════════
  RINGKASAN HASIL
══════════════════════════════════════════════════════════
  sukses  ->  rumah_tangga
  sukses  ->  usaha
  sukses  ->  bangunan

  File .parquet tersimpan di:
     /path/ke/output_parquet

  Selesai. Upload file .parquet ke Portal -> Menu Upload Data
══════════════════════════════════════════════════════════
```

---

### Langkah 4 -- Upload ke Portal

1. Buka Portal Web TENTORING
2. Masuk ke menu **Upload Data**
3. Pilih kategori data yang sesuai
4. Upload file `.parquet` dari folder `output_parquet/`
5. Sistem Streamlit akan otomatis membaca data terbaru

---

## Cara Cek / Debug Data

Setelah konversi, anda bisa mengecek isi file parquet dengan:

```bash
python cek_data.py
```

Akan muncul menu interaktif:
```
══════════════════════════════════════════════════════════
  TENTORING DATA CONVERTER -- CEK DATA
══════════════════════════════════════════════════════════

  FILE PARQUET TERSEDIA
  OK  [1] Rumah Tangga          (8.3 MB)
  OK  [2] Usaha                 (12.1 MB)
  --  [3] Bangunan              (belum ada)

  Pilih aksi:
  [1] Ringkasan & struktur kolom
  [2] Preview baris data
  [3] Cari data (filter by keyword)
  [4] Distribusi per IDSLS
  [5] Cek koordinat (lat/lon)
  [6] Query SQL bebas
  [0] Keluar
```

### Fitur Query SQL Bebas

Dengan pilihan `[6]`, anda bisa tulis query SQL apapun. Contoh:

```sql
-- Hitung total per kecamatan
SELECT kecamatan, COUNT(*) as total
FROM data
GROUP BY kecamatan
ORDER BY total DESC;

-- Cari usaha dengan nama tertentu
SELECT * FROM data WHERE LOWER("nama final") LIKE '%bakso%';

-- Cek baris dengan koordinat null
SELECT * FROM data WHERE "lat final" IS NULL LIMIT 10;
```

---

## Troubleshooting

### `python` tidak dikenali di terminal

Python belum masuk PATH.
- Windows: reinstall Python dan centang **"Add to PATH"**
- macOS/Linux: coba pakai `python3` ganti `python`

---

### `pip install` error / gagal

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

---

### Error: `Kolom lat/lon tidak ditemukan`

Script tidak bisa mendeteksi kolom secara otomatis. Cek nama kolom di file Excel anda, lalu isi manual di KONFIGURASI:
```python
"kolom_lat": "NAMA_KOLOM_LAT_ANDA",
"kolom_lon": "NAMA_KOLOM_LON_ANDA",
```

---

### Error: `Missing geo metadata in Parquet`

File parquet lama tidak punya metadata GeoParquet. Konversi ulang dengan script ini, hasilnya akan kompatibel dengan sistem.

---

### File hasil terlalu besar (> 50 MB)

Data mungkin punya banyak kolom yang tidak perlu. Hapus kolom yang tidak dipakai di file Excel sebelum dikonversi.

---

## Pertanyaan

Hubungi admin sistem TENTORING SE2026.
