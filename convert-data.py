"""
╔══════════════════════════════════════════════════════════════════╗
║         TENTORING DATA CONVERTER                                 ║
║         Alat Konversi Data -> Parquet                            ║
║         TENTORING SE2026 -- Kabupaten Banyuwangi                 ║
╚══════════════════════════════════════════════════════════════════╝

Cara Pakai:
  1. Taruh file data kamu di folder 'data_input/'
  2. Edit bagian KONFIGURASI di bawah sesuai nama file kamu
  3. Jalankan: python konversi_data.py
  4. Ambil hasil di folder 'output_parquet/' lalu upload ke Portal
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import sys
import time

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ══════════════════════════════════════════════════════════════════
#  KONFIGURASI — SESUAIKAN DI SINI
# ══════════════════════════════════════════════════════════════════
#
#  Isi nama file input kamu. Taruh file di folder 'data_input/'.
#  Kosongkan ("") jika tidak punya data tersebut.
#
#  kolom_lat   → nama kolom latitude  (kosongkan = auto-detect)
#  kolom_lon   → nama kolom longitude (kosongkan = auto-detect)
#  kolom_idsls → nama kolom ID SLS    (kosongkan = auto-detect)
#  sheet       → nomor sheet Excel (0 = sheet pertama)
#  is_spatial  → True jika file GeoJSON/Shapefile, False jika Excel/CSV
#
# ══════════════════════════════════════════════════════════════════

KECAMATAN_BANYUWANGI = {
    "3510010": "pesanggaran",
    "3510020": "bangorejo",
    "3510030": "purwoharjo",
    "3510040": "tegaldlimo",
    "3510050": "muncar",
    "3510060": "cluring",
    "3510070": "gambiran",
    "3510080": "srono",
    "3510090": "genteng",
    "3510100": "glenmore",
    "3510110": "kalibaru",
    "3510120": "rogojampi",
    "3510130": "kabat",
    "3510140": "singojuruh",
    "3510150": "sempu",
    "3510160": "songgon",
    "3510170": "glagah",
    "3510180": "giri",
    "3510190": "wongsorejo",
    "3510200": "banyuwangi",
    "3510210": "kalipuro",
    "3510220": "siliragung",
    "3510230": "tegalsari",
    "3510240": "licin",
    "3510250": "blimbingsari",
}

KONFIGURASI = {

    # ── Rumah Tangga / KK ────────────────────────────────────────
    "rumah_tangga": {
        "input":       "KK010_050_gabungan_2sept.xlsx",
        "output":      "rumah-tangga.parquet",
        "sheet":       0,
        "kolom_lat":   "",           # auto-detect (geotag_latitude)
        "kolom_lon":   "",           # auto-detect (geotag_longitude)
        "kolom_idsls": "",           # auto-detect (level_6_full_code)
        "is_spatial":  False,
    },

    # ── Bangunan Lainnya (Tempat Ibadah, Rumah Kosong, Fasum, dll)
    "bangunan_lainnya": {
        "input":       "bangunan010_050_gabungan_2sept.xlsx",
        "output":      "bangunan-lainnya.parquet",
        "alias_output": "bangunan-terklasifikasi.parquet",
        "sheet":       0,
        "kolom_lat":   "",           # auto-detect (geotag_latitude)
        "kolom_lon":   "",           # auto-detect (geotag_longitude)
        "kolom_idsls": "",           # auto-detect (level_6_full_code)
        "is_spatial":  False,
    },

    # ── Usaha digital / bisnis digital ──────────────────────────
    "usaha_digital": {
        "input":       "",   # ← diisi jika ada file data-usaha-digital.xlsx
        "output":      "data-usaha-digital.parquet",
        "sheet":       0,
        "kolom_lat":   "",
        "kolom_lon":   "",
        "kolom_idsls": "",
        "is_spatial":  False,
    },

    # ── Usaha pertanian ─────────────────────────────────────────
    "usaha_pertanian": {
        "input":       "pertanian_gabungan_2sept.xlsx",
        "output":      "titik-usaha-pertanian.parquet",
        "sheet":       0,
        "kolom_lat":   "",           # auto-detect (geotag_latitude)
        "kolom_lon":   "",           # auto-detect (geotag_longitude)
        "kolom_idsls": "",           # auto-detect (level_6_full_code)
        "is_spatial":  False,
    },

    # ── Usaha biasa / UMKM ──────────────────────────────────────
    "usaha_biasa": {
        "input":       "usahanonA_gabungan_2sept.xlsx",
        "output":      "data-usaha-biasa-2.parquet",
        "sheet":       0,
        "kolom_lat":   "",           # auto-detect (geotag_latitude)
        "kolom_lon":   "",           # auto-detect (geotag_longitude)
        "kolom_idsls": "",           # auto-detect (level_6_full_code)
        "is_spatial":  False,
    },

    "usaha_rogojampi": {
        "input":       "",
        "output":      "usaha-rogojampi-se.parquet",
        "sheet":       0,
        "kolom_lat":   "geotag_latitude",
        "kolom_lon":   "geotag_longitude",
        "kolom_idsls": "level_6_full_code",
        "is_spatial":  False,
        "split_pertanian": True,
    },

    "bangunan": {
        "input":       "",
        "output":      "bangunan-terklasifikasi.parquet",
        "sheet":       0,
        "kolom_lat":   "",
        "kolom_lon":   "",
        "kolom_idsls": "",
        "is_spatial":  True,
    },

}

# ══════════════════════════════════════════════════════════════════
#  JANGAN UBAH DI BAWAH SINI KECUALI KAMU TAU APA YANG DILAKUKAN
# ══════════════════════════════════════════════════════════════════

WARNA_OK    = "\033[92m"
WARNA_WARN  = "\033[93m"
WARNA_ERROR = "\033[91m"
WARNA_INFO  = "\033[96m"
RESET       = "\033[0m"

def ok(msg):    print(f"  {WARNA_OK}✓{RESET} {msg}")
def warn(msg):  print(f"  {WARNA_WARN}⚠{RESET} {msg}")
def err(msg):   print(f"  {WARNA_ERROR}✗{RESET} {msg}")
def info(msg):  print(f"  {WARNA_INFO}→{RESET} {msg}")


def cari_kolom(df, kandidat: list) -> str | None:
    lower_map = {c.lower(): c for c in df.columns}
    for nama in kandidat:
        if nama.lower() in lower_map:
            return lower_map[nama.lower()]
    return None


def normalisasi_idsls(df, kolom: str):
    def _norm(v):
        if pd.isna(v):
            return None
        try:
            return str(int(float(v)))
        except (ValueError, TypeError):
            return str(v).strip()
    df["idsls_str"] = df[kolom].apply(_norm)
    return df


def ukuran_file(path: Path) -> str:
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def baca_file(path: Path, sheet=0) -> pd.DataFrame | None:
    ext = path.suffix.lower()
    try:
        if ext in (".xlsx", ".xlsm"):
            info(f"Membaca Excel: {path.name}")
            return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
        elif ext == ".xls":
            info(f"Membaca Excel (legacy .xls): {path.name}")
            return pd.read_excel(path, sheet_name=sheet, engine="xlrd")
        elif ext == ".csv":
            info(f"Membaca CSV: {path.name}")
            sample = path.read_text(encoding="utf-8", errors="replace")[:2000]
            sep = ";" if sample.count(";") > sample.count(",") else ","
            info(f"Separator terdeteksi: '{sep}'")
            return pd.read_csv(path, sep=sep, low_memory=False)
        elif ext == ".parquet":
            info(f"Membaca Parquet: {path.name}")
            return pd.read_parquet(path)
        else:
            err(f"Format tidak dikenali: {ext}")
            return None
    except Exception as e:
        err(f"Gagal membaca file: {e}")
        return None


def baca_spasial(path: Path) -> gpd.GeoDataFrame | None:
    try:
        info(f"Membaca data spasial: {path.name}")
        gdf = gpd.read_file(path).to_crs(epsg=4326)
        return gdf
    except Exception as e:
        err(f"Gagal membaca file spasial: {e}")
        return None


def proses_titik(nama: str, cfg: dict, input_dir: Path, output_dir: Path) -> bool:
    print(f"\n{'─'*58}")
    print(f"  {nama.upper().replace('_', ' ')}")
    print(f"{'─'*58}")

    path_input = input_dir / cfg["input"]
    if not path_input.exists():
        warn(f"File tidak ditemukan: {path_input}")
        warn("Pastikan file sudah ditaruh di folder 'data_input/'")
        return False

    info(f"Ukuran input  : {ukuran_file(path_input)}")
    t0 = time.time()

    df = baca_file(path_input, cfg["sheet"])
    if df is None:
        return False

    info(f"Jumlah baris  : {len(df):,} | Kolom: {len(df.columns)}")

    # Auto-detect kolom koordinat & ID SLS
    kandidat_lat  = ["lat final", "latitude", "geotag_latitude", "lat", "y", "Lat", "LATITUDE"]
    kandidat_lon  = ["long final", "longitude", "geotag_longitude", "lon", "long", "x", "Lon", "LONGITUDE"]
    kandidat_sls  = ["idsls final", "level_6_full_code", "idsls", "id_sls", "IDSLS", "kode_sls", "sls"]

    kolom_lat  = cfg["kolom_lat"]   or cari_kolom(df, kandidat_lat)
    kolom_lon  = cfg["kolom_lon"]   or cari_kolom(df, kandidat_lon)
    kolom_sls  = cfg["kolom_idsls"] or cari_kolom(df, kandidat_sls)

    if not kolom_lat or not kolom_lon:
        err("Kolom lat/lon tidak ditemukan secara otomatis!")
        err(f"Kolom tersedia: {list(df.columns)}")
        err("→ Isi 'kolom_lat' dan 'kolom_lon' di KONFIGURASI secara manual.")
        return False

    info(f"Kolom lat     : {kolom_lat}")
    info(f"Kolom lon     : {kolom_lon}")
    info(f"Kolom idsls   : {kolom_sls or '(tidak ditemukan, diisi null)'}")

    # Rename ke nama standar sistem
    rename_map = {kolom_lat: "lat final", kolom_lon: "long final"}
    if kolom_sls and kolom_sls != "idsls final":
        rename_map[kolom_sls] = "idsls final"
    
    # Auto-map nama usaha & alamat jika ada
    kandidat_nama = ["nama final", "nama_usaha", "nama_usaha_edit", "nama_komersial", "nama_principal"]
    kolom_nama = cari_kolom(df, kandidat_nama)
    if kolom_nama and "nama final" not in df.columns:
        df["nama final"] = df[kolom_nama]
        info(f"Kolom 'nama final' disalin dari '{kolom_nama}'")

    kandidat_alamat = ["alamat final", "alamat_usaha_view", "alamat", "alamat_usaha"]
    kolom_alamat = cari_kolom(df, kandidat_alamat)
    if kolom_alamat and "alamat final" not in df.columns:
        df["alamat final"] = df[kolom_alamat]
        info(f"Kolom 'alamat final' disalin dari '{kolom_alamat}'")

    # Auto-map nama KK jika ada (khusus data rumah tangga/KK)
    if any(k in nama.lower() for k in ["rumah_tangga", "kk", "rt"]):
        kandidat_kk = ["nama_kk", "nama_principal", "nama_kepala_keluarga"]
        kolom_kk = cari_kolom(df, kandidat_kk)
        if kolom_kk and "nama_kk" not in df.columns:
            df["nama_kk"] = df[kolom_kk]
            info(f"Kolom 'nama_kk' disalin dari '{kolom_kk}'")

    # Auto-map kategori/status bangunan jika ada kode_bang_value
    kandidat_kode_bang = ["kode_bang_value", "kode_bang"]
    kolom_kode_bang = cari_kolom(df, kandidat_kode_bang)
    if kolom_kode_bang:
        kode_map = {
            4: "Tempat Ibadah",
            5: "Fasilitas Umum / Kantor",
            6: "Bangunan / Rumah Kosong",
            7: "Usaha Tanpa Bangunan",
            8: "Sosial / Pesantren / Khusus",
            9: "Bangunan Lainnya",
        }
        df["kategori_bangunan"] = pd.to_numeric(df[kolom_kode_bang], errors="coerce").map(kode_map).fillna("Bangunan Lainnya")
        if "status_bangunan" not in df.columns:
            df["status_bangunan"] = df["kategori_bangunan"]
        info("Kolom 'kategori_bangunan' & 'status_bangunan' berhasil dipetakan dari 'kode_bang_value'")

    df = df.rename(columns=rename_map)

    # Bersihkan koordinat invalid
    sebelum = len(df)
    df["lat final"]  = pd.to_numeric(df["lat final"],  errors="coerce")
    df["long final"] = pd.to_numeric(df["long final"], errors="coerce")
    df = df.dropna(subset=["lat final", "long final"])
    df = df[df["lat final"].between(-90, 90)]
    df = df[df["long final"].between(-180, 180)]
    terhapus = sebelum - len(df)
    if terhapus > 0:
        warn(f"{terhapus:,} baris dihapus (koordinat kosong / invalid)")

    # Normalisasi idsls
    if "idsls final" in df.columns:
        df = normalisasi_idsls(df, "idsls final")
        ok("Kolom idsls_str berhasil dinormalisasi")
    else:
        warn("Kolom idsls tidak ditemukan — idsls_str diisi null")
        df["idsls_str"] = None

    # Sanitasi kolom mixed-type
    kolom_mixed = []
    for col in df.select_dtypes(include="object").columns:
        if df[col].apply(lambda x: not isinstance(x, (str, type(None)))).any():
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)
            kolom_mixed.append(col)
    if kolom_mixed:
        warn(f"Kolom mixed-type dikonversi ke string: {kolom_mixed}")

    # Simpan output utama di root output_parquet/
    path_output_full = output_dir / cfg["output"]
    df.to_parquet(path_output_full, index=False, engine="pyarrow", compression="snappy")

    # Simpan salinan alias_output jika didefinisikan (misal: bangunan-terklasifikasi.parquet)
    if cfg.get("alias_output"):
        path_alias = output_dir / cfg["alias_output"]
        df.to_parquet(path_alias, index=False, engine="pyarrow", compression="snappy")
        info(f"Salinan alias tersimpan: {path_alias.name}")

    # Group & simpan per kecamatan (Opsional, hanya jika "split_kecamatan": True di KONFIGURASI)
    if cfg.get("split_kecamatan", False) and "idsls_str" in df.columns:
        df["_kode_kec"] = df["idsls_str"].astype(str).str[:7]
        kec_list = [k for k in df["_kode_kec"].unique() if k and k != "None" and len(k) == 7]

        if kec_list:
            info(f"Membagi output Parquet per kecamatan ({len(kec_list)} kecamatan terdeteksi)...")
            for kode_kec in kec_list:
                df_kec = df[df["_kode_kec"] == kode_kec].drop(columns=["_kode_kec"]).copy()
                nama_folder = KECAMATAN_BANYUWANGI.get(kode_kec, f"kec_{kode_kec}")
                folder_kec = output_dir / f"kec_{kode_kec}_{nama_folder}"
                folder_kec.mkdir(parents=True, exist_ok=True)

                # Simpan full per kecamatan
                path_kec_full = folder_kec / cfg["output"]
                df_kec.to_parquet(path_kec_full, index=False, engine="pyarrow", compression="snappy")

                # Split otomatis Pertanian vs Biasa per kecamatan
                if cfg.get("split_pertanian") and "kategori" in df_kec.columns:
                    df_pertanian = df_kec[df_kec["kategori"].astype(str).str.upper() == "A"].copy()
                    df_biasa     = df_kec[df_kec["kategori"].astype(str).str.upper() != "A"].copy()

                    p_pert = folder_kec / "titik-usaha-pertanian.parquet"
                    p_bias = folder_kec / "data-usaha-biasa.parquet"

                    df_pertanian.to_parquet(p_pert, index=False, engine="pyarrow", compression="snappy")
                    df_biasa.to_parquet(p_bias, index=False, engine="pyarrow", compression="snappy")

                    ok(f"Folder '{folder_kec.name}' Tersimpan:")
                    ok(f"  ├── {p_pert.name:<28} ({len(df_pertanian):,} baris, {ukuran_file(p_pert)})")
                    ok(f"  ├── {p_bias.name:<28} ({len(df_biasa):,} baris, {ukuran_file(p_bias)})")
                    ok(f"  └── {path_kec_full.name:<28} ({len(df_kec):,} baris, {ukuran_file(path_kec_full)})")
                else:
                    ok(f"Folder '{folder_kec.name}' Tersimpan: {path_kec_full.name} ({len(df_kec):,} baris)")

        if "_kode_kec" in df.columns:
            df = df.drop(columns=["_kode_kec"])

    elapsed = time.time() - t0
    ok(f"Tersimpan Full Utama: {path_output_full.name}")
    ok(f"Ukuran output       : {ukuran_file(path_output_full)}")
    ok(f"Baris final         : {len(df):,} baris")
    ok(f"Waktu proses        : {elapsed:.1f} detik")
    return True


def proses_bangunan(nama: str, cfg: dict, input_dir: Path, output_dir: Path) -> bool:
    print(f"\n{'─'*58}")
    print(f"  {nama.upper().replace('_', ' ')}")
    print(f"{'─'*58}")

    path_input = input_dir / cfg["input"]
    if not path_input.exists():
        warn(f"File tidak ditemukan: {path_input}")
        warn("Pastikan file sudah ditaruh di folder 'data_input/'")
        return False

    info(f"Ukuran input  : {ukuran_file(path_input)}")
    t0 = time.time()

    gdf = baca_spasial(path_input)

    if gdf is None:
        warn("Mencoba baca sebagai tabel biasa dengan kolom lat/lon...")
        df = baca_file(path_input, cfg["sheet"])
        if df is None:
            return False
        kandidat_lat = ["latitude", "lat", "y"]
        kandidat_lon = ["longitude", "lon", "long", "x"]
        kolom_lat = cfg["kolom_lat"] or cari_kolom(df, kandidat_lat)
        kolom_lon = cfg["kolom_lon"] or cari_kolom(df, kandidat_lon)
        if not kolom_lat or not kolom_lon:
            err("Tidak ada kolom geometry maupun lat/lon. Tidak bisa diproses.")
            return False
        df[kolom_lat] = pd.to_numeric(df[kolom_lat], errors="coerce")
        df[kolom_lon] = pd.to_numeric(df[kolom_lon], errors="coerce")
        df = df.dropna(subset=[kolom_lat, kolom_lon])
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[kolom_lon], df[kolom_lat]),
            crs="EPSG:4326"
        )

    info(f"Jumlah fitur  : {len(gdf):,} | CRS: {gdf.crs}")

    # Normalisasi idsls
    kandidat_sls = ["idsls_str", "idsls", "id_sls", "IDSLS", "kode_sls"]
    kolom_sls = cfg["kolom_idsls"] or cari_kolom(gdf, kandidat_sls)
    if kolom_sls:
        gdf = normalisasi_idsls(gdf, kolom_sls)
        ok(f"Kolom idsls_str dari '{kolom_sls}'")
    else:
        warn("Kolom idsls tidak ditemukan — idsls_str diisi null")
        gdf["idsls_str"] = None

    if "color" not in gdf.columns:
        warn("Kolom 'color' tidak ada — diisi default abu-abu")
        gdf["color"] = [[128, 128, 128, 160]] * len(gdf)

    # Simpan sebagai GeoParquet
    path_output = output_dir / cfg["output"]
    gdf.to_parquet(path_output, index=False, engine="pyarrow", compression="snappy")

    elapsed = time.time() - t0
    ok(f"Tersimpan     : {path_output.name}")
    ok(f"Ukuran output : {ukuran_file(path_output)}")
    ok(f"Fitur final   : {len(gdf):,}")
    ok(f"Waktu proses  : {elapsed:.1f} detik")
    return True


def auto_scan_data_input(input_dir: Path) -> list[tuple[str, dict]]:
    valid_exts = {".xlsx", ".xlsm", ".xls", ".csv", ".geojson", ".shp"}
    files = [f for f in sorted(input_dir.iterdir()) if f.is_file() and f.suffix.lower() in valid_exts]
    
    # Map input file name -> (nama, cfg) from KONFIGURASI if defined
    konf_map = {cfg["input"].strip(): (nama, cfg) for nama, cfg in KONFIGURASI.items() if cfg.get("input")}
    
    daftar_job = []
    for file_path in files:
        fname = file_path.name
        if fname in konf_map:
            nama, cfg = konf_map[fname]
            daftar_job.append((nama, cfg))
        else:
            stem = file_path.stem
            is_spatial = file_path.suffix.lower() in (".geojson", ".shp")
            cfg_default = {
                "input":       fname,
                "output":      f"{stem}.parquet",
                "sheet":       0,
                "kolom_lat":   "",
                "kolom_lon":   "",
                "kolom_idsls": "",
                "is_spatial":  is_spatial,
            }
            daftar_job.append((stem, cfg_default))
            
    return daftar_job


def pilih_menu_interaktif(daftar_job: list[tuple[str, dict]]) -> list[tuple[str, dict]]:
    if not daftar_job:
        warn("Tidak ada file data (.xlsx, .csv, .geojson) di folder data_input/!")
        return []

    print("\n" + "═"*58)
    print("  PILIH FILE DATA YANG INGIN DIPROSES")
    print("═"*58)
    print(f"  [0]  SEMUA FILE DI FOLDER 'data_input/' ({len(daftar_job)} file)")
    
    for idx, (nama, cfg) in enumerate(daftar_job, 1):
        filename = cfg['input']
        outname  = cfg['output']
        print(f"  [{idx}]  {filename:<42} -> {outname}")
    
    print("═"*58)
    try:
        pilihan = input("\n  Masukkan pilihan (0 untuk semua, atau nomor file misal: 1, 2) [Default=0]: ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)

    if not pilihan or pilihan == "0" or pilihan.lower() == "all":
        return daftar_job

    selected_jobs = []
    for part in pilihan.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            num = int(part)
            if 1 <= num <= len(daftar_job):
                job = daftar_job[num - 1]
                if job not in selected_jobs:
                    selected_jobs.append(job)
            else:
                warn(f"Pilihan nomor {num} di luar jangkauan menu.")
        elif part.lower() in ("q", "exit"):
            sys.exit(0)

    if not selected_jobs:
        warn("Pilihan tidak valid, memproses semua file sebagai default...")
        return daftar_job

    return selected_jobs


def main():
    print("\n" + "═"*58)
    print("  TENTORING DATA CONVERTER")
    print("  TENTORING SE2026 -- Kabupaten Banyuwangi")
    print("═"*58)

    base_dir   = Path(__file__).parent
    input_dir  = base_dir / "data_input"
    output_dir = base_dir / "output_parquet"

    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    info(f"Folder input  : {input_dir.resolve()}")
    info(f"Folder output : {output_dir.resolve()}")

    daftar_job    = auto_scan_data_input(input_dir)
    selected_jobs = pilih_menu_interaktif(daftar_job)
    if not selected_jobs:
        return

    hasil = {}
    for nama, cfg in selected_jobs:
        if not cfg["input"]:
            warn(f"[{nama}] Input kosong, dilewati.")
            hasil[nama] = "dilewati"
            continue

        if cfg["is_spatial"]:
            sukses = proses_bangunan(nama, cfg, input_dir, output_dir)
        else:
            sukses = proses_titik(nama, cfg, input_dir, output_dir)

        hasil[nama] = "✓ sukses" if sukses else "✗ gagal"

    # Ringkasan akhir
    print(f"\n{'═'*58}")
    print("  RINGKASAN HASIL")
    print(f"{'═'*58}")
    for nama, status in hasil.items():
        if "sukses" in status:
            warna = WARNA_OK
        elif "lewati" in status:
            warna = WARNA_WARN
        else:
            warna = WARNA_ERROR
        print(f"  {warna}{status}{RESET}  ->  {nama}")

    semua_sukses = all("sukses" in s or "lewati" in s for s in hasil.values())
    print(f"\n  File .parquet tersimpan di:")
    print(f"     {output_dir.resolve()}")
    if semua_sukses:
        print(f"\n  {WARNA_OK}Selesai. Upload file .parquet ke Portal -> Menu Upload Data{RESET}")
    else:
        print(f"\n  {WARNA_WARN}Ada proses yang gagal. Cek pesan error di atas.{RESET}")
    print(f"{'═'*58}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDibatalkan oleh user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n{WARNA_ERROR}Error tidak terduga: {e}{RESET}")
        import traceback
        traceback.print_exc()
        sys.exit(1)