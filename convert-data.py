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

KONFIGURASI = {

    "rumah_tangga": {
        "input":       "data-rumah-tangga.xlsx",   # ← ganti nama file
        "output":      "rumah-tangga.parquet",
        "sheet":       0,
        "kolom_lat":   "",
        "kolom_lon":   "",
        "kolom_idsls": "",
        "is_spatial":  False,
    },

    "usaha": {
        "input":       "data-usaha.xlsx",           # ← ganti nama file
        "output":      "uji-coba-peta.parquet",
        "sheet":       0,
        "kolom_lat":   "",
        "kolom_lon":   "",
        "kolom_idsls": "",
        "is_spatial":  False,
    },

    "bangunan": {
        "input":       "data-bangunan.geojson",     # ← ganti nama file
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

    # Auto-detect kolom koordinat
    kandidat_lat  = ["lat final", "latitude", "lat", "y", "Lat", "LATITUDE"]
    kandidat_lon  = ["long final", "longitude", "lon", "long", "x", "Lon", "LONGITUDE"]
    kandidat_sls  = ["idsls final", "idsls", "id_sls", "IDSLS", "kode_sls", "sls"]

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

    # Simpan
    path_output = output_dir / cfg["output"]
    df.to_parquet(path_output, index=False, engine="pyarrow", compression="snappy")

    elapsed = time.time() - t0
    ok(f"Tersimpan     : {path_output.name}")
    ok(f"Ukuran output : {ukuran_file(path_output)}")
    ok(f"Baris final   : {len(df):,} baris")
    ok(f"Waktu proses  : {elapsed:.1f} detik")
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

    hasil = {}
    for nama, cfg in KONFIGURASI.items():
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