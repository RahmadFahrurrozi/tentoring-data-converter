import time
import sys
from pathlib import Path
import pandas as pd

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

WARNA_OK    = "\033[92m"
WARNA_WARN  = "\033[93m"
WARNA_ERROR = "\033[91m"
WARNA_INFO  = "\033[96m"
RESET       = "\033[0m"

def ok(msg):    print(f"  {WARNA_OK}✓{RESET} {msg}")
def warn(msg):  print(f"  {WARNA_WARN}⚠{RESET} {msg}")
def err(msg):   print(f"  {WARNA_ERROR}✗{RESET} {msg}")
def info(msg):  print(f"  {WARNA_INFO}→{RESET} {msg}")

def ukuran_file(path: Path) -> str:
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

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

def main():
    print("\n" + "═"*58)
    print("  KONVERSI DATA SIDOARJO (PERTANIAN vs USAHA BIASA)")
    print("═"*58)

    base_dir   = Path(__file__).parent
    input_file = base_dir / "data_input" / "data-se-sidoarjo-merge-(usaha-biasa).xlsx"
    output_dir = base_dir / "output_parquet" / "sidoarjo"

    if not input_file.exists():
        err(f"File tidak ditemukan: {input_file}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    
    t0 = time.time()
    info(f"Membaca file Excel: {input_file.name} ({ukuran_file(input_file)})...")
    df = pd.read_excel(input_file)
    info(f"Total baris awal : {len(df):,}")

    # Standardize column mappings
    rename_map = {}
    if "geotag_latitude" in df.columns:
        rename_map["geotag_latitude"] = "lat final"
    if "geotag_longitude" in df.columns:
        rename_map["geotag_longitude"] = "long final"
    if "idsubsls" in df.columns:
        rename_map["idsubsls"] = "idsls final"
    if "nama_usaha_keluarga" in df.columns:
        rename_map["nama_usaha_keluarga"] = "nama final"

    df = df.rename(columns=rename_map)

    # Clean invalid coordinates
    sebelum = len(df)
    df["lat final"]  = pd.to_numeric(df["lat final"],  errors="coerce")
    df["long final"] = pd.to_numeric(df["long final"], errors="coerce")
    df = df.dropna(subset=["lat final", "long final"])
    df = df[df["lat final"].between(-90, 90)]
    df = df[df["long final"].between(-180, 180)]
    terhapus = sebelum - len(df)
    if terhapus > 0:
        warn(f"{terhapus:,} baris dihapus karena koordinat null/invalid")

    # Normalize IDSLS
    if "idsls final" in df.columns:
        df = normalisasi_idsls(df, "idsls final")
        ok("Kolom idsls_str berhasil dinormalisasi")
    else:
        df["idsls_str"] = None

    # Handle mixed-type columns for pyarrow compatibility
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)

    # Split by Category ('A' vs non-'A')
    cat_series = df["Category"].fillna("").astype(str).str.strip().str.upper()
    df_pertanian = df[cat_series == "A"].copy()
    df_biasa     = df[cat_series != "A"].copy()

    # Define output file paths
    file_pertanian = output_dir / "pertanian-sidoarjo.parquet"
    file_biasa     = output_dir / "usaha-biasa-sidoarjo.parquet"

    info(f"Menyimpan {file_pertanian.name} ({len(df_pertanian):,} baris)...")
    df_pertanian.to_parquet(file_pertanian, index=False, engine="pyarrow", compression="snappy")

    info(f"Menyimpan {file_biasa.name} ({len(df_biasa):,} baris)...")
    df_biasa.to_parquet(file_biasa, index=False, engine="pyarrow", compression="snappy")

    # Optional alias copies for alternative standard naming
    df_pertanian.to_parquet(output_dir / "titik-usaha-pertanian-sidoarjo.parquet", index=False, engine="pyarrow", compression="snappy")
    df_biasa.to_parquet(output_dir / "data-usaha-biasa-sidoarjo.parquet", index=False, engine="pyarrow", compression="snappy")

    elapsed = time.time() - t0

    print(f"\n{'═'*58}")
    print("  HASIL KONVERSI SIDOARJO")
    print(f"{'═'*58}")
    ok(f"Folder Output     : {output_dir.resolve()}")
    ok(f"Pertanian Sidoarjo: {file_pertanian.name:<30} ({len(df_pertanian):,} baris, {ukuran_file(file_pertanian)})")
    ok(f"Usaha Biasa       : {file_biasa.name:<30} ({len(df_biasa):,} baris, {ukuran_file(file_biasa)})")
    ok(f"Waktu Proses      : {elapsed:.1f} detik")
    print(f"{'═'*58}\n")

if __name__ == "__main__":
    main()
