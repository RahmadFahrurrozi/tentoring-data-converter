"""
╔══════════════════════════════════════════════════════════════════╗
║  SPLIT PARQUET PER KECAMATAN → JSON                              ║
║  TENTORING SE2026 -- Kabupaten Banyuwangi                        ║
╚══════════════════════════════════════════════════════════════════╝

Memecah file parquet besar menjadi JSON per kecamatan, sesuai
struktur folder di Supabase Storage (kec_XXXXXXX.json).

Cara Pakai:
  1. Pastikan file parquet sudah ada di folder output_parquet/
  2. Edit KONFIGURASI di bawah
  3. Jalankan: python split_per_kecamatan.py
  4. Upload isi folder hasil ke folder yang sesuai di Supabase
"""

import pandas as pd
from pathlib import Path
import time

# ══════════════════════════════════════════════════════════════════
#  KONFIGURASI — SESUAIKAN DI SINI
# ══════════════════════════════════════════════════════════════════

JOBS = [
    {
        "input":         "titik-usaha-pertanian.parquet",
        "output_folder": "split_usaha_pertanian",
        # Kolom 'iddesa' berisi kode desa 10 digit, ambil 7 digit pertama = kode kecamatan
        "kolom_wilayah": "iddesa",
        "digit_kec":     7,        # 7 digit pertama = kode kecamatan
        "prefix":        "kec_",
    },
    {
        "input":         "data-usaha-digital.parquet",
        "output_folder": "split_usaha_digital",
        # Kolom 'kdkec' sudah berisi kode kecamatan langsung
        "kolom_wilayah": "kdkec",
        "digit_kec":     None,     # None = pakai nilai kolom apa adanya
        "prefix":        "kec_",
    },
    # Tambah job lain di sini kalau perlu...
]

# ══════════════════════════════════════════════════════════════════
#  JANGAN UBAH DI BAWAH SINI
# ══════════════════════════════════════════════════════════════════

BASE_DIR  = Path(__file__).parent
INPUT_DIR = BASE_DIR / "output_parquet"

WARNA_OK    = "\033[92m"
WARNA_WARN  = "\033[93m"
WARNA_ERROR = "\033[91m"
WARNA_INFO  = "\033[96m"
WARNA_BOLD  = "\033[1m"
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


def split_satu_job(job: dict) -> bool:
    input_path   = INPUT_DIR / job["input"]
    output_dir   = BASE_DIR / job["output_folder"]
    kolom        = job["kolom_wilayah"]
    digit_kec    = job["digit_kec"]
    prefix       = job["prefix"]

    print(f"\n{'─'*58}")
    print(f"  INPUT  : {job['input']}")
    print(f"  OUTPUT : {job['output_folder']}/")
    print(f"{'─'*58}")

    if not input_path.exists():
        err(f"File tidak ditemukan: {input_path}")
        err("Jalankan dulu konversi_data.py untuk membuat file parquet.")
        return False

    output_dir.mkdir(exist_ok=True)

    t0 = time.time()
    info(f"Membaca {input_path.name} ({ukuran_file(input_path)})...")
    df = pd.read_parquet(input_path)
    info(f"Total baris: {len(df):,}")

    if kolom not in df.columns:
        err(f"Kolom '{kolom}' tidak ditemukan!")
        err(f"Kolom tersedia: {list(df.columns)}")
        return False

    # Buat kolom kode kecamatan
    # digit_kec=None → kolom sudah berisi kode kecamatan langsung (misal: kdkec)
    # digit_kec=N    → ambil N digit pertama dari kode wilayah (misal: iddesa → 7 digit)
    if digit_kec is None:
        df["_kode_kec"] = df[kolom].astype(str).str.strip()
    else:
        df["_kode_kec"] = df[kolom].astype(str).str[:digit_kec]

    kec_list  = sorted(df["_kode_kec"].unique())
    total_kec = len(kec_list)
    info(f"Kecamatan ditemukan: {total_kec}")
    print()

    hasil = []
    for i, kode_kec in enumerate(kec_list, 1):
        chunk     = df[df["_kode_kec"] == kode_kec].drop(columns=["_kode_kec"])
        nama_file = f"{prefix}{kode_kec}.json"
        out_path  = output_dir / nama_file

        # Ganti NaN/None → null agar JSON valid (NaN bukan valid JSON)
        chunk = chunk.where(chunk.notna(), other=None)

        # orient="records" → [{...}, {...}, ...] langsung bisa dipakai frontend
        # force_ascii=False → karakter huruf Indonesia tidak di-escape
        chunk.to_json(out_path, orient="records", force_ascii=False, indent=None)
        hasil.append((kode_kec, len(chunk), ukuran_file(out_path)))

        bar = "█" * int(i / total_kec * 30)
        print(f"\r  [{bar:<30}] {i}/{total_kec}  {nama_file}", end="", flush=True)

    print()

    elapsed = time.time() - t0

    print(f"\n  {'Kecamatan':<15} {'Baris':>10} {'Ukuran':>10}")
    print(f"  {'─'*37}")
    total_baris = 0
    for kode, baris, ukuran in hasil:
        print(f"  {kode:<15} {baris:>10,} {ukuran:>10}")
        total_baris += baris
    print(f"  {'─'*37}")
    print(f"  {'TOTAL':<15} {total_baris:>10,}")

    print()
    ok(f"Selesai: {total_kec} file JSON di folder '{job['output_folder']}/'")
    ok(f"Waktu proses: {elapsed:.1f} detik")
    return True


def main():
    print("\n" + "═"*58)
    print("  SPLIT PER KECAMATAN → JSON")
    print("  TENTORING SE2026 -- Kabupaten Banyuwangi")
    print("═"*58)

    hasil_semua = {}
    for job in JOBS:
        sukses = split_satu_job(job)
        hasil_semua[job["input"]] = "✓ sukses" if sukses else "✗ gagal"

    print(f"\n{'═'*58}")
    print("  RINGKASAN")
    print(f"{'═'*58}")
    for nama, status in hasil_semua.items():
        warna = WARNA_OK if "sukses" in status else WARNA_ERROR
        print(f"  {warna}{status}{RESET}  →  {nama}")

    print(f"\n  Langkah selanjutnya:")
    print(f"  Upload isi masing-masing folder ke Supabase Storage:")
    for job in JOBS:
        folder_supabase = job["output_folder"].replace("split_", "")
        print(f"  {WARNA_INFO}→{RESET} {job['output_folder']}/ → bucket/{folder_supabase}/")
    print(f"{'═'*58}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDibatalkan.")
    except Exception as e:
        print(f"\n\033[91mError: {e}\033[0m")
        import traceback
        traceback.print_exc()