"""
╔══════════════════════════════════════════════════════════════════╗
║  CEK DATA PARQUET -- TENTORING DATA CONVERTER                    ║
║  Gunakan script ini untuk melihat & debug isi file .parquet      ║
╚══════════════════════════════════════════════════════════════════╝

Cara Pakai:
  python cek_data.py
"""

import duckdb
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "output_parquet"

FILES = {
    "1": {"nama": "Rumah Tangga",  "file": "rumah-tangga.parquet"},
    "2": {"nama": "Usaha",         "file": "uji-coba-peta.parquet"},
    "3": {"nama": "Bangunan",      "file": "bangunan-terklasifikasi.parquet"},
}

WARNA_OK    = "\033[92m"
WARNA_WARN  = "\033[93m"
WARNA_ERROR = "\033[91m"
WARNA_INFO  = "\033[96m"
WARNA_BOLD  = "\033[1m"
RESET       = "\033[0m"

con = duckdb.connect()


def header(teks):
    print(f"\n{WARNA_BOLD}{'─'*55}")
    print(f"  {teks}")
    print(f"{'─'*55}{RESET}")


def cek_file_tersedia():
    print(f"\n{WARNA_BOLD}{'═'*55}")
    print("  FILE PARQUET TERSEDIA")
    print(f"{'═'*55}{RESET}")
    ada = False
    for key, info in FILES.items():
        path = OUTPUT_DIR / info["file"]
        if path.exists():
            size = path.stat().st_size / 1024 / 1024
            print(f"  {WARNA_OK}OK{RESET}  [{key}] {info['nama']:<20} ({size:.1f} MB)")
            ada = True
        else:
            print(f"  {WARNA_WARN}--{RESET}  [{key}] {info['nama']:<20} (belum ada)")
    if not ada:
        print(f"\n  {WARNA_ERROR}Tidak ada file parquet di folder output_parquet/{RESET}")
        print(f"  Jalankan dulu: python konversi_data.py")
    return ada


def pilih_file() -> Path | None:
    tersedia = {k: v for k, v in FILES.items() if (OUTPUT_DIR / v["file"]).exists()}
    if not tersedia:
        return None
    print(f"\n  Pilih data: ", end="")
    pilihan = input().strip()
    if pilihan not in tersedia:
        print(f"  {WARNA_ERROR}Pilihan tidak valid.{RESET}")
        return None
    return OUTPUT_DIR / tersedia[pilihan]["file"]


def ringkasan(path: Path):
    header(f"RINGKASAN -- {path.name}")
    f = str(path)

    total = con.sql(f"SELECT COUNT(*) as total FROM '{f}'").fetchone()[0]
    print(f"  Total baris   : {WARNA_OK}{total:,}{RESET}")

    schema = con.sql(f"DESCRIBE SELECT * FROM '{f}'").fetchall()
    print(f"  Jumlah kolom  : {len(schema)}")
    print(f"\n  {'Nama Kolom':<30} {'Tipe'}")
    print(f"  {'─'*45}")
    for col in schema:
        print(f"  {col[0]:<30} {WARNA_INFO}{col[1]}{RESET}")


def lihat_baris(path: Path):
    header(f"PREVIEW DATA -- {path.name}")
    f = str(path)
    print(f"  Berapa baris yang ingin dilihat? [default: 10]: ", end="")
    n = input().strip()
    n = int(n) if n.isdigit() else 10
    con.sql(f"SELECT * FROM '{f}' LIMIT {n}").show(max_width=120)


def cari_data(path: Path):
    header(f"CARI DATA -- {path.name}")
    f = str(path)

    schema = con.sql(f"DESCRIBE SELECT * FROM '{f}'").fetchall()
    kolom_teks = [c[0] for c in schema if "VARCHAR" in c[1].upper() or "TEXT" in c[1].upper()]

    if not kolom_teks:
        print(f"  {WARNA_WARN}Tidak ada kolom teks untuk dicari.{RESET}")
        return

    print(f"  Kolom teks tersedia: {', '.join(kolom_teks)}")
    print(f"  Pilih kolom: ", end="")
    kolom = input().strip()
    if kolom not in kolom_teks:
        print(f"  {WARNA_ERROR}Kolom tidak valid.{RESET}")
        return

    print(f"  Kata kunci pencarian: ", end="")
    keyword = input().strip()
    if not keyword:
        return

    result = con.sql(f"""
        SELECT * FROM '{f}'
        WHERE LOWER("{kolom}") LIKE '%{keyword.lower()}%'
        LIMIT 50
    """)
    total = con.sql(f"""
        SELECT COUNT(*) FROM '{f}'
        WHERE LOWER("{kolom}") LIKE '%{keyword.lower()}%'
    """).fetchone()[0]

    print(f"\n  Ditemukan {WARNA_OK}{total:,}{RESET} baris")
    result.show(max_width=120)


def distribusi_idsls(path: Path):
    header(f"DISTRIBUSI PER IDSLS -- {path.name}")
    f = str(path)

    if "idsls_str" not in [c[0] for c in con.sql(f"DESCRIBE SELECT * FROM '{f}'").fetchall()]:
        print(f"  {WARNA_WARN}Kolom 'idsls_str' tidak ditemukan di file ini.{RESET}")
        return

    print(f"  Tampilkan berapa SLS teratas? [default: 20]: ", end="")
    n = input().strip()
    n = int(n) if n.isdigit() else 20

    con.sql(f"""
        SELECT
            idsls_str,
            COUNT(*) as jumlah
        FROM '{f}'
        WHERE idsls_str IS NOT NULL
        GROUP BY idsls_str
        ORDER BY jumlah DESC
        LIMIT {n}
    """).show()

    stats = con.sql(f"""
        SELECT
            COUNT(DISTINCT idsls_str) as total_sls,
            SUM(COUNT(*)) OVER () as total_data,
            AVG(COUNT(*)) OVER () as rata_rata
        FROM '{f}'
        WHERE idsls_str IS NOT NULL
        GROUP BY idsls_str
        LIMIT 1
    """).fetchone()

    if stats:
        print(f"\n  Total SLS unik : {WARNA_OK}{stats[0]:,}{RESET}")
        print(f"  Total data     : {WARNA_OK}{stats[1]:,}{RESET}")
        print(f"  Rata-rata/SLS  : {WARNA_OK}{stats[2]:.1f}{RESET}")


def cek_koordinat(path: Path):
    header(f"CEK KOORDINAT -- {path.name}")
    f = str(path)

    schema = {c[0]: c[1] for c in con.sql(f"DESCRIBE SELECT * FROM '{f}'").fetchall()}

    kandidat_lat = ["lat final", "latitude", "lat"]
    kandidat_lon = ["long final", "longitude", "lon"]
    kolom_lat = next((k for k in kandidat_lat if k in schema), None)
    kolom_lon = next((k for k in kandidat_lon if k in schema), None)

    if not kolom_lat or not kolom_lon:
        print(f"  {WARNA_WARN}Kolom koordinat tidak ditemukan.{RESET}")
        return

    result = con.sql(f"""
        SELECT
            COUNT(*) as total,
            COUNT("{kolom_lat}") as lat_isi,
            COUNT("{kolom_lon}") as lon_isi,
            MIN("{kolom_lat}") as lat_min,
            MAX("{kolom_lat}") as lat_max,
            MIN("{kolom_lon}") as lon_min,
            MAX("{kolom_lon}") as lon_max
        FROM '{f}'
    """).fetchone()

    print(f"  Total baris    : {result[0]:,}")
    print(f"  Lat tidak null : {WARNA_OK}{result[1]:,}{RESET}")
    print(f"  Lon tidak null : {WARNA_OK}{result[2]:,}{RESET}")
    null_count = result[0] - result[1]
    if null_count > 0:
        print(f"  {WARNA_WARN}Koordinat null : {null_count:,} baris{RESET}")
    print(f"\n  Rentang Latitude  : {result[3]:.6f} -> {result[4]:.6f}")
    print(f"  Rentang Longitude : {result[5]:.6f} -> {result[6]:.6f}")


def query_bebas(path: Path):
    header(f"QUERY SQL BEBAS -- {path.name}")
    f = str(path)
    print(f"  Tulis query SQL (ketik 'exit' untuk keluar)")
    print(f"  Gunakan nama tabel: data\n")
    con.execute(f"CREATE OR REPLACE VIEW data AS SELECT * FROM '{f}'")

    while True:
        print(f"  {WARNA_INFO}SQL>{RESET} ", end="")
        query = input().strip()
        if query.lower() in ("exit", "quit", "keluar"):
            break
        if not query:
            continue
        try:
            con.sql(query).show(max_width=140)
        except Exception as e:
            print(f"  {WARNA_ERROR}Error: {e}{RESET}")


def menu_utama():
    print(f"\n{WARNA_BOLD}{'═'*55}")
    print("  TENTORING DATA CONVERTER -- CEK DATA")
    print(f"{'═'*55}{RESET}")

    if not cek_file_tersedia():
        return

    while True:
        print(f"\n  {'─'*40}")
        print(f"  Pilih aksi:")
        print(f"  {WARNA_INFO}[1]{RESET} Ringkasan & struktur kolom")
        print(f"  {WARNA_INFO}[2]{RESET} Preview baris data")
        print(f"  {WARNA_INFO}[3]{RESET} Cari data (filter by keyword)")
        print(f"  {WARNA_INFO}[4]{RESET} Distribusi per IDSLS")
        print(f"  {WARNA_INFO}[5]{RESET} Cek koordinat (lat/lon)")
        print(f"  {WARNA_INFO}[6]{RESET} Query SQL bebas")
        print(f"  {WARNA_INFO}[0]{RESET} Keluar")
        print(f"  {'─'*40}")
        print(f"  Pilihan: ", end="")

        aksi = input().strip()

        if aksi == "0":
            print(f"\n  Selesai.\n")
            break

        if aksi not in ("1", "2", "3", "4", "5", "6"):
            print(f"  {WARNA_WARN}Pilihan tidak valid.{RESET}")
            continue

        print(f"\n  Pilih data yang ingin dicek:")
        cek_file_tersedia()
        path = pilih_file()
        if path is None:
            continue

        if aksi == "1":
            ringkasan(path)
        elif aksi == "2":
            lihat_baris(path)
        elif aksi == "3":
            cari_data(path)
        elif aksi == "4":
            distribusi_idsls(path)
        elif aksi == "5":
            cek_koordinat(path)
        elif aksi == "6":
            query_bebas(path)


if __name__ == "__main__":
    try:
        menu_utama()
    except KeyboardInterrupt:
        print("\n\n  Dibatalkan.\n")