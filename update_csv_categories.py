import pandas as pd
import csv

def update_spk_categories_new_logic(csv_file_path):
    """
    Update kolom Kategori SPK di file CSV yang sudah ada
    berdasarkan logika baru:
    - Filter hanya data dengan Layanan = "SPK PANDU"
    - Jika Verifikator = "PELABUHAN INDONESIA (Persero)" maka PELINDO
    - Selain itu NON PELINDO
    """
    try:
        # Baca CSV
        df = pd.read_csv(csv_file_path, dtype=str)

        # Pastikan kolom yang dibutuhkan ada
        required_cols = ['Verifikator', 'Layanan', 'Kategori SPK']
        for col in required_cols:
            if col not in df.columns:
                print(f"Error: Kolom '{col}' tidak ditemukan di CSV")
                return

        # Filter hanya baris dengan Layanan = "SPK PANDU"
        original_count = len(df)
        df = df[df['Layanan'] == 'SPK PANDU']
        filtered_count = len(df)

        print(f"Filtered {original_count} rows to {filtered_count} SPK PANDU rows")

        # Update kategori SPK berdasarkan logika baru
        def categorize_spk_new(row):
            verifikator = str(row.get('Verifikator', ''))

            pelindo_verifikators = [
                "PT. PELABUHAN INDONESIA (Persero)",
                "PT PELABUHAN INDONESIA (PERSERO) REGIONAL 2 PONTIANAK",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 2 BANTEN",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 3 Tj. Emas",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Gresik",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. MAKASSAR",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. BALIKPAPAN",
                "PT PELINDO JASA MARITIM",
                "PT. PELABUHAN INDONESIA (Persero) CABANG KUPANG",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Belawan",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Palembang",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. TERNATE",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. KENDARI",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Pulau Ba'ai",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. TARAKAN",
                "PELABUHAN INDONESIA",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Tanjung Pandan",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. AMBON",
                "PT. PELABUHAN INDONESIA (PERSERO) REGIONAL 4 CAB. GORONTALO",
                "KANTOR KESYAHBANDAR DAN OTORITAS PELABUHAN UTAMA TANJUNG PRIOK",
                "PT. PELABUHAN INDONESIA (Persero) Batulicin",
                "PT. Pelabuhan Indonesia (Persero) Regional 1 Cabang Dumai",
                "PT. PELABUHAN INDONESIA (Persero) CABANG SATUI",
                "PT. PELABUHAN INDONESIA (Persero) CABANG SAMPIT",
                "PT Pelabuhan Indonesia",
                "PT. PELABUHAN INDONESIA (Persero) CABANG LEMBAR",
                "PT. PELABUHAN INDONESIA (Persero) Cab. Cilacap",
                "PT. PELABUHAN INDONESIA (Persero) CABANG TANJUNG WANGI",
                "PT PELABUHAN INDONESIA (PERSERO)"
            ]

            if verifikator in pelindo_verifikators:
                return 'PELINDO'
            else:
                return 'NON PELINDO'

        # Terapkan fungsi ke setiap baris
        df['Kategori SPK'] = df.apply(categorize_spk_new, axis=1)

        # Simpan kembali ke file yang sama
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
        print(f"Berhasil update {len(df)} baris SPK PANDU di {csv_file_path}")
        print(f"PELINDO: {len(df[df['Kategori SPK'] == 'PELINDO'])} baris")
        print(f"NON PELINDO: {len(df[df['Kategori SPK'] == 'NON PELINDO'])} baris")

    except Exception as e:
        print(f"Error updating CSV: {e}")

# Contoh penggunaan
if __name__ == "__main__":
    # Ganti dengan path file CSV Anda
    csv_file = "ina.csv"  # atau "ina1.csv"
    update_spk_categories_new_logic(csv_file)