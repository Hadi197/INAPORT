try:
    import requests  # type: ignore
    HAS_REQUESTS = True
except ImportError:
    requests = None
    HAS_REQUESTS = False
    # fallback modules for when requests is not available
    import urllib.request  # type: ignore
    import urllib.parse  # type: ignore
    import urllib.error  # type: ignore

try:
    from bs4 import BeautifulSoup  # type: ignore
except ImportError:
    raise SystemExit("Missing dependency 'beautifulsoup4'. Install with: pip install beautifulsoup4")

# tambahan imports untuk perbaikan CSV
try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None
import asyncio
import aiohttp
from lxml import html
import json
import sys
import csv
import os
from typing import Dict, Optional, Tuple, List
import argparse
import datetime
import concurrent.futures


def get_json(url: str, headers: dict, max_retries: int = 3, timeout: int = 20):
    import requests
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429,) or 500 <= r.status_code < 600:
                last_err = RuntimeError(f"HTTP {r.status_code}")
                import time
                time.sleep(min(10, 2**attempt))
                continue
            r.raise_for_status()
        except Exception as e:
            last_err = e
            if attempt == max_retries:
                break
            import time
            time.sleep(min(10, 2**attempt))
    if last_err:
        raise last_err
    raise RuntimeError("Unknown error without response")

def get_all_ports() -> List[str]:
    # Hardcode list of common Indonesian port codes
    # In a real scenario, this could be scraped from the website
    return [
        "IDGRE", "IDJKT", "IDSUB", "IDPNK", "IDSRG", "IDMES", "IDBPN", "IDMAK", "IDPLM", "IDBJM",
        "IDTGR", "IDBTH", "IDBLW", "IDKOE", "IDAMI", "IDTRK", "IDBDO", "IDPBL", "IDTTE", "IDBTJ",
        "IDKDI", "IDKBR", "IDPDG", "IDUPG", "IDMDC", "IDBJW", "IDTKG", "IDBKS", "IDSWQ", "IDTIM",
        "IDAMQ", "IDFKQ", "IDNAH", "IDMEQ", "IDWMU", "IDJBR", "IDSIQ", "IDTJQ", "IDGTO", "IDKAU","IDKBU","IDDUM","IDBTN",
        "IDCXP", "IDBJU", "IDCEB", "IDLBR", "IDKUM", "IDSMQ", "IDSTU"
    ]

def scrape_pkk_list(kode_pelabuhan: str, tahun: int, bulan: int, jenis: str) -> list:
    url = f"https://monitoring-inaportnet.dephub.go.id/monitoring/byPort/list/{kode_pelabuhan}/{jenis}/{tahun}/{bulan:02d}"
    try:
        payload = get_json(url, HEADERS)
    except Exception as e:
        print(f"[WARN] Gagal JSON {jenis} {tahun}-{bulan:02d}: {e}")
        return []
    data = payload.get("data") or []
    return [item.get("nomor_pkk") for item in data if item.get("nomor_pkk")]

# Config / input from provided request info
BASE_URL = "https://monitoring-inaportnet.dephub.go.id/monitoring/detail"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0; +https://example.org/bot)"
}


async def fetch_page_async(session: aiohttp.ClientSession, url: str, params: dict) -> Optional[str]:
    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None

async def process_pkk(session: aiohttp.ClientSession, npk: str) -> List[dict]:
    params = {"nomor_pkk": npk}
    html_text = await fetch_page_async(session, BASE_URL, params)
    if not html_text:
        return []

    soup = BeautifulSoup(html_text, 'html.parser')

    # Extract title
    title = extract_title(soup)
    if not title:
        return []

    # Extract ship_info and dates
    ship_info, dates, status, other_services = extract_ship_info_and_dates(soup)

    # Parse title
    if " - " in title:
        no_pkk, nama_kapal = title.split(" - ", 1)
    else:
        no_pkk = title
        nama_kapal = ""

    # Extract specific columns
    bendera_call_imo = ship_info.get("Bendera / Call Sign / IMO", "")
    parts = [x.strip() for x in bendera_call_imo.split(" / ")]
    bendera = parts[0] if len(parts) > 0 else ""
    # call_sign = parts[1] if len(parts) > 1 else ""  # Removed
    # imo = parts[2] if len(parts) > 2 else ""  # Removed

    gt_dwt = ship_info.get("GT / DWT", "")
    parts = [x.strip() for x in gt_dwt.split(" / ")]
    gt = parts[0] if len(parts) > 0 else ""
    dwt = parts[1] if len(parts) > 1 else ""

    draft = ship_info.get("Draft Depan / Belakang / Max", "")
    parts = [x.strip() for x in draft.split(" / ")]
    # draft_depan = parts[0] if len(parts) > 0 else ""  # Removed
    # belakang = parts[1] if len(parts) > 1 else ""  # Removed
    max_draft = parts[2] if len(parts) > 2 else ""

    panjang_lebar = ship_info.get("Panjang / Lebar", "")
    parts = [x.strip() for x in panjang_lebar.split(" / ")]
    # panjang = parts[0] if len(parts) > 0 else ""  # Removed
    lebar = parts[1] if len(parts) > 1 else ""

    # Common fields
    common = {
        "No PKK": no_pkk,
        "Nama Kapal": nama_kapal,
        "ETA": dates.get("ETA", ""),
        "ETD": dates.get("ETD", ""),
        "Nama Perusahaan": ship_info.get("Nama Perusahaan", ""),
        "GT": gt,
        "Jenis Trayek": dates.get("Jenis Trayek", ""),
        "Singgah": dates.get("Singgah", ""),
    }

    # Unpivot: create rows for arrival and departure
    rows = []
    # Arrival row
    arrival_row = common.copy()
    arrival_row["Tipe"] = "Kedatangan"
    arrival_row["Layanan"] = status.get("Layanan Kedatangan", "")
    arrival_row["Verifikator"] = status.get("Verifikator Kedatangan", "")
    arrival_row["Nomor Produk"] = status.get("Nomor Produk Kedatangan", "")
    arrival_row["Lokasi Sandar"] = status.get("Lokasi Sandar Kedatangan", "")
    # Add SPK if applicable
    arrival_row["Nomor SPK"] = status.get("Nomor Produk Kedatangan", "") if "SPK" in status.get("Layanan Kedatangan", "") else ""
    if arrival_row["Nomor SPK"]:
        waktu = status.get("Waktu Permohonan Kedatangan", "")
        arrival_row["Waktu SPK"] = waktu if waktu else ""
    else:
        arrival_row["Waktu SPK"] = ""
    # Add kategori SPK
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
        "KANTOR KESYAHBANDARAN DAN OTORITAS PELABUHAN UTAMA TANJUNG PRIOK",
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
    
    # Filter hanya untuk layanan SPK PANDU
    if arrival_row["Layanan"] == "SPK PANDU":
        if arrival_row["Verifikator"] in pelindo_verifikators:
            arrival_row["Kategori SPK"] = "PELINDO"
        else:
            arrival_row["Kategori SPK"] = "NON PELINDO"
        rows.append(arrival_row)

    # Departure row
    departure_row = common.copy()
    departure_row["Tipe"] = "Keberangkatan"
    departure_row["Layanan"] = status.get("Layanan Keberangkatan", "")
    departure_row["Verifikator"] = status.get("Verifikator Keberangkatan", "")
    departure_row["Nomor Produk"] = status.get("Nomor Produk Keberangkatan", "")
    departure_row["Lokasi Sandar"] = status.get("Lokasi Sandar Keberangkatan", "")
    # Add SPK if applicable
    departure_row["Nomor SPK"] = status.get("Nomor Produk Keberangkatan", "") if "SPK" in status.get("Layanan Keberangkatan", "") else ""
    if departure_row["Nomor SPK"]:
        waktu = status.get("Waktu Permohonan Keberangkatan", "")
        departure_row["Waktu SPK"] = waktu if waktu else ""
    else:
        departure_row["Waktu SPK"] = ""
    # Filter hanya untuk layanan SPK PANDU
    if departure_row["Layanan"] == "SPK PANDU":
        if departure_row["Verifikator"] in pelindo_verifikators:
            departure_row["Kategori SPK"] = "PELINDO"
        else:
            departure_row["Kategori SPK"] = "NON PELINDO"
        rows.append(departure_row)

    # Other services (e.g., ship movement)
    for other_service in other_services:
        other_row = common.copy()
        other_row["Tipe"] = other_service.get("Layanan", "Lainnya")
        other_row["Layanan"] = other_service.get("Layanan", "")
        other_row["Verifikator"] = other_service.get("Verifikator", "")
        other_row["Nomor Produk"] = other_service.get("Nomor Produk", "")
        other_row["Lokasi Sandar"] = other_service.get("Lokasi Sandar", "")
        # Add SPK if applicable
        other_row["Nomor SPK"] = other_service.get("Nomor Produk", "") if "SPK" in other_service.get("Layanan", "") else ""
        if other_row["Nomor SPK"]:
            waktu = other_service.get("Waktu Permohonan", "")
            other_row["Waktu SPK"] = waktu if waktu else ""
        else:
            other_row["Waktu SPK"] = ""
        # Filter hanya untuk layanan SPK PANDU
        if other_row["Layanan"] == "SPK PANDU":
            if other_row["Verifikator"] in pelindo_verifikators:
                other_row["Kategori SPK"] = "PELINDO"
            else:
                other_row["Kategori SPK"] = "NON PELINDO"
            rows.append(other_row)

    return rows


def extract_title(soup: BeautifulSoup) -> Optional[str]:
    # Cari heading yang mengandung pola PKK.
    for tag_name in ("h1", "h2", "h3", "h4", "title"):
        for tag in soup.find_all(tag_name):
            txt = tag.get_text(strip=True)
            if txt and "PKK." in txt:
                return txt
    # fallback: cari teks besar yang mirip PKK di seluruh dokumen
    full = soup.get_text(" ", strip=True)
    # sederhana: cari substring 'PKK.' dan ambil sampai 80 char
    idx = full.find("PKK.")
    if idx != -1:
        return full[idx: idx + 120].splitlines()[0]
    return None


def extract_captain(soup: BeautifulSoup) -> Optional[str]:
    # Heuristik:
    # 1) cari elemen yang mengandung kata 'Nakhoda' atau 'Captain' lalu ambil badge di dekatnya
    labels = soup.find_all(string=lambda s: s and ("nakhoda" in s.lower() or "captain" in s.lower()))
    for lbl in labels:
        parent = lbl.parent
        if parent:
            badge = parent.find(class_=lambda c: c and "badge" in c)
            if badge:
                return badge.get_text(strip=True)
            # cek siblings
            sib = parent.find_next_sibling()
            if sib:
                b = sib.find(class_=lambda c: c and "badge" in c)
                if b:
                    return b.get_text(strip=True)
    # 2) fallback: ambil badge pertama yang terlihat dan panjangnya > 3 (kemungkinan nama)
    badge = soup.find(class_=lambda c: c and "badge" in c)
    if badge:
        txt = badge.get_text(strip=True)
        if txt:
            return txt
    return None


def table_to_dict(table_tag) -> Dict[str, str]:
    data = {}
    if not table_tag:
        return data

    for row in table_tag.find_all("tr"):
        cols = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
        # Handle the specific structure: key : value key : value
        i = 0
        while i < len(cols) - 2:
            if cols[i+1].strip() in (":", ""):
                key = cols[i].strip(": ")
                value = cols[i+2].strip()
                if key:
                    data[key] = value
                i += 3
            else:
                i += 1
        # If odd number or remaining, try to pair as key:value
        if len(cols) == 2:
            key, val = cols
            data[key.strip(": ")] = val
    return data


def extract_ship_info_and_dates(soup: BeautifulSoup) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], List[dict]]:
    tables = soup.find_all("table")
    ship_info = {}
    dates = {}
    status = {}
    other_services = []
    if not tables:
        return ship_info, dates, status, other_services
    # Ambil tabel pertama untuk info kapal
    ship_info = table_to_dict(tables[0])
    # Ambil ETA/ETD dari tabel kedua jika ada
    if len(tables) >= 2:
        second = table_to_dict(tables[1])
        # Cari kunci yang mengandung 'ETA' atau 'ETD'
        for k, v in second.items():
            kk = k.upper()
            if "ETA" in kk:
                dates["ETA"] = v
            if "ETD" in kk:
                dates["ETD"] = v
        # jika tidak ditemukan di keys, cek values
        if "ETA" not in dates or "ETD" not in dates:
            for k, v in second.items():
                val_up = v.upper()
                if "ETA" in val_up and "ETA" not in dates:
                    dates["ETA"] = v
                if "ETD" in val_up and "ETD" not in dates:
                    dates["ETD"] = v
        # Add other fields from table 1
        for k, v in second.items():
            if k not in ["ETA", "ETD"]:  # Avoid duplicates
                dates[k] = v
    # Ambil status pelayanan dari tabel layanan (tabel 2,3,4,5, dll)
    arrival_services = []
    departure_services = []
    other_services = []
    for table in tables[1:]:  # Skip table 0 (ship info), check others
        table_dict = table_to_dict(table)
        if 'Layanan' in table_dict or any('Layanan' in str(row) for row in table.find_all("tr")):
            # This is a service table
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cols = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
                if len(cols) >= 5:
                    service_info = {
                        "Layanan": cols[0],
                        "Waktu Permohonan": cols[1],
                        "Waktu Persetujuan": cols[2],
                        "Proses": cols[3],
                        "Status": cols[4],
                        "Verifikator": cols[5] if len(cols) > 5 else "",
                        "Nomor Produk": cols[6] if len(cols) > 6 else "",
                        "Lokasi Sandar": cols[7] if len(cols) > 7 else "",
                        "Status Integrasi": cols[8] if len(cols) > 8 else ""
                    }
                    if cols[0] in ['RPKRO', 'PPK', 'PKK', 'SPM']:  # Arrival services
                        arrival_services.append(service_info)
                    elif cols[0] in ['SPOG', 'SPB', 'SPK PANDU']:  # Departure services
                        departure_services.append(service_info)
                    else:  # Other services like ship movement
                        other_services.append(service_info)
    # Summarize status
    status["Status Kedatangan"] = "; ".join([s["Status"] for s in arrival_services]) if arrival_services else ""
    status["Status Keberangkatan"] = "; ".join([s["Status"] for s in departure_services]) if departure_services else ""
    # Extract first service details for columns
    first_arrival = arrival_services[0] if arrival_services else {}
    first_departure = departure_services[0] if departure_services else {}
    status["Layanan Kedatangan"] = first_arrival.get("Layanan", "")
    status["Waktu Permohonan Kedatangan"] = first_arrival.get("Waktu Permohonan", "")
    status["Waktu Persetujuan Kedatangan"] = first_arrival.get("Waktu Persetujuan", "")
    status["Proses Kedatangan"] = first_arrival.get("Proses", "")
    status["Verifikator Kedatangan"] = first_arrival.get("Verifikator", "")
    status["Nomor Produk Kedatangan"] = first_arrival.get("Nomor Produk", "")
    status["Lokasi Sandar Kedatangan"] = first_arrival.get("Lokasi Sandar", "")
    status["Status Integrasi Kedatangan"] = first_arrival.get("Status Integrasi", "")
    status["Layanan Keberangkatan"] = first_departure.get("Layanan", "")
    status["Waktu Permohonan Keberangkatan"] = first_departure.get("Waktu Permohonan", "")
    status["Waktu Persetujuan Keberangkatan"] = first_departure.get("Waktu Persetujuan", "")
    status["Proses Keberangkatan"] = first_departure.get("Proses", "")
    status["Verifikator Keberangkatan"] = first_departure.get("Verifikator", "")
    status["Nomor Produk Keberangkatan"] = first_departure.get("Nomor Produk", "")
    status["Lokasi Sandar Keberangkatan"] = first_departure.get("Lokasi Sandar", "")
    status["Status Integrasi Keberangkatan"] = first_departure.get("Status Integrasi", "")
    # Keep detailed columns if needed
    # status["Detail Kedatangan"] = json.dumps(arrival_services, ensure_ascii=False) if arrival_services else ""
    # status["Detail Keberangkatan"] = json.dumps(departure_services, ensure_ascii=False) if departure_services else ""
    # fallback: cari langsung label di seluruh dokumen
    if "ETA" not in dates or "ETD" not in dates:
        text = soup.get_text(" | ", strip=True)
        for token in ("ETA", "ETD"):
            if token in text and token not in dates:
                # simple extraction: take substring after token up to 40 chars
                idx = text.find(token)
                if idx != -1:
                    snippet = text[idx: idx + 80]
                    # ambil setelah token:
                    part = snippet.split(token, 1)[1].lstrip(" :|-")
                    dates[token] = part.split("|")[0].strip()
    # fallback untuk status
    if "Status Kedatangan" not in status or "Status Keberangkatan" not in status:
        text = soup.get_text(" | ", strip=True)
        # Cari "STATUS PELAYANAN"
        status_idx = text.upper().find("STATUS PELAYANAN")
        if status_idx != -1:
            snippet = text[status_idx: status_idx + 200]
            # Ambil setelah "STATUS PELAYANAN"
            after_status = snippet.split("STATUS PELAYANAN", 1)[1].lstrip(" :|-")
            # Split by | or space
            parts = after_status.split("|")[0].strip().split()
            if len(parts) >= 2:
                status["Status Kedatangan"] = parts[0]
                status["Status Keberangkatan"] = parts[1]
        for token in ("KEDATANGAN", "KEBERANGKATAN"):
            if token in text and f"Status {token.lower().capitalize()}" not in status:
                idx = text.find(token)
                if idx != -1:
                    snippet = text[idx: idx + 100]
                    part = snippet.split(token, 1)[1].lstrip(" :|-")
                    status[f"Status {token.lower().capitalize()}"] = part.split("|")[0].strip()
    return ship_info, dates, status, other_services


def pretty_print(title: Optional[str], captain: Optional[str], ship_info: Dict[str, str], dates: Dict[str, str]):
    print("=" * 80)
    print("PKK DETAIL SCRAPER")
    print("=" * 80)
    print(f"Title : {title or 'N/A'}")
    print(f"Captain: {captain or 'N/A'}")
    print("-" * 80)
    print("Ship Information:")
    if ship_info:
        # align keys
        maxk = max(len(k) for k in ship_info.keys())
        for k, v in ship_info.items():
            print(f"  {k.ljust(maxk)} : {v}")
    else:
        print("  (no ship info found)")
    print("-" * 80)
    print("Dates:")
    print(f"  ETA : {dates.get('ETA', 'N/A')}")
    print(f"  ETD : {dates.get('ETD', 'N/A')}")
    print("=" * 80)


def _clean_key(k: str) -> str:
    return k.strip().rstrip(":").strip()

def _clean_value(v: str) -> str:
    return v.strip()

def _clean_captain(raw: Optional[str]) -> str:
    if not raw:
        return ""
    s = raw
    # remove common labels
    for label in ("nakhoda", "nakhoda :", "nakhoda:", "captain", "captain :", "captain:"):
        s = s.replace(label, "")
        s = s.replace(label.upper(), "")
        s = s.replace(label.capitalize(), "")
    return s.strip(" :,-").strip()

def fix_csv(input_file: str = "raw.csv", output_file: str = "output_fixed.csv"):
    """
    Baca CSV input_file, parse kolom 'ShipInfo' (JSON/string) menjadi kolom terpisah,
    lalu simpan ke output_file.
    """
    if pd is None:
        print("Missing dependency 'pandas'. Install with: pip install pandas")
        return
    try:
        df = pd.read_csv(input_file, dtype=str)
    except Exception as e:
        print(f"Failed to read '{input_file}': {e}")
        return

    if "ShipInfo" in df.columns:
        def _parse_shipinfo_cell(x):
            if pd.isna(x):
                return {}
            s = str(x).strip()
            # beberapa CSV mungkin menyimpan JSON dengan double quotes escaped -> coba beberapa pendekatan
            for attempt in (
                lambda v: json.loads(v),
                lambda v: json.loads(v.replace("''", '"').replace("'", '"')),
            ):
                try:
                    return attempt(s)
                except Exception:
                    continue
            # fallback: try ast.literal_eval
            try:
                import ast
                return ast.literal_eval(s)
            except Exception:
                return {}
        shipinfo_expanded = df["ShipInfo"].apply(_parse_shipinfo_cell)
        if not shipinfo_expanded.empty:
            shipinfo_df = pd.json_normalize(shipinfo_expanded)
            # gabungkan dan drop kolom lama
            df = pd.concat([df.drop(columns=["ShipInfo"]), shipinfo_df], axis=1)
    # simpan hasil
    try:
        df.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"[✓] Data berhasil diperbaiki → {output_file}")
    except Exception as e:
        print(f"Failed to write '{output_file}': {e}")


def save_table_as_json_csv(soup: BeautifulSoup, json_path: str = "hasil_scrap.json", csv_path: str = "hasil_scrap.csv"):
    """
    Ambil tabel pertama dari BeautifulSoup `soup`, ekstrak baris menjadi list of dict,
    simpan ke json_path dan csv_path.
    - Jika baris pertama mengandung <th> dianggap header.
    - Jika tidak ada header, gunakan keys generik col_0, col_1, ...
    """
    table = soup.find("table")
    if not table:
        print("No table found to save.")
        return

    rows = table.find_all("tr")
    if not rows:
        print("No rows found in table.")
        return

    # tentukan header dari first row jika ada <th>, atau kosong
    first_cols = rows[0].find_all(["th", "td"])
    has_header = any(c.name == "th" for c in first_cols)
    header = [c.get_text(" ", strip=True) for c in first_cols] if has_header else []

    data_list = []
    start_idx = 1 if has_header else 0
    for row in rows[start_idx:]:
        cols = row.find_all("td")
        if not cols:
            continue
        item = {}
        for i, col in enumerate(cols):
            key = header[i] if i < len(header) else f"col_{i}"
            item[key] = col.get_text(" ", strip=True)
        if item:
            data_list.append(item)

    if not data_list:
        print("No data rows found in table.")
        return

    # tulis JSON
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data_list, f, ensure_ascii=False, indent=4)
        # tulis CSV (gabungkan semua keys yang muncul)
        all_keys = []
        for d in data_list:
            for k in d.keys():
                if k not in all_keys:
                    all_keys.append(k)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(data_list)
        print(f"[✓] Table saved → {json_path}, {csv_path}")
    except Exception as exc:
        print(f"Failed to save table to files: {exc}")


async def test_single_pkk(npk: str):
    """Test function for a single PKK"""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0; +https://example.org/bot)"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        rows = await process_pkk(session, npk)
        print(f"Total rows for {npk}: {len(rows)}")
        for row in rows:
            print(f"  Tipe: {row.get('Tipe', 'N/A')}, Status: {row.get('Status', 'N/A')}, Layanan: {row.get('Layanan', 'N/A')}")
async def gather_all_details(session: aiohttp.ClientSession, pkk_list: List[str]) -> List[dict]:
    semaphore = asyncio.Semaphore(100)  # Limit concurrency - balanced for speed and stability
    results = []

    async def bounded_process(npk: str):
        async with semaphore:
            rows = await process_pkk(session, npk)
            if rows:
                results.extend(rows)

    await asyncio.gather(*[bounded_process(npk) for npk in pkk_list])
    return results

def run_for_port(kode: str, bulan_list: List[int], jenis_list: List[str], tahun: int) -> List[dict]:
    async def inner():
        connector = aiohttp.TCPConnector(limit=0, force_close=False, ttl_dns_cache=300)
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0; +https://example.org/bot)"
        }
        rows = []
        async with aiohttp.ClientSession(headers=headers, connector=connector, trust_env=True) as session:
            for bulan in bulan_list:
                for jenis in jenis_list:
                    print(f"Fetching for {kode} {tahun}-{bulan:02d} {jenis}...")
                    pkk_list = scrape_pkk_list(kode, tahun, bulan, jenis)
                    if not pkk_list:
                        print(f"No PKK for {kode} {tahun}-{bulan:02d} {jenis}")
                        continue
                    print(f"Found {len(pkk_list)} PKK for {kode} {tahun}-{bulan:02d} {jenis}")
                    batch_rows = await gather_all_details(session, pkk_list)
                    rows.extend(batch_rows)
        return rows
    return asyncio.run(inner())

def main():
    parser = argparse.ArgumentParser(description="Scrape PKK details from INAPORTNET")
    parser.add_argument("--kode", nargs='+', default=["all"], help="Kode pelabuhan (bisa multiple atau 'all' untuk semua)")
    parser.add_argument("--tahun", type=int, default=2025, help="Tahun")
    parser.add_argument("--bulan", type=int, nargs='*', help="Bulan (opsional, default semua bulan)")
    parser.add_argument("--jenis", nargs='*', default=["dn", "ln"], help="Jenis: dn atau ln (default keduanya)")
    parser.add_argument("--test-pkk", help="Test single PKK number and save to CSV")
    args = parser.parse_args()

    if args.test_pkk:
        # Test single PKK and save to CSV
        async def run_test():
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; Scraper/1.0; +https://example.org/bot)"
            }
            async with aiohttp.ClientSession(headers=headers) as session:
                rows = await process_pkk(session, args.test_pkk)
                if rows:
                    out_path = f"test_{args.test_pkk}.csv"
                    with open(out_path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                        writer.writeheader()
                        writer.writerows(rows)
                    print(f"Saved {len(rows)} rows to {out_path}")
                else:
                    print("No data found.")
        asyncio.run(run_test())
        return

    if "all" in args.kode:
        kode_list = get_all_ports()
    else:
        kode_list = args.kode

    bulan_list = args.bulan if args.bulan else list(range(1, datetime.datetime.now().month + 1))
    jenis_list = args.jenis if args.jenis else ["dn", "ln"]

    # Use multiprocessing for parallel port processing
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_for_port, kode, bulan_list, jenis_list, args.tahun) for kode in kode_list]
        all_rows = []
        for future in concurrent.futures.as_completed(futures):
            all_rows.extend(future.result())

    if all_rows:
        out_dir = os.path.dirname(__file__) or "."
        out_path = os.path.join(out_dir, "ina.csv")
        try:
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
                writer.writeheader()
                writer.writerows(all_rows)
            print(f"Saved {len(all_rows)} results to {out_path}")
        except Exception as e:
            print(f"Failed to save CSV: {e}")
    else:
        print("No data to save.")


if __name__ == "__main__":
    main()
