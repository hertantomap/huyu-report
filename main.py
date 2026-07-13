import asyncio
import requests
import time
import random
import string
import csv
import io
import re
import os
import pandas as pd
import urllib.request
import urllib.parse
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from zoneinfo import ZoneInfo
import pytz
from dotenv import load_dotenv
import yfinance as yf
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from google import genai
from google.genai import types
from firecrawl import Firecrawl
import feedparser 
# Tambahkan import ini di bagian atas main.py
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Memuat variabel dari file .env
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEB_APP_SCRIPT_URL = os.getenv("WEB_APP_SCRIPT_URL")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
# Muat ketiga API Key
GEMINI_API_KEY_1 = os.getenv("GEMINI_API_KEY_1")
GEMINI_API_KEY_2 = os.getenv("GEMINI_API_KEY_2")
GEMINI_API_KEY_3 = os.getenv("GEMINI_API_KEY_3")
GEMINI_API_KEY_4 = os.getenv("GEMINI_API_KEY_4")

# =====================================================================
# INISIALISASI GOOGLE SHEETS API
# =====================================================================
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def inisialisasi_sheets_client_from_env():
    client_email = os.getenv("GOOGLE_CLIENT_EMAIL")
    private_key = os.getenv("GOOGLE_PRIVATE_KEY")
    
    if not client_email or not private_key:
        print("[!] Kredensial GOOGLE_CLIENT_EMAIL atau GOOGLE_PRIVATE_KEY tidak ditemukan di .env")
        return None
        
    formatted_private_key = private_key.replace('\\n', '\n')
    
    info = {
        "type": "service_account",
        "client_email": client_email,
        "private_key": formatted_private_key,
        "token_uri": "https://oauth2.googleapis.com/token"
    }
    
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service.spreadsheets()

try:
    sheets_client = inisialisasi_sheets_client_from_env()
except Exception as e:
    print(f"[!] Gagal inisialisasi Google Sheets API: {e}")
    sheets_client = None


# Masukkan ke dalam list dan abaikan yang kosong (jika sewaktu-waktu Anda hanya pakai 2 key)
api_keys = [k for k in [GEMINI_API_KEY_1, GEMINI_API_KEY_2, GEMINI_API_KEY_3, GEMINI_API_KEY_4] if k]

if not api_keys:
    raise ValueError("Tidak ada GEMINI_API_KEY yang ditemukan di dalam file .env!")

# Inisialisasi multiple client untuk setiap API Key
clients = [genai.Client(api_key=key) for key in api_keys]

# Class untuk merotasi pemakaian client (Round-Robin) secara aman (thread-safe)
class ClientRotator:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        # Ubah menjadi pasangan tuple: (objek_client, string_api_key_asli)
        self.clients = [(genai.Client(api_key=key), key) for key in api_keys]
        self.index = 0

    async def get_client(self):
        if not self.clients:
            return None, None
        client_obj, api_key = self.clients[self.index]
        self.index = (self.index + 1) % len(self.clients)
        return client_obj, api_key # Kembalikan berpasangan

client_rotator = ClientRotator(api_keys)
gemini_concurrency_limiter = asyncio.Semaphore(len(api_keys))

# =====================================================================
# SYSTEM SMART RATE LIMITER
# =====================================================================
class SmartRateLimiter:
    def __init__(self, max_requests=12, window_seconds=70):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_history = [] 
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.time()
                # Bersihkan riwayat lama
                self.request_history = [t for t in self.request_history if now - t < self.window_seconds]
                
                if len(self.request_history) < self.max_requests:
                    self.request_history.append(now)
                    return
                
                sleep_time = self.window_seconds - (now - self.request_history[0])
            
            # Tidur dilakukan di LUAR scope 'async with self.lock' agar tidak mengunci task lain
            if sleep_time > 0:
                print(f"    [!] Kuota Gemini Penuh ({self.max_requests} req / {self.window_seconds}s). Menunggu {sleep_time:.2f} detik...")
                await asyncio.sleep(sleep_time)
            else:
                # Jika hitungan terlalu mepet/negatif, beri jeda paksa 1 detik sebelum cek ulang
                await asyncio.sleep(2)

# Inisialisasi limiter global (Otomatis menyesuaikan jumlah API Key: misal 3 key x 12 = 36 req/70s)
kapasitas_maksimal = 12 * len(api_keys)
gemini_limiter = SmartRateLimiter(max_requests=kapasitas_maksimal, window_seconds=70)
print(f"[*] Sistem Rate Limiter diatur ke {gemini_limiter.max_requests} request per {gemini_limiter.window_seconds} detik.")

# ==========================================
# AMBIL DATA DINAMIS DARI GOOGLE APPS SCRIPT
# ==========================================
async def fetch_dynamic_config(url, max_retries=3, retry_delay=5):
    print("[-] Mengambil konfigurasi dinamis (Websites, Prompts, Tickers) dari Google Spreadsheet...")
    
    # Pindahkan definisi ke sini agar hanya dibuat satu kali di memori
    def fetch_url_sync(target_url):
        with urllib.request.urlopen(target_url, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    for attempt in range(1, max_retries + 1):
        try:
            # Panggil menggunakan to_thread
            res_data = await asyncio.to_thread(fetch_url_sync, url)
            return res_data.get("websites", []), res_data.get("prompts", {}), res_data.get("tickers", {})
                
        except Exception as e:
            print(f"    [!] Percobaan ke-{attempt} gagal: {e}")
            if attempt < max_retries:
                print(f"    [-] Menunggu {retry_delay} detik sebelum mencoba kembali...")
                await asyncio.sleep(retry_delay)
            else:
                # Alarm terakhir ke Telegram jika benar-benar gagal setelah 3 kali percobaan
                error_msg = (
                    f"🚨 <b>CRITICAL ERROR</b>\n\n"
                    f"Gagal mengambil konfigurasi dari Spreadsheet setelah {max_retries} kali percobaan.\n"
                    f"<b>Detail Kendala:</b> <code>{e}</code>\n\n"
                    f"Sistem otomatis dihentikan."
                )
                # Catatan: Pastikan fungsi send_telegram_message Anda memang fungsi sinkron (bukan async def). 
                # Jika sudah async def, cukup gunakan: await send_telegram_message(error_msg)
                await asyncio.to_thread(send_telegram_message, error_msg)
                
    return [], {}, {}

# ==========================================
# FUNGSI 1: FILTER LINK DENGAN ENGINE GEMINI
# ==========================================
async def process_task_with_gemini(prompt, csv_string):
    print("    [-] Kuota terverifikasi. Mengirim request filter link ke Gemini...")
    
    models_fallback_order = ['gemini-3.1-flash-lite', 'gemma-4-31b-it', 'gemma-4-26b-a4b-it', 'gemini-3.1-flash-lite', 'gemini-3.5-flash']
    data_csv_mentah = csv_string.encode('utf-8')
    
    current_client, current_api_key = await client_rotator.get_client() # Ambil giliran client
    
    for model_name in models_fallback_order:
        await gemini_limiter.acquire()
        
        async with gemini_concurrency_limiter:
            current_client, current_api_key = await client_rotator.get_client()
            try:
                print(f"    [-] Mencoba memfilter konten berita menggunakan model Async: {model_name}...")
                komponen_csv = types.Part.from_bytes(data=data_csv_mentah, mime_type="text/csv")
                generate_content_config = types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level="MINIMAL"),
                )
                
                # [NATIVE ASYNC]: Menggunakan .aio dan langsung di-await
                response = await current_client.aio.models.generate_content(
                    model=model_name,
                    contents=[komponen_csv, prompt],
                    config=generate_content_config
                )
                
                if response and response.text:
                    return response.text
                    
            except Exception as e:
                print(f"    [!] Model {model_name} Error: {e}. Mencoba model fallback dengan API Key yang sama...")

    print("    [!] Semua model untuk Ekstraksi Konten gagal merespons pada API Key ini.")
    return ""

# ==========================================
# FUNGSI 3: ANALISIS DATA BERITA MASTER
# ==========================================
async def ask_gemini_with_inline_csv(prompt, csv_data):
    """
    Fungsi Async Gemini dengan Fallback mendukung peninjauan key aktif via Client Rotator Tuple.
    """
    models_fallback_order = ['gemini-3.1-flash-lite', 'gemini-3.5-flash', 'gemini-3.1-flash-lite', 'gemma-4-31b-it', 'gemma-4-26b-a4b-it', 'gemini-3.1-flash-lite']
    
    for model_name in models_fallback_order:
        await gemini_limiter.acquire()
        
        # Cegah burst concurrency (Lonjakan paralel)
        async with gemini_concurrency_limiter:
            current_client, current_api_key = await client_rotator.get_client() 
            
            try:
                print(f"    [-] Mencoba menganalisis data menggunakan model Async: {model_name}...")
                
                if isinstance(csv_data, str):
                    data_csv_mentah = csv_data.encode('utf-8')
                    komponen_input = types.Part.from_bytes(data=data_csv_mentah, mime_type="text/csv")
                elif isinstance(csv_data, dict):
                    if current_api_key in csv_data:
                        komponen_input = csv_data[current_api_key]
                    else:
                        print(f"    [!] File untuk API Key aktif tidak ditemukan. Skip...")
                        continue
                else:
                    komponen_input = csv_data

                # Tambahkan parameter timeout eksplisit jika dimungkinkan oleh SDK, atau biarkan default
                response = await current_client.aio.models.generate_content(
                    model=model_name,
                    contents=[komponen_input, prompt]
                )
                
                if response and response.text:
                    return response.text
                    
            except Exception as e:
                print(f"    [!] Model {model_name} Error: {e}. Mencoba model fallback...")

    print("    [!] Semua model untuk Analisis Data gagal merespons pada API Key ini.")
    return ""

# ==========================================
# FUNGSI AMBIL DATA SAHAM TERBAIK IDX (SESUAI HEADER 3 KOLOM)
# ==========================================
async def saham_lq45_terbaik_idx():
    print("[-] Mengambil data Saham LQ45 terbaik dari IDX...")

    jumlah_pilihan = 15
    
    saham_lq45 = {
        "AADI", "ADMR", "ADRO", "AKRA", "AMMN", "AMRT", "ANTM", 
        "ASII", "BBCA", "BBNI", "BBRI", "BBTN", "BMRI", "BRPT", 
        "BUMI", "CPIN", "CUAN", "DEWA", "EMTK", "ESSA", "EXCL", 
        "GOTO", "HRTA", "ICBP", "INCO", "INDF", "INKP", "ISAT", 
        "ITMG", "JPFA", "KLBF", "MAPI", "MBMA", "MDKA", "MEDC", 
        "PGAS", "PGEO", "PTBA", "SCMA", "SMGR", "TLKM", "TOWR", 
        "UNTR", "UNVR", "WIFI"
    }
    
    data_saham = []

    firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

    # Kode firecrawl.scrape NYA TETAP SAMA, hanya ditambahkan await asyncio.to_thread di depannya
    doc = await asyncio.to_thread(
        firecrawl.scrape,
        "https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=1000&start=0", 
        formats=["html"]
    )

    # Mengambil teks langsung dari atribut objek Document
    # Jika format yang diminta "html", maka hasilnya ada di doc.html
    raw_json_str = doc.html 

    try:
        # Parsing string tersebut menjadi object JSON Python
        json_data = json.loads(raw_json_str)
        data_saham = json_data.get('data', [])
    except json.JSONDecodeError:
        try:
            # Gunakan Regex untuk mengekstrak struktur JSON (tanda kurawal) terlepas dari ada/tidaknya tag HTML
            match = re.search(r'\{.*\}', raw_json_str, re.DOTALL)
            if match:
                clean_json = match.group(0)
                json_data = json.loads(clean_json)
                data_saham = json_data.get('data', [])
            else:
                return None
        except Exception as e:
            print("Gagal parsing JSON. Teks mentah yang diterima:")
            print(raw_json_str)
            return None

    pool_saham = []
    
    for stock in data_saham:
        ticker = stock.get('StockCode', '').strip()
        stock_name = stock.get('StockName', '').strip()
        
        if ticker in saham_lq45:
            harga_previous_close = stock.get('Previous', 0)
            harga_open = stock.get('OpenPrice', 0)
            harga_high = stock.get('High', 0)
            harga_low = stock.get('Low', 0)
            harga_close = stock.get('Close', 0)
            
            volume_lembar = stock.get('Volume', 0)
            nilai_transaksi_total = stock.get('Value', 0)
            
            if volume_lembar > 0 and nilai_transaksi_total > 0:
                harga_rata_rata = nilai_transaksi_total / volume_lembar
            else:
                harga_rata_rata = harga_close
            
            foreign_buy_shares = stock.get('ForeignBuy', 0)
            foreign_sell_shares = stock.get('ForeignSell', 0)
            foreign_net_shares = foreign_buy_shares - foreign_sell_shares
            
            domestic_buy_shares = volume_lembar - foreign_buy_shares
            domestic_sell_shares = volume_lembar - foreign_sell_shares
            domestic_net_shares = domestic_buy_shares - domestic_sell_shares
            
            foreign_buy_val = foreign_buy_shares * harga_rata_rata
            foreign_sell_val = foreign_sell_shares * harga_rata_rata
            foreign_net_val = foreign_buy_val - foreign_sell_val
            
            domestic_buy_val = nilai_transaksi_total - foreign_buy_val
            domestic_sell_val = nilai_transaksi_total - foreign_sell_val
            domestic_net_val = domestic_buy_val - domestic_sell_val
            
            pool_saham.append({
                "Ticker": ticker,
                "Stock_Name": stock_name,
                "Harga_Previous_Close": harga_previous_close,
                "Harga_Open": harga_open,
                "Harga_High": harga_high,
                "Harga_Low": harga_low,
                "Harga_Close": harga_close,
                "Harga_Rata_Rata": harga_rata_rata,
                "Volume": volume_lembar,
                "Nilai_Transaksi": nilai_transaksi_total,
                
                "Foreign_Buy_Vol": foreign_buy_shares,
                "Foreign_Sell_Vol": foreign_sell_shares,
                "Foreign_Net_Vol": foreign_net_shares,
                "Domestic_Buy_Vol": domestic_buy_shares,
                "Domestic_Sell_Vol": domestic_sell_shares,
                "Domestic_Net_Vol": domestic_net_shares,
                
                "Foreign_Buy_Val": foreign_buy_val,
                "Foreign_Sell_Val": foreign_sell_val,
                "Foreign_Net_Val": foreign_net_val,
                "Domestic_Buy_Val": domestic_buy_val,
                "Domestic_Sell_Val": domestic_sell_val,
                "Domestic_Net_Val": domestic_net_val
            })
            
    if not pool_saham:
        print("[!] Tidak ada saham LQ45 yang cocok ditemukan hari ini.")
        return None
        
    df = pd.DataFrame(pool_saham)
    df_sorted = df.sort_values(by="Nilai_Transaksi", ascending=False)
    top_rekomendasi = df_sorted.head(jumlah_pilihan)
    
    # --- LOGIKA FORMAT WAKTU (SAMA DENGAN YAHOO FINANCE) ---
    hari_en_to_id = {
        "Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu", 
        "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"
    }
    bulan_en_to_id = {
        "January": "Januari", "February": "Februari", "March": "Maret", "April": "April",
        "May": "Mei", "June": "Juni", "July": "Juli", "August": "Agustus",
        "September": "September", "October": "Oktober", "November": "November", "December": "Desember"
    }
    
    zona_wib = pytz.timezone('Asia/Jakarta')
    now = datetime.now(zona_wib)
    hari_indo = hari_en_to_id.get(now.strftime("%A"), now.strftime("%A"))
    bulan_indo = bulan_en_to_id.get(now.strftime("%B"), now.strftime("%B"))
    waktu_sekarang = f"{hari_indo}, {now.strftime('%d')} {bulan_indo} {now.strftime('%Y')}"
    
    # --- PROSES GABUNG TEXT KE LIST ---
    financial_summary = []
    total_tickers = len(top_rekomendasi)
    
    for idx, row in enumerate(top_rekomendasi.itertuples(), start=1):
        f_net_val_str = f"+Rp{row.Foreign_Net_Val:,.0f}" if row.Foreign_Net_Val >= 0 else f"-Rp{abs(row.Foreign_Net_Val):,.0f}"
        d_net_val_str = f"+Rp{row.Domestic_Net_Val:,.0f}" if row.Domestic_Net_Val >= 0 else f"-Rp{abs(row.Domestic_Net_Val):,.0f}"
        
        f_net_vol_str = f"+{row.Foreign_Net_Vol:,.0f}" if row.Foreign_Net_Vol >= 0 else f"{row.Foreign_Net_Vol:,.0f}"
        d_net_vol_str = f"+{row.Domestic_Net_Vol:,.0f}" if row.Domestic_Net_Vol >= 0 else f"{row.Domestic_Net_Vol:,.0f}"
        
        emoji_foreign = "🔴" if row.Foreign_Net_Val < 0 else "🟢"
        emoji_domestic = "🔴" if row.Domestic_Net_Val < 0 else "🟢"

        financial_summary.append(f"<b>[{idx}] {row.Ticker}</b> - {row.Stock_Name}")
        financial_summary.append(f" - Open/Pre    : Rp {row.Harga_Open:,.0f} / Rp {row.Harga_Previous_Close:,.0f}")          
        financial_summary.append(f" - High/Low    : Rp {row.Harga_High:,.0f} / Rp {row.Harga_Low:,.0f}")          
        financial_summary.append(f" - Close/Avg   : Rp {row.Harga_Close:,.0f} / Rp {row.Harga_Rata_Rata:,.2f}")       
        financial_summary.append(f" - Volume Total    : {row.Volume:,.0f} lembar")
        financial_summary.append(f" - Transaksi Total : Rp {row.Nilai_Transaksi:,.0f}")
        financial_summary.append("-"*45)
        
        financial_summary.append(f"{emoji_foreign} <b>Foreign (Asing)</b>")
        financial_summary.append(f"├─ Buy: {row.Foreign_Buy_Vol:,.0f} Lbr")
        financial_summary.append(f"│    ↳ Rp {row.Foreign_Buy_Val:,.0f}")
        financial_summary.append(f"├─ Sell: {row.Foreign_Sell_Vol:,.0f} Lbr")
        financial_summary.append(f"│    ↳ Rp {row.Foreign_Sell_Val:,.0f}")
        financial_summary.append(f"└─ Net: {f_net_vol_str} Lbr")
        financial_summary.append(f"       ↳ {f_net_val_str}")
        financial_summary.append("")
        
        financial_summary.append(f"{emoji_domestic} <b>Domestic (Lokal)</b>")
        financial_summary.append(f"├─ Buy: {row.Domestic_Buy_Vol:,.0f} Lbr")
        financial_summary.append(f"│    ↳ Rp {row.Domestic_Buy_Val:,.0f}")
        financial_summary.append(f"├─ Sell: {row.Domestic_Sell_Vol:,.0f} Lbr")
        financial_summary.append(f"│    ↳ Rp {row.Domestic_Sell_Val:,.0f}")
        financial_summary.append(f"└─ Net: {d_net_vol_str} Lbr")
        financial_summary.append(f"       ↳  {d_net_val_str}")
        financial_summary.append("")
            
    if len(financial_summary) > 1:
        isi_berita_finansial = "\n".join(financial_summary)
        return [waktu_sekarang, isi_berita_finansial, "https://www.idx.co.id"]
    
    return None

# ==========================================
# FUNGSI AMBIL DATA YAHOO FINANCE (SESUAI HEADER 3 KOLOM)
# ==========================================
def fetch_yahoo_finance_data(tickers_dict):
    print("[-] Mengambil data pasar finansial terbaru dari Yahoo Finance...")
    if not tickers_dict:
        print("[!] Daftar Tickers kosong. Melewati...")
        return None
        
    hari_en_to_id = {
        "Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu", 
        "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"
    }
    bulan_en_to_id = {
        "January": "Januari", "February": "Februari", "March": "Maret", "April": "April",
        "May": "Mei", "June": "Juni", "July": "Juli", "August": "Agustus",
        "September": "September", "October": "Oktober", "November": "November", "December": "Desember"
    }
    
    zona_wib = pytz.timezone('Asia/Jakarta')
    now = datetime.now(zona_wib)
    hari_indo = hari_en_to_id.get(now.strftime("%A"), now.strftime("%A"))
    bulan_indo = bulan_en_to_id.get(now.strftime("%B"), now.strftime("%B"))
    waktu_sekarang = f"{hari_indo}, {now.strftime('%d')} {bulan_indo} {now.strftime('%Y')}"
    
    financial_summary = []
    
    # Mengonversi dict_items menjadi list agar kita bisa tahu posisi index akhir
    tickers_list = list(tickers_dict.items())
    total_tickers = len(tickers_list)
    
    for idx, (nama, detail) in enumerate(tickers_list):
        try:
            kode = detail.get("ticker")
            fmt_pola = detail.get("format", "{}") 
            # Mengambil parameter periode, jika tidak diatur maka default-nya 5 hari ('5d')
            periode = detail.get("period", "5d") 
            interval = detail.get("interval", "1d") 
            
            if not kode:
                continue
                
            ticker = yf.Ticker(str(kode).strip())
            # Memasukkan variabel periode yang dinamis ke dalam history()
            hist = ticker.history(period=periode, interval=interval)
            
            if not hist.empty:
                financial_summary.append(f"➡️ <b>{nama}</b>")
                
                # Inisialisasi variabel untuk menampung baris data sebelumnya
                prev_baris = None
                
                for tanggal, baris in hist.iterrows():
                    perubahan_persen = 0
                    harga_hari_kemarin = None
                    harga_hari_ini = None
                    tgl_str = tanggal.strftime("%d-%b-%Y")

                    # Jika ini adalah baris pertama, kita belum punya data hari sebelumnya.
                    # Maka kita simpan baris ini sebagai prev_baris dan lanjut ke hari berikutnya.
                    if prev_baris is None:
                        prev_baris = baris
                        harga_hari_ini = baris['Close']
                    else:
                        # "harga_hari_kemarin" adalah data sebelumnya (Close dari hari sebelumnya)
                        harga_hari_kemarin = prev_baris['Close']
                        
                        # "harga_hari_ini" adalah harga saat ini (Close dari hari ini/saat ini)
                        harga_hari_ini = baris['Close']
                        
                    # 1. Hitung perubahan persen harian
                    if harga_hari_kemarin and harga_hari_kemarin != 0:
                        perubahan_persen = ((harga_hari_ini - harga_hari_kemarin) / harga_hari_kemarin) * 100
                    else:
                        perubahan_persen = 0
                    
                    # 2. Logika penentuan emoji panah (Naik = 🔼 hijau/biru di beberapa device, atau bisa diganti 🟢)
                    if perubahan_persen > 0:
                        panah = "🟢"  # Panah Naik
                    elif perubahan_persen < 0:
                        panah = "🔴"  # Panah Turun Merah
                    else:
                        panah = "🔵"  # Stagnan

                    # 3. Format harga dasar sesuai pola dari dictionary
                    harga_terformat = fmt_pola.format(harga_hari_ini)
                    
                    # 4. Gabungkan harga terformat dengan persentase
                    baris_laporan = f"• {tgl_str} : <b>{harga_terformat}</b> ({perubahan_persen:+.2f}%) {panah}"
                    
                    financial_summary.append(baris_laporan)

                    # Simpan baris saat ini untuk menjadi 'prev_baris' pada perulangan hari berikutnya
                    prev_baris = baris
                
                # MODIFIKASI: Hanya tambahkan garis pembatas jika BUKAN item terakhir
                if idx < total_tickers - 1:
                    financial_summary.append("")
                
        except Exception as e:
            print(f"    [!] Gagal mengambil data {nama}: {e}")
            
    if len(financial_summary) > 1:
        isi_berita_finansial = "\n".join(financial_summary)
        return [waktu_sekarang, isi_berita_finansial, "https://finance.yahoo.com"]
    
    return None

# ==========================================
# FUNGSI AMBIL DAFTAR URL BERITA YAHOO FINANCE
# ==========================================
async def fetch_yahoo_finance_news_urls(ticker_code="BTC-USD"):
    """
    Mengambil data berita terbaru dari Yahoo Finance berdasarkan kode ticker.
    Mengembalikan list berisi kumpulan URL berita saja.
    """
    print(f"[-] Mengambil daftar URL berita terbaru untuk {ticker_code} dari Yahoo Finance...")
    daftar_berita = set()
    
    try:
        ticker = yf.Ticker(str(ticker_code).strip())
        # Menjalankan objek pemanggilan data I/O blocking di thread terpisah agar asinkron aman
        btc_news = await asyncio.to_thread(lambda: ticker.news)
        
        if not btc_news:
            print(f"[!] Tidak ada berita ditemukan untuk {ticker_code}.")
            return daftar_berita
            
        if btc_news:
            for news in btc_news:
                content = news.get('content', {})
                if not content:
                    continue
                
                # 1. Coba ambil dari canonicalUrl (Sering digunakan oleh tipe STORY/Artikel)
                link = content.get('canonicalUrl', {}).get('url')
                
                # 2. Jika tidak ada, coba ambil dari clickThroughUrl (Sering digunakan oleh tipe VIDEO)
                if not link:
                    link = content.get('clickThroughUrl', {}).get('url')
                    
                # 3. Jalur alternatif terakhir (pubShortUrl)
                if not link:
                    link = content.get('pubShortUrl')
                
                # Masukkan ke set jika link valid
                if link and isinstance(link, str) and link.startswith("http"):
                    daftar_berita.add(link)
                
        print(f" [+] Berhasil menemukan {len(daftar_berita)} berita dari Yahoo {ticker_code}.")
        
        return daftar_berita
        
    except Exception as e:
        print(f"    [!] Gagal mengambil berita Yahoo Finance untuk {ticker_code}: {e}")
        return daftar_berita
    
# =====================================================================
# FUNGSI NOTIFIKASI TELEGRAM (DENGAN RETRY & TIMEOUT LEBIH TINGGI)
# =====================================================================
session = requests.Session()
def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    # 1. Fungsi Pembersihan Karakter Ilegal HTML
    def escape_html(teks_mentah):
        if not teks_mentah:
            return ""
        t = teks_mentah
        t = t.replace("<b>", "__B_OPEN__").replace("</b>", "__B_CLOSE__")
        t = t.replace("<code>", "__CODE_OPEN__").replace("</code>", "__CODE_CLOSE__")
        t = t.replace("<i>", "__I_OPEN__").replace("</i>", "__I_CLOSE__")
        
        t = t.replace("<", "&lt;")
        t = t.replace(">", "&gt;")
        
        t = t.replace("__B_OPEN__", "<b>").replace("__B_CLOSE__", "</b>")
        t = t.replace("__CODE_OPEN__", "<code>").replace("__CODE_CLOSE__", "</code>")
        t = t.replace("__I_OPEN__", "<i>").replace("__I_CLOSE__", "</i>")
        return t

    # 2. Fungsi Membersihkan Kebocoran Simbol Markdown
    def fix_markdown_leak(teks_input):
        t = teks_input
        t = re.sub(r'\*\*(.*?)\*\* ', r'<b>\1</b>', t)
        t = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', t)
        t = re.sub(r'#+\s*', '', t)
        return t

    # 3. Fungsi Pintar Memisah Teks Menjadi Beberapa Bagian Safely
    def split_text_chunks(full_text, max_chunk_size=3800):
        chunks = []
        lines = full_text.splitlines()
        current_chunk = []
        current_length = 0
        
        for line in lines:
            # Jika satu baris saja sudah melebihi batas (kasus ekstrem), potong paksa
            if len(line) > max_chunk_size:
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                    current_chunk = []
                    current_length = 0
                chunks.append(line[:max_chunk_size])
                continue
                
            if current_length + len(line) + 1 > max_chunk_size:
                chunks.append("\n".join(current_chunk))
                current_chunk = [line]
                current_length = len(line)
            else:
                current_chunk.append(line)
                current_length += len(line) + 1
                
        if current_chunk:
            chunks.append("\n".join(current_chunk))
        return chunks
    
    # 1. Pembersihan & Pemotongan Teks (Tetap sama)
    cleaned_text = fix_markdown_leak(text)
    safe_text = escape_html(cleaned_text)
    pesan_potongan = split_text_chunks(safe_text, max_chunk_size=3800)
    total_bagian = len(pesan_potongan)
    
    # 2. Loop untuk mengirim setiap potongan
    for i, chunk in enumerate(pesan_potongan):
        text_payload = chunk
        if total_bagian > 1:
            text_payload += f"\n\n<i>[Bagian {i + 1}/{total_bagian}]</i>"
            
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text_payload,
            "parse_mode": "HTML"
        }
        
        # Eksekusi dengan requests.Session
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                # requests akan mengurus encoding dictionary ke form-data secara otomatis
                response = session.post(url, data=payload, timeout=(10, 60))
                response.raise_for_status() 
                print(f" [+] Potongan pesan {i + 1}/{total_bagian} sukses terkirim.")
                break 
            except requests.exceptions.RequestException as e:
                print(f" [!] Gagal kirim potongan {i + 1} (Attempt {attempt}): {e}")
                if attempt < max_retries:
                    time.sleep(2.5)
                else:
                    # Fallback Plain Text
                    print(" [!] Mencoba fallback plain text...")
                    try:
                        plain_payload = {
                            "chat_id": TELEGRAM_CHAT_ID,
                            "text": re.sub(r'<[^>]*>', '', chunk),
                        }
                        session.post(url, data=plain_payload, timeout=60)
                    except Exception as e_fb:
                        print(f" [!] Pengiriman cadangan gagal: {e_fb}")
        
        time.sleep(1.5)
        

# ==========================================
# FUNGSI PEMBANTU BROWSER
# ==========================================
async def auto_scroll(page, max_scroll_steps=15):
    """Scroll menggunakan Native Mouse Wheel Playwright"""
    print("    [-] Melakukan scrolling (Native Mouse Wheel)...")
    try:
        # Arahkan kursor mouse ke tengah layar agar aman dari sisi pinggir/iframe iklan
        viewport_size = page.viewport_size
        if viewport_size:
            await page.mouse.move(viewport_size['width'] / 2, viewport_size['height'] / 2)
        
        for step in range(max_scroll_steps):
            # Scroll roda mouse ke bawah sejauh 600 pixel
            await page.mouse.wheel(delta_x=0, delta_y=600)
            
            # Beri jeda 0.3 - 0.5 detik per guliran agar website sempat memuat gambar/XHR (Lazy Load)
            await asyncio.sleep(0.4)
            
    except Exception as e:
        print(f"    [!] Peringatan saat scrolling: {e}")
        
    await asyncio.sleep(1) # Jeda final sebelum lanjut

async def handle_infinite_scroll(page, scroll_count):
    for _ in range(scroll_count):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        await page.wait_for_timeout(2000)

async def handle_load_more(page, selector, click_count):
    for _ in range(click_count):
        try:
            button = page.locator(selector)
            if await button.is_visible():
                await button.scroll_into_view_if_needed()
                await button.click()
                await page.wait_for_timeout(2500)
            else:
                break
        except:
            break

async def handle_rss_feed(rss_url, engine="firecrawl", page=None):
    """
    Mengambil tautan dari RSS feed dengan opsi engine: 'firecrawl' atau 'playwright'.
    Jika memilih 'playwright', pastikan mengoper objek 'page' aktif ke fungsi ini.
    """
    print(f"[-] Mengambil RSS feed dari URL: {rss_url} menggunakan {engine.upper()}...")
    daftar_berita = []
    raw_html_content = ""

    try:
        # ==========================================
        # OPSIONIL 1: MENGGUNAKAN FIRECRAWL
        # ==========================================
        if engine.lower() == "firecrawl":
            if not FIRECRAWL_API_KEY:
                print(" [!] API Key Firecrawl tidak ditemukan. Mencoba fallback ke Playwright...")
                engine = "playwright"
            else:
                firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)
                doc = await asyncio.to_thread(firecrawl.scrape, rss_url, formats=["html"])
                if doc and hasattr(doc, 'html') and doc.html:
                    raw_html_content = doc.html
                else:
                    print(" [!] Firecrawl mengembalikan response kosong. Mencoba fallback ke Playwright...")
                    engine = "playwright"

        # ==========================================
        # OPSIONIL 2: MENGGUNAKAN PLAYWRIGHT (Reuse Page)
        # ==========================================
        if engine.lower() == "playwright":
            if page is None:
                print(" [!] Error: Parameter 'page' tidak disediakan untuk engine Playwright!")
                return []
            
            # Gunakan page yang sudah ada, langsung arahkan ke URL RSS
            # Menggunakan timeout 30 detik untuk mengantisipasi jaringan lambat di GitHub Actions
            response = await page.goto(rss_url, wait_until="domcontentloaded", timeout=30000)
            
            if response and response.status == 200:
                raw_html_content = await page.content()
            else:
                status_code = response.status if response else "Unknown"
                print(f" [!] Playwright gagal memuat halaman RSS. HTTP Status: {status_code}")

        # ==========================================
        # PROSES PARSING RSS DENGAN FEEDPARSER
        # ==========================================
        if raw_html_content:
            feed = feedparser.parse(raw_html_content)
            
            # Ekstraksi aman dengan pengecekan ketersediaan atribut 'link'
            for entry in feed.entries:
                if hasattr(entry, 'link'):
                    daftar_berita.append(entry.link)
                elif 'link' in entry:
                    daftar_berita.append(entry['link'])
                    
            print(f" [+] Berhasil menemukan {len(daftar_berita)} link berita dari {rss_url}.")
        else:
            print(" [!] Tidak ada konten yang berhasil diambil dari RSS feed.")

        return daftar_berita

    except Exception as e:
        print(f" [!] Gagal memproses RSS feed menggunakan {engine.upper()}: {e}")
        return []
    
async def fetch_article_data(context, url, semaphore, selector_extract=None, max_scroll_steps=5):
    async with semaphore:
        page = await context.new_page()
        try:

            # === MODIFIKASI LOGIKA RETRY DENGAN STRATEGI BERBEDA ===
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    # Percobaan 1: domcontentloaded | Percobaan 2: commit
                    strategi_tunggu = "domcontentloaded" if attempt == 0 else "commit"
                    
                    if attempt > 0:
                        print(f"    [!] Percobaan pertama timeout. Mencoba kembali {url} (Percobaan {attempt + 1}/{max_retries}) dengan strategi: {strategi_tunggu}...")
                    
                    await page.goto(url, wait_until=strategi_tunggu, timeout=30000)
                    break  # Berhasil memuat, keluar dari loop retry
                except Exception as e:
                    if attempt == max_retries - 1:
                        # Jika percobaan kedua (terakhir) masih gagal, lempar error ke block except luar
                        raise e
                    await asyncio.sleep(2)  # Jeda 2 detik sebelum mencoba lagi
            # =======================================================
            
            await auto_scroll(page, max_scroll_steps=max_scroll_steps)
            
            # --- MEKANISME FALLBACK EKSTRAKSI TEKS ---
            inner_text = ""
            if selector_extract:
                try:
                    # Ambil berdasarkan selector spesifik terlebih dahulu
                    inner_text = await page.locator(selector_extract).first.inner_text()
                except Exception as selector_err:
                    # Jika selector gagal/tidak ditemukan, ambil seluruh isi body text
                    print(f"    [!] Selector '{selector_extract}' gagal pada {url} ({selector_err}). Menggunakan fallback seluruh teks...")
                    inner_text = await page.evaluate("document.body.innerText")
            else:
                # Jika memang dari awal tidak ada selector_extract
                inner_text = await page.evaluate("document.body.innerText")
            
            inner_text = re.sub(r'\n+', '\n', inner_text).strip()
            return {"url": url, "text": inner_text}
            
        except Exception as e:
            print(f"    [!] Gagal mengambil artikel {url}: {e}")
            return None
        finally:
            await page.close()

# ==========================================
# PROSES [9]: ANALISIS BERITA MASTER AI (BERURUTAN)
# ==========================================
async def proses_analisis_berita_master(master_file_name, prompts_data, prompt_dasar_format):
    """
    Fungsi analisis berita master menggunakan strategi Multi-Key Upload Chunking:
    - Memecah data per 200 baris.
    - Mengunggah file ke cloud storage masing-masing API Key (mengantisipasi error 403).
    - Menjalankan rotasi key penuh saat pemrosesan prompt.
    - Mengirim pesan ke Telegram dengan proteksi splitter 4096 karakter.
    """
    
    if not os.path.exists(master_file_name):
        print(f"[!] File master {master_file_name} tidak ditemukan. Analisis dibatalkan.")
        return

    try:
        df = pd.read_csv(master_file_name)
    except Exception as e:
        print(f"[!] Gagal membaca file CSV: {e}")
        return

    total_baris = len(df)
    if total_baris == 0:
        print("[!] File CSV kosong. Tidak ada data untuk dianalisis.")
        return

    CHUNK_SIZE = 200 
    
    print(f"\n==================================================")
    print(f"[9] MEMULAI PROSES MULTI-KEY UPLOAD DAN ANALISIS BERITA")
    print(f"==================================================")

    # Dapatkan daftar semua client/API Key yang Anda miliki dari rotator
    semua_client = client_rotator.clients

    # Tempat menyimpan referensi file cloud ter-mapping berdasarkan indeks chunk dan API Key
    uploaded_chunks_map = []
    chunk_counter = 1

    # -------------------------------------------------------------------------
    # TAHAP 1: UPLOAD DATA CHUNK KE SEMUA API KEY (Masing-masing punya salinan)
    # -------------------------------------------------------------------------
    for i in range(0, total_baris, CHUNK_SIZE):
        df_chunk = df.iloc[i : i + CHUNK_SIZE]
        temp_chunk_path = f"temp_chunk_{chunk_counter}.csv"
        df_chunk.to_csv(temp_chunk_path, index=False)
        
        chunk_key_mapping = {}
        print(f"    [-] Menyiapkan Chunk ke-{chunk_counter} ({len(df_chunk)} baris)...")

        # Upload file chunk yang sama ke setiap API Key
        for client, api_key_str in semua_client:
            masked_key = f"...{api_key_str[-6:]}" if api_key_str else "Unknown"

            try:
                print(f"        [-] Mengunggah ke Cloud Storage untuk Key {masked_key}...")
                
                file_ref = client.files.upload(
                    file=temp_chunk_path,
                    config=types.UploadFileConfig(mime_type="text/csv")
                )
                # Petakan referensi file ke string API Key-nya
                chunk_key_mapping[api_key_str] = file_ref
            except Exception as e:
                print(f"        [!] Gagal mengunggah untuk Key {masked_key}: {e}")
        
        if chunk_key_mapping:
            uploaded_chunks_map.append(chunk_key_mapping)
            
        if os.path.exists(temp_chunk_path):
            os.remove(temp_chunk_path)
            
        chunk_counter += 1

    if not uploaded_chunks_map:
        print("[!] Tidak ada chunk data yang berhasil diunggah ke key manapun. Proses dibatalkan.")
        return

    # -------------------------------------------------------------------------
    # TAHAP 2: EKSEKUSI PROMPTS DAN CHUNKS (PARALEL TOTAL)
    # -------------------------------------------------------------------------
    async def eksekusi_single_chunk(prompt_key, prompt_text, batch_num, chunk_map):
        """Fungsi pekerja kecil untuk mengeksekusi satu chunk spesifik"""
        prompt_lengkap = f"{prompt_text}\n\n{prompt_dasar_format}"
        respons_ai = await ask_gemini_with_inline_csv(prompt_lengkap, chunk_map)
        
        return {
            "prompt_key": prompt_key,
            "batch": batch_num,
            "text": respons_ai if respons_ai else "[AI Gagal Merespons]"
        }

    # Kumpulkan semua kombinasi Prompt x Chunk ke dalam satu list antrean raksasa
    daftar_tugas_flat = []
    for k, v in prompts_data.items():
        if k == "PROMPT_DASAR_FORMAT":
            continue
        for idx, chunk_map in enumerate(uploaded_chunks_map):
            batch_num = idx + 1
            daftar_tugas_flat.append(
                eksekusi_single_chunk(k, v, batch_num, chunk_map)
            )

    print(f"[-] Menjalankan total {len(daftar_tugas_flat)} sub-proses analisis AI secara Paralel Total...")
    semua_hasil_flat = await asyncio.gather(*daftar_tugas_flat)

    # Susun ulang struktur data flat menjadi format dictionary hasil_dict lama Anda
    hasil_dict = {k: [] for k in prompts_data.keys() if k != "PROMPT_DASAR_FORMAT"}
    for res in semua_hasil_flat:
        hasil_dict[res["prompt_key"]].append({
            "batch": res["batch"],
            "text": res["text"]
        })

    # =========================================================================
    # AMBIL HASIL EKSPOR (TETAP AMAN & TIDAK HILANG)
    # =========================================================================
    hasil_ekspor_ai = None 
    if "PROMPT_BISNIS_EKSPOR" in hasil_dict and hasil_dict["PROMPT_BISNIS_EKSPOR"]:
        # Karena urutan batch paralel bisa acak, kita sort dulu berdasarkan nomor batch-nya
        data_ekspor_sorted = sorted(hasil_dict["PROMPT_BISNIS_EKSPOR"], key=lambda x: x['batch'])
        # Ambil teks dari BATCH 1 sebagai sampel ekspor untuk kebutuhan Leads / Spreadsheet Anda
        hasil_ekspor_ai = data_ekspor_sorted[0]['text']

    # -------------------------------------------------------------------------
    # TAHAP 3: KIRIM KE TELEGRAM (BERURUTAN)
    # -------------------------------------------------------------------------
    for prompt_key in prompts_data.keys():
        if prompt_key == "PROMPT_DASAR_FORMAT" or prompt_key not in hasil_dict:
            continue
            
        print(f"[-] Mengirim urutan hasil untuk: {prompt_key}...")
        
        data_batch = sorted(hasil_dict[prompt_key], key=lambda x: x['batch'])
        gabungan_text = "\n\n────────────────────\n\n".join([f"<b>[BATCH {b['batch']}]</b>\n{b['text']}" for b in data_batch])
        
        tz_jkt = pytz.timezone('Asia/Jakarta')
        waktu_sekarang = datetime.now(tz_jkt)
        tanggal_kirim_indo = waktu_sekarang.strftime("%A, %d %B %Y")
        waktu_wib_realtime = waktu_sekarang.strftime("%H:%M:%S WIB")
        nama_bersih = prompt_key.replace("_", " ")
        if nama_bersih.startswith("PROMPT "):
            nama_bersih = nama_bersih.replace("PROMPT ", "", 1)
        
        
        header_pesan = (
            f"📌 <code>{tanggal_kirim_indo} pukul {waktu_wib_realtime}</code>\n"
            f"<b>{nama_bersih}</b>\n"
            f"────────────────────\n\n"
        )
        
        pesan_full = header_pesan + gabungan_text
        
        await asyncio.to_thread(send_telegram_message, pesan_full)

    # -------------------------------------------------------------------------
    # TAHAP 4: BERSIHKAN FILE DI SEMUA API KEY STORAGE (Housekeeping)
    # -------------------------------------------------------------------------
    print("[-] Membersihkan semua berkas chunk dari Google AI Storage (Seluruh Key)...")
    for chunk_map in uploaded_chunks_map:
        for client, api_key_str in semua_client:
            if api_key_str and api_key_str in chunk_map:
                try:
                    target_file_ref = chunk_map[api_key_str]
                    await asyncio.to_thread(client.files.delete, target_file_ref.name)
                except Exception as e:
                    pass

    print("[+] Seluruh proses unggah, analisis, dan pembersihan selesai dengan sukses!")

    return hasil_ekspor_ai

async def proses_analisis_supply_demand_ke_spreadsheet(spreadsheet_id, sheet_name, data_respons_ai):
    if not sheets_client:
        print("[!] Client Google Sheets tidak aktif. Proses dihentikan.")
        return

    if not data_respons_ai or not data_respons_ai.strip():
        print("[!] Data data_respons_ai kosong. Proses dilewati.")
        return

    print(f"\n──────────────────────────────────────")
    print(f"[-] MEMULAI PROSES STRUKTURISASI DATA VIA GEMINI FILE API")
    print(f"──────────────────────────────────────")

    # --- PENGAMBILAN TANGGAL BAHASA INDONESIA (LOKAL FUNGSI) ---
    def get_tanggal_indo():
        hari_indo = ["Senin", "Selasa", "Rabu", "Kamis", "Jumat", "Sabtu", "Minggu"]
        bulan_indo = [
            "", "Januari", "Februari", "Maret", "April", "Mei", "Juni", 
            "Juli", "Agustus", "September", "Oktober", "November", "Desember"
        ]
        
        now = datetime.now(ZoneInfo("Asia/Jakarta"))
        # .weekday() mengembalikan 0=Senin, 6=Minggu
        nama_hari = hari_indo[now.weekday()]
        nama_bulan = bulan_indo[now.month]
        
        return f"{nama_hari}, {now.day} {nama_bulan} {now.year}"

    tanggal_hari_ini = get_tanggal_indo()

    # =====================================================================
    # AMBIL DATA KOORDINAT LAMA UNTUK PENGECEKAN DUPLIKAT
    # =====================================================================
    def get_existing_coords_supply_demand():
        try:
            # Kita hanya mengambil kolom I (Latitude) dan J (Longitude)
            range_to_read = f"{sheet_name}!I:J"
            result = sheets_client.values().get(spreadsheetId=spreadsheet_id, range=range_to_read).execute()
            rows = result.get('values', [])
            
            coords_set = set()
            for row in rows:
                # Karena rentang data hanya 2 kolom (I:J), kolom I menjadi row[0] dan J menjadi row[1]
                if len(row) >= 2:
                    lat_str = row[0].strip()
                    lon_str = row[1].strip()
                    if lat_str and lon_str and lat_str != "0" and lon_str != "0":
                        coords_set.add(f"{lat_str}|{lon_str}")
            return coords_set
        except Exception as e:
            print(f"[!] Gagal membaca koordinat lama di sheet {sheet_name}: {e}")
            return set()

    existing_coords = await asyncio.to_thread(get_existing_coords_supply_demand)
    print(f"[*] Menemukan {len(existing_coords)} koordinat yang sudah ada di sheet '{sheet_name}'.")

    # 1. Buat file teks sementara secara lokal berisi narasi respons_ai
    temp_txt_file = "temp_ekspor_report.txt"
    try:
        def tulis_file_lokal():
            with open(temp_txt_file, "w", encoding="utf-8") as f:
                f.write(data_respons_ai)
        await asyncio.to_thread(tulis_file_lokal)
        print("[-] Berhasil menulis laporan narasi ke file lokal sementara.")
    except Exception as e:
        print(f"[!] Gagal membuat file teks lokal: {e}")
        return

    current_client, current_api_key = await client_rotator.get_client()  
    
    uploaded_file = None
    response_text = ""

    try:
        # 2. Unggah file narasi tersebut ke Gemini File API cloud
        def upload_ke_gemini_cloud():
            return current_client.files.upload(
                file=temp_txt_file,
                config=types.UploadFileConfig(mime_type="text/plain")
            )
        
        uploaded_file = await asyncio.to_thread(upload_ke_gemini_cloud)
        print(f"[+] File narasi berhasil diunggah ke Gemini Cloud. URI: {uploaded_file.uri}")

        # 3. Definisikan PROMPT khusus yang mendeskripsikan isi File Teks Perdagangan tersebut
        prompt_ekstraksi = """
        Bertindaklah sebagai Ahli Data Perdagangan Internasional.
        Tugas Anda adalah memetakan arus aktivitas Ekspor dan Impor BARANG/KOMODITAS FISIK nyata berdasarkan berita di dokumen file teks rangkuman berita ekspor yang saya lampirkan.
        
        ATURAN STRATEGIS UTAMA (WAJIB DIPATUHI - PENYARINGAN KETAT):
        1. Anda HANYA BOLEH mengekstrak informasi yang membahas perdagangan, transaksi, kebutuhan atau potensi komoditas/barang Ekspor dan Impor.
        2. LARANGAN KERAS (BLOCKLIST): Anda WAJIB mengabaikan dan tidak memproses data yang berkaitan dengan komoditas strategis/sensitif seperti:
           - Bahan nuklir atau radioaktif (contoh: Uranium, Plutonium, bahan baku senjata nuklir).
           - Senjata militer, amunisi, peralatan tempur, teknologi pertahanan, atau komponen persenjataan.
           - Bahan peledak industri yang secara eksplisit ditujukan untuk penggunaan militer.
           - Komoditas terlarang lainnya yang bersifat rahasia negara atau melanggar hukum perdagangan internasional yang umum.
        3. Fokuslah HANYA pada komoditas perdagangan umum seperti produk pertanian, manufaktur, elektronik konsumen, pakaian, tekstil, bahan baku industri non-militer, dan sejenisnya.
        
        PANDUAN KECERDASAN MULTI-ENTITAS (DEMAND & SUPPLY SPLITTING):
        - Dokumen ini berisi rangkuman berita ekspor-impor. Jika sebuah berita membahas lalu lintas perdagangan, pergeseran pasar atau transaksi yang melibatkan banyak pihak (baik banyak pemasok maupun banyak pasar tujuan), Anda WAJIB memecahnya menjadi beberapa objek JSON terpisah berdasarkan perannya:
          * Sisi 'Demand': Negara/pihak yang membutuhkan, membeli, atau menjadi pasar tujuan impor komoditas.
          * Sisi 'Supply': Negara/pihak yang menyediakan, menjual, menghasilkan, atau mengekspor komoditas.
        - Analisis kalimat secara mendalam: 
          * Jika satu negara mengalihkan pembelian dari Negara A ke Negara B, maka muncul 3 objek: 1 Demand (pembeli) dan 2 Supply (pemasok lama dan pemasok baru).
          * Jika satu negara mengekspor ke beberapa negara tujuan sekaligus (atau beberapa negara menjadi pasar tujuan dari satu negara asal), maka pisahkan setiap negara tujuan sebagai objek 'Demand' tersendiri dan negara asal sebagai objek 'Supply'.

        CONTOH LOGIKA EKSTRAKSI (IKUTI POLA PIKIR INI):
        
        Contoh Kasus 1 (Pengalihan Pasokan):
        Teks: "Industri manufaktur kemasan plastik nasional mengalihkan pasokan biji plastik dan nafta dari Timur Tengah ke regional ASEAN dan China akibat disrupsi rantai pasok..."
        Hasil Ekstraksi Harus Menghasilkan 3 Objek JSON:
        1. Negara: Indonesia -> Status_Pasar: Demand (Karena industri nasional membutuhkan komoditas)
        2. Negara: Timur Tengah -> Status_Pasar: Supply (Karena merupakan asal pasokan awal)
        3. Negara: China -> Status_Pasar: Supply (Karena menjadi tujuan pengalihan pasokan baru)

        Contoh Kasus 2 (Multi-Pasar Tujuan):
        Teks: "Eropa dan Amerika Serikat merupakan pasar tujuan bagi komoditas daun ketapang asal Indonesia."
        Hasil Ekstra Extraction Harus Menghasilkan 3 Objek JSON:
        1. Negara: Eropa -> Status_Pasar: Demand (Karena merupakan pasar tujuan/pembeli)
        2. Negara: Amerika Serikat -> Status_Pasar: Demand (Karena merupakan pasar tujuan/pembeli)
        3. Negara: Indonesia -> Status_Pasar: Supply (Karena merupakan negara asal komoditas)

        Contoh Kasus 3 (Ekspor):
        Teks: "Indonesia memperkuat posisi di bioenergi sawit (B50) dan ekonomi halal global. Permintaan tinggi dari pasar negara anggota D-8 (Total 1,3 miliar populasi)"
        Hasil Ekstra Extraction Harus Menghasilkan 3 Objek JSON:
        1. Negara: Indonesia -> Status_Pasar: Supply (Karena merupakan negara penghasil Bioenergi sawit B50)
        2. Negara: Indonesia -> Status_Pasar: Supply (Karena merupakan negara penghasil produk halal)
        3. Negara: D-8 Member Countries -> Status_Pasar: Demand (Karena merupakan negara dengan permintaan tinggi untuk Bioenergi sawit B50)
        3. Negara: D-8 Member Countries -> Status_Pasar: Demand (Karena merupakan negara dengan permintaan tinggi untuk produk halal)

        Instruksi Pengisian Bidang JSON (Hasilkan Array of Objects):
        1. 'tanggal': (TIDAK PERLU DIISI, KOSONGKAN SAJA).
        2. 'isi_berita_ringkas': Ringkasan inti dari dinamika ekspor komoditas tersebut (maksimal 2-5 kalimat).
        3. 'sumber_berita': Ambil url dari berita tersebut. Jika tidak ada maka ambil dari tanda kurung siku di akhir paragraf (contoh dari '[Tempo, Bisnis]' menjadi 'Tempo, Bisnis'). Jika tidak ada, isi "-".
        4. 'komoditas': Nama komoditas/barang fisik utama (contoh: "Biji Plastik dan Nafta" atau "Daun Ketapang").
        5. 'status_pasar': Hanya boleh diisi 'Supply' atau 'Demand'.
        6. 'negara': Nama negara pelaku.
        7. 'kota': Nama kota yang disebutkan, jika tidak ada tulis nama Ibu Kota negara tersebut or "-".
        8. 'latitude': Koordinat perkiraan garis lintang (latitude) dari negara/kota tersebut.
        9. 'longitude': Koordinat perkiraan garis bujur (longitude) dari negara/kota tersebut.
        10. 'analisis_makro': Analisis secara makro untuk data ini sesuai teks dokumen.
        """

        # Skema Output JSON Array untuk Google Sheets (10 Kolom data olahan)
        schema_ekstraksi = types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "tanggal": types.Schema(type=types.Type.STRING),
                    "isi_berita_ringkas": types.Schema(type=types.Type.STRING),
                    "sumber_berita": types.Schema(type=types.Type.STRING),
                    "komoditas": types.Schema(type=types.Type.STRING),
                    "status_pasar": types.Schema(type=types.Type.STRING),
                    "negara": types.Schema(type=types.Type.STRING),
                    "kota": types.Schema(type=types.Type.STRING),
                    "latitude": types.Schema(type=types.Type.STRING),
                    "longitude": types.Schema(type=types.Type.STRING),
                    "analisis_makro": types.Schema(type=types.Type.STRING),
                },
                required=[
                    "tanggal", "isi_berita_ringkas", "sumber_berita", "komoditas",
                    "status_pasar", "negara", "kota", "latitude", "longitude", "analisis_makro"
                ]
            )
        )

        models_fallback_order = ['gemini-3.1-flash-lite', 'gemini-3.5-flash', 'gemini-3.1-flash-lite', 'gemma-4-31b-it', 'gemma-4-26b-a4b-it']

        # 4. Kirim referensi file cloud (uploaded_file) beserta prompt ke Gemini
        for model_name in models_fallback_order:
            await gemini_limiter.acquire()
            try:
                print(f"[-] Mengekstrak data menggunakan model: {model_name}...")
                response = await current_client.aio.models.generate_content(
                    model=model_name,
                    contents=[uploaded_file, prompt_ekstraksi],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema_ekstraksi
                    )
                )
                if response and response.text:
                    response_text = response.text.strip()
                    break
            except Exception as e:
                print(f"        [!] Model {model_name} Gagal/Timeout: {e}. Mengajukan fallback...")

    finally:
        # PEMBERSIHAN MUTLAK: Selalu hapus file lokal dan file cloud dari sistem Gemini agar hemat ruang
        if os.path.exists(temp_txt_file):
            try:
                os.remove(temp_txt_file)
                print("[-] Berhasil membersihkan file lokal sementara.")
            except:
                pass
        
        if uploaded_file:
            try:
                await asyncio.to_thread(lambda: current_client.files.delete(name=uploaded_file.name))
                print("[-] Berhasil menghapus file dari storage Gemini Cloud.")
            except Exception as clear_err:
                print(f"[!] Gagal menghapus file cloud: {clear_err}")

    if not response_text:
        print("[!] Gagal menstrukturkan data lewat AI Fallback File API.")
        return None

    # 5. Parsing Hasil JSON Array dan masukkan ke format baris Google Sheets
    values_to_append = []
    data_rows = []

    try:
        match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if match: 
            response_text = match.group(0)

        clean_json = re.sub(r'```json|```', '', response_text).strip()
        data_rows = json.loads(clean_json)

        for item in data_rows:
            # ========================================================
            # PROSES PARSING & JITTERING KOORDINAT (500M - 1KM)
            # ========================================================
            lat_val, lon_val = "0", "0"
            try:
                lat_float = float(item.get("latitude", "0"))
                lon_float = float(item.get("longitude", "0"))
            except ValueError:
                lat_float, lon_float = 0.0, 0.0

            if lat_float != 0.0 and lon_float != 0.0:
                current_coord_str = f"{lat_float:.6f}|{lon_float:.6f}"
                hitung_geser = 1
                
                # Jika koordinat sudah terdaftar di sheet, lakukan pergeseran sejauh 500m - 1km
                while current_coord_str in existing_coords:
                    # 500 meter ~ 0.0045 derajat, 1 km ~ 0.0090 derajat ke Bumi
                    delta_lat = random.uniform(0.0045, 0.0090) * random.choice([-1, 1])
                    delta_lon = random.uniform(0.0045, 0.0090) * random.choice([-1, 1])
                    
                    lat_geser = lat_float + (delta_lat * hitung_geser)
                    lon_geser = lon_float + (delta_lon * hitung_geser)
                    
                    current_coord_str = f"{lat_geser:.6f}|{lon_geser:.6f}"
                    hitung_geser += 1
                
                lat_val, lon_val = current_coord_str.split("|")
                existing_coords.add(current_coord_str)
            else:
                lat_val = item.get("latitude", "0")
                lon_val = item.get("longitude", "0")

            url_maps = f"https://www.google.com/maps?q={lat_val},{lon_val}"
            
            # Susun kolom rapi untuk baris Spreadsheet Anda
            row_data = [
                tanggal_hari_ini,
                item.get("isi_berita_ringkas", ""),
                item.get("sumber_berita", ""),
                item.get("komoditas", "").title(),
                item.get("status_pasar", "").strip().capitalize(),
                item.get("negara", ""),
                item.get("kota", ""),
                url_maps,
                lat_val,
                lon_val,
                item.get("analisis_makro", "")
            ]
            values_to_append.append(row_data)

        # 6. Push Data Hasil Olahan ke Google Sheets API
        if values_to_append:
            def append_to_sheets():
                body = {'values': values_to_append}
                sheets_client.values().append(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:A",
                    valueInputOption="USER_ENTERED",
                    body=body
                ).execute()

            await asyncio.to_thread(append_to_sheets)
            print(f"[+] BERHASIL: Menyimpan {len(values_to_append)} data dari File API laporan ekspor ke Google Sheets (Proteksi Jittering Koordinat Aktif)!")
        else:
            print("[-] Tidak ada data tabel valid yang berhasil diekstrak.")
        
    except Exception as err:
        print(f"[!] Kendala saat parsing JSON atau pengiriman ke Google Sheets: {err}")

    return data_rows

def get_existing_data(spreadsheet_id, target_sheet_name):
    """Mengambil Nama Usaha, Kota, serta Latitude dan Longitude dari Sheets untuk deteksi duplikat."""
    try:
        # Mengambil kolom D (Nama Usaha) sampai I (Longitude)
        # D=4, E=5, F=6, G=7, H=8 (Lat), I=9 (Lon)
        range_to_read = f"{target_sheet_name}!D:I"
        result = sheets_client.values().get(spreadsheetId=spreadsheet_id, range=range_to_read).execute()
        rows = result.get('values', [])
        
        existing_keys = set()
        existing_coords = set()
        
        for row in rows:
            # Cek duplikat Nama Usaha + Kota (Kolom D dan F)
            if len(row) >= 3:
                key = f"{row[0].strip().lower()}|{row[2].strip().lower()}"
                existing_keys.add(key)
            
            # Cek data koordinat (Kolom H dan I -> indeks 4 dan 5 di slice D:I)
            if len(row) >= 6:
                lat = row[4].strip()
                lon = row[5].strip()
                if lat and lon and lat != "0" and lon != "0":
                    existing_coords.add(f"{lat}|{lon}")
                    
        return existing_keys, existing_coords
    except Exception as e:
        print(f"[!] Gagal membaca data lama untuk cek duplikat: {e}")
        return set(), set()

async def proses_pencarian_leads_bisnis(data_entitas_ai, spreadsheet_id, target_sheet_name="Data Utama"):
    if not sheets_client:
        print("[!] Client Google Sheets tidak aktif. Proses dibatalkan.")
        return

    print(f"\n──────────────────────────────────────")
    print(f"[-] MEMULAI PROSES GOOGLE MAPS WEB SCRAPER (NO API KEY REQUIRED)")
    print(f"──────────────────────────────────────")

    # 1. Ambil data duplikat yang ada di sheet saat ini
    existing_keys, existing_coords = await asyncio.to_thread(get_existing_data, spreadsheet_id, target_sheet_name)
    print(f"[*] Menemukan {len(existing_keys)} nama unik dan {len(existing_coords)} koordinat di sheet.")

    current_client, current_api_key = await client_rotator.get_client()
    valid_items_to_analyze = []
    temp_leads_file = f"temp_leads_to_analyze_{int(time.time())}.csv"
    
    try:
        def create_temp_leads_csv():
            lookup_items = []
            with open(temp_leads_file, 'w', encoding='utf-8', newline='') as f_out:
                writer = csv.writer(f_out)
                writer.writerow(["ID_Entitas", "Komoditas", "Status_Pasar", "Kota", "Negara"])
                counter = 1
                for entitas in data_entitas_ai:
                    komoditas = entitas.get("komoditas", "").strip()
                    status_pasar = entitas.get("status_pasar", "").strip()
                    negara = entitas.get("negara", "").strip()
                    kota = entitas.get("kota", "").strip()
                    analisis_makro = entitas.get("analisis_makro", "").strip()
                    
                    if not komoditas or not status_pasar: continue
                    writer.writerow([counter, komoditas, status_pasar, kota, negara])
                    
                    lookup_items.append({
                        "id_entitas": counter,
                        "komoditas": komoditas.title(),
                        "stakeholder": "Pembeli" if status_pasar.lower() == "demand" else "Supplier",
                        "negara": negara,
                        "kota": kota,
                        "analisis_makro": analisis_makro,
                    })
                    counter += 1
            return lookup_items
        valid_items_to_analyze = await asyncio.to_thread(create_temp_leads_csv)
    except Exception as err:
        print(f"[!] Gagal mempersiapkan file CSV lokal: {err}")
        return

    uploaded_leads_file = None
    queries_lookup = {}

    try:
        def upload_leads_to_ai():
            return current_client.files.upload(file=temp_leads_file, config=types.UploadFileConfig(mime_type="text/csv"))
        uploaded_leads_file = await asyncio.to_thread(upload_leads_to_ai)

        # 2. PROMPT BATCH AI: MERUMUSKAN 3 TIER KUERI GOOGLE MAPS BERDASARKAN SKALA BISNIS
        prompt_batch_query = """
        Bertindaklah sebagai B2B Lead Generation Specialist & Market Intelligence Internasional.
        Saya adalah seorang eksportir. Saya ingin melihat demand maupun supply dari suatu komoditas.
        Tugas Anda adalah merumuskan TIGA (3) kueri pencarian lokal spesifik (Bahasa Inggris) untuk dimasukkan ke Google Maps berdasarkan file CSV yang dilampirkan.

        TARGET STRATEGI STRUKTUR TIER KUERI (WAJIB PATUH):
        - Jika 'status_pasar' merupakan Demand (Pembeli), pecah kueri berdasarkan 3 tingkatan skala bisnis dari kecil ke besar:
          * Kueri 1 (Tier 1 - Skala Kecil / Konsumen Ritel Komersial): Fokus mencari bisnis pengguna akhir yang langsung menyerap produk (contoh: Kafe, Roastery lokal, Bakery, Restoran lokal, dan sebagainya).
          * Kueri 2 (Tier 2 - Skala Menengah / Grosir & Distributor): Fokus mencari rantai distribusi tengah yg berhubungan dengan produk (contoh: B2B Wholesaler, local supplier, distributor bahan baku, dan sebagainya).
          * Kueri 3 (Tier 3 - Skala Besar / Importir & Industri Manufaktur): Fokus mencari penyerap volume masif produk (contoh: Main Importer, Trading House internasional, F&B factory, dan sebagainya).
          
        - Jika 'status_pasar' merupakan Supply (Supplier/Penjual), pecah kueri berdasarkan tingkatan pasokan:
          * Kueri 1 (Tier 1 - Pengrajin/Produsen Kecil): Pembuat lokal, workshop, asosiasi petani lokal atau sebagainya.
          * Kueri 2 (Tier 2 - Pabrik/Supplier Menengah): Supplier B2B lokal, pabrikasi wilayah, processing mill menengah atau sebagainya.
          * Kueri 3 (Tier 3 - Pabrik Besar/Eksportir Utama): Pabrik manufaktur utama skala industri, Perusahaan perdagangan ekspor atau sebagainya.

        - Jika 'status_pasar' merupakan entitas lain (contoh: Forwarder, Bea Cukai, Agen Logistik, dll), pecah kueri berdasarkan jangkauan atau skala operasi:
          * Kueri 1 (Tier 1 - Skala Lokal/Cabang): Kantor cabang lokal, perantara logistik kecil, atau jasa custom clearance perorangan/lokal.
          * Kueri 2 (Tier 2 - Skala Menengah/Nasional): Perusahaan forwarder/logistik skala nasional atau perusahaan B2B kepabeanan.
          * Kueri 3 (Tier 3 - Skala Besar/Pusat/Internasional): Otoritas pelabuhan utama (Port Authority), instansi resmi Bea Cukai pusat (Customs Office), atau perusahaan logistik multinasional.

        Respons HARUS berupa JSON Array murni berisi list objek per id_entitas (tanpa markdown ```json, tanpa penjelasan teks pembuka/penutup):
        [
          {
            "id_entitas": 1,
            "search_targets": [
              { "tipe_bisnis": "Konsumen Hilir (Cafe/Roastery)", "maps_search_query": "Specialty Coffee Cafe Kuala Lumpur" },
              { "tipe_bisnis": "Distributor/Grosir", "maps_search_query": "Coffee Wholesaler Supplier Kuala Lumpur" },
              { "tipe_bisnis": "Importir/Pabrik Besar", "maps_search_query": "Coffee Importer Processing Factory Malaysia" }
            ]
          }
        ]
        """
        batch_query_schema = types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "id_entitas": types.Schema(type=types.Type.INTEGER),
                    "search_targets": types.Schema(
                        type=types.Type.ARRAY,
                        items=types.Schema(
                            type=types.Type.OBJECT,
                            properties={
                                "tipe_bisnis": types.Schema(type=types.Type.STRING),
                                "maps_search_query": types.Schema(type=types.Type.STRING)
                            },
                            required=["tipe_bisnis", "maps_search_query"]
                        )
                    )
                },
                required=["id_entitas", "search_targets"]
            )
        )

        models_fallback_order = ['gemini-3.1-flash-lite', 'gemma-4-31b-it', 'gemini-3.1-flash-lite', 'gemini-3.5-flash', 'gemma-4-26b-a4b-it']
        response_text = ""
        for model_name in models_fallback_order:
            await gemini_limiter.acquire()
            try:
                print(f"[-] Merumuskan perluasan kueri Google Maps dengan model: {model_name}...")
                response = await current_client.aio.models.generate_content(
                    model=model_name, contents=[uploaded_leads_file, prompt_batch_query],
                    config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=batch_query_schema, temperature=0.1)
                )
                if response and response.text:
                    response_text = response.text.strip()
                    break
            except Exception as e: pass

        match = re.search(r'\[.*\]', response_text, re.DOTALL)
        if match: response_text = match.group(0)
        parsed_queries = json.loads(response_text)
        queries_lookup = {item["id_entitas"]: item["search_targets"] for item in parsed_queries}
    except Exception as err:
        print(f"[!] Gagal memproses File API: {err}")
        return
    finally:
        if uploaded_leads_file:
            try: current_client.files.delete(name=uploaded_leads_file.name)
            except Exception: pass
        if os.path.exists(temp_leads_file): os.remove(temp_leads_file)

    # 3. LIVE GOOGLE MAPS WEB SCRAPING VIA PLAYWRIGHT
    values_to_append = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36")
        page = await context.new_page()

        for item in valid_items_to_analyze:
            search_targets = queries_lookup.get(item["id_entitas"], [])
            
            for target in search_targets:
                maps_query = target.get("maps_search_query", "").strip()
                tipe_bisnis_target = target.get("tipe_bisnis", "").strip()
                if not maps_query: continue

                print(f"    [*] Membuka Google Maps Web -> Kueri: '{maps_query}'...")
                jumlah_per_kueri = 0  # Counter lokal untuk mencatat hasil per kueri spesifik

                try:
                    # Langsung tembak URL pencarian Google Maps Web resmi
                    encoded_q = urllib.parse.quote_plus(maps_query)
                    maps_url = f"https://www.google.com/maps/search/{encoded_q}?hl=id"
                    
                    await page.goto(maps_url, timeout=30000, wait_until="domcontentloaded")
                    await page.wait_for_timeout(2000) # Tunggu Maps memproses rendering lokasi pin

                    # Selektor kontainer sidebar kiri tempat list hasil Google Maps berada
                    scrollable_sidebar_selector = "div[role='feed']"
                    
                    try:
                        # Pastikan sidebar termuat terlebih dahulu
                        await page.wait_for_selector(scrollable_sidebar_selector, timeout=15000)
                        
                        # Lakukan scroll sebanyak 4-5 kali ke bawah untuk memuat hingga 15-20 tempat usaha
                        for scroll_step in range(4):
                            # Arahkan mouse/pointer ke koordinat kontainer sidebar kiri
                            sidebar_element = await page.query_selector(scrollable_sidebar_selector)
                            if sidebar_element:
                                # Mengambil bounding box kontainer untuk menentukan posisi tengah koordinat mouse
                                box = await sidebar_element.bounding_box()
                                if box:
                                    # Pindahkan mouse ke tengah kontainer sidebar, lalu lakukan scroll wheel ke bawah
                                    await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                    await page.mouse.wheel(0, 3000) # Scroll ke bawah sejauh 3000 pixel
                            
                            await page.wait_for_timeout(1500) # Jeda singkat menunggu data baru dimuat hulu
                    except Exception as scroll_err:
                        print(f"        [!] Peringatan kendala scrolling sidebar: {scroll_err}")
                    
                    # Ambil elemen kontainer tempat bisnis yang terdaftar di sidebar Google Maps (`div.Nv2y3c` atau `a.hfpxzc`)
                    places_elements = await page.query_selector_all("a[href*='/maps/place/']")
                    
                    if not places_elements:
                        print("        [?] Tautan lokasi fisik a[href*='/maps/place/'] belum termuat di sidebar.")
                        continue
                        
                    print(f"        [+] Menemukan {len(places_elements)} profil bisnis terdaftar resmi. Mengekstrak data...")
                    
                    for link_element in places_elements: 
                        place_name = await link_element.get_attribute("aria-label")
                        place_url = await link_element.get_attribute("href")
                        
                        if not place_name or not place_url:
                            continue
                        
                        # ========================================================
                        # 1. CEK DUPLIKAT NAMA & KOTA LEBIH AWAL (OPTIMASI)
                        # ========================================================
                        current_key = f"{place_name.strip().lower()}|{item['kota'].strip().lower()}"
                        if current_key in existing_keys:
                            continue 

                        existing_keys.add(current_key) 
                        
                        # ========================================================
                        # 2. PROSES PARSING & JITTERING KOORDINAT (~100 METER)
                        # ========================================================
                        lat_val, lon_val = "0", "0"
                        coord_match = re.search(r'!3d([-.\d]+)!4d([-.\d]+)', place_url)
                        if coord_match:
                            try:
                                lat_float = float(coord_match.group(1))
                                lon_float = float(coord_match.group(2))
                                
                                current_coord_str = f"{lat_float:.6f}|{lon_float:.6f}"
                                
                                hitung_geser = 1
                                while current_coord_str in existing_coords:
                                    delta_lat = (0.0009 + random.uniform(-0.0001, 0.0001)) * random.choice([-1, 1])
                                    delta_lon = (0.0009 + random.uniform(-0.0001, 0.0001)) * random.choice([-1, 1])
                                    
                                    lat_geser = lat_float + (delta_lat * hitung_geser)
                                    lon_geser = lon_float + (delta_lon * hitung_geser)
                                    
                                    current_coord_str = f"{lat_geser:.6f}|{lon_geser:.6f}"
                                    hitung_geser += 1
                                
                                lat_val, lon_val = current_coord_str.split("|")
                                existing_coords.add(current_coord_str)
                                
                            except Exception as e:
                                lat_val = coord_match.group(1)
                                lon_val = coord_match.group(2)
                            
                        # Blok duplikat pengecekan nama di bawah ini sudah dihapus
                        kategori_detail = tipe_bisnis_target 
                        
                        try:
                            parent_article = await link_element.query_selector("xpath=ancestor::div[@role='article']")
                            if parent_article:
                                desc_element = await parent_article.query_selector(".fontBodyMedium div:nth-child(4) div:nth-child(1)")
                                if desc_element:
                                    text_kategori = await desc_element.inner_text()
                                    if text_kategori and len(text_kategori.strip()) > 1:
                                        kategori_detail = text_kategori.strip()
                        except Exception:
                            pass

                        acak_4_char = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
                        id_unik = f"UD{acak_4_char}" 

                        row_data = [
                            id_unik,
                            str(item["stakeholder"]),
                            str(item["komoditas"]),
                            str(place_name),
                            str(item["negara"]),
                            str(item["kota"]),
                            str(place_url),
                            f"{lat_val}",
                            f"{lon_val}",
                            "",
                            "",
                            f"{kategori_detail}",
                            f"Profil Usaha Fisik ({kategori_detail}) Terdaftar Resmi di Google Maps wilayah {item['kota']}, {item['negara']}.",
                            str(maps_query)
                        ]
                        values_to_append.append(row_data)
                        jumlah_per_kueri += 1

                    print(f"        [√] Sukses mengekstrak {jumlah_per_kueri} profil untuk kueri ini.")
                    
                except Exception as maps_err:
                    print(f"        [!] Gagal scraping Google Maps Web pada kueri ini: {maps_err}")
                    continue
                    
                await page.wait_for_timeout(2000) # Jeda aman anti-bot

        await browser.close()

    # 4. PUSH DATA BERSIH KE GOOGLE SPREADSHEET
    # Asumsikan 'values_to_append' adalah list berisi baris-baris data akhir yang siap ditulis.
    # Jika tidak ada data, langsung lewati.
    if not values_to_append:
        print("[!] Tidak ada profil toko fisik/leads yang berhasil lolos filter untuk ditulis.")
        return

    # =====================================================================
    # LOGIKA BARU: CHUNKING & RETRY UNTUK GOOGLE SHEETS
    # =====================================================================
    CHUNK_SIZE = 50       # Jumlah baris maksimal per 1x request (Bisa diatur ulang 50-100)
    MAX_RETRIES = 3       # Maksimal percobaan ulang jika gagal
    RETRY_DELAY = 5       # Jeda waktu (detik) sebelum mengulang
    
    total_data = len(values_to_append)
    print(f"\n[-] Memulai penyimpanan {total_data} leads ke Spreadsheet '{target_sheet_name}'...")
    print(f"[-] Sistem akan memecah pengiriman menjadi batch berukuran {CHUNK_SIZE} baris.")

    total_baris_berhasil = 0

    # Pecah list utama 'values_to_append' menjadi potongan-potongan kecil (chunk)
    for i in range(0, total_data, CHUNK_SIZE):
        chunk = values_to_append[i:i + CHUNK_SIZE]
        batch_num = (i // CHUNK_SIZE) + 1
        
        # Fungsi pembantu lokal untuk menulis 1 chunk ke Google Sheets
        def append_chunk_to_google():
            body = {'values': chunk}
            return sheets_client.values().append(
                spreadsheetId=spreadsheet_id,
                range=f"{target_sheet_name}!A:A",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

        # Mulai Logika Retry untuk Chunk Spesifik ini
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Panggil ke Google Sheets secara Asinkron
                result = await asyncio.to_thread(append_chunk_to_google)
                updates = result.get('updates', {})
                baris_terupdate = updates.get('updatedRows', 0)
                
                print(f"    [+] BATCH {batch_num}: Berhasil menulis {baris_terupdate} baris.")
                total_baris_berhasil += baris_terupdate
                break  # Berhasil -> Keluar dari loop Retry, lanjut ke Batch berikutnya

            except Exception as sheet_err:
                print(f"    [!] BATCH {batch_num} - Percobaan {attempt} Gagal: {sheet_err}")
                
                if attempt < MAX_RETRIES:
                    print(f"        [-] Menunggu {RETRY_DELAY} detik sebelum mencoba lagi...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    print(f"    [!] BATCH {batch_num} Gagal total setelah {MAX_RETRIES} percobaan. Dilewati.")
                    # Opsional: Jika dibutuhkan, Anda bisa menyimpan 'chunk' yang gagal ke dalam
                    # file error_log.csv lokal agar tidak ada data leads berharga yang hilang.
        
        # Jeda antar-batch (Rate-limit throttle proteksi)
        # Jangan melakukan jeda di batch terakhir
        if i + CHUNK_SIZE < total_data:
            await asyncio.sleep(2) 

    print(f"\n[+] REPOT MAPS FINAL: Secara keseluruhan berhasil menulis {total_baris_berhasil} dari {total_data} leads ke sheet '{target_sheet_name}'!")

# ==========================================
# PARALLEL SCRAPER PER SITE
# ==========================================
async def scrape_single_site(site, context, tab_semaphore, master_file_name):
    print(f"\nMulai memproses situs: {site['name']}")
    page = await context.new_page()
    urls_to_scrape = []
    
    try:
        if site["handling_method"] == "rss":
            rss_links = await handle_rss_feed(site["url"], site['engine_browser'], page)
            urls_to_scrape = list(set(rss_links))[:int(site['max_articles'])]
        
        elif site["handling_method"] in ["infinite_scroll", "load_more_button"]:
            print(f"[-] Membuka beranda {site['name']}...")
            
            try:
                # Set timeout 30 detik untuk goto, wait_until cukup "load" saja
                await page.goto(site['url'], wait_until="domcontentloaded", timeout=30000)
                
            except (asyncio.TimeoutError, PlaywrightTimeoutError):
                # Jika timeout 5 detik di atas habis, jangan anggap ini error fatal.
                # Cetak informasi ini dan biarkan skrip lanjut ke auto_scroll bawah.
                print(f"    [-] Jaringan tidak sepenuhnya idle dalam 5 detik (banyak iklan/tracker), mengabaikan dan lanjut...")
            except Exception as e:
                print(f"    [!] Ada kendala lain saat memuat halaman: {e}")
        
            await auto_scroll(page, max_scroll_steps=10)
            if site["handling_method"] == "infinite_scroll":
                await handle_infinite_scroll(page, int(site.get("click_count", 3)))
            elif site["handling_method"] == "load_more_button":
                await handle_load_more(page, site["load_more_button_selector"], int(site.get("click_count", 3)))
            
            raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
            unique_links = list(set([link for link in raw_links if link]))

            print(f" [+] Berhasil menemukan {len(unique_links)} link berita dari {site['url']}.")

            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await process_task_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

        # =====================================================================
        # PENANGANAN METODE PAGINATION DENGAN TOMBOL "NEXT"
        # =====================================================================
        elif site["handling_method"] == "next_button_pagination":
            all_raw_links = []
            total_pages = int(site.get("total_pages", 1)) if site.get("total_pages") else int(site.get("click_count", 1))
            
            print(f"[-] Membuka beranda awal {site['name']}...")

            try:
                # Set timeout 30 detik untuk goto, wait_until cukup "load" saja
                await page.goto(site['url'], wait_until="load", timeout=30000)
                
            except (asyncio.TimeoutError, PlaywrightTimeoutError):
                # Jika timeout 5 detik di atas habis, jangan anggap ini error fatal.
                # Cetak informasi ini dan biarkan skrip lanjut ke auto_scroll bawah.
                print(f"    [-] Gagal menunggu load selama 30 detik, mengabaikan dan lanjut...")
            except Exception as e:
                print(f"    [!] Ada kendala lain saat memuat halaman: {e}")

            await auto_scroll(page, max_scroll_steps=10)
            
            # Ambil tautan dari halaman pertama terlebih dahulu
            raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
            all_raw_links.extend(raw_links)
            
            # Perulangan untuk menekan tombol "Next" sebanyak halaman berikutnya
            for page_num in range(2, total_pages + 1):
                try:
                    # Ambil selektor tombol next dari konfigurasi Spreadsheet Anda (misal: site['next_button_selector'])
                    next_selector = site.get("next_button_selector") or site.get("load_more_button_selector")
                    if not next_selector:
                        print(f"    [!] Selektor tombol Next tidak ditemukan untuk {site['name']}. Menghentikan pagination.")
                        break
                        
                    button = page.locator(next_selector)
                    if await button.is_visible():
                        print(f"    [-] Menekan tombol Next untuk menuju ke Halaman {page_num}...")
                        await button.scroll_into_view_if_needed()
                        await button.click()
                        
                        # Berikan jeda waktu agar konten halaman baru selesai dimuat
                        await page.wait_for_timeout(3000)

                        # --- MODIFIKASI: Memaksa halaman kembali ke posisi paling atas ---
                        await page.evaluate("window.scrollTo(0, 0);")
                        await page.wait_for_timeout(1000) # Jeda stabilisasi posisi atas

                        await auto_scroll(page, max_scroll_steps=10)
                        
                        # Ambil tautan dari halaman baru ini
                        raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
                        all_raw_links.extend(raw_links)
                    else:
                        print(f"    [!] Tombol Next tidak terlihat pada halaman {page_num - 1}. Menghentikan pagination.")
                        break
                except Exception as pagination_err:
                    print(f"    [!] Gagal berpindah ke halaman {page_num}: {pagination_err}")
                    break
            
            unique_links = list(set([link for link in all_raw_links if link]))

            print(f" [+] Berhasil menemukan {len(unique_links)} link berita dari {site['url']}.")

            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await process_task_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

        elif site["handling_method"] == "pagination":
            all_raw_links = []
            total_pages = int(site.get("total_pages", 1)) if site.get("total_pages") else int(site.get("click_count", 1))
            
            # CEK: Jika engine menggunakan firecrawl
            if site.get("engine_browser") == "firecrawl":
                print(f" [*] Menggunakan Firecrawl dengan mode Pagination untuk {site['url']}")
                if not FIRECRAWL_API_KEY:
                    print(" [!] Gagal: FIRECRAWL_API_KEY tidak ditemukan di .env")
                    return
                
                app = Firecrawl(api_key=FIRECRAWL_API_KEY)
                
                for page_num in range(1, total_pages + 1):
                    target_url = f"{site['url']}{page_num}"
                    try:
                        print(f" [+] Scraping via Firecrawl Halaman {page_num}: {target_url}")
                        scrape_result = app.scrape(target_url, formats=["links"])
                        raw_links = scrape_result.links
                        all_raw_links.extend(raw_links)
                    except Exception as e:
                        print(f" [!] Error Firecrawl di halaman {page_num}: {e}. Melompati...")
                        continue
                    
            # JIKA BUKAN FIRECRAWL (Gunakan Playwright seperti biasa)
            else:
                for page_num in range(1, total_pages + 1):
                    target_url = f"{site['url']}{page_num}"
                    try:
                        # 1. Gunakan wait_until="load" dengan timeout eksplisit (misal 20 detik)
                        await page.goto(target_url, wait_until="domcontentloaded", timeout=30000)

                        # 2. Lakukan scroll dan ambil data jika halaman berhasil terbuka
                        await auto_scroll(page, max_scroll_steps=10)
                        raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
                        all_raw_links.extend(raw_links)
                        
                    except (asyncio.TimeoutError, PlaywrightTimeoutError) as t_err:
                        print(f"    [!] Halaman {page_num} lambat/gagal dimuat (Timeout). Melompati ke halaman berikutnya...")
                        continue
                    except Exception as page_err:
                        print(f"    [!] Error tidak terduga di halaman {page_num}: {page_err}. Melompati...")
                        continue
            # --- Proses Pemfilteran (Bagian ini dipakai bersama oleh Firecrawl maupun Playwright) ---
            unique_links = list(set([link for link in all_raw_links if link]))

            print(f" [+] Berhasil menemukan {len(unique_links)} link berita dari {site['url']}.")
            
            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await process_task_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

        elif site["handling_method"] == "firecrawl":
            print(f" [*] Menggunakan Firecrawl untuk membuka {site['url']}")
            try:
                # Inisialisasi firecrawl (pola ini mirip dengan fungsi saham_lq45_terbaik_idx Anda)
                if not FIRECRAWL_API_KEY:
                    print(" [!] Gagal: FIRECRAWL_API_KEY tidak ditemukan di .env")
                    return
                
                app = Firecrawl(api_key=FIRECRAWL_API_KEY)
                
                # Melakukan scrape menggunakan firecrawl dengan format ekstrak link
                # Kita arahkan agar firecrawl mengembalikan objek yang bersih
                scrape_result = app.scrape(
                    site['url'], 
                    formats=["links"]
                )
                
                # Mengambil daftar tautan yang berhasil diekstrak oleh Firecrawl
                raw_links = scrape_result.links
                
                # Filter tautan agar hanya mengambil yang unik dan valid
                unique_links = list(set([link for link in raw_links if link]))
                
                # Cetak jumlah hasil pencarian sesuai format Anda
                print(f" [+] Berhasil menemukan {len(unique_links)} link berita dari {site['url']}.")

                if unique_links:
                    memory_links_csv = io.StringIO()
                    writer = csv.writer(memory_links_csv)
                    writer.writerow(["Raw_URL"])
                    for link in unique_links: writer.writerow([link])
                    
                    filtered_links_response = await process_task_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                    urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

            except Exception as e:
                print(f" [!] Error saat menggunakan Firecrawl pada {site['url']}: {e}")
                unique_links = []

        elif site["handling_method"] == "yahoo_news":
            yahoo_news_links = await fetch_yahoo_finance_news_urls(site["url"])
            urls_to_scrape = list(set(yahoo_news_links))[:int(site['max_articles'])]
            

        if not urls_to_scrape: 
            return
        
        tasks = [fetch_article_data(context, url, tab_semaphore, site.get("selector_extract"), int(site.get("max_scroll_article", 12))) for url in urls_to_scrape]
        scraped_results = await asyncio.gather(*tasks)
        valid_results = [res for res in scraped_results if res is not None]
        
        if not valid_results:
            return
        
        print(f" [+] Mengambil {len(valid_results)} isi berita dari {site['url']}.")
        
        memory_data_csv = io.StringIO()
        csv_writer = csv.writer(memory_data_csv)
        csv_writer.writerow(["URL", "RawText"])
        for res in valid_results: 
            csv_writer.writerow([res['url'], res['text'][:8000].replace('\n', ' ')])
        
        final_extracted_data = await process_task_with_gemini(site['data_prompt'], memory_data_csv.getvalue())

        if final_extracted_data.strip():
            print(f"[8] Menyimpan hasil ekstraksi berita ({site['name']}) oleh AI ke file master lokal...")
            
            # Solusi 3: Menggunakan io.StringIO agar teks dibaca layaknya file utuh
            f_input = io.StringIO(final_extracted_data.strip())
            # Solusi 2: Memanfaatkan modul csv.reader resmi Python untuk parsing text
            reader_gemini = csv.reader(f_input, delimiter=',', quotechar='"')
            
            with open(master_file_name, 'a', newline='', encoding='utf-8') as f_append:
                writer = csv.writer(f_append, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                for parsed_row in reader_gemini:
                    if not parsed_row:
                        continue
                    
                    # Solusi 1: Deteksi Header Dinamis di baris mana pun
                    if any(header_word in parsed_row[0] for header_word in ["Tanggal", "Isi Berita", "URL"]):
                        continue
                        
                    try:
                        # Sekarang parsed_row sudah otomatis menjadi LIST yang bersih ([kolom1, kolom2, kolom3])
                        if len(parsed_row) >= 3:
                            tanggal = parsed_row[0].strip()
                            isi_berita = parsed_row[1].strip()
                            url = parsed_row[2].strip()
                            
                            if len(isi_berita) > 10:
                                writer.writerow([tanggal, isi_berita, url])
                        else:
                            # Fallback jika baris tidak sengaja kekurangan kolom
                            join_row = " ".join(parsed_row).strip()
                            if len(join_row) > 10:
                                writer.writerow(["-", join_row, site.get('url', '-')])
                    except Exception as parse_err:
                        print(f"    [!] Gagal memproses baris data: {parse_err}")
                            
    except Exception as e:
        print(f"[!] Kendala di situs {site['name']}: {e}")
    finally:
        await page.close()
        
# ==========================================
# MAIN ROUTINE
# ==========================================
async def main():
    # [1] Memuat data dinamis di awal program
    # [1] Memuat data dinamis di awal program
    WEBSITES, PROMPTS_DATA, TICKERS = await fetch_dynamic_config(WEB_APP_SCRIPT_URL)
    PROMPT_DASAR_FORMAT = PROMPTS_DATA.get("PROMPT_DASAR_FORMAT", "")

    if not WEBSITES:
        print("[!] Program dihentikan karena kegagalan pemuatan konfigurasi Spreadsheet.")
        return
    
    if not WEB_APP_SCRIPT_URL:
        print("[!] Error: Variabel WEB_APP_SCRIPT_URL belum dikonfigurasi!")
        return

    # Lokasi penyimpanan database historis lokal
    folder_db = "database_historis"
    if not os.path.exists(folder_db):
        os.makedirs(folder_db)

    # [2] Membuat file master lokal baru berdasarkan timestamp eksekusi hari ini
    # [2] Membuat file master lokal baru berdasarkan timestamp eksekusi hari ini
    waktu_skrg = datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y%m%d_%H%M%S")
    master_file_name = f"{folder_db}/master_berita_{waktu_skrg}.csv"

    try:
        with open(master_file_name, 'r', encoding='utf-8') as f:
            file_is_empty = not f.read(1)
    except FileNotFoundError:
        file_is_empty = True

    if file_is_empty:
        with open(master_file_name, 'w', newline='', encoding='utf-8') as final_csv_file:
            writer = csv.writer(final_csv_file)
            writer.writerow(["Tanggal", "Isi Berita", "URL"])

    # [3] Ambil data Saham LQ45 IDX secara dinamis
    # [3] Ambil data Saham LQ45 IDX secara dinamis
    idx_lq45_row = await saham_lq45_terbaik_idx()
    if idx_lq45_row:
        waktu_idx = idx_lq45_row[0]
        isi_konten_idx = idx_lq45_row[1]
        url_idx = idx_lq45_row[2]

        isi_konten_simpan_idx = "DATA Top SAHAM LQ45 IDX TERBARU\n" + isi_konten_idx
        data_simpan_idx_lq45_row = [waktu_idx, isi_konten_simpan_idx, url_idx]
       
        with open(master_file_name, 'a', newline='', encoding='utf-8') as final_csv_file:
            writer = csv.writer(final_csv_file)
            writer.writerow(data_simpan_idx_lq45_row)
        print(f"[+] Sukses menyimpan data Saham LQ45 IDX ke file master lokal: {master_file_name}\n")
        
        try:
            zona_wib = pytz.timezone('Asia/Jakarta')
            now_realtime = datetime.now(zona_wib)
            hari_en_to_id = {"Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu", "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"}
            bulan_en_to_id = {"January": "Januari", "February": "Februari", "March": "Maret", "April": "April", "May": "Mei", "June": "Juni", "July": "Juli", "August": "Agustus", "September": "September", "October": "Oktober", "November": "November", "December": "Desember"}
            hari_realtime = hari_en_to_id.get(now_realtime.strftime("%A"), now_realtime.strftime("%A"))
            bulan_realtime = bulan_en_to_id.get(now_realtime.strftime("%B"), now_realtime.strftime("%B"))
            waktu_wib_realtime = now_realtime.strftime("%H:%M:%S") + " WIB"
            tanggal_kirim_indo = f"{hari_realtime}, {now_realtime.strftime('%d')} {bulan_realtime} {now_realtime.strftime('%Y')}"
            
            header_pesan_idx = (
                f"📌 <code>{tanggal_kirim_indo} pukul {waktu_wib_realtime}</code>\n"
                f"<b>REKOMENDASI SAHAM LQ45 IDX TERBAIK HARI INI</b>\n"
                f"────────────────────\n\n"
            )
            
            pesan_full_idx = header_pesan_idx + isi_konten_idx
            
            print("[-] Mengirimkan data Saham LQ45 IDX ke Telegram...")
            await asyncio.to_thread(send_telegram_message, pesan_full_idx)
            print("[+] Data Saham LQ45 IDX berhasil dikirim ke Telegram.")
            await asyncio.sleep(3)
        except Exception as telegram_idx_err:
            print(f"[!] Gagal mengirim data IDX ke Telegram: {telegram_idx_err}")

    # [4] Ambil data finansial Yahoo secara dinamis
    # [4] Ambil data finansial Yahoo secara dinamis
    finansial_row = await asyncio.to_thread(fetch_yahoo_finance_data, TICKERS)
    if finansial_row:
        waktu_yahoo = finansial_row[0]
        isi_konten_yahoo = finansial_row[1]
        url_yahoo = finansial_row[2]
        
        isi_konten_simpan_yahoo = "DATA HARGA MULTI ASET TERBARU\n" + isi_konten_yahoo
        data_simpan_yahoo_row = [waktu_yahoo, isi_konten_simpan_yahoo, url_yahoo]

        with open(master_file_name, 'a', newline='', encoding='utf-8') as final_csv_file:
            writer = csv.writer(final_csv_file)
            writer.writerow(data_simpan_yahoo_row)
        print(f"[+] Sukses menyimpan data finansial Yahoo ke file master lokal: {master_file_name}\n")
        
        # =====================================================================
        # TAMBAHAN: KIRIM DATA YAHOO FINANCE LANGSUNG KE TELEGRAM
        # =====================================================================
        try:
            zona_wib = pytz.timezone('Asia/Jakarta')
            now_realtime = datetime.now(zona_wib)
            
            hari_en_to_id = {
                "Monday": "Senin", "Tuesday": "Selasa", "Wednesday": "Rabu", 
                "Thursday": "Kamis", "Friday": "Jumat", "Saturday": "Sabtu", "Sunday": "Minggu"
            }
            bulan_en_to_id = {
                "January": "Januari", "February": "Februari", "March": "Maret", "April": "April",
                "May": "Mei", "June": "Juni", "July": "Juli", "August": "Agustus",
                "September": "September", "October": "Oktober", "November": "November", "December": "Desember"
            }
            
            hari_realtime = hari_en_to_id.get(now_realtime.strftime("%A"), now_realtime.strftime("%A"))
            bulan_realtime = bulan_en_to_id.get(now_realtime.strftime("%B"), now_realtime.strftime("%B"))
            
            waktu_wib_realtime = now_realtime.strftime("%H:%M:%S") + " WIB"
            tanggal_kirim_indo = f"{hari_realtime}, {now_realtime.strftime('%d')} {bulan_realtime} {now_realtime.strftime('%Y')}"
            
            # Header sesuai permintaan Anda
            header_pesan_yahoo = (
                f"📌 <code>{tanggal_kirim_indo} pukul {waktu_wib_realtime}</code>\n"
                f"<b>Data Pergerakan Harga</b>\n"
                f"────────────────────\n\n"
            )
            
            # isi_berita_finansial ada di index ke-1 dari finansial_row
            isi_konten_yahoo = re.sub(r'(===.*?===)', r'<b>\1</b>', isi_konten_yahoo)
            pesan_full_yahoo = header_pesan_yahoo + isi_konten_yahoo
            
            print("[-] Mengirimkan data Yahoo Finance ke Telegram...")
            await asyncio.to_thread(send_telegram_message, pesan_full_yahoo)
            print("[+] Data Yahoo Finance berhasil dikirim ke Telegram.")
            
            # Jeda singkat setelah kirim pesan agar aman dari rate limit Telegram
            await asyncio.sleep(3)
            
        except Exception as telegram_yahoo_err:
            print(f"[!] Gagal mengirim data Yahoo Finance ke Telegram: {telegram_yahoo_err}")
        
    # [5] Proses Scraping Multi-Situs Web Berdasarkan Config Spreadsheet
    # [5] Proses Scraping Multi-Situs Web Berdasarkan Config Spreadsheet
    if not WEBSITES:
        print("[!] Tidak ada target website yang dimuat dari Spreadsheet. Langsung melompat ke analisis.")
    else:
        async with async_playwright() as p:
            # Mengaktifkan headless=True agar berjalan mulus tanpa antarmuka GUI di GitHub Actions
            browser = await p.chromium.launch(
                headless=True
            )

            # Berikan User-Agent manusia asli agar tidak dicurigai
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            )

            # ==============================================================
            # Memblokir Pemuatan Gambar, CSS, dan Font di Playwright
            # ==============================================================
            await context.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "stylesheet", "media", "font"] 
                else route.continue_()
            )
            
            # Semaphore 1: Membatasi maksimal 5 situs yang berjalan PARALEL dalam satu waktu
            site_semaphore = asyncio.Semaphore(5)
            
            # Semaphore 2: Membatasi max 10 tab artikel terbuka bersamaan di internal seluruh situs
            tab_semaphore = asyncio.Semaphore(10)
            
            # Fungsi pembungkus (wrapper) untuk menerapkan limitasi site_semaphore
            async def scrape_with_limit(site):
                async with site_semaphore:
                    await scrape_single_site(site, context, tab_semaphore, master_file_name)
            
            # Membuat list coroutine/tasks menggunakan fungsi pembungkus baru
            site_tasks = [
                scrape_with_limit(site) 
                for site in WEBSITES
            ]
            
            print(f"[-] Menjalankan scraping untuk {len(WEBSITES)} situs dengan sistem antrean (Maks 5 situs paralel)...")
            # Memicu eksekusi paralel yang sudah dibatasi
            await asyncio.gather(*site_tasks)
            
            await browser.close()

    # [6] Eksekusi analisis berurutan setelah data terkumpul lengkap
    # [6] Eksekusi analisis berurutan setelah data terkumpul lengkap
    hasil_ekspor = await proses_analisis_berita_master(master_file_name, PROMPTS_DATA, PROMPT_DASAR_FORMAT)

    # [7] Menyimpan berita ekspor ke spreadsheet ekspor-impor map
    # [7] Menyimpan berita ekspor ke spreadsheet ekspor-impor map
    data_untuk_leads = []
    SPREADSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
    SHEET_NAME = "Berita"

    # Jalankan pemrosesan ke Spreadsheet menggunakan metode File API yang aman dari token limit
    if hasil_ekspor:
        data_untuk_leads = await proses_analisis_supply_demand_ke_spreadsheet(
            spreadsheet_id=SPREADSHEET_ID,
            sheet_name=SHEET_NAME,
            data_respons_ai=hasil_ekspor
        )
    else:
        print("[-] Data hasil_ekspor kosong, lewati sinkronisasi Spreadsheet.")

    # [8] Meneruskan data hasil analisis ke proses pencarian leads bisnis
    # [8] Meneruskan data hasil analisis ke proses pencarian leads bisnis
    if data_untuk_leads:
        print("\n[-] Melanjutkan ke pencarian leads bisnis berdasarkan hasil analisis...")
        await proses_pencarian_leads_bisnis(
            data_entitas_ai=data_untuk_leads,
            spreadsheet_id=SPREADSHEET_ID,
            target_sheet_name="Data Utama"
        )
    else:
        print("\n[!] Lewati pencarian leads bisnis karena tidak ada data entitas yang tersedia.")

if __name__ == "__main__":
    asyncio.run(main())