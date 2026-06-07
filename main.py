import asyncio
import time
import random
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
import pytz
from dotenv import load_dotenv
import yfinance as yf
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from google import genai
from google.genai import types

# Memuat variabel dari file .env
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
WEB_APP_SCRIPT_URL = os.getenv("WEB_APP_SCRIPT_URL")

# Inisialisasi Klien Gemini
client = genai.Client(api_key=GEMINI_API_KEY)

# =====================================================================
# SYSTEM SMART RATE LIMITER (MAKSIMAL 12 REQUEST PER 1 MENIT 10 DETIK)
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

# Inisialisasi limiter global (Maksimal 12 request per 70 detik)
gemini_limiter = SmartRateLimiter(max_requests=12, window_seconds=70)

# ==========================================
# AMBIL DATA DINAMIS DARI GOOGLE APPS SCRIPT
# ==========================================
async def fetch_dynamic_config(url, max_retries=3, retry_delay=5):
    print("[-] Mengambil konfigurasi dinamis (Websites, Prompts, Tickers) dari Google Spreadsheet...")
    for attempt in range(1, max_retries + 1):
        try:
            # Gunakan urllib dengan timeout ketat
            with urllib.request.urlopen(url, timeout=15) as response:
                res_data = json.loads(response.read().decode("utf-8"))
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
                send_telegram_message(error_msg)
                
    return [], {}, {}

# ==========================================
# FUNGSI 1: FILTER LINK DENGAN ENGINE GEMINI
# ==========================================
async def filter_links_with_gemini(prompt, csv_string):
    await gemini_limiter.acquire()

    print("    [-] Kuota terverifikasi. Mengirim request filter link ke Gemini...")
    await asyncio.sleep(random.uniform(0.1, 0.5)) 
    
    models_fallback_order = ['gemma-4-31b-it', 'gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite', 'gemini-3.5-flash']
    data_csv_mentah = csv_string.encode('utf-8')
    
    for model_name in models_fallback_order:
        def send_request():
            try:
                print(f"    [-] Mencoba memfilter link menggunakan model: {model_name}...")
                komponen_csv = types.Part.from_bytes(data=data_csv_mentah, mime_type="text/csv")
                generate_content_config = types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level="MINIMAL"),
                )
                response = client.models.generate_content(
                    model=model_name,
                    contents=[komponen_csv, prompt],
                    config=generate_content_config
                )
                return response.text if response and response.text else ""
            except Exception as e:
                print(f"    [!] Model {model_name} Error: {e}")
                return None

        result = await asyncio.to_thread(send_request)
        if result is not None:
            return result
            
    print("    [!] Semua model untuk Filter Link gagal merespons.")
    return ""

# ==========================================
# FUNGSI 2: FILTER CONTENT BERITA DENGAN ENGINE GEMINI
# ==========================================
async def extract_content_with_gemini(prompt, csv_string):
    await gemini_limiter.acquire()

    print("    [-] Kuota terverifikasi. Mengirim request filter konten berita ke Gemini...")
    await asyncio.sleep(random.uniform(0.1, 0.5)) 
    
    models_fallback_order = ['gemma-4-31b-it', 'gemma-4-26b-a4b-it', 'gemma-4-31b-it', 'gemini-3.1-flash-lite', 'gemini-3.5-flash']
    data_csv_mentah = csv_string.encode('utf-8')
    
    for model_name in models_fallback_order:
        def send_request():
            try:
                print(f"    [-] Mencoba memfilter konten berita menggunakan model: {model_name}...")
                komponen_csv = types.Part.from_bytes(data=data_csv_mentah, mime_type="text/csv")
                generate_content_config = types.GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(thinking_level="MINIMAL"),
                )
                response = client.models.generate_content(
                    model=model_name,
                    contents=[komponen_csv, prompt],
                    config=generate_content_config
                )
                return response.text if response and response.text else ""
            except Exception as e:
                print(f"    [!] Model {model_name} Error: {e}")
                return None

        result = await asyncio.to_thread(send_request)
        if result is not None:
            return result
            
    print("    [!] Semua model untuk Ekstraksi Konten gagal merespons.")
    return ""

# ==========================================
# FUNGSI 3: ANALISIS DATA BERITA MASTER
# ==========================================
async def ask_gemini_with_inline_csv(prompt, csv_string):
    await gemini_limiter.acquire()
    
    models_fallback_order = ['gemini-3.5-flash', 'gemini-3.1-flash-lite', 'gemma-4-31b-it']
    data_csv_mentah = csv_string.encode('utf-8')

    for model_name in models_fallback_order:
        def send_request():
            try:
                print(f"    [-] Mencoba menganalisis data menggunakan model: {model_name}...")
                komponen_csv = types.Part.from_bytes(data=data_csv_mentah, mime_type="text/csv")
                response = client.models.generate_content(
                    model=model_name,
                    contents=[komponen_csv, prompt]
                )
                return response.text if response and response.text else ""
            except Exception as e:
                print(f"    [!] Model {model_name} Error: {e}")
                return None

        result = await asyncio.to_thread(send_request)
        if result is not None:
            return result

    print("    [!] Semua model untuk Analisis Data gagal merespons.")
    return ""

# ==========================================
# FUNGSI AMBIL DATA SAHAM TERBAIK IDX (SESUAI HEADER 3 KOLOM)
# ==========================================
async def saham_lq45_terbaik_idx():
    print("[-] Mengambil data Saham LQ45 terbaik dari IDX...")

    jumlah_pilihan = 10
    
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

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled', # Menyembunyikan jejak otomatisasi webdriver
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--window-position=0,0',
                '--ignore-certificate-errors',
                '--ignore-certificate-errors-spki-list',
                '--window-size=1366,768',
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="id-ID", # Menyamakan regional seolah dari Indonesia
            timezone_id="Asia/Jakarta"
        )
        page = await context.new_page()
        
        target_url = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=1000&start=0"
        
        try:
            try:
                # Jalankan perintah ini TEPAT SEBELUM page.goto(target_url)
                # Sembunyikan tanda bahwa ini adalah bot/automated webdriver
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                    window.navigator.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {}, app: {} };
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ? Promise.resolve({ state: Notification.permission }) : originalQuery(parameters)
                    );
                """)
                await page.goto(target_url, wait_until="domcontentloaded")
                await auto_scroll(page, max_scroll_steps=5)
            except (asyncio.TimeoutError, PlaywrightTimeoutError):
                # Hanya log biasa, bukan error fatal
                print(f"    [-] Halaman https://www.idx.co.id mengalami timeout, lanjut ambil data...")
            except Exception:
                pass
            raw_text = await page.locator("body").inner_text()
            json_data = json.loads(raw_text)
            data_saham = json_data.get('data', [])
        except Exception as e:
            print(f"[!] Terjadi kesalahan saat membaca data https://www.idx.co.id: {e}")
            await browser.close()
            return None
            
        await browser.close()

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
# FUNGSI NOTIFIKASI TELEGRAM
# ==========================================
# =====================================================================
# FUNGSI NOTIFIKASI TELEGRAM (AUTO-SPLIT JIKA LEBIH DARI 4000 KARAKTER)
# =====================================================================
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

    # Jalankan pembersihan awal
    cleaned_text = fix_markdown_leak(text)
    safe_text = escape_html(cleaned_text)

    # Bagi teks menjadi beberapa potongan jika melebihi batas
    pesan_potongan = split_text_chunks(safe_text, max_chunk_size=3800)
    total_bagian = len(pesan_potongan)

    # Loop untuk mengirim setiap potongan pesan
    for i, chunk in enumerate(pesan_potongan):
        # Tambahkan indikator halaman di akhir jika pesan terbagi (Contoh: [Bagian 1/3])
        text_payload = chunk
        if total_bagian > 1:
            text_payload += f"\n\n<i>[Bagian {i + 1}/{total_bagian}]</i>"

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text_payload,
            "parse_mode": "HTML"
        }
        
        data = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                print(f"    [+] Potongan pesan {i + 1}/{total_bagian} sukses terkirim.")
                response.read() # Selesaikan pembacaan stream
        except Exception as e:
            print(f"    [!] Gagal mengirim potongan {i + 1} dengan format HTML: {e}")
            
            # SEKOCI PENYELAMAT: Kirim sebagai teks biasa tanpa format jika HTML internalnya masih error
            try:
                print("    [!] Mencoba mengirim ulang bagian ini sebagai Plain Text...")
                # Ambil teks asli non-HTML untuk bagian ini
                raw_lines = text.splitlines()
                # Estimasi kasar potongan teks asli yang setara
                raw_chunk = "\n".join(raw_lines)
                plain_text_fallback = raw_chunk[:3500] + f"\n\n[Dikirim dalam mode teks biasa karena gangguan HTML - Bagian {i + 1}/{total_bagian}]"
                
                payload_fallback = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": plain_text_fallback,
                    "parse_mode": "" 
                }
                data_retry = urllib.parse.urlencode(payload_fallback).encode("utf-8")
                req_retry = urllib.request.Request(url, data=data_retry, method="POST")
                with urllib.request.urlopen(req_retry, timeout=12) as resp_retry:
                    resp_retry.read()
                    print(f"[+] Sukses mengirimkan laporan darurat teks biasa untuk bagian {i + 1}.")
            except Exception as retry_err:
                print(f"    [!] Pengiriman cadangan gagal total: {retry_err}")

        # Berikan jeda singkat 1,5 detik antar-potongan pesan agar mematuhi batasan spamming Telegram
        if total_bagian > 1 and i < total_bagian - 1:
            time.sleep(1.5)

    return "Proses pengiriman selesai"

# ==========================================
# FUNGSI PEMBANTU BROWSER
# ==========================================
async def auto_scroll(page, max_scroll_steps=15):
    await page.evaluate("""
        async (maxSteps) => {
            await new Promise((resolve) => {
                var totalHeight = 0;
                var distance = 200;
                var steps = 0;
                var timer = setInterval(() => {
                    var scrollHeight = document.body.scrollHeight;
                    window.scrollBy(0, distance);
                    totalHeight += distance;
                    steps += 1;
                    if(totalHeight >= scrollHeight - window.innerHeight || steps >= maxSteps){
                        clearInterval(timer);
                        resolve();
                    }
                }, 150);
            });
        }
    """, max_scroll_steps)
    await page.wait_for_timeout(1500)

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

async def handle_rss_feed(page, rss_url):
    """
    Mengambil tautan dari RSS feed menggunakan Playwright untuk menghindari HTTP Error 403 (Forbidden)
    """
    links = []
    try:
        print(f"    [-] Mengunduh XML RSS via Browser: {rss_url}")
        
        # Menggunakan wait_until="commit" agar proses instan begitu data XML masuk ke browser
        await page.goto(rss_url, wait_until="commit", timeout=20000)
        
        # Berikan jeda super singkat agar data XML terserap penuh di memory page
        await page.wait_for_timeout(1000)
        
        # Ambil teks mentah XML dari halaman
        xml_text = await page.content()
        
        # Bersihkan string XML jika Playwright membungkusnya dalam tag HTML bawaan browser (<pre> atau <html>)
        if "<pre" in xml_text:
            match = re.search(r'<pre[^>]*>(.*?)</pre>', xml_text, re.DOTALL)
            if match:
                xml_text = match.group(1)
        
        # Decode entitas HTML standar jika ada yang terkonversi oleh browser
        xml_text = xml_text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&").strip()
        
        # Cari tag <link> di dalam elemen <item> menggunakan regex agar lebih fleksibel 
        # jika struktur XML sedikit rusak saat dibaca via page.content()
        item_contents = re.findall(r'<item>(.*?)</item>', xml_text, re.DOTALL)
        for item in item_contents:
            link_match = re.search(r'<link>(.*?)</link>', item, re.DOTALL)
            if link_match:
                clean_url = link_match.group(1).strip()
                # Singkirkan CDATA jika ada (misal: <![CDATA[https://...]])
                clean_url = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', clean_url)
                if clean_url.startswith("http"):
                    links.append(clean_url)
                    
        return links # <-- POSISI REKOMENDASI: Mengembalikan data langsung saat proses SUKSES

    except Exception as e:
        print(f"    [!] Gagal memproses RSS via Browser: {e}")
        return [] # <-- POSISI REKOMENDASI: Mengembalikan list kosong saat proses GAGAL

async def fetch_article_data(context, url, semaphore, selector_extract=None, max_scroll_steps=15):
    async with semaphore:
        page = await context.new_page()
        try:
            # Memberikan jeda acak antara 1 sampai 3 detik sebelum memuat halaman
            await asyncio.sleep(random.uniform(1.0, 3.0))

            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await auto_scroll(page, max_scroll_steps=max_scroll_steps)
            
            # --- MEKANISME FALLBACK EKSTRAKSI TEKS ---
            inner_text = ""
            if selector_extract:
                try:
                    # Ambil berdasarkan selector spesifik terlebih dahulu
                    inner_text = await page.locator(selector_extract).innerText()
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
async def proses_analisis_berita_master(master_file_name, prompts_dict, prompt_dasar_format):
    print(f"\n======================================")
    print(f"[9] MEMULAI PROSES ANALISIS DATA BERITA MASTER AI")
    print(f"======================================")
    
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
    now_init = datetime.now(zona_wib)
    hari_init = hari_en_to_id.get(now_init.strftime("%A"), now_init.strftime("%A"))
    bulan_init = bulan_en_to_id.get(now_init.strftime("%B"), now_init.strftime("%B"))
    tanggal_init_indo = f"{hari_init}, {now_init.strftime('%d')} {bulan_init} {now_init.strftime('%Y')} pukul {now_init.strftime('%H:%M:%S')} WIB"
    
    try:
        with open(master_file_name, 'r', encoding='utf-8') as f:
            csv_content = f.read()
            
        if len(csv_content.strip()) <= 50:
            print("[!] Berkas master kosong atau hanya berisi header.")
            return

        daftar_prompt_keys = list(prompts_dict.keys())
        prompt_tugas = [key for key in daftar_prompt_keys if key != "PROMPT_DASAR_FORMAT"]
        
        for index, key_name in enumerate(prompt_tugas):
            print(f"[-] Menjalankan AI untuk Prompt: '{key_name}' ({index + 1}/{len(prompt_tugas)})...")
            instruksi_ai = f"{prompts_dict[key_name]}\n\n{prompt_dasar_format}"
            
            hasil_analisis = await ask_gemini_with_inline_csv(instruksi_ai, csv_content)
            
            if hasil_analisis.strip():
                now_realtime = datetime.now(zona_wib)
                hari_realtime = hari_en_to_id.get(now_realtime.strftime("%A"), now_realtime.strftime("%A"))
                bulan_realtime = bulan_en_to_id.get(now_realtime.strftime("%B"), now_realtime.strftime("%B"))
                
                waktu_wib_realtime = now_realtime.strftime("%H:%M:%S") + " WIB"
                tanggal_kirim_indo = f"{hari_realtime}, {now_realtime.strftime('%d')} {bulan_realtime} {now_realtime.strftime('%Y')}"
                
                nama_bersih = key_name.replace("_", " ")
                if nama_bersih.startswith("PROMPT "):
                    nama_bersih = nama_bersih.replace("PROMPT ", "", 1)
                
                header_pesan = (
                    f"📌 <code>{tanggal_kirim_indo} pukul {waktu_wib_realtime}</code>\n"
                    f"<b>{nama_bersih}</b>\n"
                    f"────────────────────\n\n"
                )
                
                pesan_full = header_pesan + hasil_analisis
                await asyncio.to_thread(send_telegram_message, pesan_full)
                
                # Jeda anti-spam Telegram (5-10 detik)
                if index < len(prompt_tugas) - 1:
                    await asyncio.sleep(random.randint(5, 10))
            else:
                print(f"[!] Hasil analisis untuk '{key_name}' kosong.")

        print("[+] Semua prompt analisis sukses diproses.")
        
    except Exception as e:
        print(f"[!] Gagal pada proses analisis master berita: {e}")

# ==========================================
# PARALLEL SCRAPER PER SITE
# ==========================================
async def scrape_single_site(site, context, tab_semaphore, master_file_name):
    print(f"\nMulai memproses situs: {site['name']}")
    page = await context.new_page()
    urls_to_scrape = []
    
    try:
        if site["handling_method"] == "rss":
            rss_links = await handle_rss_feed(page, site["url"])
            urls_to_scrape = list(set(rss_links))[:int(site['max_articles'])]
        
        elif site["handling_method"] in ["infinite_scroll", "load_more_button"]:
            print(f"[-] Membuka beranda {site['name']}...")
            
            try:
                # Set timeout 30 detik untuk goto, wait_until cukup "load" saja
                await page.goto(site['url'], wait_until="load", timeout=30000)
                
                # Coba tunggu jaringan sepi selama maks 5 detik
                await page.wait_for_load_state("networkidle", timeout=5000)
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
            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await filter_links_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
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
                
                # Coba tunggu jaringan sepi selama maks 5 detik
                await page.wait_for_load_state("networkidle", timeout=5000)
            except (asyncio.TimeoutError, PlaywrightTimeoutError):
                # Jika timeout 5 detik di atas habis, jangan anggap ini error fatal.
                # Cetak informasi ini dan biarkan skrip lanjut ke auto_scroll bawah.
                print(f"    [-] Jaringan tidak sepenuhnya idle dalam 5 detik (banyak iklan/tracker), mengabaikan dan lanjut...")
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

                        try:
                            await page.wait_for_load_state("networkidle", timeout=5000)
                        except (asyncio.TimeoutError, PlaywrightTimeoutError):
                            print(f"    [-] Jaringan tidak idle dalam 5 detik untuk {site['name']}, melanjutkan...")
                        except Exception:
                            pass

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
            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await filter_links_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

        elif site["handling_method"] == "pagination":
            all_raw_links = []
            total_pages = int(site.get("total_pages", 1)) if site.get("total_pages") else int(site.get("click_count", 1))
            
            for page_num in range(1, total_pages + 1):
                target_url = f"{site['url']}{page_num}"
                try:
                    # 1. Gunakan wait_until="load" dengan timeout eksplisit (misal 20 detik)
                    # Ini mencegah skrip menggantung terlalu lama di satu halaman yang rusak
                    await page.goto(target_url, wait_until="load", timeout=30000)

                    # 2. Tunggu sebentar untuk networkidle, jika timeout (karena iklan), abaikan saja
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except (asyncio.TimeoutError, PlaywrightTimeoutError):
                        # Hanya log biasa, bukan error fatal
                        print(f"    [-] Jaringan halaman {page_num} tidak idle sepenuhnya (iklan aktif), lanjut scroll...")
                    except Exception:
                        pass

                    # 3. Lakukan scroll dan ambil data jika halaman berhasil terbuka
                    await auto_scroll(page, max_scroll_steps=10)
                    raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
                    all_raw_links.extend(raw_links)
                    
                except (asyncio.TimeoutError, PlaywrightTimeoutError) as t_err:
                    # Jika page.goto yang timeout, lewati halaman ini dan lanjut ke page_num berikutnya
                    print(f"    [!] Halaman {page_num} lambat/gagal dimuat (Timeout). Melompati ke halaman berikutnya...")
                    continue
                except Exception as page_err:
                    # Jika ada error aneh lainnya pada halaman tersebut
                    print(f"    [!] Error tidak terduga di halaman {page_num}: {page_err}. Melompati...")
                    continue
            
            unique_links = list(set([link for link in all_raw_links if link]))
            if unique_links:
                memory_links_csv = io.StringIO()
                writer = csv.writer(memory_links_csv)
                writer.writerow(["Raw_URL"])
                for link in unique_links: writer.writerow([link])
                
                filtered_links_response = await filter_links_with_gemini(site['link_prompt'], memory_links_csv.getvalue())
                urls_to_scrape = re.findall(r'(https?://[^\s\'",\]]+)', filtered_links_response)[:int(site['max_articles'])]

        if not urls_to_scrape: 
            return
        
        tasks = [fetch_article_data(context, url, tab_semaphore, site.get("selector_extract"), int(site.get("max_scroll_article", 12))) for url in urls_to_scrape]
        scraped_results = await asyncio.gather(*tasks)
        valid_results = [res for res in scraped_results if res is not None]
        
        if not valid_results:
            return
        
        memory_data_csv = io.StringIO()
        csv_writer = csv.writer(memory_data_csv)
        csv_writer.writerow(["URL", "RawText"])
        for res in valid_results: 
            csv_writer.writerow([res['url'], res['text'][:5000].replace('\n', ' ')])
        
        final_extracted_data = await extract_content_with_gemini(site['data_prompt'], memory_data_csv.getvalue())
        
        if "berita tidak sesuai dengan topik yang diinginkan" in final_extracted_data.lower():
            return

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
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled', # Menyembunyikan jejak otomatisasi webdriver
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-infobars',
                    '--window-position=0,0',
                    '--ignore-certificate-errors',
                    '--ignore-certificate-errors-spki-list',
                ]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
                locale="id-ID", # Menyamakan regional seolah dari Indonesia
                timezone_id="Asia/Jakarta"
            )
            
            # Semaphore 1: Membatasi maksimal 2 situs yang berjalan PARALEL dalam satu waktu
            site_semaphore = asyncio.Semaphore(2)
            
            # Semaphore 2: Membatasi max 5 tab artikel terbuka bersamaan di internal seluruh situs
            tab_semaphore = asyncio.Semaphore(5)
            
            # Fungsi pembungkus (wrapper) untuk menerapkan limitasi site_semaphore
            async def scrape_with_limit(site):
                async with site_semaphore:
                    await scrape_single_site(site, context, tab_semaphore, master_file_name)
            
            # Membuat list coroutine/tasks menggunakan fungsi pembungkus baru
            site_tasks = [
                scrape_with_limit(site) 
                for site in WEBSITES
            ]
            
            print(f"[-] Menjalankan scraping untuk {len(WEBSITES)} situs dengan sistem antrean (Maks 2 situs paralel)...")
            # Memicu eksekusi paralel yang sudah dibatasi
            await asyncio.gather(*site_tasks)
            
            await browser.close()

    # [6] Eksekusi analisis berurutan setelah data terkumpul lengkap
    # [6] Eksekusi analisis berurutan setelah data terkumpul lengkap
    await proses_analisis_berita_master(master_file_name, PROMPTS_DATA, PROMPT_DASAR_FORMAT)

if __name__ == "__main__":
    asyncio.run(main())