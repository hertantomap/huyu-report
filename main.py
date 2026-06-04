import asyncio
import time
import random
import csv
import io
import re
import os
import urllib.request
import urllib.parse
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import pytz
from dotenv import load_dotenv
import yfinance as yf
from playwright.async_api import async_playwright
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
    for nama, detail in tickers_dict.items():
        try:
            kode = detail.get("ticker")
            periode = detail.get("period", "1d")
            if not kode:
                continue
                
            ticker = yf.Ticker(str(kode).strip())
            hist = ticker.history(period=periode)
            if not hist.empty:
                harga_terakhir = hist['Close'].iloc[-1]
                harga_buka = hist['Open'].iloc[-1]
                perubahan_persen = ((harga_terakhir - harga_buka) / harga_buka) * 100
                tanda = "+" if perubahan_persen >= 0 else ""
                financial_summary.append(f"{nama}: {harga_terakhir:,.2f} ({tanda}{perubahan_persen:.2f}%)")
        except Exception as e:
            print(f"    [!] Gagal mengambil data {nama}: {e}")
            
    if financial_summary:
        isi_berita_finansial = " | ".join(financial_summary)
        # Menghasilkan baris data yang tepat sesuai urutan ["Tanggal", "Isi Berita", "URL"]
        return [waktu_sekarang, f"Rangkuman Pasar Finansial Global - {isi_berita_finansial}", "https://finance.yahoo.com"]
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

async def handle_rss_feed(rss_url):
    links = []
    try:
        req = urllib.request.Request(rss_url, headers={'User-Agent': 'Mozilla/5.0'})
        def parse_xml():
            with urllib.request.urlopen(req, timeout=15) as response:
                root = ET.fromstring(response.read())
                return [item.find('link').text for item in root.findall('.//item') if item.find('link') is not None]
        links = await asyncio.to_thread(parse_xml)
    except Exception as e:
        print(f"    [!] Gagal memproses RSS: {e}")
    return links

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
                    inner_text = await page.inner_text(selector_extract)
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
            rss_links = await handle_rss_feed(site["url"])
            urls_to_scrape = list(set(rss_links))[:int(site['max_articles'])]
        
        elif site["handling_method"] in ["infinite_scroll", "load_more_button"]:
            print(f"[-] Membuka beranda {site['name']}...")
            await page.goto(site['url'], wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=5000)
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
            await page.goto(site['url'], wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=5000)
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

                        await page.wait_for_load_state("networkidle", timeout=5000)
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
                    await page.goto(target_url, wait_until="domcontentloaded")
                    await page.wait_for_load_state("networkidle", timeout=5000)
                    await auto_scroll(page, max_scroll_steps=10)
                    raw_links = await page.evaluate("Array.from(document.querySelectorAll('a')).map(a => a.href)")
                    all_raw_links.extend(raw_links)
                except:
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
            lines = final_extracted_data.strip().splitlines()
            
            # Membuang baris header jika Gemini tidak sengaja menyertakannya
            if lines and ("Tanggal" in lines[0] or "Isi Berita" in lines[0] or "URL" in lines[0]):
                lines_to_process = lines[1:]
            else:
                lines_to_process = lines
            
            # Membuka berkas Master CSV dengan mode Append ('a') secara aman
            with open(master_file_name, 'a', newline='', encoding='utf-8') as f_append:
                writer = csv.writer(f_append, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                
                for line in lines_to_process:
                    if not line.strip():
                        continue
                    
                    try:
                        # SOLUSI ROBUST: Gunakan csv.reader untuk membaca satu baris teks dari Gemini.
                        # csv.reader sangat cerdas, ia tahu bahwa koma di dalam tanda kutip ganda 
                        # (baik pada tanggal maupun isi berita) BUKANLAH pembatas kolom.
                        reader = csv.reader(io.StringIO(line.strip()), delimiter=',', quotechar='"')
                        parsed_row = next(reader)
                        
                        if len(parsed_row) >= 3:
                            tanggal = parsed_row[0].strip()
                            isi_berita = parsed_row[1].strip()
                            url = parsed_row[2].strip()
                            
                            if len(isi_berita) > 10:
                                writer.writerow([tanggal, isi_berita, url])
                        else:
                            # Fallback jika baris tidak lengkap
                            clean_line = line.strip().strip('"')
                            if len(clean_line) > 10:
                                writer.writerow(["-", clean_line, site.get('url', '-')])
                    except Exception as parse_err:
                        # Jika baris benar-benar rusak fatal dan gagal di-parse oleh csv.reader
                        clean_line = line.strip().strip('"')
                        if len(clean_line) > 10:
                            writer.writerow(["-", clean_line, site.get('url', '-')])
                            
    except Exception as e:
        print(f"[!] Kendala di situs {site['name']}: {e}")
    finally:
        await page.close()
        
# ==========================================
# MAIN ROUTINE
# ==========================================
async def main():
    # Memuat data dinamis di awal program
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

    # Membuat file master lokal baru berdasarkan timestamp eksekusi hari ini
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

    # Ambil data finansial Yahoo secara dinamis
    finansial_row = await asyncio.to_thread(fetch_yahoo_finance_data, TICKERS)
    if finansial_row:
        with open(master_file_name, 'a', newline='', encoding='utf-8') as final_csv_file:
            writer = csv.writer(final_csv_file)
            writer.writerow(finansial_row)
        # Mengubah baris print agar mencetak nama file yang spesifik sesuai tanggal
        print(f"[+] Sukses menyimpan data finansial Yahoo ke file master lokal: {master_file_name}\n")
        
    # Proses Scraping Multi-Situs Web Berdasarkan Config Spreadsheet
# Proses Scraping Multi-Situs Web Berdasarkan Config Spreadsheet
    if not WEBSITES:
        print("[!] Tidak ada target website yang dimuat dari Spreadsheet. Langsung melompat ke analisis.")
    else:
        async with async_playwright() as p:
            # Mengaktifkan headless=True agar berjalan mulus tanpa antarmuka GUI di GitHub Actions
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
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

    # Eksekusi analisis berurutan setelah data terkumpul lengkap
    await proses_analisis_berita_master(master_file_name, PROMPTS_DATA, PROMPT_DASAR_FORMAT)

if __name__ == "__main__":
    asyncio.run(main())