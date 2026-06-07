from curl_cffi import requests
import json

url = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=1000&start=0"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.idx.co.id/id/data-pasar/ringkasan-perdagangan/ringkasan-saham/",
    "Origin": "https://www.idx.co.id"
}

try:
    # Menggunakan impersonate='chrome' untuk meniru TLS fingerprint browser Chrome asli
    proxies = {
        'http': 'socks5://127.0.0.1:9050',
        'https': 'socks5://127.0.0.1:9050'
    }

    # Jika menggunakan requests biasa (butuh pip install requests[socks])
    response = requests.get(url, headers=headers, impersonate="chrome110", proxies=proxies)
    
    if response.status_code == 200:
        data = response.json()
        # Filter data untuk mencari Foreign Buy / Sell
        # Contoh mengambil 5 sampel pertama
        for stock in data.get('data', [])[:5]:
            print(f"Ticker: {stock['StockCode']}, Foreign Buy: {stock['ForeignBuy']}, Foreign Sell: {stock['ForeignSell']}")
    else:
        print(f"Gagal! Status Code: {response.status_code}")
        
except Exception as e:
    print(f"Terjadi error: {e}")
