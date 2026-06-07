import os
from firecrawl import Firecrawl

firecrawl = Firecrawl(api_key=os.environ.get("FIRECRAWL_API_KEY"))

# Scrape a website:
doc = firecrawl.scrape("https://www.idx.co.id/primary/TradingSummary/GetStockSummary?length=1000&start=0", formats=["markdown", "html"])
print(doc)
