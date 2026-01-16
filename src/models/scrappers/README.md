# Scrappers Module Documentation

## Overview

The `src/models/scrappers` module implements the data collection pipeline for the Auris AASI system. It consists of two main scraping strategies that work together to collect comprehensive company and place information from external sources.

## Architecture Overview

```
┌─────────────────┐
│ city_collector  │
│    (Lambda)     │
└────────┬────────┘
         │ Sends SQS message: {city, state, niche}
         ↓
┌─────────────────────────┐
│  ScraperTaskQueue (SQS) │
└────────┬────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────────┐
│ data_scrapper Lambda (handler.py)                                   │
│                                                                       │
│  → Instantiates GMapsScrapper                                        │
│  → Collects places from Google Places API                           │
│  → Saves to DynamoDB (companies + places tables)                    │
│  → Queues website scraping tasks                                    │
└────────┬────────────────────────────────────────────────────────────┘
         │ For each place with website URL
         │ Sends SQS message: {company_id, website}
         ↓
┌──────────────────────────────────┐
│ WebsiteScraperTaskQueue (SQS)    │
└────────┬─────────────────────────┘
         │
         ↓
┌─────────────────────────────────────────────────────────────────────┐
│ website_scrapper Lambda (website_handler.py)                        │
│                                                                       │
│  → Instantiates WebsiteScrapper                                      │
│  → Fetches HTML from company website                                │
│  → Extracts structured data using Google Gemini LLM                 │
│  → Updates company record in DynamoDB                               │
└──────────────────────────────────────────────────────────────────────┘
```

#### Data Flow

```
collect_data()
    │
    ├─→ _normalize_url(website)
    │   └─→ Validates URL format, adds https:// if missing
    │
    ├─→ _check_robots_txt(base_url)
    │   └─→ Uses RobotFileParser to check if scraping allowed
    │       • User-Agent: 'AurisBot/1.0'
    │       • Returns False if disallowed → stops scraping
    │
    ├─→ _discover_pages(base_url)
    │   │   # Hybrid page discovery strategy
    │   │
    │   ├─→ Strategy 1: _discover_pages_from_sitemap()
    │   │   │
    │   │   ├─→ Try common sitemap paths:
    │   │   │   • /sitemap.xml
    │   │   │   • /sitemap_index.xml
    │   │   │   • /sitemap-index.xml
    │   │   │   • /sitemap1.xml
    │   │   │
    │   │   ├─→ Parse XML with BeautifulSoup
    │   │   ├─→ Extract <loc> URLs
    │   │   ├─→ Filter out:
    │   │   │   • Blog posts (/blog/, /news/)
    │   │   │   • Pagination (/page/, /p/)
    │   │   │   • Query params and anchors
    │   │   │
    │   │   └─→ Prioritize by keywords (sobre, contato, etc.)
    │   │
    │   ├─→ Strategy 2: _discover_pages_from_homepage()
    │   │   │
    │   │   ├─→ Fetch homepage HTML
    │   │   ├─→ Find <nav>, <header>, <footer> elements
    │   │   ├─→ Extract all <a href> links
    │   │   ├─→ Filter same-domain links only
    │   │   ├─→ Clean URLs (remove anchors/params)
    │   │   └─→ _prioritize_links() by keywords
    │   │
    │   └─→ Strategy 3: _discover_pages_common_paths()
    │       │   # Fallback if above strategies fail
    │       │
    │       └─→ Return common paths:
    │           • /
    │           • /sobre, /quem-somos, /about
    │           • /contato, /fale-conosco, /contact
    │           • /produtos, /products
    │           • /servicos, /services
    │           • (Limited to MAX_PAGES_PER_SITE = 7)
    │
    ├─→ For each discovered page (max 7):
    │   │
    │   ├─→ Rate limiting: sleep 2-3 seconds
    │   │
    │   └─→ _fetch_page_content(url)
    │       └─→ GET request with:
    │           • User-Agent: 'AurisBot/1.0'
    │           • Timeout: 10 seconds
    │           • Returns HTML text or None if failed
    │           • ensamble['pages_fetched']++ or pages_failed++
    │
    ├─→ _extract_structured_data(pages_content)
    │   │
    │   ├─→ For each page:
    │   │   │
    │   │   ├─→ _extract_text_from_html(html)
    │   │   │   └─→ BeautifulSoup parsing
    │   │   │       • Remove <script>, <style>, <nav>, <footer>
    │   │   │       • Extract visible text
    │   │   │       • Clean whitespace
    │   │   │
    │   │   └─→ Concatenate all text (max ~300,000 chars)
    │   │
    │   ├─→ Load schema from website_gemini_schema.json
    │   │
    │   └─→ gemini_handler.generate_structured_json(
    │       │   prompt=combined_text,
    │       │   schema=json_schema,
    │       │   timeout=60
    │       └─→ )
    │           └─→ Google Gemini API call
    │               • Model: gemini-1.5-pro (or configured)
    │               • Returns: Structured JSON matching schema
    │               • Extracts: brand_name, addresses, phones,
    │                          history, products, services, etc.
    │
    └─→ _save_to_database(website_data)
        │
        └─→ Update company record:
            • website_data: {...}  # Extracted structured data
            • website_scraping_status: 'completed' | 'partial' | 'failed'
            • website_scraping_reason: 'Successfully scraped 5 pages'
            • website_scraped_at: '2025-11-29T10:30:00Z'
```

#### Anti-Ban Measures

1. **robots.txt Compliance**: Checks and respects robots.txt before scraping
2. **User-Agent Identification**: Uses `AurisBot/1.0` with contact info
3. **Rate Limiting**: 2-3 second delays between page requests
4. **Page Limit**: Maximum 7 pages per website (MAX_PAGES_PER_SITE)
5. **Timeout**: 10-second timeout per request to avoid hanging

#### Error Handling

- **robots.txt disallowed**: Status `completed` with reason (respects site policy)
- **No pages fetched**: Status `partial` with reason
- **Some pages failed**: Status `partial` with success/failure counts
- **LLM extraction failed**: Status `failed` with error details
- **Database save failed**: Status `failed_database_error`