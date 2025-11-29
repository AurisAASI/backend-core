"""
Test cases for the WebsiteScrapper class.

This test file validates website scraping functionality including:
- Page discovery strategies (sitemap, navigation, fallback)
- HTML content fetching with politeness measures
- LLM-powered data extraction with Gemini
- Database integration and status tracking
"""

import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest
from bs4 import BeautifulSoup

# Add src directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.models.scrappers.website_scrapper import WebsiteScrapper


# Fixtures
@pytest.fixture
def mock_settings():
    """Mock settings for testing."""
    with patch('src.models.scrappers.website_scrapper.settings') as mock:
        mock.stage = 'dev'
        mock.companies_table_name = 'dev-auris-core-companies'
        yield mock


@pytest.fixture
def mock_db_handler():
    """Mock DatabaseHandler."""
    with patch('src.models.scrappers.website_scrapper.DatabaseHandler') as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_gemini_handler():
    """Mock GoogleGeminiHandler."""
    with patch(
        'src.models.scrappers.website_scrapper.GoogleGeminiHandler'
    ) as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def sample_html_homepage():
    """Sample HTML for homepage with navigation."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Audicare - Hearing Solutions</title></head>
    <body>
        <nav>
            <a href="/">Home</a>
            <a href="/sobre">About Us</a>
            <a href="/contato">Contact</a>
            <a href="/produtos">Products</a>
            <a href="/servicos">Services</a>
        </nav>
        <main>
            <h1>Welcome to Audicare</h1>
            <p>Leading provider of hearing solutions in Brazil</p>
        </main>
    </body>
    </html>
    """


@pytest.fixture
def sample_html_about():
    """Sample HTML for about page."""
    return """
    <!DOCTYPE html>
    <html>
    <body>
        <h1>About Audicare</h1>
        <p>Founded in 2010, Audicare has been serving customers for over 10 years.</p>
        <p>CNPJ: 12.345.678/0001-90</p>
        <p>Phone: (11) 3214-5678</p>
        <p>WhatsApp: (11) 98765-4321</p>
        <address>
            Av. Paulista, 1578 - Bela Vista, São Paulo - SP, 01310-200
        </address>
        <p>Products: Hearing aids, Accessories, Batteries</p>
        <p>Services: Hearing tests, Fitting, Maintenance</p>
        <p>Brands: Phonak, Oticon, Widex</p>
    </body>
    </html>
    """


@pytest.fixture
def sample_sitemap_xml():
    """Sample sitemap XML."""
    return """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url>
            <loc>https://audicare.com.br/</loc>
            <lastmod>2024-01-01</lastmod>
        </url>
        <url>
            <loc>https://audicare.com.br/sobre</loc>
            <lastmod>2024-01-01</lastmod>
        </url>
        <url>
            <loc>https://audicare.com.br/contato</loc>
            <lastmod>2024-01-01</lastmod>
        </url>
        <url>
            <loc>https://audicare.com.br/produtos</loc>
            <lastmod>2024-01-01</lastmod>
        </url>
        <url>
            <loc>https://audicare.com.br/blog/article-123</loc>
            <lastmod>2024-01-01</lastmod>
        </url>
    </urlset>
    """


@pytest.fixture
def sample_gemini_response():
    """Sample structured data from Gemini."""
    return {
        'brand_name': 'Audicare',
        'addresses': [
            {
                'street': 'Av. Paulista',
                'number': '1578',
                'complement': '',
                'district': 'Bela Vista',
                'city': 'São Paulo',
                'state': 'SP',
                'postal_code': '01310-200',
                'full_address': 'Av. Paulista, 1578 - Bela Vista, São Paulo - SP, 01310-200',
            }
        ],
        'phones': [
            {'number': '(11) 3214-5678', 'type': 'fixed'},
            {'number': '(11) 98765-4321', 'type': 'whatsapp'},
        ],
        'history': 'Founded in 2010, Audicare has been serving customers for over 10 years.',
        'products': ['Hearing aids', 'Accessories', 'Batteries'],
        'services': ['Hearing tests', 'Fitting', 'Maintenance'],
        'brands': ['Phonak', 'Oticon', 'Widex'],
        'social_links': {
            'facebook': None,
            'instagram': None,
            'youtube': None,
            'tiktok': None,
            'linkedin': None,
            'twitter': None,
            'others': [],
        },
        'cnpj': '12.345.678/0001-90',
        'offers_summary': None,
    }


# Test WebsiteScrapper initialization
class TestWebsiteScrapperInit:
    """Tests for WebsiteScrapper initialization."""

    def test_init_success(self, mock_settings, mock_db_handler, mock_gemini_handler):
        """Test successful initialization."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-api-key',
            timeout=10,
        )

        assert scrapper.company_id == 'test-uuid-123'
        assert scrapper.website == 'https://audicare.com.br'
        assert scrapper.gemini_api_key == 'test-api-key'
        assert scrapper.timeout == 10
        assert scrapper.ensamble['status'] == 'in_progress'

    def test_init_normalizes_url_without_scheme(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test URL normalization adds https scheme."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='audicare.com.br',
            gemini_api_key='test-api-key',
        )

        assert scrapper.website == 'https://audicare.com.br'

    def test_init_invalid_url_raises_error(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test initialization with invalid URL raises ValueError."""
        with pytest.raises(ValueError, match='Invalid URL format'):
            WebsiteScrapper(
                company_id='test-uuid-123',
                website='not a valid url',
                gemini_api_key='test-api-key',
            )

    def test_init_gemini_handler_failure(self, mock_settings, mock_db_handler):
        """Test initialization fails when Gemini handler cannot be created."""
        with patch(
            'src.models.scrappers.website_scrapper.GoogleGeminiHandler',
            side_effect=Exception('API key invalid'),
        ):
            with pytest.raises(Exception, match='API key invalid'):
                WebsiteScrapper(
                    company_id='test-uuid-123',
                    website='https://audicare.com.br',
                    gemini_api_key='invalid-key',
                )


# Test URL normalization
class TestURLNormalization:
    """Tests for URL normalization."""

    def test_normalize_url_adds_https(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test normalization adds https:// to URLs without scheme."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid', website='example.com', gemini_api_key='test-key'
        )
        assert scrapper.website == 'https://example.com'

    def test_normalize_url_preserves_http(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test normalization preserves http:// scheme."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='http://example.com',
            gemini_api_key='test-key',
        )
        assert scrapper.website == 'http://example.com'

    def test_normalize_url_strips_whitespace(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test normalization strips whitespace."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid', website='  example.com  ', gemini_api_key='test-key'
        )
        assert scrapper.website == 'https://example.com'


# Test robots.txt checking
class TestRobotsTxtCheck:
    """Tests for robots.txt checking."""

    @patch('src.models.scrappers.website_scrapper.RobotFileParser')
    def test_check_robots_txt_allowed(
        self,
        mock_robot_parser_class,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
    ):
        """Test robots.txt allows scraping."""
        mock_rp = Mock()
        mock_rp.can_fetch.return_value = True
        mock_robot_parser_class.return_value = mock_rp

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://example.com',
            gemini_api_key='test-key',
        )

        result = scrapper._check_robots_txt('https://example.com')
        assert result is True

    @patch('src.models.scrappers.website_scrapper.RobotFileParser')
    def test_check_robots_txt_disallowed(
        self,
        mock_robot_parser_class,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
    ):
        """Test robots.txt disallows scraping."""
        mock_rp = Mock()
        mock_rp.can_fetch.return_value = False
        mock_robot_parser_class.return_value = mock_rp

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://example.com',
            gemini_api_key='test-key',
        )

        result = scrapper._check_robots_txt('https://example.com')
        assert result is False

    @patch('src.models.scrappers.website_scrapper.RobotFileParser')
    def test_check_robots_txt_not_found_allows(
        self,
        mock_robot_parser_class,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
    ):
        """Test missing robots.txt allows scraping by default."""
        mock_rp = Mock()
        mock_rp.read.side_effect = Exception('404 Not Found')
        mock_robot_parser_class.return_value = mock_rp

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://example.com',
            gemini_api_key='test-key',
        )

        result = scrapper._check_robots_txt('https://example.com')
        assert result is True


# Test page discovery strategies
class TestPageDiscovery:
    """Tests for page discovery strategies."""

    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_discover_pages_from_sitemap(
        self,
        mock_get,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_sitemap_xml,
    ):
        """Test page discovery from sitemap.xml."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = sample_sitemap_xml.encode('utf-8')
        mock_get.return_value = mock_response

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        pages = scrapper._discover_pages_from_sitemap('https://audicare.com.br')

        # Should exclude blog posts
        assert 'https://audicare.com.br/blog/article-123' not in pages
        # Should include main pages
        assert (
            'https://audicare.com.br/' in pages
            or 'https://audicare.com.br/sobre' in pages
        )

    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_discover_pages_from_homepage(
        self,
        mock_get,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_html_homepage,
    ):
        """Test page discovery from homepage navigation."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = sample_html_homepage
        mock_get.return_value = mock_response

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        pages = scrapper._discover_pages_from_homepage('https://audicare.com.br')

        assert len(pages) > 0
        assert any('sobre' in page.lower() for page in pages)
        assert any('contato' in page.lower() for page in pages)

    def test_discover_pages_common_paths(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test fallback common paths discovery."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        pages = scrapper._discover_pages_common_paths('https://audicare.com.br')

        assert len(pages) > 0
        # Check that homepage is included
        assert 'https://audicare.com.br/' in pages
        # Check that some expected paths are present within the MAX_PAGES_PER_SITE limit
        # Note: MAX_PAGES_PER_SITE = 7, so only first 7 paths are returned
        assert any('sobre' in page for page in pages)
        # Check for 'about' or 'index' which are within the first 7 paths
        assert any('about' in page or 'index' in page for page in pages)


# Test HTML fetching
class TestHTMLFetching:
    """Tests for HTML content fetching."""

    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_fetch_page_content_success(
        self,
        mock_get,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_html_homepage,
    ):
        """Test successful HTML fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = sample_html_homepage
        mock_get.return_value = mock_response

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        html = scrapper._fetch_page_content('https://audicare.com.br')

        assert html == sample_html_homepage
        assert scrapper.ensamble['pages_fetched'] == 1
        assert scrapper.ensamble['pages_failed'] == 0

    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_fetch_page_content_404(
        self, mock_get, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test HTML fetch handles 404 error."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        html = scrapper._fetch_page_content('https://audicare.com.br/notfound')

        assert html is None
        assert scrapper.ensamble['pages_failed'] == 1

    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_fetch_page_content_timeout(
        self, mock_get, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test HTML fetch handles timeout."""
        import requests

        mock_get.side_effect = requests.exceptions.Timeout('Request timed out')

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        html = scrapper._fetch_page_content('https://audicare.com.br')

        assert html is None
        assert scrapper.ensamble['pages_failed'] == 1


# Test text extraction
class TestTextExtraction:
    """Tests for HTML text extraction."""

    def test_extract_text_from_html(
        self, mock_settings, mock_db_handler, mock_gemini_handler, sample_html_about
    ):
        """Test clean text extraction from HTML."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        text = scrapper._extract_text_from_html(sample_html_about)

        assert 'About Audicare' in text
        assert 'Founded in 2010' in text
        assert 'CNPJ: 12.345.678/0001-90' in text
        assert '<html>' not in text  # No HTML tags
        assert '<body>' not in text

    def test_extract_text_removes_scripts(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test script and style tags are removed."""
        html = """
        <html>
        <head>
            <style>body { color: red; }</style>
            <script>alert('test');</script>
        </head>
        <body>
            <p>Content here</p>
        </body>
        </html>
        """

        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        text = scrapper._extract_text_from_html(html)

        assert 'Content here' in text
        assert 'color: red' not in text
        assert 'alert' not in text


# Test structured data extraction
class TestStructuredDataExtraction:
    """Tests for LLM-powered data extraction."""

    @patch(
        'builtins.open',
        new_callable=mock_open,
        read_data='{"type": "object", "properties": {}}',
    )
    def test_extract_structured_data_success(
        self,
        mock_file,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_html_about,
        sample_gemini_response,
    ):
        """Test successful structured data extraction with Gemini."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        # Mock Gemini response
        mock_gemini_handler.generate_content.return_value = json.dumps(
            sample_gemini_response
        )

        pages_content = {'https://audicare.com.br/sobre': sample_html_about}

        data = scrapper._extract_structured_data(pages_content)

        assert data['brand_name'] == 'Audicare'
        assert len(data['addresses']) == 1
        assert data['addresses'][0]['city'] == 'São Paulo'
        assert len(data['phones']) == 2
        assert data['cnpj'] == '12.345.678/0001-90'

    @patch(
        'builtins.open',
        new_callable=mock_open,
        read_data='{"type": "object", "properties": {}}',
    )
    def test_extract_structured_data_no_content(
        self, mock_file, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test extraction with no page content."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        pages_content = {}
        data = scrapper._extract_structured_data(pages_content)

        assert data == {}

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_extract_structured_data_schema_not_found(
        self,
        mock_file,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_html_about,
    ):
        """Test extraction handles missing schema file."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        pages_content = {'https://audicare.com.br/sobre': sample_html_about}

        data = scrapper._extract_structured_data(pages_content)

        assert data == {}


# Test database operations
class TestDatabaseOperations:
    """Tests for database save operations."""

    def test_save_to_database_success(
        self,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_gemini_response,
    ):
        """Test successful database save."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )
        scrapper.ensamble['status'] = 'completed'
        scrapper.ensamble['status_reason'] = 'Successfully scraped 5 pages'

        result = scrapper._save_to_database(sample_gemini_response)

        assert result is True
        mock_db_handler.update_item.assert_called_once()

        # Verify call arguments
        call_args = mock_db_handler.update_item.call_args
        assert call_args[1]['table_name'] == 'dev-auris-core-companies'
        assert call_args[1]['key'] == {'companyID': 'test-uuid-123'}
        assert 'website_data' in call_args[1]['update_data']
        assert 'website_scraping_status' in call_args[1]['update_data']
        assert call_args[1]['update_data']['website_scraping_status'] == 'completed'

    def test_save_to_database_no_handler(self, mock_settings, mock_gemini_handler):
        """Test save fails gracefully when DB handler unavailable."""
        with patch(
            'src.models.scrappers.website_scrapper.DatabaseHandler',
            side_effect=Exception('DB error'),
        ):
            scrapper = WebsiteScrapper(
                company_id='test-uuid-123',
                website='https://audicare.com.br',
                gemini_api_key='test-key',
            )

            result = scrapper._save_to_database({})

            assert result is False

    def test_save_to_database_update_error(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test save handles database update errors."""
        mock_db_handler.update_item.side_effect = Exception('Database error')

        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        result = scrapper._save_to_database({})

        assert result is False
        assert scrapper.ensamble['status'] == 'failed'
        assert 'Database save failed' in scrapper.ensamble['status_reason']


# Test full collect_data workflow
class TestCollectDataWorkflow:
    """Tests for complete data collection workflow."""

    @patch('src.models.scrappers.website_scrapper.time.sleep')
    @patch('src.models.scrappers.website_scrapper.requests.get')
    @patch(
        'builtins.open',
        new_callable=mock_open,
        read_data='{"type": "object", "properties": {}}',
    )
    def test_collect_data_full_workflow(
        self,
        mock_file,
        mock_get,
        mock_sleep,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
        sample_html_homepage,
        sample_html_about,
        sample_gemini_response,
    ):
        """Test complete data collection workflow."""
        # Mock robots.txt (allow)
        # Mock page fetching
        def get_side_effect(url, **kwargs):
            response = Mock()
            response.status_code = 200
            if 'robots.txt' in url:
                response.text = 'User-agent: *\nAllow: /'
            elif 'sobre' in url:
                response.text = sample_html_about
            else:
                response.text = sample_html_homepage
            return response

        mock_get.side_effect = get_side_effect

        # Mock Gemini response
        mock_gemini_handler.generate_content.return_value = json.dumps(
            sample_gemini_response
        )

        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        scrapper.collect_data()

        # Verify workflow
        assert scrapper.ensamble['status'] in ['completed', 'partial']
        assert scrapper.ensamble['pages_fetched'] > 0
        mock_db_handler.update_item.assert_called()

    @patch('src.models.scrappers.website_scrapper.RobotFileParser')
    def test_collect_data_robots_txt_disallowed(
        self,
        mock_robot_parser_class,
        mock_settings,
        mock_db_handler,
        mock_gemini_handler,
    ):
        """Test workflow respects robots.txt disallow."""
        mock_rp = Mock()
        mock_rp.can_fetch.return_value = False
        mock_robot_parser_class.return_value = mock_rp

        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        scrapper.collect_data()

        assert scrapper.ensamble['status'] == 'completed'
        assert 'robots.txt' in scrapper.ensamble['status_reason']
        mock_db_handler.update_item.assert_called()

    def test_collect_data_invalid_url(
        self, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test workflow handles invalid URL."""
        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://validurl.com',
            gemini_api_key='test-key',
        )
        scrapper.website = ''  # Simulate invalid URL

        scrapper.collect_data()

        assert scrapper.ensamble['status'] == 'failed'
        assert 'Invalid URL' in scrapper.ensamble['status_reason']
        mock_db_handler.update_item.assert_called()

    @patch('src.models.scrappers.website_scrapper.time.sleep')
    @patch('src.models.scrappers.website_scrapper.requests.get')
    def test_collect_data_no_pages_fetched(
        self, mock_get, mock_sleep, mock_settings, mock_db_handler, mock_gemini_handler
    ):
        """Test workflow handles failure to fetch any pages."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        scrapper = WebsiteScrapper(
            company_id='test-uuid-123',
            website='https://audicare.com.br',
            gemini_api_key='test-key',
        )

        scrapper.collect_data()

        assert scrapper.ensamble['status'] == 'partial'
        assert 'Failed to fetch any pages' in scrapper.ensamble['status_reason']
        mock_db_handler.update_item.assert_called()
