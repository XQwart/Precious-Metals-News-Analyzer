import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class ContentExtractor:
    """Извлечение контента из веб-страниц"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        })
        
        self.problematic_domains = [
            'finam.ru/publications/item',
        ]
        
        logger.info("Инициализирован экстрактор контента")
    
    def should_skip_url(self, url: str) -> bool:
        return any(domain in url for domain in self.problematic_domains)
    
    def extract_article_content(self, url: str) -> str:
        try:
            if self.should_skip_url(url):
                logger.debug(f"Пропуск проблемного URL: {url}")
                return ""
            
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()
            
            content_selectors = [
                'article', '.article-body', '.article-content', '.news-content',
                '.text', '.content', '.post-content', '[itemprop="articleBody"]',
                '.js-mediator-article', '.article__text'
            ]
            
            content = ""
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    content = ' '.join([elem.get_text(strip=True) for elem in elements])
                    break
            
            if not content:
                paragraphs = soup.find_all('p')
                content = ' '.join([p.get_text(strip=True) for p in paragraphs])
            
            return content[:2500]
            
        except Exception as e:
            logger.debug(f"Ошибка извлечения контента {url}: {e}")
            return ""