import json
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import feedparser
import logging
from typing import List, Dict
from dataclasses import dataclass

from filters import NewsPreFilter
from analyzer import OpenRouterAnalyzer
from extractor import ContentExtractor

logger = logging.getLogger(__name__)

@dataclass
class NewsItem:
    """Структура новости"""
    title: str
    url: str
    source: str
    metals: List[str]
    published: str
    ai_summary: str
    relevance_score: float

class MetalsNewsParser:
    """Главный класс парсера с модульной архитектурой"""
    
    def __init__(self):
        """Инициализация всех компонентов"""
        self.pre_filter = NewsPreFilter()
        self.content_extractor = ContentExtractor()
        
        try:
            self.ai_analyzer = OpenRouterAnalyzer()
            if not self.ai_analyzer.test_connection():
                logger.warning("Проблемы с OpenRouter, будет использован fallback")
        except Exception as e:
            logger.error(f"Ошибка инициализации OpenRouter: {e}")
            self.ai_analyzer = None
        
        self.news_sources = {
            'rbc_economics': {
                'name': 'РБК Экономика',
                'rss_urls': ['https://rssexport.rbc.ru/rbcnews/news/20/full.rss'],
                'base_url': 'https://www.rbc.ru'
            },
            'finam_news': {
                'name': 'Финам',
                'rss_urls': ['https://www.finam.ru/analysis/conews/rsspoint'],
                'base_url': 'https://www.finam.ru'
            },
            'investing_commodities': {
                'name': 'Investing Сырье',
                'rss_urls': ['https://ru.investing.com/rss/news.rss'],
                'base_url': 'https://ru.investing.com'
            },
            'vedomosti_economics': {
                'name': 'Ведомости Экономика',
                'rss_urls': ['https://www.vedomosti.ru/rss/economics'],
                'base_url': 'https://www.vedomosti.ru'
            }
        }
        
        self.stats = {
            'total_processed': 0,
            'pre_filtered_out': 0,
            'ai_analyzed': 0,
            'relevant_found': 0
        }
        
        logger.info(f"Парсер инициализирован с {len(self.news_sources)} источниками")

    def parse_rss_feed(self, rss_url: str, source_name: str, max_age_hours: int = 24) -> List[NewsItem]:
        """Парсит RSS ленту с многоуровневой фильтрацией"""
        news_items = []
        
        try:
            logger.info(f"📡 Парсинг RSS: {rss_url}")
            
            feed = feedparser.parse(rss_url)
            
            if feed.bozo:
                logger.warning(f"⚠️ Проблемы с RSS: {feed.bozo_exception}")
            
            logger.info(f"📊 RSS содержит {len(feed.entries)} записей")
            
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            logger.info(f"⏰ Ищем новости после: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            filtered_by_time = 0
            
            for entry in feed.entries:
                try:
                    self.stats['total_processed'] += 1
                    
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        try:
                            pub_date = datetime(*entry.published_parsed[:6])
                        except (ValueError, TypeError):
                            pub_date = datetime.now()
                    else:
                        pub_date = datetime.now()
                    
                    if pub_date < cutoff_time:
                        filtered_by_time += 1
                        continue
                    
                    title = getattr(entry, 'title', '')
                    summary = getattr(entry, 'summary', '')
                    url = getattr(entry, 'link', '')
                    
                    if not title or not url:
                        continue
                    
                    if summary:
                        summary = BeautifulSoup(summary, 'html.parser').get_text()
                    
                    should_process, preliminary_metals, reason = self.pre_filter.pre_filter_news(title, summary)
                    
                    if not should_process:
                        self.stats['pre_filtered_out'] += 1
                        continue
                    
                    logger.info(f"✅ Предфильтр пройден: {title[:50]}... (металлы: {', '.join(preliminary_metals)})")
                    
                    full_content = self.content_extractor.extract_article_content(url)
                    content_for_analysis = f"{title} {summary} {full_content}"
                    
                    self.stats['ai_analyzed'] += 1
                    logger.info(f"🤖 DeepSeek анализ: {title[:50]}...")
                    
                    if self.ai_analyzer:
                        analysis = self.ai_analyzer.analyze_news(title, content_for_analysis, preliminary_metals)
                    else:
                        analysis = {
                            "is_relevant": False, "metals": [], "summary": "", 
                            "score": 0.0, "reason": "AI недоступен"
                        }
                    
                    if not analysis.get('is_relevant', False):
                        logger.info(f"❌ DeepSeek отклонил: {analysis.get('reason', 'не релевантно')}")
                        continue
                    
                    self.stats['relevant_found'] += 1
                    news_item = NewsItem(
                        title=title.strip(),
                        url=url,
                        source=source_name,
                        metals=analysis.get('metals', preliminary_metals),
                        published=pub_date.isoformat(),
                        ai_summary=analysis.get('summary', '')[:500],
                        relevance_score=analysis.get('score', 0.0)
                    )
                    
                    news_items.append(news_item)
                    logger.info(f"✅ ПРИНЯТО: {title[:60]}... (score: {analysis.get('score', 0):.2f})")
                    
                    time.sleep(1.5)
                    
                except Exception as e:
                    logger.error(f"Ошибка обработки записи: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Ошибка парсинга RSS {rss_url}: {e}")
        
        if 'filtered_by_time' not in locals():
            filtered_by_time = 0
        
        logger.info(f"📈 RSS статистика: всего={len(feed.entries) if 'feed' in locals() else 0}, отфильтровано по времени={filtered_by_time}, найдено релевантных={len(news_items)}")
        
        return news_items

    def parse_all_sources(self, max_age_hours: int = 24) -> List[NewsItem]:
        """Парсит все источники с подробной статистикой"""
        all_news = []
        
        logger.info("🚀 Начинаем модульный парсинг новостей")
        
        for source_key, source_data in self.news_sources.items():
            logger.info(f"📰 Обработка источника: {source_data['name']}")
            source_news = []
            
            for rss_url in source_data['rss_urls']:
                try:
                    rss_news = self.parse_rss_feed(rss_url, source_data['name'], max_age_hours)
                    source_news.extend(rss_news)
                    time.sleep(2)
                except Exception as e:
                    logger.error(f"Ошибка источника {source_key}: {e}")
            
            all_news.extend(source_news)
            logger.info(f"📊 Источник {source_data['name']}: {len(source_news)} релевантных новостей")
        
        seen_urls = set()
        unique_news = []
        for news in all_news:
            if news.url not in seen_urls:
                seen_urls.add(news.url)
                unique_news.append(news)
        
        unique_news.sort(key=lambda x: x.relevance_score, reverse=True)
        
        self.print_processing_stats()
        
        logger.info(f"🎯 Всего уникальных релевантных новостей: {len(unique_news)}")
        return unique_news

    def print_processing_stats(self):
        """Выводит детальную статистику обработки"""
        logger.info("📊 СТАТИСТИКА ОБРАБОТКИ:")
        logger.info(f"   Всего обработано новостей: {self.stats['total_processed']}")
        logger.info(f"   Отфильтровано предфильтром: {self.stats['pre_filtered_out']}")
        logger.info(f"   Отправлено на AI анализ: {self.stats['ai_analyzed']}")
        logger.info(f"   Найдено релевантных: {self.stats['relevant_found']}")
        
        if self.stats['ai_analyzed'] > 0:
            efficiency = (self.stats['relevant_found'] / self.stats['ai_analyzed']) * 100
            logger.info(f"   Эффективность AI анализа: {efficiency:.1f}%")

    def save_to_json(self, news_items: List[NewsItem], filename: str = "metals_news.json") -> str:
        """Сохраняет новости в JSON с фиксированным именем файла"""
        
        news_data = []
        for item in news_items:
            news_data.append({
                'title': item.title,
                'url': item.url,
                'source': item.source,
                'published': item.published,
                'ai_summary': item.ai_summary,
                'relevance_score': item.relevance_score
            })
        
        result = {
            'metadata': {
                'parsed_at': datetime.now().isoformat(),
                'total_news': len(news_items),
                'ai_provider': 'DeepSeek via OpenRouter.ai',
                'model': 'deepseek/deepseek-chat',
                'sources_count': len(set(item.source for item in news_items)),
                'metals_distribution': self.get_metals_stats(news_items),
                'average_relevance': sum(item.relevance_score for item in news_items) / len(news_items) if news_items else 0,
                'processing_stats': self.stats
            },
            'news': news_data
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, sort_keys=True)
        
        logger.info(f"💾 Новости сохранены: {filename}")
        return filename

    def get_metals_stats(self, news_items: List[NewsItem]) -> Dict[str, int]:
        """Статистика по металлам"""
        stats = {}
        for item in news_items:
            for metal in item.metals:
                stats[metal] = stats.get(metal, 0) + 1
        return stats

    def print_summary(self, news_items: List[NewsItem]):
        """Выводит подробную сводку"""
        print(f"\n{'='*80}")
        print(f"🚀 МОДУЛЬНЫЙ DEEPSEEK ПАРСЕР ДРАГОЦЕННЫХ МЕТАЛЛОВ")
        print(f"{'='*80}")
        print(f"🤖 AI: DeepSeek через OpenRouter.ai")
        print(f"📊 Релевантных новостей: {len(news_items)}")
        
        if news_items:
            print(f"🎯 Средняя релевантность: {sum(item.relevance_score for item in news_items) / len(news_items):.2f}")
        
        print(f"\n📈 ЭФФЕКТИВНОСТЬ ОБРАБОТКИ:")
        print(f"   Всего проверено: {self.stats['total_processed']}")
        print(f"   Предфильтр исключил: {self.stats['pre_filtered_out']}")
        print(f"   AI проанализировал: {self.stats['ai_analyzed']}")
        print(f"   Итого релевантных: {self.stats['relevant_found']}")
        
        if self.stats['ai_analyzed'] > 0:
            efficiency = (self.stats['relevant_found'] / self.stats['ai_analyzed']) * 100
            api_savings = 100 - (self.stats['ai_analyzed'] / self.stats['total_processed']) * 100
            print(f"   Точность AI: {efficiency:.1f}%")
            print(f"   Экономия API: {api_savings:.1f}%")
        
        metals_stats = self.get_metals_stats(news_items)
        if metals_stats:
            print(f"\n🥇 ПО МЕТАЛЛАМ:")
            for metal, count in metals_stats.items():
                print(f"   {metal.capitalize()}: {count}")
        
        sources_stats = {}
        for item in news_items:
            sources_stats[item.source] = sources_stats.get(item.source, 0) + 1
        
        if sources_stats:
            print(f"\n📰 ПО ИСТОЧНИКАМ:")
            for source, count in sources_stats.items():
                print(f"   {source}: {count}")
        
        if news_items:
            print(f"\n🏆 ТОП-3 НОВОСТИ:")
            for i, item in enumerate(news_items[:3], 1):
                print(f"\n{i}. {item.title}")
                print(f"   📊 Релевантность: {item.relevance_score:.2f} | 🏢 {item.source}")
                if item.ai_summary:
                    print(f"   🤖 {item.ai_summary}")
        
        print(f"\n{'='*80}\n")