import requests
import json
import re
import os
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class OpenRouterAnalyzer:
    """Анализатор новостей через OpenRouter.ai API с DeepSeek"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('OPENROUTER_API_KEY')
        self.base_url = "https://openrouter.ai/api/v1"
        self.model = "deepseek/deepseek-chat"
        self.session = requests.Session()
        
        if not self.api_key:
            raise ValueError("OpenRouter API ключ не найден. Установите OPENROUTER_API_KEY в .env файле")
        
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/metals-news-parser",
            "X-Title": "Metals News Parser"
        })
        
        logger.info(f"OpenRouter анализатор инициализирован с моделью: {self.model}")
    
    def test_connection(self) -> bool:
        """Тестирует подключение к OpenRouter API"""
        try:
            response = self.session.get(f"{self.base_url}/models")
            if response.status_code == 200:
                models = response.json()
                available_models = [model['id'] for model in models.get('data', [])]
                if self.model in available_models:
                    logger.info("✅ Подключение к OpenRouter успешно")
                    return True
                else:
                    logger.warning(f"⚠️ Модель {self.model} недоступна")
                    return False
            else:
                logger.error(f"❌ Ошибка подключения к OpenRouter: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка тестирования OpenRouter: {e}")
            return False
    
    def analyze_news(self, title: str, content: str, preliminary_metals: List[str]) -> Dict:
        
        prompt = f"""Проанализируй новость о возможном упоминании драгоценных металлов.

Предварительно найдены упоминания: {', '.join(preliminary_metals)}

Заголовок: {title}
Содержание: {content[:1000]}

Определи:
1. Относится ли новость к драгоценным металлам (золото, серебро, платина, палладий) как к ТОВАРАМ, ИНВЕСТИЦИЯМ или ПРОМЫШЛЕННОМУ СЫРЬЮ?
2. Какие конкретно металлы упоминаются в контексте торговли/инвестиций?
3. Краткий пересказ (2-3 предложения) с важной экономической информацией.

ВАЖНО: 
- Игнорируй переносные значения ("золотая медаль", "серебряный призер", "золотой ключ")
- Учитывай только прямые упоминания металлов как товаров или активов
- Новости о ценах, курсах, добыче, инвестициях = релевантны
- Новости о наградах, юбилеях, цветах = нерелевантны

Ответь СТРОГО в JSON:
{{"is_relevant": true/false, "metals": ["золото"], "summary": "краткий пересказ", "score": 0.9, "reason": "объяснение"}}"""

        try:
            response = self.session.post(
                f"{self.base_url}/chat/completions",
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 400,
                    "top_p": 0.9
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                ai_response = result['choices'][0]['message']['content']
                
                try:
                    json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
                    if json_match:
                        analysis = json.loads(json_match.group())
                        return {
                            "is_relevant": bool(analysis.get('is_relevant', False)),
                            "metals": analysis.get('metals', preliminary_metals),
                            "summary": str(analysis.get('summary', '')),
                            "score": float(analysis.get('score', 0.0)),
                            "reason": str(analysis.get('reason', ''))
                        }
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.warning(f"Ошибка парсинга JSON: {e}")
                    return self._parse_ai_response(ai_response, preliminary_metals)
                
            else:
                logger.error(f"OpenRouter API ошибка: {response.status_code}")
                return self._fallback_analysis(title, content, preliminary_metals)
                
        except Exception as e:
            logger.error(f"Ошибка запроса к OpenRouter: {e}")
            return self._fallback_analysis(title, content, preliminary_metals)
    
    def _parse_ai_response(self, response: str, preliminary_metals: List[str]) -> Dict:
        """Парсит текстовый ответ AI"""
        text = response.lower()
        is_relevant = 'true' in text and any(
            word in text for word in ['релевант', 'relevant', 'цена', 'курс', 'инвестиц']
        )
        
        return {
            "is_relevant": is_relevant,
            "metals": preliminary_metals if is_relevant else [],
            "summary": response[:200] if is_relevant else "",
            "score": 0.7 if is_relevant else 0.0,
            "reason": "Parsed from non-JSON response"
        }
    
    def _fallback_analysis(self, title: str, content: str, preliminary_metals: List[str]) -> Dict:
        """Резервный анализ без AI"""
        text = f"{title} {content}".lower()
        
        economic_terms = [
            'цена', 'курс', 'стоимость', 'подорожал', 'подешевел', 
            'растет', 'падает', 'инвестиции', 'торги', 'биржа',
            'унция', 'тройская', 'добыча', 'запасы'
        ]
        
        has_economic_context = any(term in text for term in economic_terms)
        
        if has_economic_context and preliminary_metals:
            return {
                "is_relevant": True,
                "score": 0.6,
                "metals": preliminary_metals,
                "summary": f"{title[:150]}...",
                "reason": "Fallback: найдены экономические термины"
            }
        
        return {
            "is_relevant": False,
            "score": 0.0,
            "metals": [],
            "summary": "",
            "reason": "Fallback: нет экономического контекста"
        }