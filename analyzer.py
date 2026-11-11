import json
import logging
import os
import re
from typing import Dict, List

from openai import OpenAI

logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """Анализатор новостей через любой OpenAI-совместимый API"""

    def __init__(self, api_key: str = None, model: str = None, base_url: str = None):
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.model = model or os.getenv('OPENAI_MODEL')
        self.base_url = base_url or os.getenv('OPENAI_BASE_URL') or None

        if not self.api_key:
            raise ValueError("API ключ не найден. Установите OPENAI_API_KEY в .env файле")
        if not self.model:
            raise ValueError("Модель не указана. Установите OPENAI_MODEL в .env файле")

        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url, timeout=60)
        logger.info(f"LLM анализатор: модель {self.model}, API {self.client.base_url}")

    def test_connection(self) -> bool:
        """Проверяет, что API доступен и модель в нём есть"""
        try:
            available = {model.id for model in self.client.models.list()}
        except Exception as e:
            logger.error(f"Ошибка подключения к API: {e}")
            return False

        if self.model not in available:
            logger.warning(f"Модель {self.model} не найдена в списке моделей API")
            return False

        logger.info("Подключение к API успешно")
        return True

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
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=400,
                top_p=0.9,
            )
            ai_response = response.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"Ошибка запроса к API: {e}")
            return self._fallback_analysis(title, content, preliminary_metals)

        json_match = re.search(r'\{.*\}', ai_response, re.DOTALL)
        if json_match:
            try:
                analysis = json.loads(json_match.group())
                return {
                    "is_relevant": bool(analysis.get('is_relevant', False)),
                    "metals": analysis.get('metals', preliminary_metals),
                    "summary": str(analysis.get('summary', '')),
                    "score": float(analysis.get('score', 0.0)),
                    "reason": str(analysis.get('reason', ''))
                }
            except (json.JSONDecodeError, ValueError, TypeError) as e:
                logger.warning(f"Ошибка парсинга JSON: {e}")

        return self._parse_ai_response(ai_response, preliminary_metals)

    def _parse_ai_response(self, response: str, preliminary_metals: List[str]) -> Dict:
        """Парсит текстовый ответ модели"""
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
        """Резервный анализ без LLM"""
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
