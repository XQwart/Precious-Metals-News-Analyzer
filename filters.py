from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

class NewsPreFilter:
    """Предварительная фильтрация новостей по ключевым словам"""
    
    def __init__(self):
        self.metal_keywords = {
            'золото': [
                'золот', 'золота', 'золоте', 'золотой', 'золотая', 'золотое', 'золотых', 'золотые',
                'gold', 'xau', 'слиток', 'слитки', 'слитков', 'унция', 'унций', 'тройская',
                'aurum', 'золотодобыч', 'золотодобытчик', 'золотодобывающ', 'золоторудн'
            ],
            'серебро': [
                'серебр', 'серебра', 'серебре', 'серебряный', 'серебряная', 'серебряное', 'серебряных', 'серебряные',
                'silver', 'xag', 'argentum'
            ], 
            'платина': [
                'платин', 'платина', 'платине', 'платиновый', 'платиновая', 'платиновое', 'платиновых', 'платиновые',
                'platinum', 'xpt', 'плат'
            ],
            'палладий': [
                'палладий', 'палладия', 'палладии', 'палладиевый', 'палладиевая', 'палладиевое', 'палладиевых', 'палладиевые',
                'palladium', 'xpd', 'pd'
            ]
        }
        
        logger.info(f"Инициализирован предфильтр с {len(self.metal_keywords)} металлами")
    
    def contains_metal_keywords(self, text: str) -> Tuple[bool, List[str]]:
        text_lower = text.lower()
        found_metals = []
        
        import re
        for metal, keywords in self.metal_keywords.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text_lower):
                    found_metals.append(metal)
                    break
        
        return len(found_metals) > 0, found_metals
    
    def pre_filter_news(self, title: str, summary: str) -> Tuple[bool, List[str], str]:
        full_text = f"{title} {summary}"
        
        has_metals, found_metals = self.contains_metal_keywords(full_text)
        
        if not has_metals:
            return False, [], "нет упоминаний металлов"
        
        return True, found_metals, "прошел предфильтр"