import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from parser import MetalsNewsParser

def setup_logging():
    """Настройка системы логирования"""
    # Создаем папку для логов если её нет
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Имя файла лога с датой
    log_filename = os.path.join(log_dir, f"metals_parser_{datetime.now().strftime('%Y%m%d')}.log")
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # Устанавливаем уровни для разных модулей
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

def create_env_file():
    """Создает пример .env файла"""
    env_content = """# OpenRouter.ai API ключ
OPENROUTER_API_KEY=your_openrouter_api_key_here

# Получите ключ на https://openrouter.ai/keys
# Пополните баланс для использования DeepSeek модели
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        print("📝 Создан файл .env с примером настроек")
        return True
    else:
        print("📁 Файл .env уже существует")
        return False

def check_env_configuration():
    """Проверяет конфигурацию окружения"""
    # Проверяем .env файл
    if not os.path.exists('.env'):
        print("\n📝 Файл .env не найден. Создаю пример...")
        create_env_file()
        print("\n❗ ВАЖНО: Отредактируйте файл .env и добавьте ваш OpenRouter API ключ")
        print("🔗 Получите ключ на: https://openrouter.ai/keys")
        return False
    
    # Загружаем переменные
    load_dotenv()
    api_key = os.getenv('OPENROUTER_API_KEY')
    
    if not api_key or api_key == 'your_openrouter_api_key_here':
        print("\n❌ OpenRouter API ключ не настроен в .env файле")
        print("🔧 Отредактируйте .env и добавьте ваш ключ")
        print("🔗 Получите ключ на: https://openrouter.ai/keys")
        return False
    
    return True

def run_parser():
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("🚀 Запуск модульного парсера...")
        
        parser = MetalsNewsParser()
        
        news_items = parser.parse_all_sources(max_age_hours=168)
        
        if news_items:
            filename = parser.save_to_json(news_items)
            parser.print_summary(news_items)
            print(f"✅ Парсинг завершен!")
            print(f"📄 Данные обновлены в файле: {filename}")
            logger.info(f"Парсинг завершен успешно. Найдено {len(news_items)} новостей")
        else:
            print("❌ Релевантные новости не найдены")
            parser.print_summary([])
            logger.warning("Релевантные новости не найдены")
            
        return True
            
    except KeyboardInterrupt:
        print("\n⏹️ Прервано пользователем")
        logger.info("Парсинг прерван пользователем")
        return False
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        print(f"❌ Ошибка: {e}")
        return False

def main():
    """Основная функция"""
    print("🚀 МОДУЛЬНЫЙ DEEPSEEK ПАРСЕР ДРАГОЦЕННЫХ МЕТАЛЛОВ")
    print("=" * 80)
    print("🏗️ Модульная архитектура")
    print("🎯 Оптимизированная фильтрация")
    print("🤖 DeepSeek через OpenRouter.ai")
    print("📊 Детальное логирование")
    print("=" * 80)
    
    # Настраиваем логирование
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("Запуск парсера новостей по драгоценным металлам")
    logger.info("=" * 50)
    
    # Проверяем конфигурацию
    if not check_env_configuration():
        return
    
    # Запускаем парсер
    success = run_parser()
    
    if success:
        logger.info("Программа завершена успешно")
    else:
        logger.error("Программа завершена с ошибками")

if __name__ == "__main__":
    main()