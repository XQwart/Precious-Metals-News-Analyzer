import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from parser import MetalsNewsParser

ENV_TEMPLATE = """# Любой OpenAI-совместимый API: OpenAI, OpenRouter, Ollama, vLLM и другие
OPENAI_API_KEY=your_api_key_here
OPENAI_MODEL=

# Адрес API, если это не OpenAI, например https://openrouter.ai/api/v1
# OPENAI_BASE_URL=
"""


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
    logging.getLogger('httpx').setLevel(logging.WARNING)

    return logging.getLogger(__name__)


def check_env_configuration():
    """Проверяет, что заданы ключ API и модель"""
    load_dotenv()

    if not os.path.exists('.env') and not os.getenv('OPENAI_API_KEY'):
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(ENV_TEMPLATE)
        print("Создан файл .env. Укажите в нём OPENAI_API_KEY и OPENAI_MODEL")
        return False

    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key or api_key == 'your_api_key_here':
        print("Не задан OPENAI_API_KEY в .env")
        return False

    if not os.getenv('OPENAI_MODEL'):
        print("Не задан OPENAI_MODEL в .env")
        return False

    return True


def run_parser():
    logger = logging.getLogger(__name__)

    try:
        logger.info("Запуск парсера")

        parser = MetalsNewsParser()

        news_items = parser.parse_all_sources(max_age_hours=168)

        if news_items:
            filename = parser.save_to_json(news_items)
            parser.print_summary(news_items)
            print(f"Готово, результаты в файле {filename}")
            logger.info(f"Парсинг завершен успешно. Найдено {len(news_items)} новостей")
        else:
            print("Релевантные новости не найдены")
            parser.print_summary([])
            logger.warning("Релевантные новости не найдены")

        return True

    except KeyboardInterrupt:
        print("\nПрервано пользователем")
        logger.info("Парсинг прерван пользователем")
        return False
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        print(f"Ошибка: {e}")
        return False


def main():
    """Основная функция"""
    if not check_env_configuration():
        return

    logger = setup_logging()
    logger.info("Запуск парсера новостей по драгоценным металлам")

    success = run_parser()

    if success:
        logger.info("Программа завершена успешно")
    else:
        logger.error("Программа завершена с ошибками")


if __name__ == "__main__":
    main()
