# Precious Metals News Analyzer

Collects Russian financial news from RBC, Finam, Investing.com and Vedomosti and keeps only articles about gold, silver, platinum and palladium as commodities or investments. A keyword filter drops most articles. An LLM checks the rest, rejects figurative uses such as "golden medal" and writes a short summary.

## Run

```bash
pip install -r requirements.txt
cp .env.example .env   # set OPENAI_API_KEY and OPENAI_MODEL
python main.py
```

The script looks at the last 7 days and saves the relevant articles to `metals_news.json`, each with its link, source, metals, summary and relevance score. If the API is unavailable, it falls back to a keyword check.

Any OpenAI-compatible API works: OpenAI, OpenRouter, Ollama, vLLM and others. For anything other than OpenAI, set `OPENAI_BASE_URL` too.
