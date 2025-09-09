import os
from langchain_openai import ChatOpenAI
from langchain.agents import AgentType, initialize_agent, load_tools
from dotenv import load_dotenv

# Загружаем ключи из .env
load_dotenv()

# Проверяем наличие обоих ключей
if not os.getenv("OPENROUTER_API_KEY") or not os.getenv("SERPAPI_API_KEY"):
    print("ОШИБКА: Убедитесь, что OPENROUTER_API_KEY и SERPAPI_API_KEY прописаны в файле .env")
else:
    print("--- Запуск PoC v3: Анализ Сентимента через OpenRouter и SerpApi ---")
    
    # Инициализируем LLM через OpenRouter, используя ChatOpenAI с измененным base_url
    llm = ChatOpenAI(
        model="tngtech/deepseek-r1t2-chimera:free",
        openai_api_key=os.getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
        temperature=0.1,
        streaming=True
    )
    
    # Загружаем инструмент поиска SerpApi
    tools = load_tools(["serpapi"])
    
    # Создаем агента
    agent = initialize_agent(
        tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True
    )
    
    # Наш "сложный" промпт на русском языке
    search_query = """
    Каков текущий новостной сентимент для 'криптовалюта Solana' прямо сейчас?
    Используй инструмент поиска, чтобы найти 3-5 последних новостных заголовков на РУССКОМ ЯЗЫКЕ.
    Проанализируй эти заголовки с точки зрения риск-менеджера хедж-фонда.
    Являются ли позитивные новости подлинными или это может быть 'exit liquidity' (ликвидность для выхода) для крупных игроков?
    В конце дай ответ ОДНИМ СЛОВОМ: ПОЗИТИВНЫЙ, НЕГАТИВНЫЙ или НЕЙТРАЛЬНЫЙ.
    После этого дай краткое обоснование на РУССКОМ ЯЗЫКЕ в одном предложении. Весь твой финальный ответ должен быть на русском языке.
    """
    
    print(f"\n[АРХИТЕКТОР] Агент, вот твой запрос: {search_query.strip()}")
    print("\n[АГЕНТ] Запрос принят. Начинаю работу с моделью tngtech/deepseek-r1t2-chimera:free и инструментом SerpApi...")
    
    try:
        # Запускаем агента
        response = agent.run(search_query)
        
        print("\n--- РЕЗУЛЬТАТ АНАЛИЗА ---")
        print(response)
        print("--------------------------")
    except Exception as e:
        print(f"\n--- КРИТИЧЕСКАЯ ОШИБКА ВО ВРЕМЯ ВЫПОЛНЕНИЯ АГЕНТА ---")
        print(f"Ошибка: {e}")
        print("----------------------------------------------------")