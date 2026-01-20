import csv
from pathlib import Path
import time
import os
import sys
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from supabase import create_client, Client

script_dir = Path(__file__).parent.absolute()
load_dotenv(dotenv_path=script_dir / ".env")

BASE_URL = "https://finance.yahoo.com/markets/stocks/large-cap-stocks/"
LOCAL_FILENAME = script_dir / "market_data.csv"
REMOTE_FILENAME = os.getenv("REMOTE_FILENAME", "market.csv")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if SUPABASE_URL and not SUPABASE_URL.endswith("/"):
    SUPABASE_URL += "/"

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Erro: Variaveis de ambiente nao encontradas.")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 10)
stock_urls = []
resultados = []

try:
    print("Fase 1: A recolher links dos ativos.")
    
    for offset in [0, 100, 200]:
        url_paginada = f"{BASE_URL}?start={offset}&count=100"
        driver.get(url_paginada)
        
        if offset == 0:
            time.sleep(3)
            try:
                cookie_btn = driver.find_element(By.XPATH, "//button[contains(., 'Accept') or contains(., 'Agree') or contains(., 'Aceitar') or contains(., 'Concordo')]")
                cookie_btn.click()
            except:
                pass

        wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
        time.sleep(2)
        
        elementos_links = driver.find_elements(By.XPATH, "//table//tr/td[1]//a[contains(@href, '/quote/')]")
        for el in elementos_links:
            link = el.get_attribute("href")
            if link and link not in stock_urls:
                stock_urls.append(link)
        
        print(f"Links recolhidos: {len(stock_urls)}")
        if len(stock_urls) >= 300:
            break

    print(f"\nFase 2: A extrair dados de {len(stock_urls[:300])} ativos.")
    
    for i, url in enumerate(stock_urls[:300]):
        try:
            driver.get(url)
            time.sleep(1.5)

            titulo_aba = driver.title
            
            if "(" in titulo_aba and ")" in titulo_aba:
                parte_inicial = titulo_aba.split(")")[0]
                divisao = parte_inicial.split(" (")
                nome = divisao[0].strip()
                ticker = divisao[1].strip()
            else:
                nome = "Desconhecido"
                ticker = "N/A"

            def obter_estatistica(label):
                try:
                    xpath = f"//li[.//span[contains(text(), '{label}')]]//span[contains(@class, 'value')]"
                    return driver.find_element(By.XPATH, xpath).text
                except:
                    return "N/A"

            registo = {
                "Ticker": ticker,
                "Nome": nome,
                "Market Cap": obter_estatistica("Market Cap"),
                "Change%": "0%", 
                "Previous Close": obter_estatistica("Previous Close"),
                "Open": obter_estatistica("Open"),
                "Day's Range": obter_estatistica("Day's Range"),
                "52 Week Range": obter_estatistica("52 Week Range"),
                "PE Ratio (TTM)": obter_estatistica("PE Ratio"),
                "EPS (TTM)": obter_estatistica("EPS (TTM)"),
                "Beta (5Y Monthly)": obter_estatistica("Beta")
            }

            try:
                registo["Change%"] = driver.find_element(By.XPATH, "//section[@data-testid='quote-price']//fin-streamer[contains(@data-field, 'Percent')]").text
            except:
                pass

            resultados.append(registo)
            print(f"   [{i+1}] Processado: {ticker}")

        except Exception as e:
            print(f"   [{i+1}] Erro no ativo {url}: {e}")

finally:
    driver.quit()

if resultados:
    print(f"\nA guardar dados em {LOCAL_FILENAME}")
    with open(LOCAL_FILENAME, "w", encoding="utf-8", newline='') as f:
        cabecalhos = ["Ticker", "Nome", "Market Cap", "Change%", "Previous Close", "Open", "Day's Range", "52 Week Range", "PE Ratio (TTM)", "EPS (TTM)", "Beta (5Y Monthly)"]
        escritor = csv.DictWriter(f, fieldnames=cabecalhos)
        escritor.writeheader()
        escritor.writerows(resultados)

    print("A enviar dados para o Supabase.")
    try:
        with open(LOCAL_FILENAME, 'rb') as f:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                file=f,
                path=REMOTE_FILENAME,
                file_options={"content-type": "text/csv", "upsert": "true"}
            )
        print("Sucesso: O ficheiro foi carregado.")
    except Exception as e:
        print(f"Erro no envio: {e}")
else:
    print("Erro: Nenhum dado foi recolhido.")