import csv
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from supabase import create_client, Client

BASE_URL = "https://finance.yahoo.com/markets/stocks/most-active/?start=0&count=100"
LOCAL_FILENAME = "yahoo_finance_most_active_sorted.csv"
REMOTE_FILENAME = os.getenv("REMOTE_FILENAME", "market.csv")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

resultados = []

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox") 
options.add_argument("--disable-dev-shm-usage") 
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 15)

try:
    driver.get(BASE_URL)
    time.sleep(2)  

    try:
        go_to_end_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Go to end') or contains(., 'Ver tudo')]")
        ))
        go_to_end_button.click()
        time.sleep(1)
    except (NoSuchElementException, TimeoutException):
        pass

    try:
        reject_cookies_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'Reject all') or contains(., 'Rejeitar todos')]")
        ))
        reject_cookies_button.click()
        time.sleep(1)
    except (NoSuchElementException, TimeoutException):
        pass

    try:
        market_cap_header = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//span[text()='Market Cap']")
        ))
        market_cap_header.click()
        time.sleep(3)
    except (NoSuchElementException, TimeoutException):
        print("Não foi possível ordenar por 'Market Cap'.")

    stock_rows = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//table/tbody/tr")))
    stock_urls = []
    for row in stock_rows:
        try:
            link = row.find_element(By.XPATH, ".//td[1]/a").get_attribute("href")
            stock_urls.append(link)
        except NoSuchElementException:
            continue

    print(f"Encontrados {len(stock_urls)} stocks. A processar...")
    
    for url in stock_urls: 
        driver.get(url)
        time.sleep(1) 

        try:
            ticker_el = driver.find_element(By.XPATH, "//h1")
            full_text = ticker_el.text
            
            if "(" in full_text:
                parts = full_text.split(" (")
                nome = parts[0]
                ticker = parts[1].replace(")", "")
            else:
                ticker = full_text
                nome = full_text

            def get_data(label):
                try:
                    return driver.find_element(
                        By.XPATH, f"//span[contains(text(), '{label}')]/../following-sibling::span | //td[contains(text(), '{label}')]/following-sibling::td"
                    ).text
                except:
                    return ""

            market_cap = get_data("Market Cap")
            previous_close = get_data("Previous Close")
            open_price = get_data("Open")
            days_range = get_data("Day's Range")
            week_52 = get_data("52 Week Range")
            pe_ratio = get_data("PE Ratio (TTM)")
            eps = get_data("EPS (TTM)")
            beta = get_data("Beta (5Y Monthly)")
            
            try:
                change_percent = driver.find_element(By.XPATH, "//fin-streamer[contains(@data-field, 'regularMarketChangePercent')]").text
            except:
                change_percent = "0%"

            resultados.append({
                "Ticker": ticker,
                "Nome": nome,
                "Market Cap": market_cap,
                "Change%": change_percent,
                "Previous Close": previous_close,
                "Open": open_price,
                "Day's Range": days_range,
                "52 Week Range": week_52,
                "PE Ratio (TTM)": pe_ratio,
                "EPS (TTM)": eps,
                "Beta (5Y Monthly)": beta
            })

            print(f"Extraído: {ticker}")

        except Exception as e:
            print(f"Erro em {url}: {e}")

finally:
    driver.quit()

    if resultados:
        print(f"A guardar {len(resultados)} linhas localmente...")
        with open(LOCAL_FILENAME, "w", encoding="utf-8", newline='') as f:
            headers = [
                "Ticker", "Nome", "Market Cap", "Change%", "Previous Close", "Open", 
                "Day's Range", "52 Week Range", "PE Ratio (TTM)", "EPS (TTM)", "Beta (5Y Monthly)"
            ]
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(resultados)

        print("A enviar para o bucket...")
        try:
            with open(LOCAL_FILENAME, 'rb') as f:
                supabase.storage.from_(SUPABASE_BUCKET).upload(
                    file=f,
                    path=REMOTE_FILENAME,
                    file_options={"content-type": "text/csv", "upsert": "true"}
                )
            os.remove(f"./{LOCAL_FILENAME}")
            print("Sucesso! Ficheiro no Supabase.")
        except Exception as e:
            print(f"Erro no upload para Supabase: {e}")
    else:
        print("Nenhum resultado para enviar.")