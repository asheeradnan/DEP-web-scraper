from bs4 import BeautifulSoup
import requests
import json
import pandas as pd

class KrakenDataScraper:
    def __init__(self, base_url='https://api.kraken.com/0/public/OHLC?pair='):
        self.base_url = base_url
        self.data = None

    def fetch_data(self, symbol):
        url = f'{self.base_url}{symbol.lower()}usd&interval=1440'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = json.loads(soup.prettify())
        return data['result'][f'X{symbol.upper()}ZUSD']
    
    def create_dataframe(self, symbol, currency_name, data):
        df = pd.DataFrame(data, columns=['TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSING PRICE', 'VWAP', 'VOLUME', 'COUNT'])
        df['TIME'] = pd.to_datetime(df['TIME'], unit='s')
        df['CURRENCY'] = f'{currency_name}'
        return df
    
    def scrape_and_save(self, symbols, currency_names, output_filename='kraken_data.csv'):
        dfs = []
        for symbol, currency_name in zip(symbols, currency_names):
            data = self.fetch_data(symbol)
            df = self.create_dataframe(symbol, currency_name, data)
            dfs.append(df)

        merged_df = pd.concat(dfs)
        merged_df.to_csv(output_filename, index=False)
        return merged_df
    

symbols_to_scrape = ['xbt', 'eth', 'xrp']
currency_names = ['BITCOIN (BTC)', 'ETHEREUM (ETH)', 'RIPPLE (XRP)']
kraken_scraper = KrakenDataScraper()

result_df = kraken_scraper.scrape_and_save(symbols_to_scrape, currency_names)
print(result_df)
