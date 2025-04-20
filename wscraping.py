import os
import json
import duckdb
import logging
import datetime
from firecrawl import FirecrawlApp
from bs4 import BeautifulSoup
from dotenv import load_dotenv


class BookScraper:
    def __init__(self, url, db_file):
        current_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.basicConfig(
            level=logging.INFO,
            format=f'%(asctime)s - {current_date} - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        load_dotenv(override=True)
        self.api_key = os.getenv('FIRECRAWL_API')
        self.url = url
        self.db_file = db_file
        self.page_processing_options = {'onlyMainContent': False}
        self.extractor_options = {}
        self.output_formats = ['html']

        if not self.api_key:
            logging.error('Não foi identificada nenhuma API KEY para o firecrawl.')
        else:
            logging.info('Firecrawl API KEY obtida com sucesso!')

        self.app = None

    def initialize_app(self):
        try:
            self.app = FirecrawlApp(api_key=self.api_key)
            logging.info("FireCrawl App inicializado com sucesso!")
        except Exception as e:
            logging.error(f"Erro ao inicializar o FireCrawl App -> {e}")

    def scrape(self):
        if not self.app:
            logging.warning("FireCrawl App não foi inicializado.")
            return None

        params = {'formats': self.output_formats}
        params.update(self.page_processing_options)
        params.update(self.extractor_options)

        logging.info(f'Preparando scraping da página: {self.url}')
        logging.info(f'Parâmetros: {json.dumps(params, indent=2)}')

        try:
            scraped_data = self.app.scrape_url(self.url, params=params)
            if scraped_data and 'html' in scraped_data and scraped_data['html']:
                logging.info('HTML obtido com sucesso.')
                return scraped_data['html']
            else:
                logging.error('Erro ao obter o HTML.')
                return None
        except Exception as e:
            logging.error(f'Erro durante o scraping -> {e}')
            return None

    def parse_html(self, html_data):
        soup = BeautifulSoup(html_data, 'lxml')
        pods = soup.select('article.product_pod')
        logging.info(f'Foram encontrados {len(pods)} product_pods.')

        books = []

        for pod in pods:
            title_tag = pod.select_one('h3 a')
            title = title_tag['title'].strip() if title_tag and title_tag.has_attr('title') else None
            price_tag = pod.select_one('div.product_price p.price_color')
            price = None

            if price_tag:
                try:
                    price_cleaned = price_tag.get_text().replace('£', '').strip()
                    price = float(price_cleaned)
                except ValueError:
                    logging.warning(f"Erro ao converter o preço do livro '{title}'.")

            if title and price is not None:
                books.append({'title': title, 'price': price})
            else:
                logging.warning(f"Dados incompletos para um livro. Título: {title}, Preço: {price}")

        logging.info(f"{len(books)} livros extraídos com sucesso.")
        return books

    def save_to_duckdb(self, books):
        if not books:
            logging.warning("Nenhum livro válido para salvar.")
            return

        try:
            with duckdb.connect(database=self.db_file, read_only=False) as con:
                logging.info(f"Conectado ao DuckDB: {self.db_file}")

                con.execute("""
                    CREATE TABLE IF NOT EXISTS books (
                        title VARCHAR,
                        price DECIMAL(10, 2)
                    );
                """)

                logging.info("Tabela 'books' verificada/criada.")

                con.execute("DELETE FROM books;")
                con.executemany("INSERT INTO books (title, price) VALUES (?, ?)",
                                [(book['title'], book['price']) for book in books])

                logging.info(f"{len(books)} registros inseridos com sucesso na tabela 'books'.")
        except Exception as e:
            logging.error(f"Erro ao salvar no DuckDB -> {e}")

    def run(self):
        logging.info("Iniciando o processo completo de scraping.")
        self.initialize_app()
        html = self.scrape()

        if html:
            books = self.parse_html(html)
            self.save_to_duckdb(books)


if __name__ == "__main__":
    scraper = BookScraper(
        url='https://books.toscrape.com/',
        db_file='web_scrape_data.duckdb'
    )
    scraper.run()