import re
import json
import requests
from functools import reduce
from bs4 import BeautifulSoup, Tag
from multiprocessing import Pool, cpu_count

class MeuBuzuCrawler:
    URL = 'https://www.meubuzu.com.br/linhas'
    SUFIXO = '?page={}'

    def _parse_percurso(self, link):
        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')

        ida, volta = soup.select_one('#itinerary').select('.percurso')
        ida_list = list(map(lambda p: p.get_text().strip('\n'),
                            ida.select('div[class="col-xs-12 line-item-name"]')))
        volta_list = list(map(lambda p: p.get_text().strip('\n'),
                              volta.select('div[class="col-xs-12 line-item-name"]')))

        item = {'itinerario_ida': ida_list,
                'itinerario_volta': volta_list}

        return item

    def _parse_pagina(self, link):
        response = requests.get(link)

        soup = BeautifulSoup(response.text, 'html.parser')

        pat = '\(([\w\d]\d{3}(?:-\d{1,2})?]?)\)\s(.*)'
        items = []

        for linha in soup.select('div[class="row box line-item hovereble"] div a[href]')[1::2]:
            link = linha.get('href')
            info_linha = list(filter(lambda child: isinstance(child, Tag), linha.children))[0].get_text()
            try:
                cod_linha, nome_linha = re.match(pat, info_linha).groups()
            except Exception as ex:
                print(ex)
                print('INFO: ', info_linha)
                exit(1)

            item = {'link': link, 'cod': cod_linha, 'nome': nome_linha}
            percurso = self._parse_percurso(item.get('link'))
            item.update(percurso)

            items.append(item)

        return items

    def _find_max_pg(self):
        response = requests.get(MeuBuzuCrawler.URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        paginas = soup.select('ul.pagination li a[href]')
        pg_nums = map(lambda tag: int(tag.get_text()),
                      filter(lambda l: l.get_text().isnumeric(),
                             paginas))
        return max(pg_nums)

    def crawl(self):
        max_pg = self._find_max_pg()
        links = [MeuBuzuCrawler.URL + MeuBuzuCrawler.SUFIXO.format(i)\
                 for i in range(1, max_pg+1)]

        pool = Pool(cpu_count())
        resultados = pool.imap(func=self._parse_pagina,
                               iterable=links)

        pool.close()
        pool.join()

        items = list(reduce(lambda l1,l2: l1+l2, resultados))
        return items

if __name__ == '__main__':
    print('\n\t\t*** MEU BUZU Crawler ***\n')
    op = str(input('Deseja salvar localmente o resultado (em .json)? (s/[n]): '))
    
    print('\t> INICIALIZANDO...')
    crawler = MeuBuzuCrawler()

    print('\t> CRAWLEANDO...')
    items = crawler.crawl()
    
    if op.lower() == 's':
        print('\t> SALVANDO RESULTADOS...')
        with open('./meu_buzu.json', 'w', encoding='utf8') as file:
            json.dump(items, file, ensure_ascii=False, indent=4)
    else:
        print('\t> PRINTANTO RESULTADOS: ')
        print(items)
    
    print('\t> FEITO.')
