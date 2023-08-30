
from bs4 import BeautifulSoup
from dotenv import dotenv_values
from tqdm import tqdm
from rich.console import Console
import grequests
import requests
import json
import pandas as pd
import copy
import datetime




headers = {
    'auth' : {
        'authority': 'sellerstats.ru',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://sellerstats.ru',
        'referer': 'https://sellerstats.ru/login/',
        'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    },
    'category': {
        'authority': 'sellerstats.ru',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://sellerstats.ru',
        'referer': 'https://sellerstats.ru/stat/categories',
        'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    
}

cookies = {
    'auth' : {
         'csrftoken': ''
    },
    'rubricator': {
        'csrftoken': '',
        'sessionid': ''
    }
}




payload = {
    'auth': {
        'csrfmiddlewaretoken': '',
        'username': dotenv_values('.env').get('USERNAME'),
        'password': dotenv_values('.env').get('PASSWORD')

    },
    'category': {
        'site': 'ym',
        'sell_type': '0',
        'period': '30',
        'cat_id': '1',
        'csrfmiddlewaretoken': '',
        'limit_start': '0',
        'limit_end': '200',
        'sort': '-turnover',
        'filter_model': '{}',
    }
}

categories_paths = []
console = Console()

class ProgressSession():
    def __init__(self, urls,session):
        self.pbar = tqdm(total = len(urls), desc = console.log("[yellow]geting individual categorie pages[/yellow]"))
        self.urls = urls
        self.session = session
    def update(self, r, *args, **kwargs):
        if not r.is_redirect:
            self.pbar.update()
    def __enter__(self):
        sess = self.session
        sess.hooks['response'].append(self.update)
        return sess
    def __exit__(self, *args):
        self.pbar.close()
        console.log(f'[bold][green]Done!')

def get_categories(session: requests.sessions.Session, cat_id) -> dict:
    payload.get('category')['cat_id'] = cat_id
    str_cat = session.post('https://sellerstats.ru/api/stat/categories',
                 data=payload.get('category'),
                 cookies=cookies.get('rubricator'),
                 headers=headers.get('category')
                 )
    return json.loads(str_cat.text)



def depth_first_traversal(depth: int,
                          node: dict,
                          session: requests.sessions.Session,
                          cat_id:int,
                          path=[],
                          bar=None) -> dict:
    bar.update(1)
    path = path + [node['name']]
    result = {'name': node['name']}

    if depth > 0 and 'childs' in node and node['childs'] > 0:
        result['children'] = []

        node = get_categories(session=session, 
                              cat_id=node.get('id'))
        for child in node['data']:
            child_data = depth_first_traversal(depth=depth-1,
                                               node=child, 
                                               session=session, 
                                               cat_id=child.get('id'),
                                               path=path,
                                               bar=bar)  # Рекурсивно обходим дочерние узлы
            result['children'].append(child_data)

    else:
        result['sold'] = node['sold']
        result['path'] = path
        path.insert(0, f'https://sellerstats.ru/stat/category/ym/{node["id"]}/')
        path.insert(1,node['sold'])
    categories_paths.append(copy.deepcopy(path))
    return result


def get_url_to_ym(category_url):

    url = ''

    try:
        soup = BeautifulSoup(category_url.text, 'html.parser')
        url = soup.find(attrs={'target': '_blank'})['href']
    except Exception as e:
        url = None
    return url




def get_urls_async(urls, session):
  with ProgressSession(urls, session) as sess:
      rs = (grequests.get(url, session = sess, timeout = 20) for url in urls)
      return grequests.map(rs)
      
if __name__ == '__main__': 
    with requests.sessions.Session() as session:

        auth_response = session.get('https://sellerstats.ru/login', 
                                    headers=headers.get('auth'))

        cookies.get('auth')['csrftoken'] = auth_response.cookies.get_dict().get('csrftoken')
        soup = BeautifulSoup(auth_response.text, 'html.parser')
        csrfmiddlewaretoken = soup.find('input')['value']

        headers.get('auth')['content-type'] = 'application/x-www-form-urlencoded'
        payload.get('auth')['csrfmiddlewaretoken'] = csrfmiddlewaretoken

        response_redirect_to_home = session.post('https://sellerstats.ru/login/', 
                                data=payload.get('auth'), 
                                headers=headers.get('auth'), 
                                cookies=cookies.get('auth'))

        rubricator = session.get('https://sellerstats.ru/stat/categories')


        cookies.get(
            'rubricator'
            )['csrftoken'] = payload.get(
                'category'
                )['csrfmiddlewaretoken'] = csrfmiddlewaretoken
        
        cookies.get('rubricator')['sessionid'] = rubricator.cookies.get('sessionid')

    
        response = session.post('https://sellerstats.ru/api/stat/categories',
                                cookies=cookies.get('rubricator'), 
                                headers=headers.get('category'), 
                                data=payload.get('category'))


        data = get_categories(session=session,cat_id='1')
        

        root_node = data['data']
        result_data = []
        bar = tqdm(total=3859, desc=console.log("[yellow]receiving Ym categories[/yellow]"))
        for node in root_node:
            node_data = depth_first_traversal(100,node, session=session, cat_id=node.get('id'),bar=bar)
            result_data.append(node_data)
        console.log(f'[bold][green]Done!')
        flatten_data = pd.DataFrame(result_data)

        remove_index = []

        for index,path in enumerate(categories_paths):
            if 'https://sellerstats.ru/' not in path[0]:
                remove_index.append(index)
                    
        
        for index in sorted(remove_index, reverse=True):
            if 0 <= index < len(categories_paths):
                categories_paths.pop(index)


        categories_urls = [row[0] for row in categories_paths]
        f = get_urls_async(categories_urls,session)
        for index,url in enumerate(tqdm(f,desc= console.log("[yellow]preparing category urls[/yellow]"))):
            categories_paths[index][0] = get_url_to_ym(url)
        console.log(f'[bold][green]Done!')

    
        df = pd.DataFrame(categories_paths, columns=['url',
                                                    'товаров',
                                                    'катег_1', 
                                                    'катег_2', 
                                                    'катег_3',
                                                    'катег_4',
                                                    'катег_5',])
        console.log("[yellow]start creating xlsx report[/yellow]")
        report_name = (lambda name : name +"_"+ datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))('result')
        df.to_excel(report_name+'.xlsx',
                    index=False)
        console.log(f'[bold][green]Created with name {report_name}!')
        with open('hierarchical_data.json', 'w', encoding='utf-8') as json_file:

            json.dump(result_data, json_file,ensure_ascii=False, indent=4)



