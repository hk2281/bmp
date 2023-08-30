import grequests
import requests
# from utils import calculate_time
from lxml import html, etree
from prx import ProgressSession
import bs4
from bs4 import BeautifulSoup
from typing import Union
import re
# requre for parallel working
import threading
import multiprocessing
import time
import random

from proxy_manager import ProxyManager


from dataclasses import dataclass


PROXY_LIST_FILE = 'proxy.txt'

@dataclass
class glob_var:
    flag: bool
    collector: list


var = glob_var(False, [])

headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}

def prepare_urls(url_template):
    """
        подготовка ссылок на страницы с номерами
        возвращает список ссылок на страницы от 1-60
    """
    if '?' in url_template:
        url_template += '&PAGEN_1='
    else:
         url_template += '?PAGEN_1='
    return [url_template + str(i)  for i in range(1,61)]    


file=open('marki.txt', encoding='utf-8')
marki = []
for i in file:
    marki.append(i.strip())
file.close()

marki = set(marki)

toplivo=[]

file=open('toplivo.txt', encoding='utf-8')
for i in file:
    toplivo.append(i.strip())
file.close()

toplivo = set(toplivo)

p_manager = ProxyManager(PROXY_LIST_FILE, with_auth=False, spus=True)
proxy_count = len(p_manager.available_proxies)

def func_chunks_generators(lst, n):
    # функция для разбиения списка на x частей по n эллементов
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# @calculate_time
def parce_batch(target_url:str):

    url_template = target_url + '?sort=PRICE-DESC&PAGEN_1='

    prepare_urls = [url_template+str(i) for i in range(1,61)]
    # rs = (grequests.get(u, timeout = 20) for u in prepare_urls)

    with open('t.html', 'w', encoding='utf-8') as file :
        with requests.sessions.Session() as session:
            with ProgressSession(prepare_urls, session) as sess:
                # p_manager = ProxyManager(PROXY_LIST_FILE, with_auth=False, spus=False)
                proxy_count = len(p_manager.available_proxies)
                print(f'count of proxy: {proxy_count}')
                if proxy_count == 0:
                    proxy_count = 5
                filtered = []
                broken_request = []
                for i in range(proxy_count):
                    try:
                        pr = p_manager.get_proxy()
                        print(pr)
            
                        rs = (grequests.get(url, session = sess, timeout = 20, headers=headers, proxies=pr) for url in prepare_urls)
                        rs = list(func_chunks_generators(list(rs), 25))

                        for index,rs_chunk in enumerate(rs):
                            print(f'fetch chunk: {index}, with len requests of: {len(rs_chunk)}')
                            rs_chunk = (gen for gen in rs_chunk)
                            list_resp = grequests.map(rs_chunk)
                            for x in list_resp:
                                if x!=None:
                                    if x.status_code == 200:
                                        filtered.append(x)
                                    else:
                                        broken_request.append({
                                            'status_code': x.status_code,
                                            'url': x.url,
                                        })
                            time.sleep(1)
                        break
                    except Exception as e:
                        print('cant fretch target url in batch: ',e)
                        time.sleep(2)


                print('good ',len(filtered),filtered)
                print('broken ',len(broken_request),broken_request)
                # file.write(list_resp[1].text)
                coll = []
                for page in filtered:
                    parts_page  = html.document_fromstring(page.text)
                    soup = BeautifulSoup(page.text, 'html.parser')
                    all_ads = soup.find('div', {'id': 'allAds'})
                    all_parts = all_ads.find_all(class_='item-list')

                    for index,part in enumerate(all_parts):

                        try:
                            name = part.find('b').text
                        except Exception as e:
                            name=''

                        try:
                            part_details = part.find(class_='add-details')
                            part_title=part_details.find(class_='add-title')
                            text_in_a = part_title.find('a').text
                            text_in_a = text_in_a.replace(name, '')
                            oni = text_in_a.split(',')

                            try:
                                part_url = 'https://bamper.by' + part_title.find('a')['href']
                            except:
                                part_url = ''

                            try:
                                if(' г.' in oni[-1]):
                                    year=oni[-1].replace(' г.','').strip()
                            except Exception as e:
                                year = ''

                            try:
                                if(year!=''):
                                    new_chast=','.join(oni[:-1]).replace(' к ','')
                                else:
                                    new_chast=part_title.replace(' к ','')
                                    
                                temp_new_chast = set(new_chast.split())
                                marka_1 = temp_new_chast & marki
                                marka_1 = next(iter(marka_1))
                            except Exception as e: 
                                marka_1 = ''
                            
                            try:
                                model=new_chast.replace(marka_1,'').strip()
                            except: 
                                model = ''

                            try:
                                info_row = part_details.find('div', {'style': 'color:#333'}).text.replace('\xa0','').strip()
                                info_row = list(map(lambda word : word.strip(), info_row.split(',')))
                                # print(info_row)
                                for kam in info_row:
                                    if(kam[-2:]==' л'):
                                        dvigatel=kam.strip()
                                set_row_info = set(info_row)
                                toplivo_1 = next(iter(set_row_info & toplivo))
                                # print(type(info_row))
                                articul = part_details.find(class_='date').text
                                 
                                description = part_details.find_all('div')[1].text.replace('\n','').strip()
                                try:
                                    part_number =  part_details.find_all(class_='date')[1].text
                                    # print(part_number)
                                except:
                                    part_number = ''

                                try:
                                    city = part_details.find(class_='city').text
                                except:
                                    city = ''
                            except:
                                info_row = dvigatel = toplivo_1 = articul = description = part_number = city = ''

                            try:

                                prises = part.find(class_='item-price')
                                prises = prises.text.replace('\n', ' ').split(' ')
                                prises = list(filter(lambda x: x != '', prises))
                                min_price_byn = ''
                                for price_chank in prises:
                                    if price_chank == 'р.':
                                        break
                                    else: 
                                        min_price_byn += price_chank
                                byn = min_price_byn[:-2]
                                usd = prises[2].replace('~', '')
                                rub = prises[3].replace('~', '')
                                # print(byn,usd,rub)

                            except:
                                byn = usd = rub = ''

                        except:
                            part_details=''



                        coll.append([
                                page.url,
                                part_url,
                                index,
                                name,
                                year,
                                marka_1,
                                model,
                                dvigatel,
                                toplivo_1,
                                articul,
                                description,
                                part_number,
                                city,
                                byn, rub, usd,
                        ])

            return coll
                        



   

def cheack_more_word(start_url:str) -> str:
    """
        функция для удаления more из ссылки
        возвращает ссылку
    """

    if '&more=Y' in start_url: 
        return start_url.replace('&more=Y', '')
    elif '?more=Y' in start_url:
        return start_url.replace('&more=Y', '')  
    else: 
        return start_url

def cheack_slash(start_url:str) -> str:
    """
        проверка на наличие / в конце строки
        возвращает ссылку
    """
    return start_url if start_url[-1] == '/' else start_url + '/'

def prepare_high_price_url(start_urls:str) -> str:
    """
        подготавливает ссылку с фильтом по максимальной цене
        возвращает ссылку
    """
    return cheack_slash(cheack_more_word(start_urls)) + '?sort=PRICE-DESC'

def plug_cheak_exist_part_page(target_url:str) -> Union[bs4.element.Tag,None]:
    """
        проверяет на наличие товаров, если товаров нет на странице отображается заглушка
        обнаружение заглушки
        возвращает тег заглушки или None
    """
    # p_manager = ProxyManager(PROXY_LIST_FILE, with_auth=False, spus=True)
    proxy_count = len(p_manager.available_proxies)
    print(f'count of proxy: {proxy_count}')
    if proxy_count == 0:
        proxy_count = 5
    for i in range(proxy_count):
        try:
            pr = p_manager.get_proxy()
            print('plug current proxy: ', pr)
            response = requests.get(target_url, headers=headers, proxies=pr)
            soup = BeautifulSoup(response.text, 'html.parser')
            plug = soup.find('a', {'title' : 'Подать заявку на поиск'})
            # print(plug)
            return plug
        except Exception as e:
            print("ошибка получения страницы заглушки")
            time.sleep(5)
    return None

def prepare_url_last_price_page(start_url:str) -> str:
    """
        функция для получения ссылки на 60ю последню страницу 
        возвращет ссылку
    """
    return prepare_high_price_url(start_url) + '&PAGEN_1=60'



def get_max_price(start_url_side:str) -> str:
    """
        функция для нахождения максимальной цены на странице
        возвращеет цену без учета сотых

    """
    # p_manager = ProxyManager(PROXY_LIST_FILE, with_auth=False, spus=True)
    proxy_count = len(p_manager.available_proxies)
    print(f'count of proxy: {proxy_count}')
    if proxy_count == 0:
        proxy_count = 5
    for i in range(proxy_count):
        try:
            pr = p_manager.get_proxy()
            print(pr)
            
            response  = requests.get(start_url_side, headers=headers, proxies=pr)
            print('response page with max prise: ',response.status_code)
            # with open('t.html', 'r', encoding='utf-8') as file:
            #     response = file.read()
                # print(response)
            soup = BeautifulSoup(response.text, 'html.parser')
            item_price = soup.find(class_='item-price')


            item_price = item_price.text.replace('\n', ' ').split(' ')
            item_price = list(filter(lambda x: x != '', item_price))
            max_price_byn = ''
            for price_chank in item_price:
                if price_chank == 'р.':
                    break
                else: 
                    max_price_byn += price_chank
            print(max_price_byn[:-2])
            return max_price_byn[:-2]
        except Exception as e:
            print(f'cant get max price cuz {e}')
            time.sleep(3)
    return None
    



def replace_last_number(string, new_number):
    """
        функция для замены номера страницы внутри ссылки на целевой
        возвращает ссылку

    """
    pattern = r"\d+$"  # Регулярное выражение для поиска последнего числа
    
    # Ищем последнее число в строке
    match = re.search(pattern, string)
    
    if match:
        last_number = match.group()
        new_string = string[:match.start()] + str(new_number)
        return (new_string,last_number)
    else:
        return (string, None)

def binary_search(start_page: int, end_page:int , cheaking_url:str) -> str:
    """
        функция для получения максимального номера страницы на котором есть товары
        возвращает ссылку
        TODO: теряем на односторонности

    """
    while start_page >= end_page:

        mid_page = start_page - (start_page - end_page) // 2

        # изменяем в ссылке крайнее значение которое указывает номер страницы
        # на центральное число
        # Открываем страницу mid_page в браузере и обрабатываем ее
        url , _ = replace_last_number(cheaking_url,mid_page)
        # print(mid_page)
        # Если находим требуемый элемент на странице, возвращаем адрес
        if plug_cheak_exist_part_page(url) is None: 
            return url
        
        # Если требуемый элемент не найден, смещаем начальную точку поиска выше
        start_page = mid_page - 1

def find_min_price(target_url:str) -> str:
    """
    функция для нахождения минимальной стоимости товара на данной странице
    возвращает минимальную стоимость товара без учета сотых

    """
    # p_manager = ProxyManager(PROXY_LIST_FILE , with_auth=False, spus=True)
    proxy_count = len(p_manager.available_proxies)
    print(f'count of proxy: {proxy_count}')
    if proxy_count == 0:
        proxy_count = 5
    for i in range(proxy_count):
        try:
            pr = p_manager.get_proxy()
            print(pr)
            
            resp = requests.get(target_url, headers=headers, proxies=pr)
            soup = BeautifulSoup(resp.text, 'html.parser')
            item_price = soup.find_all(class_='item-price')[-1]
            item_price = item_price.text.replace('\n', ' ').split(' ')
            item_price = list(filter(lambda x: x != '', item_price))
            min_price_byn = ''
            for price_chank in item_price:
                if price_chank == 'р.':
                    break
                else: 
                    min_price_byn += price_chank
            # print(max_price_byn[:-2])
            return min_price_byn[:-2]
        except Exception as e:
            print('cant get min price:',e)
            time.sleep(5)
    return None

def get_min_price_for_range(url_last_price_page:str) -> str:
    """
        функция для получения минимальной цены 
    """
    # проверка на наличие заглушки если нет для 60й страницы получить минимальную цену
    # для списка запчастей на странице
    
    if plug_cheak_exist_part_page(url_last_price_page) is None: 
        return find_min_price(url_last_price_page)
    # если есть заглушка вызвать функцию бинарного поиска для нахождения максимального номера страницы
    # на котором есть товары без заглушки, получить пинимальную стоимость товара на этой странице
    else:
        return find_min_price(binary_search(60,1,url_last_price_page))
    

def total_finger():
    """
        функция вызывается в случае когда мин и макс цена одинаковые
        идет перебор запчастей по маркам авто
        
    """

def process_data(num_points):
    import pickle
    print(f'get data: {num_points} about proccess: {multiprocessing.current_process().pid}')
    # # Здесь вы можете выполнить обработку элемента данных

    dt = parce_batch(num_points)
    # print(f'data: {num_points} processed!!! with res: {points_inside_circle}')
    # print(num_points)
    with open(f'res2/res{multiprocessing.current_process().pid}-{random.randint(1, 1000)}.pkl', 'wb') as f:
        pickle.dump(dt, f)
    

# Функция для обработки данных в другом потоке
def data_processor(data_list):

    while True:

        if data_list:
            # создание пула кол-вом свободных ядер
            pool =  multiprocessing.Pool()
            
            # передаем ссылки которые находяся в коллекторе в пул
            for _ in range(len(data_list)):
                pool.apply_async(process_data,args=(data_list.pop(),))
            # пока есть рабочие процессы в пуле джем для получения новых ссылок в коллекторе
            while pool._cache:
                print("number of jobs pending: ", len(pool._cache))
                time.sleep(2)
            # после завершения всех  процессов, в пуле 
            # закрывавает пул
            pool.close()
            pool.join()

        # проверка для выхода из цикла
        elif len(data_list) == 0 and var.flag:
            break
        # ожидать новые ссылки 
        else:
            time.sleep(1)
          
# @calculate_time
def main():
    with requests.sessions.Session() as session:

        url = 'https://bamper.by/zchbu/god_2021-2021/store_Y/isused_Y/'
        # магическое число, надо для получения максимальной чены товара
        # рекоторые пользователи вводят цену товара по приколу 
        min_price = 2**100


        data_thread = threading.Thread(
            target=data_processor,
            args=(var.collector,)
        )
        data_thread.start()

        try:
            max_price = get_max_price(start_url_side= prepare_high_price_url(
                                        cheack_slash(
                                            cheack_more_word(
                                                start_url=url
                                    )))) 
            if max_price is None:
                raise Exception('максимальная цена не найдена, страница не отвечает')
            pattern = r'god_\d{4}-\d{4}'

            target_url = re.sub(pattern, (r'\g<0>/'+f'price-do_{max_price}'),url)
            print(url)

        except Exception as e:
            print(f'cant get start max_price cuz{e}')
        
        max_price = '40' 
   
        while int(min_price) >= 1:  

            try:

                max_price = get_max_price(start_url_side= prepare_high_price_url(
                                        cheack_slash(
                                            cheack_more_word(
                                                start_url=target_url
                                    )))) 
                
                # processed_url =prepare_url_last_price_page(target_url) 
                print(max_price)
                min_price = get_min_price_for_range(
                                                url_last_price_page=prepare_url_last_price_page(target_url) 
                                                )
                
                # отлавливать когда макс и мин цена одинаковые запускать план зачистка
                print(max_price,min_price)
                result_url = re.sub(pattern, (r'\g<0>/'+f'price-ot_{min_price}/price-do_{max_price}'),url)
                var.collector.append(result_url)

                if max_price == min_price:
                    min_price = str(int(min_price) -1)


                if "ценанеуказа" in str(min_price):
                    break

                target_url = re.sub(pattern, (r'\g<0>/'+f'price-do_{min_price}'),url)
                print(target_url)


            except Exception as e: 
                print(f'cant get max-min pricess cuz {e}')
                min_price = 0
            
        var.flag = True


        # print(len(var.collector))
        
        # for url in var.collector:
        #     print(url)

        


if __name__ == '__main__':
    

    main()
    # parce_batch('https://bamper.by/zchbu/god_1990-1990/price-ot_347/price-do_9600/store_Y/isused_Y/')