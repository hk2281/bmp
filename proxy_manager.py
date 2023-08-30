import grequests
import requests


class ProxyManager:
    def __init__(self, proxy_file, with_auth, spus):
        """
            init manager object 
            and geting works proxy list
        """
        self.with_auth = with_auth
        self.spus = spus
        if self.spus:
            self.download_spus_me_txt()
        self.proxy_file = proxy_file
        self.proxies = self.get_check_proxies()
        self.available_proxies = self.proxies.copy()
        self.used_proxies = []
        


    def get_proxy(self):
        """
            method for return available proxie
        """
        if not self.available_proxies:
            # if all proxy used reload available list 
            self.available_proxies = self.used_proxies.copy()
            self.used_proxies = []

        if not self.available_proxies:
            # raise Exception("haven't got available")
            return None

        proxy = self.available_proxies.pop(0)
        self.used_proxies.append(proxy)
        return {
            'http':proxy,
            # 'https':proxy
        }

    def release_proxy(self, proxy):
        """
            method for reloadind available proxy list
            get all proxy in used list and put to avalable 
            while used proxy list not empty
        """
        if proxy in self.used_proxies:
            self.used_proxies.remove(proxy)
            self.available_proxies.append(proxy)


    def get_prepared_proxie_list(self,proxies:list):
        """
            method for geting list of proxyes dict
        """
        return [
            {
                'http': f'http://{proxy}',
                # 'https': f'http://{proxy}',
            } for proxy in proxies
        ]

    def preparator_file(self):
        with open(self.proxy_file, 'r', encoding='utf-8') as f:
            proxies = [i.strip().split(' ')[0] for i in f.readlines()]
            if self.with_auth:
                return [f"{i.split(':')[2]}:{i.split(':')[3]}@{i.split(':')[0]}:{i.split(':')[1]}" for i in proxies]
            else:
                return proxies


    def get_check_proxies(self, cheak_host='http://ip-api.com/json/'):

        """
            start cheacking proxies
            return working proxie list
        """
        print(f'[INFO] Запуск проверки прокси.')
        
        proxies = self.preparator_file()

        proxies_options = self.get_prepared_proxie_list(proxies)


        req = (grequests.get(
                    cheak_host, 
                    timeout = 5, 
                    proxies=proxie, 
                    headers={'proxy': proxie.get('http', '')}
                ) for proxie in proxies_options)
        
        responses = (grequests.map(req,size=10))

        responses = list(
                        filter(
                            lambda resp: True if resp != None and resp.status_code == 200 else False,
                            responses
                        ))

        return [x.request.headers.get('proxy', None) for x in responses]

    def download_spus_me_txt(self, url=None):
        print('start download')
        if not url:
            url = 'https://spys.me/proxy.txt'

        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter below
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    #if chunk: 
                    f.write(chunk)
        return local_filename


# if __name__ == "__main__":
#     proxy_manager = ProxyManager('proxy.txt')

#     print(proxy_manager.get_proxy())


# for i in range(20):
#     try:
#         proxy = proxy_manager.get_proxy()
#         print("Используется прокси:", proxy)
#         # Ваш код, использующий прокси
#     except Exception as e:
#         print(e)
#     finally:
#         # Вернуть прокси после использования
#         proxy_manager.release_proxy(proxy)
    # url = None
    # if not url:
    #     url = 'https://spys.me/proxy.txt'

    # local_filename = url.split('/')[-1]
    # print(local_filename)
    # # NOTE the stream=True parameter below
    # with requests.get(url, stream=True) as r:
    #     r.raise_for_status()
    #     with open(local_filename, 'wb') as f:
    #         for chunk in r.iter_content(chunk_size=8192): 
    #             # If you have chunk encoded response uncomment if
    #             # and set chunk_size parameter to None.
    #             #if chunk: 
    #             f.write(chunk)
