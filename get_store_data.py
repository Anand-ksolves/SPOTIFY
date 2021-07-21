import os
import zipfile
import requests
import configparser
from bs4 import BeautifulSoup
from pydoop import hdfs


class GetStoreData:

    def __init__(self, conf_file='app.conf'):
        try:
            dir_path = os.path.dirname(os.path.realpath(__file__))
            options = self.get_config(os.path.join(dir_path, conf_file))
            if options:
                self.tmp_dir_absolute = os.path.join(dir_path, options.get("tmp_dir"))
                self.zip_file_absolute = self.tmp_dir_absolute + "/" + options.get("zip_file_name")
                self.hdfs_dir = options.get("hdfs_dir")
                self.retry = int(options.get('retry')) or 3
                if not os.path.isdir(self.tmp_dir_absolute):
                    print(f"tmp_dir: {self.tmp_dir_absolute} is not a directory. Please fix in the conf file")
                    raise ValueError("This program needs path of the temporary location for json file arguments.")
        except Exception as e:
            print(e)

    def get_config(self, conf_file):
        config = configparser.ConfigParser()
        config.read(conf_file)
        return config['options']

    def request_url_content(self):
        page_text = None
        try:
            url_to_hit = "https://bikeshare.metro.net/about/data/"
            req_headers = {
                "authority": "bikeshare.metro.net",
                "method": "GET",
                "path": "/about/data/",
                "scheme": "https",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-language": "en-GB,en;q=0.9",
                "sec-ch-ua-mobile": "?0",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
            }
            resp = requests.get(url_to_hit, headers=req_headers)
            if resp.status_code == 200:
                page_text = resp.text
        except Exception as e:
            print(e)
        return page_text

    def download_url(self, url):
        flag = False
        try:
            req_headers_file = {
                "authority": "11ka1d3b35pv1aah0c3m9ced-wpengine.netdna-ssl.com",
                "method": "GET",
                # "path": "/about/data/",
                "scheme": "https",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "accept-language": "en-GB,en;q=0.9",
                "sec-ch-ua-mobile": "?0",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36"
            }
            resp = requests.get(url, headers=req_headers_file)
            if resp.status_code == 200:
                with open(self.zip_file_absolute, 'wb') as fd:
                    fd.write(resp.content)
                    flag = True
        except Exception as e:
            print(e)
        return flag

    def save_csv_file_from_zip_file(self):
        flag = False
        try:
           with zipfile.ZipFile(self.zip_file_absolute, 'r') as zip_ref:
               zip_ref.extractall(self.tmp_dir_absolute)
               flag = True
        except Exception as e:
            print(e)
        return flag

    def get_trip_data(self, bs_object):
        data_url_list = []
        try:
            div_tag_main = bs_object.find("div", {"class": "fl-module fl-module-rich-text fl-node-57ffe67143549"})
            if div_tag_main:
                div_tag = div_tag_main.find("div", {"class": "fl-rich-text"})
                if div_tag:
                    ul_tag = div_tag.find("ul")
                    if ul_tag:
                        li_tag = ul_tag.findAll("li")
                        if li_tag:
                            for each_li in li_tag:
                                href_tag = each_li.find('a', href=True)
                                if href_tag:
                                    data_url_list.append(href_tag.get("href"))
        except Exception as e:
            print(e)
        return data_url_list

    def _put_hdfs(self, source_path):
        try:
            _, file_name = os.path.split(source_path)
            remote_path = os.path.join(self.hdfs_dir, file_name)
            hdfs.put(source_path, remote_path)
        except Exception as e:
            print(e)

    def store_csv_file_on_hdfs(self):
        file_to_store = None
        flag = False
        try:
            path = self.tmp_dir_absolute
            os.chdir(path)
            for file in os.listdir():
                if file.endswith("-q2.csv"):
                    file_to_store = path + "/" + file
            if file_to_store:
                self._put_hdfs(file_to_store)
                flag = True
        except Exception as e:
            print(e)
        return flag

    def process_all(self):
        try:
            page_text = self.request_url_content()
            if page_text:
                bs_object = BeautifulSoup(page_text, 'html.parser')
                if bs_object:
                    data_url_list = self.get_trip_data(bs_object)
                    if type(data_url_list) == list and data_url_list:
                        data_url = data_url_list[0]
                        if data_url:
                           # import pdb; pdb.set_trace()
                            print("Downloading data......")
                            if self.download_url(data_url):
                                print("Extracting data from .zip file.....")
                                if self.save_csv_file_from_zip_file():
                                    print("Storing .csv file on HDFS")
                                    if self.store_csv_file_on_hdfs():
                                        print("Stored file on HDFS")
        except Exception as e:
            print(e)

if __name__ == "__main__":
    obj = GetStoreData()
    obj.process_all()
