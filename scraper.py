import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import re

beg = 1
end = 371


def url(beginning: int):
    return "http://www.ztm.waw.pl/zmiany.php?c=102&a=1&p={}&k=0&l=1".format(beginning)

def info_url(idx: int):
    return "http://www.ztm.waw.pl/zmiany.php?c=102&i={}".format(idx)

class NoImageException(Exception):
    """Happens whenever there is no image to extract information about type"""
    pass


def split_period(period: str) -> tuple:
    from_ = None
    to_ = None
    if "od" in period:
        if "do" in period:
            idx = period.index("do")
            period = period[:idx] + " " + period[idx:]
            m = re.findall(r'([0-9]+\.[0-9]+\.[0-9]+)', period)
            from_, to_ = m
        else:
            from_ = period[3:]
            to_ = None
    elif "dn." in period:
        from_ = period[4:]
        to_ = from_
    if from_ is not None:
        from_ = datetime.strptime(from_, '%d.%m.%Y')
    if to_ is not None:
        to_ = datetime.strptime(to_, '%d.%m.%Y')
    return period, from_, to_


def get_data_from_sample(sample) -> tuple:
    id = sample.get("id")
    int_id = re.findall("komunikat_(\d+)_", id)
    if int_id:
        id = int_id[0]
    data = [i for i in sample]
    try:
        type = data[0].next["alt"]
    except KeyError:
        raise NoImageException("There is no image")
    period = data[1].text
    period, from_, to_ = split_period(period)

    description = data[2].text
    return id, type, period, from_, to_, description, info_url(id)


def _scrape(beg, end):
    columns = ["id", "type", "period", "from", "to", "description", "url"]

    df = pd.DataFrame(columns=columns)
    df_idx = 0

    for page in tqdm(range(beg, end)):
        result = requests.get(url(page))
        if result.status_code != 200:
            break
        c = result.content
        soup = BeautifulSoup(c, "lxml")
        samples = soup.find_all("tr")

        for sample in samples:
            try:
                entry = get_data_from_sample(sample)
            except NoImageException:
                continue
            except Exception as e:
                print("Problem with page:{},error - {}".format(page, e))
            df.loc[df_idx] = entry
            df_idx += 1
    return df


def get_scraped_df(beg, end):
    df = _scrape(beg, end)
    return df


def get_scraped_csv(name, beg, end):
    df = _scrape(beg, end)
    df.to_csv(name)


def main():
    namefile = "data.csv"
    get_scraped_csv("data/{}".format(namefile), beg, end)


if __name__ == '__main__':
    main()
