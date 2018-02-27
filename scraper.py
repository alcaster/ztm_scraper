import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import re

beg = 1
end = 372


def url(beginning: int):
    return "http://www.ztm.waw.pl/zmiany.php?c=102&a=1&p={}&k=0&l=1".format(beginning)


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
    data = [i for i in sample]
    type = data[0].next["alt"]
    period = data[1].text
    period, from_, to_ = split_period(period)

    description = data[2].text
    return id, type, period, from_, to_, description


def _scrape(beg, end):
    columns = ["id", "type", "period", "from", "to", "description"]

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
            print(sample)
            try:
                entry = get_data_from_sample(sample)
            except Exception as e:
                print("Exception ! - {}".format(e))
                continue
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
    get_scraped_csv("data.csv", beg, end)


if __name__ == '__main__':
    main()
