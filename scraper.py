import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import re

beg = 1
end = 2


def url(beginning: int):
    return "http://www.ztm.waw.pl/zmiany.php?c=102&a=1&p={}&k=0&l=1".format(beginning)


def split_period(period: str):
    from_ = ""
    to_ = ""
    if "do" in period and "od" in period:
        idx = period.index("do")
        period = period[:idx] + " " + period[idx:]
        m = re.findall(r'([0-9]+\.[0-9]+\.[0-9]+)', period)
        print(m)
        from_, to_ = m

    if "dn." in period:
        from_, to_ = period[3:]
        pass
    # TODO case only "do"
    return from_, to_


def get_data_from_sample(sample) -> tuple:
    id = sample.get("id")

    data = [i for i in sample]
    type = data[0].next["alt"]
    period = data[1].text
    from_, to_ = split_period(period)

    description = data[2].text
    return id, type, from_, to_, description


def scrape():
    columns = ["id", "type", "from", "to" "description"]

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
            entry = get_data_from_sample(sample)
            print(entry)
            df.loc[df_idx] = entry
            df_idx += 1
    print("done")


def main():
    scrape()


if __name__ == '__main__':
    main()
