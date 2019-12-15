from bs4 import BeautifulSoup
import urllib.parse
from selenium import webdriver
import textwrap
from notify_run import Notify
import pandas as pd
import re
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import platform
import yaml


platform_sys = platform.system().lower()

if platform_sys == 'linux':
    from pyvirtualdisplay import Display
    display = Display(visible=0, size=(1600, 1200))
    display.start()

    root = Path('/home/pi/repo/flat-paris')
else:
    root = Path('/Users/cam/PycharmProjects/flat_paris')

fullpath_cfg = root / 'cfg.yml'
fullpath_db = root / 'db.h5'
fullpath_chromedriver = root / 'webdriver' / platform_sys / 'chromedriver'

now = pd.Timestamp.now()
now_str = f'{now:%Y%m%d.%H%M%S}'

with open(fullpath_cfg) as f:
    cfg = yaml.safe_load(f)
git_pages = cfg['git']['pages']


def url_search(page):
    return 'https://www.seloger.com/list.htm?' + urllib.parse.urlencode({**cfg['search'], 'LISTING-LISTpg': page})


def get_soup(driver, url):
    driver.get(url)
    page = driver.page_source
    return BeautifulSoup(page, 'html.parser')


def _scrap():
    with webdriver.Chrome(fullpath_chromedriver) as driver:
        page = 0
        updated_all = False
        while not updated_all:
            page += 1
            print(f"page {page}")
            print(f"======")
            soup = get_soup(driver, url_search(page))

            url_property_ = soup.find_all('a', attrs={'name': 'classified-link'})
            url_property_ = [x['href'].split('?')[0] for x in url_property_]
            if not url_property_:
                break

            try:
                db = pd.read_hdf(fullpath_db)
            except FileNotFoundError:
                db = pd.DataFrame()
            updated_all = True
            for url in url_property_:
                if 'bellesdemeures.com' in url:
                    print(f"skip bellesdemeures.com")
                    continue
                property_id = int(re.findall("(\d+)\.htm", url)[0])
                print(f"{property_id}", end=" ")
                already_in = (property_id == db.property_id).any()
                if not db.empty and already_in:
                    print(f"exists", end=" >> ")
                else:
                    print(f"new   ", end=" >> ")
                    updated_all = False
                soup = get_soup(driver, url)
                info = dict(re.findall("Object\.defineProperty\( ConfigDetail, '(\w+)', {\n          value: [\"'](.*)[\"']", soup.text))
                url_photo_ = soup.select('div[class*="carrousel_slide"]')
                url_photo_ = [re.findall('(//[^"]+\.jpg)', str(x)) for x in url_photo_]
                url_photo_ = ['http:' + x[0] for x in url_photo_ if x]
                url_photo_ = list(set(url_photo_))
                info['url_photo_'] = url_photo_
                info['property_id'] = int(info['idAnnonce'])
                info['captured'] = now
                date_available_ = re.findall("Disponibilité : (\d{2}\/\d{2}\/\d{4})", soup.text)
                if date_available_:
                    info['available'] = pd.to_datetime(date_available_[0], format="%d/%m/%Y")
                info['lift'] = bool(re.search('Ascenseur', soup.text))
                info['plus'] = {p: bool(re.search(p, soup.text)) for p in ['Balcon', 'Terrasse', 'Meublé']}
                re_orientation = re.search("Orientation (.+)\n", soup.text)
                if re_orientation:
                    info['orientation'] = re_orientation.group(1)
                row = pd.Series(info).to_frame().T
                if already_in:
                    price_old = db.loc[lambda x: x.property_id == property_id, 'prix'][0]
                    price_new = info['prix']
                    if price_new != price_old:
                        print(f"price changed ({price_old} -> {price_new}E), update")
                        db.loc[lambda x: x.property_id == property_id] = row
                    else:
                        print(f"price unchanged, skip")
                else:
                    print(f"append")
                    db = pd.concat([db, row], axis=0, sort=True)
            os.remove(fullpath_db)
            db.to_hdf(fullpath_db, 'df')


def _html():
    db = pd.read_hdf(fullpath_db)
    db = db.sort_values(['captured', 'property_id'], ascending=False)
    html_ = []
    for id_r, r in db.iterrows():
        if r.captured < db.captured.max():
            break
        k_desc_ = [
            f"<a href='{r.urlAnnonce}' target='_blank'>link</a>",
            f"{r.prix}E",
            r.surface,
            f"{r.nbPieces}p",
            f"{r.nbChambres}ch",
            f"{r.etage}e" + (not r.lift) * "!",
            f"{float(r.prix.replace(',', '.')) / float(r.surfaceT.replace(',', '.')):.0f}E/m2",
        ]
        if not pd.isnull(r.available):
            k_desc_.append(f"dispo:{r.available:%d%b}")
        if not pd.isnull(r.plus):
            for k, p in r.plus.items():
                if p:
                    k_desc_.append(f"+{k}")
        if not pd.isnull(r.orientation):
            k_desc_.append(r.orientation)
        html_k_desc = ("&thinsp;" * 5).join(k_desc_)
        html_k = f"<h2>{r.ville} - {r.nomQuartier}<br>{html_k_desc}</h2>"
        html_photo_ = []
        for u in r.url_photo_:
            html_photo_.append(f"<img src='{u}' height='300' onmousedown='this.height=\"600\"' onmouseup='this.height=\"300\"' />")
        html_k += "<br>" + ''.join(html_photo_)
        html_.append(html_k)
    html = '<br><br>'.join(html_)
    filename = now_str + '.htm'
    with open(root / 'docs' / filename, 'w') as f:
        f.write(html)
    return filename


def _email(filename):
    address = cfg['email']['address']
    to = [address]
    print("send email")
    msg = MIMEMultipart()
    msg['From'] = address
    msg['To'] = ', '.join(to)
    msg['Subject'] = f"flat-hunt: {filename}"
    body = MIMEText(f"new batch available <a href='{git_pages}/{filename}'>here</a><br>history is <a href='{git_pages}'>here</a>", "html")
    msg.attach(body)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(address, cfg['email']['password'])
    server.sendmail(address, to, msg.as_string())
    server.quit()


def _git():
    print("push on github")

    with open(root / 'docs' / 'index.md', 'w') as file:
        file.write("\n".join([f"* [{f}]({git_pages}/{f})" for f in sorted(os.listdir(root / 'docs'), reverse=True)][1:-2]))

    os.system(textwrap.dedent(f"""
        cd {root}
        git add db.h5
        git add docs/*
        git commit -m "{now_str}"
        git push -u origin master
    """).strip("\n "))


def _notify(filename):
    notify = Notify()
    notify.endpoint = cfg['notify']['endpoint']
    notify.send(f'flat-hunt: new batch available ({filename})', f'{git_pages}/{filename}')


if __name__ == '__main__':
    _scrap()
    filename_html = _html()
    _git()
    _email(filename_html)
    _notify(filename_html)
