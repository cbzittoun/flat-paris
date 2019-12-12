import requests
from bs4 import BeautifulSoup
import urllib.parse
from selenium import webdriver
import json
import textwrap
import pandas as pd
import re
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from ftplib import FTP



pd.set_option('display.max_rows', 500)

cfg = dict(
    projects='1',
    types='1,2',
    places='[{cp:75}]',
    price='NaN/3200',
    surface='60/NaN',
    sort='d_dt_crea',
    enterprise='0',
    qsVersion='1.0',
)


def url_search(cfg, page):
    return 'https://www.seloger.com/list.htm?' + urllib.parse.urlencode({**cfg, 'LISTING-LISTpg': page})


def get_soup(driver, url):
    driver.get(url)
    page = driver.page_source
    return BeautifulSoup(page, 'html.parser')


root = "/Users/cam/PycharmProjects/flat_paris"
filename_property_ = root + '/property_.h5'
# property_=pd.DataFrame()


def scrap():
    now = pd.Timestamp.now()
    with webdriver.Chrome(root + '/chromedriver') as driver:
        page = 0
        updated_all = False
        while not updated_all:
            page += 1
            print(f"page {page}")
            print(f"======")
            soup = get_soup(driver, url_search(cfg, page))

            url_property_ = soup.find_all('a', attrs={'name': 'classified-link'})
            url_property_ = [x['href'].split('?')[0] for x in url_property_]
            if not url_property_:
                break

            try:
                property_ = pd.read_hdf(filename_property_)
            except FileNotFoundError:
                property_ = pd.DataFrame()
            updated_all = True
            for url in url_property_:
                if 'bellesdemeures.com' in url:
                    print(f"skip bellesdemeures.com")
                    continue
                property_id = int(re.findall("(\d+)\.htm", url)[0])
                print(f"{property_id}", end=" ")
                already_in = (property_id == property_.property_id).any()
                if not property_.empty and already_in:
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
                    price_old = property_.loc[lambda x: x.property_id == property_id, 'prix'][0]
                    price_new = info['prix']
                    if price_new != price_old:
                        print(f"price changed ({price_old} -> {price_new}E), update")
                        property_.loc[lambda x: x.property_id == property_id] = row
                    else:
                        print(f"price unchanged, skip")
                else:
                    print(f"append")
                    property_ = pd.concat([property_, row], axis=0, sort=True)
            os.remove(filename_property_)
            property_.to_hdf(filename_property_, 'df')


def generate_html():
    stamp = f'{pd.Timestamp.now():%Y%m%d.%H%M%S}'
    property_ = pd.read_hdf(filename_property_)
    property_ = property_.sort_values(['captured', 'property_id'], ascending=False)
    html_ = []
    for id_r, r in property_.iterrows():
        if r.captured < property_.captured.max():
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
    filename = stamp + '.htm'
    with open(root + "/docs/" + filename, 'w') as f:
        f.write(html)
    return filename


def send_email(filename):
    to = ['zittounescu@gmail.com']
    print("send email")
    msg = MIMEMultipart()
    msg['From'] = 'zittounescu@gmail.com'
    msg['To'] = ', '.join(to)
    msg['Subject'] = f"flat-hunt: new batch available ({filename})"
    # attachment = MIMEApplication(open(filename, "rb").read())
    # attachment.add_header('Content-Disposition', 'attachment', filename=filename)
    # msg.attach(attachment)
    body = MIMEText(f"new batch available <a href='https://cbzittoun.github.io/flat-paris/{filename}'>here</a><br>history is <a href='https://cbzittoun.github.io/flat-paris'>here</a>", "html")
    msg.attach(body)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    with open(root + '/email.txt') as f:
        pwd = f.read().strip('\n')
    server.login("zittounescu@gmail.com", pwd)
    server.sendmail('zittounescu@gmail.com', to, msg.as_string())
    server.quit()


def git(filename_html):
    print("push on github")
    with open("docs/index.md", 'w') as file:
        file.write("\n".join([f"* [{f}](https://cbzittoun.github.io/flat-paris/{f})" for f in sorted(os.listdir("docs"), reverse=True)][1:-2]))

    os.system(textwrap.dedent(f"""
        cd {root}
        git add docs/*
        git commit -m "{filename_html}"
        git push -u origin master
    """).strip("\n "))


if __name__ == '__main__':
    scrap()
    filename_html = generate_html()
    send_email(filename_html)
    git(filename_html)

