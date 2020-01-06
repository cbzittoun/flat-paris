from bs4 import BeautifulSoup
import urllib.parse
from selenium import webdriver
import textwrap
from notify_run import Notify
import numpy as np
import pandas as pd
import re
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
import platform
import requests
import yaml
import json


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

now = pd.Timestamp.now().replace(microsecond=0)
now_str = f'{now:%Y%m%d.%H%M%S}'

with open(fullpath_cfg) as f:
    cfg = yaml.safe_load(f)
git_pages = cfg['git']['pages']


def get_soup(driver, url):
    driver.get(url)
    page = driver.page_source
    return BeautifulSoup(page, 'html.parser')


def _gdist(latitude, longitude):
    cfg_gdist = cfg['gdist']
    origins = f'{latitude},{longitude}'
    destination_ = cfg_gdist['destination_']
    destinations = '|'.join(v[1] for v in destination_.values())
    key = cfg_gdist['api_key']
    url = f'https://maps.googleapis.com/maps/api/distancematrix/json?mode=transit&departure_time=1579852800&units=metric&origins={origins}&destinations={destinations}&key={key}'
    response = requests.get(url)
    durations = [dest.get('duration', dict(value=np.nan))['value'] / 60 for dest in response.json()['rows'][0]['elements']]
    return dict(zip(destination_.keys(), durations))


def url_search_seloger(page):
    return 'https://www.seloger.com/list.htm?' + urllib.parse.urlencode({**cfg['search']['seloger'], 'LISTING-LISTpg': page})


def _parse_search_seloger(p):
    url = p.select('a[name=classified-link]')[0].attrs['href'].split('?')[0]
    property_id = int(re.findall("/(\d+)[\./]", url)[0])
    prix_new = p.select('div[class*=Price__Label]')[0].text.split(' ')[0].replace(' ', '')
    return dict(property_id=property_id, url=url, prix_new=prix_new)


def _parse_seloger(soup):
    info = dict(re.findall("Object\.defineProperty\( ConfigDetail, '(\w+)', {\n          value: [\"'](.*)[\"']", soup.text))
    url_photo_ = soup.select('div[class*="carrousel_slide"]')
    url_photo_ = [re.findall('(//[^"]+\.jpg)', str(x)) for x in url_photo_]
    url_photo_ = ['http:' + x[0] for x in url_photo_ if x]
    url_photo_ = list(set(url_photo_))
    info['url_photo_'] = url_photo_
    info['captured'] = now
    date_available_ = re.findall("Disponibilité : (\d{2}\/\d{2}\/\d{4})", soup.text)
    if date_available_:
        info['available'] = pd.to_datetime(date_available_[0], format="%d/%m/%Y")
    info['lift'] = bool(re.search('Ascenseur', soup.text))
    info['plus'] = {p: bool(re.search(p, soup.text)) for p in ['Balcon', 'Terrasse', 'Meublé']}
    re_orientation = re.search("Orientation (.+)\n", soup.text)
    if re_orientation:
        info['orientation'] = re_orientation.group(1)

    if info['mapCoordonneesLatitude'] != '':
        info['gdist'] = _gdist(info['mapCoordonneesLatitude'], info['mapCoordonneesLongitude'])

    return info


def _parse_bellesdemeures(soup):
    o = dict()
    o['captured'] = now
    o['urlAnnonce'] = soup.select('link[rel="alternate"][hreflang="fr"]')[0].attrs['data-url']
    null, null, price = soup.select("head > title")[0].text.split(', ')
    o['prix'] = price.replace(' euro', '')
    nb_room, nb_bedroom, surface = [x.text.strip('\n •') for x in soup.select('div[class="annonceSpecs"] > ul > li')]
    o['surface'] = surface
    o['surfaceT'] = surface.replace(' M²', '')
    o['nbPieces'] = nb_room.split(' ')[0]
    o['nbChambres'] = nb_bedroom.split(' ')[0]
    o['ville'] = soup.select('span[class="js_locality"]')[0].text
    o['nomQuartier'] = ''
    plus = soup.select('ul[class="detailInfosList3Cols"]')[0].text
    floor = re.search('étage (\d)+', plus)
    if floor:
        o['etage'] = floor[1]
    desc = soup.select('ul[class="detailInfosList3Cols"]')[1].text
    o['lift'] = bool(re.search('Ascenseur', desc))
    o['plus'] = {p: bool(re.search(p, desc)) for p in ['Balcon', 'Terrasse', 'Meublé']}
    o['url_photo_'] = [x.attrs.get('data-src', '') for x in soup.select('ul[class="carouselList"] > li')[:-1]]
    map = soup.select('div[id="detailMap"]')[0].attrs
    o['mapCoordonneesLatitude'], o['mapCoordonneesLongitude'] = map['data-lat'], map['data-lng']
    if o['mapCoordonneesLatitude'] != '':
        o['gdist'] = _gdist(o['mapCoordonneesLatitude'], o['mapCoordonneesLongitude'])
    return o


def _scrap_seloger():
    print("scrap seloger.com, bellesdemeures.com")
    try:
        db = pd.read_hdf(fullpath_db)
    except FileNotFoundError:
        db = pd.DataFrame()

    with webdriver.Chrome(fullpath_chromedriver) as driver:
        page = 0
        while True:
            page += 1
            soup = get_soup(driver, url_search_seloger(page))

            property_ = soup.select('div[class*=Card__ContentZone]')
            if not property_:
                break
            property_ = pd.DataFrame([_parse_search_seloger(p) for p in property_])
            l_parse = property_.merge(db, how='left', on='property_id').pipe(lambda d: d.prix_new != d.prix)

            print(f"page {page}")
            for i_r, r in property_.loc[l_parse].iterrows():
                print(f"{r.property_id}", end=" ")

                already_in = (r.property_id == db.property_id).any()
                if not db.empty and already_in:
                    print(f"exists", end=" => ")
                else:
                    print(f"new   ", end=" => ")

                soup = get_soup(driver, r.url)

                if 'bellesdemeures.com' in r.url:
                    print('bellesdemeures.com')
                    info = _parse_bellesdemeures(soup)
                else:
                    info = _parse_seloger(soup)
                info['property_id'] = r.property_id

                row = pd.Series(info).to_frame().T

                if already_in:
                    price_old = db.loc[lambda x: x.property_id == r.property_id, 'prix'][0]
                    price_new = info['prix']
                    if price_new != price_old:
                        print(f"price move => update ({price_old} -> {price_new}E)")
                        updated_all = False

                        row['price_old'] = price_old
                        db.loc[db.property_id == r.property_id] = row
                    else:
                        print(f"price unch => skip")
                else:
                    print(f"append")
                    db = pd.concat([db, row], axis=0, sort=True)
    os.remove(fullpath_db)
    db.to_hdf(fullpath_db, 'df')


def url_search_pap(page):
    o = 'https://www.pap.fr/annonce/{projects}-{type}-{places}-g439-a-partir-du-{mb_room}-pieces-jusqu-a-{price}-euros-a-partir-de-{surface}-m2'
    o = o.format(**cfg['search']['pap'])
    if page > 1:
        o += f'-{page}'
    return o


def _parse_pap(soup):
    o = dict()
    o['captured'] = now
    o['urlAnnonce'] = soup.select('meta[property="og:url"]')[0].attrs['content']
    o['prix'] = soup.select('span[class="item-price"]')[0].text[:-2].replace('.', '')
    title = soup.select('title')[0].text
    location, surface, null, null = title.split(' - ')
    o['surface'] = surface
    o['surfaceT'] = surface[:-3]
    o['ville'] = location.split('m² ')[1].split(' (')[0]
    o['nomQuartier'] = ''
    desc = soup.select('div[class*="item-description"] > div > p')[0].text
    o['plus'] = {
        'Meublé': 'meublée' in location,
        'Balcon': bool(re.search('[bB]alcon', desc)),
        'Terrasse': bool(re.search('[tT]errasse', desc)),
    }
    o['url_photo_'] = [img.attrs['src'] for img in soup.select('div[class*="owl-item"] > div > a > img')]
    o['mapCoordonneesLatitude'], o['mapCoordonneesLongitude'] = json.loads(soup.select('div[id="carte_mappy"]')[0].attrs['data-mappy'])['center']
    if o['mapCoordonneesLatitude'] != '':
        o['gdist'] = _gdist(o['mapCoordonneesLatitude'], o['mapCoordonneesLongitude'])
    nb_room, nb_bedroom, null = soup.select('ul[class*="item-tags"] > li > strong')
    o['nbPieces'] = nb_room.text.split(' ')[0]
    o['nbChambres'] = nb_bedroom.text.split(' ')[0]
    return o
    # TODO: [ ] etage
    # TODO: [ ] lift
    # TODO: [ ] available


def _scrap_pap():
    print("scrap pap.fr")
    try:
        db = pd.read_hdf(fullpath_db)
    except FileNotFoundError:
        db = pd.DataFrame()

    with webdriver.Chrome(fullpath_chromedriver) as driver:
        page = 0
        updated_all = False
        while not updated_all:
            page += 1
            soup = get_soup(driver, url_search_pap(page))
            url_search_introspect = soup.select('meta[property="og:url"]')[0].attrs['content']
            if (page > 1) and not url_search_introspect.endswith(f'-{page}'):
                break

            url_property_ = soup.select('div[class="search-list-item"] > div[class="col-left"] > a')
            url_property_ = ['https://www.pap.fr' + x.attrs['href'] for x in url_property_]
            if not url_property_:
                break

            print(f"page {page}")

            updated_all = True
            for url in url_property_:
                property_id = int(re.search("-r(\d+)", url)[1]) * 1000
                print(f"{property_id}", end=" ")

                already_in = (property_id == db.property_id).any()
                if not db.empty and already_in:
                    print(f"exists", end=" => ")
                else:
                    print(f"new   ", end=" => ")
                    updated_all = False

                soup = get_soup(driver, url)
                info = _parse_pap(soup)
                info['property_id'] = property_id

                row = pd.Series(info).to_frame().T

                if already_in:
                    price_old = db.loc[lambda x: x.property_id == property_id, 'prix'][0]
                    price_new = info['prix']
                    if price_new != price_old:
                        print(f"price move => update ({price_old} -> {price_new}E)")
                        updated_all = False

                        row['price_old'] = price_old
                        db.loc[lambda x: x.property_id == property_id] = row
                    else:
                        print(f"price unch => skip")
                else:
                    print(f"append")
                    db = pd.concat([db, row], axis=0, sort=True)
    os.remove(fullpath_db)
    db.to_hdf(fullpath_db, 'df')


def _html():
    print("generate html")
    db = pd.read_hdf(fullpath_db)
    db = db.sort_values(['captured', 'property_id'], ascending=False)

    cfg_dest = cfg['gdist']['destination_']
    gdist_w = pd.Series({k: v[0] for k, v in cfg_dest.items()})
    gdist_url_ = {k: f'https://www.google.co.uk/maps/dir/{{}}/{v[1]}/data=!4m2!4m1!3e3' for k, v in cfg_dest.items()}

    html_ = []
    for id_r, r in db.loc[db.captured == now].iterrows():
        price_str = f"{r.prix}E"
        price_sqm_str = f"{float(r.prix.replace(',', '.')) / float(r.surfaceT.replace(',', '.')):.0f}E/m2"
        if not pd.isna(r.price_old):
            price_str = f"{r.price_old}->{price_str}"
            price_sqm_str = f"{float(r.price_old.replace(',', '.')) / float(r.surfaceT.replace(',', '.')):.0f}->{price_sqm_str}"
        k_desc_ = [
            f"<a href='{r.urlAnnonce}' target='_blank'>link</a>",
            price_str,
            r.surface,
            f"{r.nbPieces}p",
            f"{r.nbChambres}ch",
            f"{r.etage}e" + (not r.lift) * "!",
            price_sqm_str,
        ]
        if not pd.isnull(r.available):
            k_desc_.append(f"dispo:{r.available:%d%b}")
        if not pd.isnull(r.plus):
            for k, p in r.plus.items():
                if p:
                    k_desc_.append(f"+{k}")
        if not pd.isnull(r.orientation):
            k_desc_.append(r.orientation)

        if not pd.isnull(r.gdist):
            coord_str = f'{r.mapCoordonneesLatitude},{r.mapCoordonneesLongitude}'
            time_str = ', '.join([f"{k}=<a href='{gdist_url_[k].format(coord_str)}' target='_blank'>{v:.0f}</a>" for k, v in r.gdist.items()])
            gtime_s = pd.Series(r.gdist)
            gtime = (gtime_s * gdist_w).sum() / gdist_w.mask(gtime_s.isna()).sum()
            k_desc_.append(f"gtime:{gtime:.0f}min ({time_str})")

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
    print("send email")
    address = cfg['email']['address']
    to = [address]
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
    print("push to github")

    with open(root / 'docs' / 'index.md', 'w') as file:
        file.write("\n".join([f"* [{f}]({git_pages}/{f})" for f in sorted(os.listdir(root / 'docs'), reverse=True)][2:-2]))

    os.system(textwrap.dedent(f"""
        cd {root}
        git add db.h5
        git add docs/*
        git commit -m "{now_str}"
        git push -u origin master
    """).strip("\n "))


def _notify(filename):
    print("notify")
    notify = Notify()
    notify.endpoint = cfg['notify']['endpoint']
    notify.send(f'flat-hunt: new batch available ({filename})', f'{git_pages}/{filename}')


if __name__ == '__main__':
    _scrap_seloger()
    _scrap_pap()
    filename_html = _html()
    _git()
    _email(filename_html)
    # _notify(filename_html)
