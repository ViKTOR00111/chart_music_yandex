import requests
from pprint import pprint
from urllib.parse import unquote
from datetime import datetime
import time
import json
import csv
import sqlite3

cookies = {
    'yandexuid': '6346385001661189492',
    'gdpr': '0',
    '_ym_uid': '16611894981049166520',
    'yuidss': '6346385001661189492',
    'ymex': '1997982015.yrts.1682622015#1982087784.yrtsi.1666727784',
    'font_loaded': 'YSv1',
    'chromecast': "''",
    'amcuid': '1716203051703860699',
    'fullscreen-saved-data': '%7B%22web_esse_music-in-movies-1%22%3A%7B%22compositeId%22%3A%22web_esse_music-in-movies-1%22%2C%22actualUntil%22%3A1734444524375%7D%2C%22S7_option_playlist_web-2%22%3A%7B%22compositeId%22%3A%22S7_option_playlist_web-2%22%2C%22actualUntil%22%3A1735559492883%7D%7D',
    'L': 'Wjt0Z1kDfV5ZZENpYnsHd019fgEGAl1FARkpEQsKVWtVAX1Z.1703937362.15572.313138.b250a3e16dc4421cf6b8ef8c5e99de8c',
    'yandex_login': 'Viktor010107',
    'yandex_gid': '13',
    'yashr': '2946459771705658099',
    'bltsr': '1',
    'KIykI': '1',
    'i': '8LHEXM6L9vM1jHjhFFX/HueSBklA5/7eulJ/1h8JflhK47WDCn5dO1hHRDuEgqHt9wDlhs9+mgBQ6MM3Ceb+So67w4A=',
    'Session_id': '3:1706721135.5.0.1684519609394:5KGMuQ:1.1.2:1|1250832972.0.2.3:1684519609|1902342808.-1.0.2:19417364.3:1703936973|3:10282439.898716.CYjbpTz4brq5HEpqt9dud1kLBvg',
    'sessar': '1.1186.CiBweDfQ8xP9Jw599HDJfIYHPCj21l2vYlua_9fdLGAYzw.eLFymZXw9CAE_jShwSKuN-d4m-ybWKny9CC-YMCK0Ss',
    'sessionid2': '3:1706721135.5.0.1684519609394:5KGMuQ:1.1.2:1|1250832972.0.2.3:1684519609|1902342808.-1.0.2:19417364.3:1703936973|3:10282439.898716.fakesign0000000000000000000',
    'my': 'YycCAAEA',
    'device_id': 'ae6d42961e672760ec441baadf0f54fa88382c4f4',
    '_ym_d': '1706909286',
    'geobase-region': '%7B%22id%22%3A13%7D',
    'redirected_from_touch': 'true',
    '_ym_isad': '1',
    'lastVisitedPage': '%7B%221902342808%22%3A%22%2Fusers%2Fnphne-lytc32u7%2Fplaylists%2F3%22%7D',
    'bh': 'EkIiTm90IEEoQnJhbmQiO3Y9Ijk5IiwgIk1pY3Jvc29mdCBFZGdlIjt2PSIxMjEiLCAiQ2hyb21pdW0iO3Y9IjEyMSIaBSJ4ODYiIg8iMTIxLjAuMjI3Ny44MyIqAj8xMgkiTmV4dXMgNSI6CSJBbmRyb2lkIkIIIjE0LjAuMCJKBCI2NCJSWyJOb3QgQShCcmFuZCI7dj0iOTkuMC4wLjAiLCJNaWNyb3NvZnQgRWRnZSI7dj0iMTIxLjAuMjI3Ny44MyIsIkNocm9taXVtIjt2PSIxMjEuMC42MTY3Ljg1IiI=',
    'ys': 'udn.cDpWaWt0b3IwMTAxMDc%3D#wprid.1706966357800452-16976313886015798484-balancer-l7leveler-kubr-yp-sas-62-BAL-6826#c_chck.1886505126',
    '_yasc': 'ytUfacnircW/Sy3ecBb9o+yO2b9PZTgFU5/KPIEjdM5J55xj8vZFJRU3LBPy3r6PLgd2BwhHcmsqzojGi8E+gjc=',
    'yp': '4294967295.skin.s#1707073530.csc.1#1727902904.p_undefined.1696366904#1707075981.ygu.1#1728042513.p_sw.1696506512#1713546256.v_smr_onb.t%3D8%3A1705770256436#1728042653.p_cl.1696506652#1722578223.szm.1_25%3A1536x864%3A1488x742#2019297362.udn.cDpWaWt0b3IwMTAxMDc%3D#2019296973.multib.1#2022326359.pcs.1#1707856201.hdrc.0#1721394407.stltp.serp_bk-map_1_1689858407#1714590382.v_sum_b_onb.2%3A1706814382337',
    'active-browser-timestamp': '1706966365911',
    'is_gdpr': '0',
    'is_gdpr_b': 'CJHuNBCA6QEoAg==',
    'bh': 'EkAiTm90IEEoQnJhbmQiO3Y9Ijk5IiwiTWljcm9zb2Z0IEVkZ2UiO3Y9IjEyMSIsIkNocm9taXVtIjt2PSIxMjEiGgUieDg2IiIPIjEyMS4wLjIyNzcuODMiKgI/MTIJIk5leHVzIDUiOgkiQW5kcm9pZCJCBSI2LjAiSgQiNjQiUlsiTm90IEEoQnJhbmQiO3Y9Ijk5LjAuMC4wIiwiTWljcm9zb2Z0IEVkZ2UiO3Y9IjEyMS4wLjIyNzcuODMiLCJDaHJvbWl1bSI7dj0iMTIxLjAuNjE2Ny44NSIi',
    '_ym_visorc': 'b',
}

headers = {
    'authority': 'music.yandex.ru',
    'accept': 'application/json, text/javascript, */*; q=0.01',
    'accept-language': 'ru,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
    'cookie': "yandexuid=6346385001661189492; gdpr=0; _ym_uid=16611894981049166520; yuidss=6346385001661189492; ymex=1997982015.yrts.1682622015#1982087784.yrtsi.1666727784; font_loaded=YSv1; chromecast=''; amcuid=1716203051703860699; fullscreen-saved-data=%7B%22web_esse_music-in-movies-1%22%3A%7B%22compositeId%22%3A%22web_esse_music-in-movies-1%22%2C%22actualUntil%22%3A1734444524375%7D%2C%22S7_option_playlist_web-2%22%3A%7B%22compositeId%22%3A%22S7_option_playlist_web-2%22%2C%22actualUntil%22%3A1735559492883%7D%7D; L=Wjt0Z1kDfV5ZZENpYnsHd019fgEGAl1FARkpEQsKVWtVAX1Z.1703937362.15572.313138.b250a3e16dc4421cf6b8ef8c5e99de8c; yandex_login=Viktor010107; yandex_gid=13; yashr=2946459771705658099; bltsr=1; KIykI=1; i=8LHEXM6L9vM1jHjhFFX/HueSBklA5/7eulJ/1h8JflhK47WDCn5dO1hHRDuEgqHt9wDlhs9+mgBQ6MM3Ceb+So67w4A=; Session_id=3:1706721135.5.0.1684519609394:5KGMuQ:1.1.2:1|1250832972.0.2.3:1684519609|1902342808.-1.0.2:19417364.3:1703936973|3:10282439.898716.CYjbpTz4brq5HEpqt9dud1kLBvg; sessar=1.1186.CiBweDfQ8xP9Jw599HDJfIYHPCj21l2vYlua_9fdLGAYzw.eLFymZXw9CAE_jShwSKuN-d4m-ybWKny9CC-YMCK0Ss; sessionid2=3:1706721135.5.0.1684519609394:5KGMuQ:1.1.2:1|1250832972.0.2.3:1684519609|1902342808.-1.0.2:19417364.3:1703936973|3:10282439.898716.fakesign0000000000000000000; my=YycCAAEA; device_id=ae6d42961e672760ec441baadf0f54fa88382c4f4; _ym_d=1706909286; geobase-region=%7B%22id%22%3A13%7D; redirected_from_touch=true; _ym_isad=1; lastVisitedPage=%7B%221902342808%22%3A%22%2Fusers%2Fnphne-lytc32u7%2Fplaylists%2F3%22%7D; bh=EkIiTm90IEEoQnJhbmQiO3Y9Ijk5IiwgIk1pY3Jvc29mdCBFZGdlIjt2PSIxMjEiLCAiQ2hyb21pdW0iO3Y9IjEyMSIaBSJ4ODYiIg8iMTIxLjAuMjI3Ny44MyIqAj8xMgkiTmV4dXMgNSI6CSJBbmRyb2lkIkIIIjE0LjAuMCJKBCI2NCJSWyJOb3QgQShCcmFuZCI7dj0iOTkuMC4wLjAiLCJNaWNyb3NvZnQgRWRnZSI7dj0iMTIxLjAuMjI3Ny44MyIsIkNocm9taXVtIjt2PSIxMjEuMC42MTY3Ljg1IiI=; ys=udn.cDpWaWt0b3IwMTAxMDc%3D#wprid.1706966357800452-16976313886015798484-balancer-l7leveler-kubr-yp-sas-62-BAL-6826#c_chck.1886505126; _yasc=ytUfacnircW/Sy3ecBb9o+yO2b9PZTgFU5/KPIEjdM5J55xj8vZFJRU3LBPy3r6PLgd2BwhHcmsqzojGi8E+gjc=; yp=4294967295.skin.s#1707073530.csc.1#1727902904.p_undefined.1696366904#1707075981.ygu.1#1728042513.p_sw.1696506512#1713546256.v_smr_onb.t%3D8%3A1705770256436#1728042653.p_cl.1696506652#1722578223.szm.1_25%3A1536x864%3A1488x742#2019297362.udn.cDpWaWt0b3IwMTAxMDc%3D#2019296973.multib.1#2022326359.pcs.1#1707856201.hdrc.0#1721394407.stltp.serp_bk-map_1_1689858407#1714590382.v_sum_b_onb.2%3A1706814382337; active-browser-timestamp=1706966365911; is_gdpr=0; is_gdpr_b=CJHuNBCA6QEoAg==; bh=EkAiTm90IEEoQnJhbmQiO3Y9Ijk5IiwiTWljcm9zb2Z0IEVkZ2UiO3Y9IjEyMSIsIkNocm9taXVtIjt2PSIxMjEiGgUieDg2IiIPIjEyMS4wLjIyNzcuODMiKgI/MTIJIk5leHVzIDUiOgkiQW5kcm9pZCJCBSI2LjAiSgQiNjQiUlsiTm90IEEoQnJhbmQiO3Y9Ijk5LjAuMC4wIiwiTWljcm9zb2Z0IEVkZ2UiO3Y9IjEyMS4wLjIyNzcuODMiLCJDaHJvbWl1bSI7dj0iMTIxLjAuNjE2Ny44NSIi; _ym_visorc=b",
    'referer': 'https://music.yandex.ru/chart',
    'sec-ch-ua': '"Not A(Brand";v="99", "Microsoft Edge";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36 Edg/121.0.0.0',
    'x-current-uid': '1250832972',
    'x-requested-with': 'XMLHttpRequest',
    'x-retpath-y': 'https://music.yandex.ru/chart',
    'x-yandex-music-client-now': '2024-02-03T16:19:48+03:00',
}

params = {
    'what': 'chart',
    'lang': 'ru',
    'external-domain': 'music.yandex.ru',
    'overembed': 'false',
    'ncrnd': '0.7461030578888799',
}


def get_json(link):
    response = requests.get(link, params=params, cookies=cookies, headers=headers)
    data = response.json()['chartPositions']
    return data


def get_data(data):
    data_dict = {'position': [], 'title': [], 'artists': [], 'listeners': [],
             'release': [], 'time': [], 'genre': [], 'Explicit': [], 'date': [], 'day_week': []}

    date = datetime.now().strftime("%Y-%m-%d")
    days_of_week = ["Monday", "Tuesday", "Wendesday", "Thursday", "Friday", "Saturday", "Sunday"]

    # Цикл по каждому элементу в data
    for item in data:
        Explicit = 0
        # Ищем содержит ли песня нецензурную лексику
        try:
            if item['track']['contentWarning']:
                Explicit = 1
        except KeyError:
            pass
        # Извлекаем информацию по каждой переменной и добавляем в словарь
        position = item['track']['chart']['position']
        data_dict['position'].append(position)
        title = item['track']['title']
        data_dict['title'].append(title)
        artists = [artist['name'] for artist in item['track']['artists']]
        data_dict['artists'].append(str(artists))
        listeners = item['chartPosition']['listeners']
        data_dict['listeners'].append(listeners)

        try:
            if item['track']['albums'][0]['releaseDate'][:10]:
                data_dict['release'].append(item['track']['albums'][0]['releaseDate'][:10])

        except KeyError:
            data_dict['release'].append('None')
            print(f'Отсутствует дата для позиции {position}')

        time = item['track']['durationMs'] / 1000
        data_dict['time'].append(time)
        genre = item['track']['albums'][0]['genre']
        data_dict['genre'].append(genre)
        data_dict['Explicit'].append(Explicit)
        data_dict['date'].append(date)
        data_dict['day_week'].append(days_of_week[datetime.now().weekday()])
    return data_dict


# Для записи в csv
# csv_file_path = f'C:\\Users\\Виктор\\OneDrive\\Рабочий стол\\databaseChart\\result-{date}.csv'
# with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
#     csv_writer = csv.writer(csv_file, delimiter=';')
#     csv_writer.writerow(data_dict.keys())
#     for row in zip(data_dict['position'], data_dict['title'], data_dict['artists'], data_dict['listeners'],
#                    data_dict['release'], data_dict['time'], data_dict['genre'], data_dict['Explicit'],
#                    data_dict['date'], data_dict['day_week']):
#         csv_writer.writerow(row)


# Для записи в базу данных
def write_to_database(data, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rows = zip(data['position'], data['title'], data['artists'], data['listeners'],
               data['release'], data['time'], data['genre'], data['Explicit'],
               data['date'], data['day_week'])

    cursor.executemany('''INSERT INTO source (position, title, artists, listeners, release, time, genre, Explicit, date, day_week) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', rows)

    conn.commit()
    conn.close()


def main():
    while True:
        response = get_json('https://music.yandex.ru/handlers/main.jsx')
        data = get_data(response)
        write_to_database(data, 'C:\\Users\\Виктор\\Downloads\\Skillbox_SQLite_Webinar-master\\Skillbox_SQLite_Webinar-master\\chinook\\chart_data.db')
        time.sleep(86400)


if __name__ == '__main__':
    main()