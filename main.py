"""
Файл с методом парсинга данных о солнечных индексах для модели атмосферы JB2006
"""
import requests
from bs4 import BeautifulSoup

from datetime import datetime

from typing import List

import os


def get_html(url: str) -> str:
    """
    Получает по url html-код страницы, затем переводит его в строку
    :param url: url страницы с файлами о солнечных индексах
    :return: строку "html-код страницы"
    """
    # получаем html-код страницы
    r = requests.get(url)
    return r.text


def get_all_links(html: str, url: str) -> None:
    """
    Получает ссылки на все нужны для солнечных индексах файлы с данного сайта, затем по полученным ссылкам записывает
    данных файлов с сайта в два файла - с индексами солнечной и геомагнитной активности соответственно в родительскую директорию
    :param html: html-строка страницы
    :param url: url-строка страницы
    :return: None
    """
    # создаем объект типа BeautifulSoup с парсером lxml и даём ему html-код страницы
    soup = BeautifulSoup(html, 'lxml')
    # отдельно храним url в виде ссылки
    link = url
    # создаем множество ссылок для нужных файлов
    file_links = set()

    # ищем файлы с помощью структуры html-кода:
    # 1) ищем table - "таблица", содержащая "тела" всех файлов
    # 2) по ней ищем td-коды файлов, где содержится прямой доступ к файлу
    tds = soup.find('table').find_all('td')

    # итерируемся по всем таким td, чтобы получить файл по прямому пути
    for td in tds:
        # ищем пометку a в html-коде файла (в table -> tr -> td -> ...)
        file_link = td.findNext('a')
        # проверяем, не None ли ссылка
        if file_link is not None:
            # в случае успеха берем href-код (по сути - прямо получаем файл с данными)
            result_link = file_link.get('href')
            # отбираем необходимые нам файлы - их два -> 'SOLFSMY.TXT' и SOLRESAP.TXT'
            if result_link == 'SOLFSMY.TXT' or result_link == 'SOLRESAP.TXT':
                # добавляем ссылки на них в наше множество
                file_links.add(result_link)

    # итерируемся по ссылкам
    for file_linkers in file_links:
        # по данной ссылке получаем содержимое файла
        get_file = requests.get(f"{link}{file_linkers}").content
        # записываем в файл с названием ссылки на файл его полученное содержимое
        with open(f"{file_linkers}", 'wb') as result_file:
            result_file.write(get_file)


def create_data_for_res_file(current_line_flux: List[str], current_line_magnitude: List[str]) -> str:
    """
    Данная функция по данным за год делает строку, содержащую данные о всех индексах солнечной и геомагнитной активности в порядке:

        MJD --- AP1 --- AP2 --- AP3 --- AP4 --- AP5 --- AP6 --- AP7 --- AP8 --- F10 --- F81 --- S10 --- S10B --- XM10 --- XM10B

    :param current_line_flux: конкретная строка в файле SOLFSMY.TXT (данные о солнечной активности)
    :param current_line_magnitude: конкретная строка в файле SOLRESAP.TXT (данные о геомагнитной активности)
    :return: строка со всеми нужными индексами и датой их получения
    """
    return str(float(current_line_flux[2]) - 2400000.5) + ',' + current_line_magnitude[3] + ',' + \
           current_line_magnitude[4] + ',' + \
           current_line_magnitude[5] + ',' + current_line_magnitude[6] + ',' + current_line_magnitude[7] + ',' + \
           current_line_magnitude[8] + ',' + current_line_magnitude[9] + ',' + current_line_magnitude[10] + ',' + \
           current_line_flux[3] + ',' + current_line_flux[4] + ',' + current_line_flux[5] + ',' + \
           current_line_flux[6] + ',' + current_line_flux[7] + ',' + current_line_flux[8] + '\n'


def make_csv_for_JB2006(start_date: datetime, end_date: datetime, csv_name='jachnia_si.csv') -> None:
    """

    :param start_date: начальная дата в формате datetime, с которой начнётся извлечение информации о индексах
    :param end_date: конечная дата в формате datetime, до которой будет продолжаться извлечение информации о индексах
    :param csv_name: название файла (по умолчанию = 'jachnia_si.csv'), в который будет записана информация в виде таблицы
    :return: csv-файл
    """

    # открываем на чтение сразу два файла для выделения нужной информации с start_date по end_date
    try:
        with open("SOLFSMY.TXT", 'r', encoding='utf-8') as SOLFMY_file, open("SOLRESAP.TXT", 'r',
                                                                             encoding='utf-8') as SOLRESAP_file, open(
            csv_name, 'w'):

            # пропускаем первые 4 строки в обоих файлах - там просто информация о времени измерения индексов
            for i in range(4):
                SOLFMY_file.readline()
                SOLRESAP_file.readline()
            # пропускаем далее ещё 23 строки в файле с геомагнитными индексами для согласования времён с солнечными
            # (тут они приведены на год раньше, чтобы не возникло конфликтов с данными, сами убираем этот кусок)
            for i in range(23):
                SOLRESAP_file.readline()

            # запоминаем строку с SOLFMY_file
            current_line_flux = SOLFMY_file.readline()

            # выделяем дату - 1 января 1997 года - как год "рождения" обоих типов индексов
            index_birth_date = datetime(year=int(current_line_flux[2:6]), month=1, day=1)
            # выделяем дату - настоящая
            index_now_date = datetime.today()
            # проверяем введённую дату на принадлежность: [рождение; сейчас]
            # если не принадлежит - выбрасываем исключение
            if start_date < index_birth_date or end_date > index_now_date:
                raise Exception(
                    "Даты не попадают в допустимый диапазон от: " + str(index_birth_date) + " до: " + str(index_now_date))
            # если введённая начальная дата больше конечной, то выбрасываем исключение
            if start_date > end_date:
                raise Exception("Начальная дата больше конечной")

            # ищем количество дней между датой "рождения" и введённой начальной датой
            start_date_delta = (start_date - index_birth_date).days

            # превращаем строку в список
            current_line_flux = current_line_flux.split()
            current_line_magnitude = SOLRESAP_file.readline().split()

            # зная количество дней между рождением и начальной, двигаемся по списку вниз,
            # пока не сделаем нужного числа шагов - достигнем начала места извлечения информации
            for i in range(start_date_delta):
                current_line_flux = SOLFMY_file.readline().split()
                current_line_magnitude = SOLRESAP_file.readline().split()

            try:
                # открываем на запись наш файл
                with open("jachnia_lala.csv", 'w') as result_file:
                    # записываем головную строчку с названиями столбцов
                    result_file.write('mjd,ap1,ap2,ap3,ap4,ap5,ap6,ap7,ap8,F10,F81,S10,S10B,XM10,XM10B\n')
                    # записываем первую строку с данными
                    result_file.write(create_data_for_res_file(current_line_flux, current_line_magnitude))

                    # ищем число шагов до конца нашего введённого промежутка дат
                    end_date_delta = (end_date - start_date).days
                    # итерируемся и каждый раз новую строчку записываем в файл
                    for i in range(end_date_delta):
                        current_line_flux = SOLFMY_file.readline().split()
                        current_line_magnitude = SOLRESAP_file.readline().split()
                        result_file.write(create_data_for_res_file(current_line_flux, current_line_magnitude))
            except FileNotFoundError:
                print("Файла больше нет!")
    except FileNotFoundError:
        print("Хотя бы одного файла нет!")


def main() -> None:
    # наша url страницы
    url = 'https://sol.spacenvironment.net/jb2008/indices/'
    get_all_links(get_html(url), url)
    # промежуток нужных дат
    start_date = datetime(year=1997, month=1, day=1)
    end_date = datetime(year=2022, month=4, day=30)
    # делаем наш файл
    make_csv_for_JB2006(start_date, end_date, 'jachnia_lala.csv')

    # получаем путь к записанным файлам с солнечными и геомагнитными индексами
    path_MY = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SOLFSMY.TXT')
    path_AP = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SOLRESAP.TXT')
    # удаляем их из родительской директории
    os.remove(path_MY)
    os.remove(path_AP)


if __name__ == '__main__':
    main()
