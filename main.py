"""
Файл с методом парсинга данных о солнечных индексах для модели атмосферы JB2006
"""
import requests

from datetime import datetime, timedelta
import astropy.time
import dateutil.parser

from typing import List

import os


def convert_calendar_to_mjd(current_line_flux: List[str]) -> str:
    """
    Данная функция переводит дату из формата календаря (см. далее) в mjd:

                            Y M D -> mjd,

    где Y - текущий год, M - текущий месяц года, а D - текущей день месяца этого года
    :param current_line_flux: список с текущей датой и значениями индексов на текущую дату
    :return: mjd-время для данной даты (в виде строки)
    """
    # берём год, месяц и день из начала списка
    calendar_str = current_line_flux[0] + '.' + current_line_flux[1] + '.' + current_line_flux[2]
    # преобразуем полученную строку в спец. объект для получения даты в jd1- и jd2-форматах, затем складываем:
    #                               mjd = time.jd1 + time.jd2 - 2400000.5
    dt = dateutil.parser.parse(calendar_str)
    time = astropy.time.Time(dt)
    return str(time.jd1 + time.jd2 - 2400000.5)


def convert_MG2_to_M10(mgii: float) -> str:
    """
    Данная функция конвертирует индекс MG2 в M10 по следующим формулам:

        M10 = -1943.85 + 7606.56 * MG2

    :param mgii: индекс MG2
    :return: индекс M10 (в виде строки)
    """
    M10 = -1943.85 + 7606.56 * mgii
    if M10 > 0:
        return str(round(M10, 1))
    else:
        return '0'


def make_S10B(array: List[List[str]]) -> str:
    """
    Данная функция вычисляет индекс S10B, зная все 81 прошлые индексы S10, по формуле:

                    S10B = (Σ S10(i) * W(i)) / (Σ W(i)),

    где i принимает значения от -80 до 0, а W(i) - весовой коэффициент для данного i, вычисляемый по формуле:

                    W(i) = 1 + ((0.5 * i) / 80)

    :param array: массив со всеми данными и датами
    :return: значение индекса S10B на последнюю дату (в виде строки)
    """
    # начальные значения верхний и нижней сумм
    sum_weights = 0
    sum = 0
    # пробегаемся не по [-80, 0], а по [-81, -1], чтобы воспользоваться взятием элементов с конца списка Python
    for i in range(-81, -1):
        # но тогда i в формуле выше надо сместить на 1
        weight = 1 + (0.5 * (i + 1)) / 80
        # обновляем суммы
        sum_weights += weight
        sum += weight * float(array[3][i])
    return str(round(sum / sum_weights, 1))


def make_XM10B(array: List[List[str]]) -> str:
    """
    Данная функция вычисляет индекс XM10B, зная все 81 прошлые индексы XM10, по формуле:

                    XM10B = (Σ XM10(i) * W(i)) / (Σ W(i)),

    где i принимает значения от -80 до 0, а W(i) - весовой коэффициент для данного i, вычисляемый по формуле:

                    W(i) = 1 + ((0.5 * i) / 80)

    :param array: массив со всеми данными и датами
    :return: значение индекса XM10B на последнюю дату (в виде строки)
    """
    # начальные значения верхний и нижней сумм
    sum_weights = 0
    sum = 0
    # пробегаемся не по [-80, 0], а по [-81, -1], чтобы воспользоваться взятием элементов с конца списка Python
    for i in range(-81, -1):
        # но тогда i в формуле выше надо сместить на 1
        weight = 1 + (0.5 * (i + 1)) / 80
        # обновляем суммы
        sum_weights += weight
        sum += weight * float(array[5][i])
    return str(round(sum / sum_weights, 1))


def convert_MG2_to_S10(mgii: float) -> str:
    """
    Данная функция конвертирует индекс MG2 в S10 по следующим формулам:

    1) Сначала - из MG2 в EUV-излучение:

            EUV = 110.324 * mgii - 28.065

    2) Затем - из EUV-излучения в S10:

            S10 = -12.01 + 141.23 * (EUV / 1.9955)

    :param mgii: индекс MG2
    :return: индекс S10 (в виде строки)
    """
    EUV = 110.324 * mgii - 28.065
    S10 = -12.01 + 141.23 * (EUV / 1.9955)
    if S10 > 0:
        return str(round(S10, 1))
    else:
        return '0'


def make_str_for_csv(array: List[List[str]], idx: int):
    """
    Данная функция по данным за год делает строку по данным из массива индексов и индексу на текущую дата,
    содержащую данные о всех индексах солнечной и геомагнитной активности в порядке:

        MJD --- AP1 --- AP2 --- AP3 --- AP4 --- AP5 --- AP6 --- AP7 --- AP8 --- F10 --- F81 --- S10 --- S10B --- XM10 --- XM10B

    :param array:
    :param idx:
    :return:
    """
    return array[0][idx] + ',' + ','.join(array[-1][idx]) + ',' + array[1][idx] + ',' + array[2][idx] + ',' + array[3][
        idx] + ',' + \
           array[4][idx] + ',' + array[5][idx] + ',' + array[6][idx]


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
           current_line_flux[6] + ',' + current_line_flux[7] + ',' + current_line_flux[8]


def make_indices_array_before_45_days(start_date: datetime, end_date: datetime) -> List[List[str]]:
    """
    Данная функция делает массив данных о всех индексах для диапазана дат:

                    [start_date; end_date - 45 days]

    :param: start_date: начальная дата
    :param: end_date: конечная дата
    :return: массив с данными о индексах за указанный диапазон дат
    """
    result_mjd = []
    result_F10 = []
    result_F10B = []
    result_S10 = []
    result_S10B = []
    result_XM10 = []
    result_XM10B = []
    result_AP = []
    try:
        # открываем на все файлы для выделения нужной информации с start_date по end_date - 45 days
        with open("SOLFSMY.TXT", 'r', encoding='utf-8') as SOLFSMY_file, open("SOLRESAP.TXT", 'r',
                                                                              encoding='utf-8') as SOLRESAP_file:
            # пропускаем первые 4 строки в обоих файлах - там просто информация о времени измерения индексов
            for i in range(4):
                SOLFSMY_file.readline()
                SOLRESAP_file.readline()
            # пропускаем далее ещё 23 строки в файле с геомагнитными индексами для согласования времён с солнечными
            # (тут они приведены на год раньше, чтобы не возникло конфликтов с данными, сами убираем этот кусок)
            for i in range(23):
                SOLRESAP_file.readline()

            # запоминаем строку с SOLFSMY_file
            current_line_flux = SOLFSMY_file.readline()

            # выделяем дату - 1 января 1997 года - как год "рождения" обоих типов индексов
            index_birth_date = datetime(year=int(current_line_flux[2:6]), month=1, day=1)
            # выделяем дату - настоящая
            index_now_date = datetime.today()
            # проверяем введённую дату на принадлежность: [рождение; сейчас]
            # если не принадлежит - выбрасываем исключение
            if start_date < index_birth_date or end_date > index_now_date - timedelta(days=1):
                raise Exception(
                    "Даты не попадают в допустимый диапазон от: " + str(index_birth_date) + " до: " + str(
                        index_now_date - timedelta(days=1)))
            # если введённая начальная дата больше конечной, то выбрасываем исключение
            if start_date > end_date:
                raise Exception("Начальная дата больше конечной")

            # ищем количество дней между датой "рождения" и введённой начальной датой
            start_date_delta = (start_date - index_birth_date).days

            # зная количество дней между рождением и начальной, двигаемся по списку вниз,
            # пока не сделаем нужного числа шагов - достигнем начала места извлечения информации
            for i in range(start_date_delta - 1):
                SOLFSMY_file.readline()
                SOLRESAP_file.readline()

            SOLRESAP_file.readline()

            # ищем число шагов до конца нашего введённого промежутка дат
            end_date_delta = (end_date - start_date).days
            # итерируемся и каждый раз записываем данные
            for i in range(0, end_date_delta):
                current_line_flux = SOLFSMY_file.readline().split()
                current_line_magnitude = SOLRESAP_file.readline().split()
                tmp_str = create_data_for_res_file(current_line_flux, current_line_magnitude).split(',')
                result_mjd.append(tmp_str[0])
                result_F10.append(tmp_str[9])
                result_F10B.append(tmp_str[10])
                result_S10.append(tmp_str[11])
                result_S10B.append(tmp_str[12])
                result_XM10.append(tmp_str[13])
                result_XM10B.append(tmp_str[14])
                result_AP.append(
                    [tmp_str[1], tmp_str[2], tmp_str[3], tmp_str[4], tmp_str[5], tmp_str[6], tmp_str[7], tmp_str[8]])
    except FileNotFoundError:
        print("Хотя бы одного файла нет!")
    # формируем выходной массив данных
    return [result_mjd, result_F10, result_F10B, result_S10, result_S10B, result_XM10, result_XM10B, result_AP]


def update_indices_array_after_45_days(array: List[List[str]], start_date: datetime, end_date: datetime) -> None:
    try:
        with open("CELESTRAK.TXT", 'r') as celestrak, open("MGII.TXT") as mgii:
            # пропускаем первые 17 строк в файле celestrak'а
            for _ in range(17):
                celestrak.readline()

            # начальные значения, чтобы двигаться по файлу дальше
            current_date_line_celestrak = '1996 01 01'
            current_date_line_mgii = '1996 01 01'
            # начальные значения
            current_celestrak_line = ''
            current_mgii_line = ''
            # ищем нужную дату в celestrak'е, с которой начнём взятие данных
            while datetime.strptime(current_date_line_celestrak, '%Y %m %d') != start_date:
                tmp = celestrak.readline()
                current_date_line_celestrak = tmp[0:10]
                current_celestrak_line = tmp
            # ищем нужную дату в mgii'е, с которой начнём взятие данных
            while datetime.strptime(current_date_line_mgii, '%Y %m %d') != start_date:
                tmp = mgii.readline()
                current_date_line_mgii = tmp[1:5] + " " + tmp[13:18]
                current_mgii_line = tmp
            # двигаемся по файлам celestrak и mgii, добавляя значения mjd и индексов F10, F10B, Ap1-Ap8
            # индексы же S10, S10B, XM10, XM10B получаем из функций - конветоров (см. выше)
            while datetime.strptime(current_celestrak_line[0:10], '%Y %m %d') != end_date:
                tmp_list = current_celestrak_line.split()
                MG2 = current_mgii_line.split()[-1]
                array[0].append(convert_calendar_to_mjd(current_date_line_celestrak.split()))
                array[1].append(tmp_list[26])
                array[2].append(tmp_list[28])
                array[3].append(convert_MG2_to_S10(float(MG2)))
                array[4].append(make_S10B(array))
                array[5].append(convert_MG2_to_M10(float(MG2)))
                array[6].append(make_XM10B(array))
                array[7].append(
                    [tmp_list[14], tmp_list[15], tmp_list[16], tmp_list[17], tmp_list[18], tmp_list[19], tmp_list[20],
                     tmp_list[21]])
                # обновляем строку для продвижения ниже
                current_celestrak_line = celestrak.readline()
                current_mgii_line = mgii.readline()

    except FileNotFoundError:
        print("Один из файлов не был найден!")


def make_csv_for_JB2006(start_date: datetime, end_date: datetime) -> None:
    """
    Данная функция делает csv-file, содержащий все нужные входные данные для модели атмосферы JB2006
    :param start_date: начальная дата в формате datetime, с которой начнётся извлечение информации о индексах
    :param end_date: конечная дата в формате datetime, до которой будет продолжаться извлечение информации о индексах
    :param csv_name: название файла (по умолчанию = 'jachnia_si.csv'), в который будет записана информация в виде таблицы
    :return: ---
    """
    delta = (end_date - start_date).days
    # если введённая дата <= сегодняшней с оставанием в 45 дней, то просто берём данные с сайта JB2008/indices
    if end_date <= datetime.today() - timedelta(days=45):
        tmp = make_indices_array_before_45_days(start_date, end_date)
        try:
            with open("jachnia_lala.csv", 'w') as file:
                file.write('mjd,ap1,ap2,ap3,ap4,ap5,ap6,ap7,ap8,F10,F81,S10,S10B,XM10,XM10B\n')
                for i in range(0, delta):
                    file.write(make_str_for_csv(tmp, i) + '\n')
        except FileNotFoundError:
            print("Файл не был найден!")

    else:
        end_date = end_date - timedelta(days=45)
        tmp = make_indices_array_before_45_days(start_date, end_date)
        update_indices_array_after_45_days(tmp, end_date - timedelta(days=45), end_date)
        try:
            with open("jachnia_lala.csv", 'w') as file:
                file.write('mjd,ap1,ap2,ap3,ap4,ap5,ap6,ap7,ap8,F10,F81,S10,S10B,XM10,XM10B\n')
                for i in range(0, delta):
                    file.write(make_str_for_csv(tmp, i) + '\n')
        except FileNotFoundError:
            print("Файл не был найден!")


def parse_all_files(url_celestrak: str, url_iup_mgii: str, url_JB2008_flux: str, url_JB2008_magnit: str) -> None:
    """
    Данная функция по данным ссылкам на соответствующие файлы получаем их содержимое:

        1) 'SOLFSMY.TXT' - данные о солнечной излучении для [start_date; end_date - 45 days]
        2) 'SOLRESAP.TXT' - данные о геомагнитной активности для [start_date; end_date - 45 days]
        3) 'SW-Last5Years.txt' - данные о солнечном излучении для [end_date - 45 days; end_date]
        4) 'GOME2B_Index_classic.dat' - данные о геомагнитной активности для [end_date - 45 days; end_date]

    :param url_celestrak: ссылка на файл 1)
    :param url_iup_mgii: ссылка на файл 2)
    :param url_JB2008_flux: ссылка на файл 3)
    :param url_JB2008_magnit: ссылка на файл 4)
    :return: ---
    """
    get_file_celestrak = requests.get(url_celestrak).content
    get_file_mgii = requests.get(url_iup_mgii).content
    get_file_JB2008_flux = requests.get(url_JB2008_flux).content
    get_file_JB2008_magnit = requests.get(url_JB2008_magnit).content

    # записываем в файлы с названиями ссылок полученное
    with open("CELESTRAK.TXT", 'wb') as result_celestrak_file, open("MGII.TXT", 'wb') as result_MGII_file, open(
            "SOLFSMY.TXT", 'wb') as result_SOLFSMY, open("SOLRESAP.TXT", 'wb') as result_SOLRESAP:
        result_celestrak_file.write(get_file_celestrak)
        result_MGII_file.write(get_file_mgii)
        result_SOLFSMY.write(get_file_JB2008_flux)
        result_SOLRESAP.write(get_file_JB2008_magnit)


def get_csv_file(start_date: datetime, end_date: datetime) -> None:
    """
    Данная функция отдаёт csv-file, содержащий входные данные о индексах для модели атмосферы JB2006
    :param start_date: начальная дата в формате datetime
    :param end_date: конечная дата в формате datetime
    :return: ---
    """
    # наша url страницы c индексами JB2008 (F10, F10B, S10, S10B, XM10, XM10B), которые будем брать для диапазона дат:
    # [start_date, end_date - 45], где 45 дней - задержка, с которой публикуются все индексы на данном сайте
    url_flux = 'https://sol.spacenvironment.net/jb2008/indices/SOLFSMY.TXT'
    url_magnit = 'https://sol.spacenvironment.net/jb2008/indices/SOLRESAP.TXT'

    # отсюда берем индексы солчнего излучения (F10, F10B) для диапазона дат:
    # [end_date - 45 дней; end_date]
    celestrak_file_url = "https://celestrak.com/SpaceData/SW-Last5Years.txt"

    # отсюда берём индекс MG2, который будем переводить в XM10, для диапазона дат:
    # [end_date - 45 дней; end_date]
    iup_mgii_url = "http://www.iup.uni-bremen.de/UVSAT/../gome/solar/GOME2B_Index_classic.dat"

    # записываем все данные в файлы
    parse_all_files(celestrak_file_url, iup_mgii_url, url_flux, url_magnit)

    # делаем наш файл
    make_csv_for_JB2006(start_date, end_date)

    # получаем путь к записанным файлам с солнечными и геомагнитными индексами
    path_MY = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SOLFSMY.TXT')
    path_AP = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SOLRESAP.TXT')
    path_HE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'CELESTRAK.TXT')
    path_RE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'MGII.TXT')
    # удаляем их из родительской директории
    os.remove(path_MY)
    os.remove(path_AP)
    os.remove(path_HE)
    os.remove(path_RE)


def main() -> None:
    # промежуток нужных дат
    get_csv_file(datetime(year=1997, month=1, day=1), datetime(year=2022, month=7, day=14))


if __name__ == '__main__':
    main()
