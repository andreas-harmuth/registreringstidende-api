import json

from Database import Database
from bs4 import BeautifulSoup
import requests
import re
import calendar

def add_zero(number):
    return "0"+str(number) if len(str(number))==1 else str(number)

def get_text_if_found(ele):
    return ele.get_text().rstrip().strip() if ele is not None else None

class RegistreringstidendeScraper:


    def __init__(self):

        # Init database
        self.db = Database(local_database=True)


    def __retrieve_id_and_reg(self, row_div):
        """ Retrieve the ID and from a row div

        :param row_div:
        :return dict(id: str, reg_date: str)
        """
        reg_date = row_div.find('td', {'id': 'row1-uci'}).getText()
        _id = None
        for a_div in row_div.find_all('a', href=True):
            if re.match('/data/registreringstidendedokument/REGISTRERING/.*', a_div['href']):
                _id = a_div['href'].split('/data/registreringstidendedokument/REGISTRERING/')[1].split('?soeg=')[0]

        return {'id': _id, 'reg_date': reg_date}

    def __get_data(self, _id):
        cvr = None
        address = None
        commune = None
        description = None

        # Get result page
        url = "https://datacvr.virk.dk/data/registreringstidendedokument/REGISTRERING/{0}".format(_id)
        request = requests.get(url)

        if request.status_code == 200:
            soup = BeautifulSoup(request.content, "html.parser")
            cvr = get_text_if_found(soup.find('div', {'class':'registreringstidende-cvr-value'}))
            address = get_text_if_found(soup.find('div', {'class':'registreringstidende-adresse-value'}))
            commune = get_text_if_found(soup.find('div', {'class':'registreringstidende-kommune-value'}))
            description = get_text_if_found(soup.find('div', {'class':'registreringstidende-tekst-value'}))

        return cvr, address, commune, description
    def get_register(self, from_year, to_year, from_month=None):


        for year in range(from_year, to_year+1):

            for month in range(1,11):

                # If we want to start from a specific monthp
                if from_month is not None:
                    month = from_month if from_month <= 10 else 10
                    from_month = None

                from_date = "01.{0}.{1}".format(add_zero(month),year)
                to_date = "{0}.{1}.{2}".format(calendar.monthrange(year, month+2)[1], add_zero(month+2),year)
                print("From {0} to {1}".format(from_date, to_date))

                page = 0
                while True:
                    # Get url

                    url = "https://datacvr.virk.dk/data/registreringstidenderesultat?page={0}&soeg=VIRKSOMHEDSREGISTRERING&startdato={1}" \
                    "&slutdato={2}&postnummer=&kommunekode=Alle&virksomhedskode=60-80-70-81-100-30-40-140-152-151-170-180-190-235-" \
                    "290-160-291-220-285-45&virksomhedsregistreringstatusser=AENDRING_KAPITAL".format(page, from_date, to_date)
                    print("Page: {0}, URL: {1}".format(page, url))
                    request = requests.get(url)

                    # Holder for the id and dates that the below scrapers scrapes
                    id_date_list = []
                    # Get id and dates
                    if request.status_code == 200:
                        soup = BeautifulSoup(request.content, "html.parser")
                        table = soup.find("table", {'class': 'table-striped'})
                        rows = table.find_all("tr")


                        # If there are multiple rows more than one because there is always a header
                        if len(rows)> 1:
                            for row in rows:
                                if len(row.find_all('td'))>0:
                                    id_date_list.append(self.__retrieve_id_and_reg(row))
                    if len(id_date_list) == 0:
                        break

                    for pre_data in id_date_list:
                        if pre_data['id'] is not None:
                            print("Getting data for {0}... ".format(pre_data['id']), end="")

                            if not self.db.does_register_exist(pre_data['id']):
                                cvr, address, commune, description = self.__get_data(pre_data['id'])
                                if cvr is not None and description is not None:

                                    # Remove everything before Kapitalforhøjelsen
                                    is_capital_raise_checker = description.split("Kapitalforhøjelse")
                                    # Only if there is a capital gain
                                    if len(is_capital_raise_checker)>1:
                                        description = "Kapitalforhøjelse"+is_capital_raise_checker[1]

                                        # Remove everything after (Assume session ends with \n)
                                        description = description.split("\n\n")[0]
                                        self.db.save_register(cvr, address, commune, description, pre_data['id'], pre_data['reg_date'])
                                        print("\033[92m Done\033[0m")
                                    else:
                                        print("\033[91m Not capital raise\033[0m")
                                else:
                                    print("\033[91m Scraping error\033[0m")
                            else:
                                print("\033[94m Already in database\033[0m")
                    page += 1

rs = RegistreringstidendeScraper()

rs.get_register(2000,2018)