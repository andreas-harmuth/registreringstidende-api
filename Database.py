import datetime
from pprint import pprint

import time
from pymongo import MongoClient, errors





class Database:
    """

    """
    def __init__(self, local_database=False, reset_db=False):

        if local_database:
            db_type = "local"
            client = MongoClient()
        else:
            db_type = "cloud"
            client = MongoClient(
                'mongodb+srv://linkedin_scraper:qjbNZcT4k5tjSsns@test-cluster-x5krn.gcp.mongodb.net/test?retryWrites=true'
            )

        try:
            print("Testing {0} database connection...".format(db_type), end=" ")
            client.server_info()  # force connection on a request as the
            # connect=True parameter of MongoClient seems
            # to be useless here
            print("Success")

        except errors.ServerSelectionTimeoutError as err:
            print("Error")
            print(err)
            exit()
        if reset_db:
            print("Resetting database")
            client.drop_database('registreringstidende')
        db = client.registreringstidende
        self.formatted_register = db.formatted_register
        self.register = db.register

    def save_register(self, cvr, address, commune, description, virk_id, registration_date, type="capital gain"):

        self.register.insert_one({'cvr': int(cvr),
                                  'address': address,
                                  'commune': commune,
                                  'description': description,
                                  'virkId': virk_id,
                                  'registrationDate': registration_date,
                                  'type': type
                                  })

    def formatted_register_many(self, reg_list):
        self.formatted_register.insert_many(reg_list)

    def does_register_exist(self, virk_id):
        """ Check if register is in the database by id
                :param VAT:
                :return bool:
                """
        return self.register.count() > 0 and self.register.find_one({"virkId": virk_id}) is not None

import re

RE_CAPITAL_VALUE = "([0-9]*\.?)*,[0-9]{2}"
RE_CURRENCY = "[a-zA-Z]*\.?"

def extract_value(string):
    c = re.search(RE_CAPITAL_VALUE, string)
    if c:
        # Return converted value (, --> .)
        return float(c.group(0).replace(".", "").replace(",", "."))
    else:
        return None

def extract_post_capital(string):
    m = re.search(".* udgooor herefter {0} {1}".format(RE_CURRENCY, RE_CAPITAL_VALUE), string)
    if m:

        amount = extract_value(m.group(0))
        _type = re.sub(" udgooor herefter {0} {1}".format(RE_CURRENCY, RE_CAPITAL_VALUE), "", m.group(0))


        return {
            'capital':{
                'amount': amount,
                'type': _type
            },
            'restructure': None}
    else:
        m = re.search('Selskabet er omdannet til (et [a-zA-Z\/]* med CVR-nr [0-9]*|[a-zA-Z\/]+[0-9]*)\.', string)

        if m:
            sub_string = m.group(0)
            m = re.search('([a-zA-Z]+[0-9]+)', sub_string)
            other = m.group(0) if m else None
            if m is None:
                # Is a substring of above but only one can be true
                m = re.search("[0-9]+", sub_string)
                cvr = int(m.group(0)) if m else None
            else:
                cvr = None

            return {
                'capital': None,
                'restructure': {
                    'cvr': cvr,
                    'other': other,
                    }
            }
        else:
            m = re.search('Kapitalnedsaettelse besluttet [0-9]{2}\.[0-9]{2}\.[0-9]{4} med udbetaling til aktionaerer %s %s til kurs %s,' % (RE_CURRENCY, RE_CAPITAL_VALUE, RE_CAPITAL_VALUE), string)
            if m:
                sub_string = m.group(0)

                return {
                        'capital': {
                            'amount': {
                                'price': extract_value(re.search(
                                    'med udbetaling til aktionaerer {0} {1}'.format(RE_CURRENCY, RE_CAPITAL_VALUE),
                                    sub_string).group(0)),
                                'payout': extract_value(
                                    re.search('til kurs {0}'.format(RE_CAPITAL_VALUE), sub_string).group(0))
                            },
                            'type': 'Kapitalnedsættelse med udbetaling til aktionærer',
                            'date': re.search('[0-9]{2}.[0-9]{2}.[0-9]{4}', sub_string).group(0)
                        },
                        'restructure': None}

    return {
        'capital': None,
        'restructure': None
        }

def extract_paid_capital(string):
    return_list = []
    for sub_string in multisearch("{0} {1} indbetalt [a-zA-Z\/\s]*, kurs {1}".format(RE_CURRENCY, RE_CAPITAL_VALUE), string):
        _paid = re.search("{0} {1} indbetalt ".format(RE_CURRENCY, RE_CAPITAL_VALUE), sub_string)
        _price = re.search(", kurs {0}".format(RE_CAPITAL_VALUE), sub_string)

        # Get type
        _type = None
        if _paid and _price:
            _type = re.search('{0}(.*){1}'.format(_paid.group(0), _price.group(0)), sub_string)


        # Return paid, price
        return_list.append({
            'currency': re.sub(" {0} .*".format(RE_CAPITAL_VALUE), "", _paid.group(0)) if _paid else None,
            'paid': extract_value(_paid.group(0)) if _paid else None,
            'price': extract_value(_price.group(0)) if _price else None,
            'type': _type.group(1).replace('aa', 'å').replace('ae','æ').replace('ooo', 'ø') if _type else None
        })

    return return_list

def extract_classes(string):
    return_dict = {}
    m = re.search("Klasser:\n.*.", string)
    if m:
        for sub_string in multisearch("[a-zA-Z0-9\-]* {0} {1}".format(RE_CURRENCY, RE_CAPITAL_VALUE), m.group(0)):
            _class = re.sub(" {0} {1}".format(RE_CURRENCY, RE_CAPITAL_VALUE), "", sub_string)

            return_dict[_class] = {
                'amount': extract_value(sub_string),
                'currency': sub_string.split(" ")[-2]
            }

    return return_dict


def multisearch(pattern, string):
    """ Find occurrence of pattern, then delete that occurrence from string and do again until no occurrences are left

    :param pattern:
    :param string:
    :return list of matches:
    """

    search_list = []
    while True:
        c = re.search(pattern, string)
        if c:
            found = c.group(0)
            string = re.sub(found, "", string, count=1)
            search_list.append(found)
        else:
            return search_list

# Convert to cloud

db_cloud = Database()


size = db_cloud.register.count()
SAMPLE_SIZE = size
_all = [c for c in db_cloud.register.find({}).limit(SAMPLE_SIZE)]

start = time.time()
reg_list = []
update_size = 5000
for i, reg in enumerate(_all):

    if i%update_size == 0:

        if len(reg_list) != 0:
            db_cloud.formatted_register_many(reg_list)
            reg_list = []
            print("{0}/{1}  || time left {2} sec".format(i, SAMPLE_SIZE, str(
                datetime.timedelta(seconds=(time.time() - start) / update_size * (SAMPLE_SIZE - i)))))
            start = time.time(),



    # Replace three whitespaces with one
    description = reg['description'].replace('   ', ' ').replace('å', 'aa').replace('æ', 'ae').replace('ø', 'ooo')

    #kr. 1.125.000,00 indbetalt kontant, kurs 400,00
    #kr. {0} indbetalt [a-zA-Z]*, kurs {0}
    #kr\. {0} indbetalt [a-zA-Z\s]*, kurs {0}
    reg['classes'] = extract_classes(description)

    reg['paidCapital'] = extract_paid_capital(description)
    reg['postCaptial'] = extract_post_capital(description)
    reg_list.append(reg)
    #pprint(reg, indent = 4)




#https://datacvr.virk.dk/data/registreringstidendedokument/REGISTRERING/26180351?soeg=VIRKSOMHEDSREGISTRERING&startdato=28.09.2018&slutdato=05.10.2018&virksomhedskode=60-80-70-81-100-30-40-140-152-151-170-180-190-235-290-160-291-220-285-45&virksomhedsregistreringstatusser=AENDRING_KAPITAL
#https://datacvr.virk.dk/data/registreringstidenderesultat?soeg=VIRKSOMHEDSREGISTRERING&startdato=28.09.2018&slutdato=05.10.2018&postnummer=&kommunekode=Alle&virksomhedskode=60-80-70-81-100-30-40-140-152-151-170-180-190-235-290-160-291-220-285-45&virksomhedsregistreringstatusser=AENDRING_KAPITAL