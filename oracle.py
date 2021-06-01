from sqlalchemy import create_engine
from pymongo import MongoClient
import random
import csv
import multiprocessing
#
# # Подключение к серверу Oracle на локальном хосте с помощью cx-Oracle DBAPI.
# engine = create_engine("oracle+cx_oracle://olaptrain:qwe123@localhost:1521/pdb1.localdomain")
# engine.connect()
# print(engine)

# //localhost:1521/pdb1.localdomain

import cx_Oracle


# connection = cx_Oracle.connect(
# "sys",
# "oracle",
# "itmo.fatalist.tech:1522/xe",
# cx_Oracle.SYSDBA
# )
#
# cursor = connection.cursor()
# print(cursor)


def add_student(id: int, name: str, home: int, nomer: int, floop: int) -> dict:
    """Генерация студента"""
    tryfalse = [True, False]
    typerooms = ['На 3 человек', 'На 2 человек']
    mani = ['бюджет', 'контракт']
    payment = [1700, 2000, 1500, 1400]
    exit = ['Зашёл в общежитие', 'Вышел из общежитие']
    date = ['01.09.2018-31.08.2021', '01.09.2019-31.08.2021', '01.09.2020-31.08.2021', '01.09.2017-31.08.2021',
            '01.09.2016-31.08.2021']
    room = random.choice(typerooms)
    if room == 'На 3 человек':
        liferoom = random.randint(1, 3)
    else:
        liferoom = random.randint(1, 2)
    post_data = {
        'Идентификатор': id,
        'ФИО': name,
        'Льготы': random.choice(tryfalse),
        'Вид обучения': random.choice(mani),
        'Сумма оплаты ': random.choice(payment),
        'Период посещения ': random.choice(exit),
        'Общежитие ': {'Местоположение': "Общежитие " + str(home),
                       'Количество комнат в здании': 180},
        'Комната': {'Номер комнаты': str(nomer) + str(floop), 'Тип комнаты': room,
                    'Проживающих': liferoom,
                    'Когда проводили дезинфекцию дата': str(random.randint(1, 31)) + '.03.2021',
                    'Клопы': random.choice(tryfalse),
                    'Предупреждения': random.randint(0, 3)},
        'Период с/по': random.choice(date)
    }
    return post_data


# INSERT INTO AWM.HOSTEL (HOSTEL_ID, LOCATION, ROOM_COUNT)
# VALUES (1, 'Общежитие №1', 180);
def add_hostel(db, HOSTEL_ID: int, LOCATION: str, ROOM_COUNT: int):
    try:
        with db.cursor() as cursor:
            sql = """INSERT INTO awm.hostel (HOSTEL_ID, LOCATION, ROOM_COUNT) """ \
                  """VALUES (:HOSTEL_ID, :LOCATION, :ROOM_COUNT)"""
            cursor.execute(sql, [HOSTEL_ID, LOCATION, ROOM_COUNT])
            db.commit()
            print("done")
        return True
    except Exception as e:
        print(e)
        return False


# INSERT INTO AWM.ROOM (ROOM_ID, "number", TYPE_ROOM, RESIDENTS, DISINFECTION, BEDBUG, WARNING)
# VALUES (2, 3, '2 fg', 2, TO_DATE('2020-05-31 19:23:55', 'YYYY-MM-DD HH24:MI:SS'), 1, 2);
def add_room(db, ROOM_ID: int, NUMBER1: int, TYPE_ROOM: str, RESIDENTS: int, BEDBUG: int, WARNING: int):
    try:
        with db.cursor() as cursor:
            date1: str = '2020-05-31 19:23:55'
            sql = """INSERT INTO awm.room(ROOM_ID, "number", TYPE_ROOM, RESIDENTS, DISINFECTION, BEDBUG, WARNING)  """ \
                  """VALUES (:ROOM_ID, :NUMBER1, :TYPE_ROOM, :RESIDENTS, to_date(:date1, 'YYYY-MM-DD HH24:MI:SS'), :BEDBUG, :WARNING)"""
            cursor.execute(sql, [ROOM_ID, NUMBER1, TYPE_ROOM, RESIDENTS, date1, BEDBUG, WARNING])
            db.commit()
            print("done")
        return True
    except Exception as e:
        print(e)
        return False


# INSERT INTO AWM.STUDENT_HOSTEL (STUDENT_HOSTEL_ID, STUDENT_ID, "privileges", TYPE_OF_TRAINING, PAYMENT, VISIT,
#                                 PERIOD_FROM, PERIOD_TO, ROOM_ID, HOSTEL_ID)
# VALUES (2, 11440149, 0, 'adas', 1777, 'dsfs', TO_DATE('2021-06-01 19:08:54', 'YYYY-MM-DD HH24:MI:SS'),
#         TO_DATE('2021-06-01 19:08:55', 'YYYY-MM-DD HH24:MI:SS'), 1, 1);



def add_stydent(db, STUDENT_HOSTEL_ID: int, STUDENT_ID: int, privileg: int, TYPE_OF_TRAINING, PAYMENT, VISIT,
                PERIOD_FROM: str, PERIOD_TO: str, ROOM_ID: int, HOSTEL_ID: int):
    try:
        with db.cursor() as cursor:
            sql = """INSERT INTO awm.student_hostel(STUDENT_HOSTEL_ID, STUDENT_ID, "privileges", TYPE_OF_TRAINING, PAYMENT, VISIT,"""\
                               """ PERIOD_FROM, PERIOD_TO, ROOM_ID, HOSTEL_ID)  """ \
                  """VALUES (:STUDENT_HOSTEL_ID, :STUDENT_ID, :privileg, :TYPE_OF_TRAINING, :PAYMENT, :VISIT,"""\
                               """ TO_DATE(:PERIOD_FROM, 'YYYY-MM-DD HH24:MI:SS'),"""\
        """ TO_DATE(:PERIOD_TO, 'YYYY-MM-DD HH24:MI:SS'), :ROOM_ID, :HOSTEL_ID)"""
            cursor.execute(sql, [STUDENT_HOSTEL_ID, STUDENT_ID, privileg, TYPE_OF_TRAINING, PAYMENT, VISIT,
                                 PERIOD_FROM, PERIOD_TO, ROOM_ID, HOSTEL_ID])
            db.commit()
            print("done")
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":
    # client = MongoClient()
    # db = client['MongoDB']
    # roomers = db['Studens']
    #
    connection = cx_Oracle.connect(
        "sys",
        "oracle",
        "itmo.fatalist.tech:1522/xe",
        cx_Oracle.SYSDBA
    )

    # add_hostel(connection,3,"общежитие 3",180)
    # add_room(connection, 34, 23, "3 chil", 2, 1, 1)
    add_stydent(db=connection,STUDENT_HOSTEL_ID=3, STUDENT_ID=11440017, privileg=0,
                TYPE_OF_TRAINING='плати55', PAYMENT=1700, VISIT='вышел', PERIOD_FROM='2021-06-01 18:37:00',
                PERIOD_TO='2021-06-01 18:37:03', ROOM_ID=1, HOSTEL_ID=1)

    file = "STUDENT.csv"
    home = 1
    nomer = 1
    floop = 1
    data = []
    i = 1
    # with open(file, newline='', encoding='utf-8') as f:
    #     reader = csv.DictReader(f,delimiter=',')
    #     for row in reader:
    #         print(row["STUDENT_ID"])

    # for row in reader:
    #     dat=add_student(int(row["\ufeffperson_id"]),row["name"],home,nomer,floop)
    #     i+=1
    #     if i%3==0:
    #         nomer+=1
    #         i=0
    #     if nomer%15==0:
    #         floop+=1
    #         nomer=0
    #     if floop%11==0:
    #         home+=1
    #         floop=0
    #         nomer=0
    #     roomers.insert(dat)
