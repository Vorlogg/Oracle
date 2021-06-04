from pymongo import MongoClient
import random
from tqdm import tqdm

import cx_Oracle

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
def add_room(db, ROOM_ID: int, NUMBER1: int, TYPE_ROOM: str, RESIDENTS: int,des:str, BEDBUG: int, WARNING: int):
    try:
        with db.cursor() as cursor:
            date1: str = '2020-05-31'
            date1: str = des #29.05.2021
            sql = """INSERT INTO awm.room(ROOM_ID, "number", TYPE_ROOM, RESIDENTS, DISINFECTION, BEDBUG, WARNING)  """ \
                  """VALUES (:ROOM_ID, :NUMBER1, :TYPE_ROOM, :RESIDENTS, to_date(:date1, 'DD-MM-YYYY'), :BEDBUG, :WARNING)"""
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

def add_stydent(db, STUDENT_HOSTEL_ID: int, STUDENT_ID: int, privil: bool, TYPE_OF_TRAINING, PAYMENT, VISIT,
                PERIOD:str, ROOM_ID: int, HOSTEL_ID: int):
    try:
        with db.cursor() as cursor:
            date = PERIOD.split('-')
            date1=date[0]
            date2=date[1]
            if privil==True:
                privileg=1
            else:
                privileg=0
            sql = """INSERT INTO awm.student_hostel(STUDENT_HOSTEL_ID, STUDENT_ID, "privileges", TYPE_OF_TRAINING, PAYMENT, VISIT,"""\
                               """ PERIOD_FROM, PERIOD_TO, ROOM_ID, HOSTEL_ID)  """ \
                  """VALUES (:STUDENT_HOSTEL_ID, :STUDENT_ID, :privileg, :TYPE_OF_TRAINING, :PAYMENT, :VISIT,"""\
                               """ to_date(:date1, 'DD-MM-YYYY'),"""\
        """ to_date(:date2, 'DD-MM-YYYY'), :ROOM_ID, :HOSTEL_ID)"""
            cursor.execute(sql, [STUDENT_HOSTEL_ID, STUDENT_ID, privileg, TYPE_OF_TRAINING, PAYMENT, VISIT,
                                 date1, date2, ROOM_ID, HOSTEL_ID])
            db.commit()
            # print("done")
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == "__main__":

    connection = cx_Oracle.connect(
        "sys",
        "oracle",
        "itmo.fatalist.tech:1522/xe",
        cx_Oracle.SYSDBA
    )
    client = MongoClient()
    db = client['MongoDB']
    roomers = db['students_hostel']

    """Заполнение общежитий"""
    hostel = {}
    for i in roomers.find():
        hostel[i["Общежитие"]["hostel_id"]]=[i["Общежитие"]["Местоположение"],i["Общежитие"]["Количество комнат в здании"]]
    for key,value in hostel.items():
        add_hostel(connection, key, value[0], value[1])
    """Заполнение комнат"""
    rooms = {}
    for i in roomers.find():
        rooms[i["Комната"]["room_id"]]=[i["Комната"]["Номер комнаты"],i["Комната"]["Тип комнаты"],i["Комната"]["Проживающих"],
                                        i["Комната"]["Когда проводили дезинфекцию дата"],i["Комната"]["Клопы"],i["Комната"]["Предупреждения"]]
    for key,value in rooms.items():
        add_room(connection, key, value[0], value[1], value[2], value[3], value[4],value[5])
    #     print(connection, key, value)


    """Заполнение студентов"""
    j=1
    data=[]
    for i in tqdm(roomers.find()):
        data.append(i)
    for i in tqdm(data):
        add_stydent(db=connection, STUDENT_HOSTEL_ID=j, STUDENT_ID=i["Идентификатор"], privil=i["Льготы"],
                    TYPE_OF_TRAINING=i["Вид обучения"], PAYMENT=i["Сумма оплаты"], VISIT=i["Период посещения"],
                    PERIOD=i["Период с/по"], ROOM_ID=i["Комната"]["room_id"], HOSTEL_ID=i["Общежитие"]["hostel_id"])
        j+=1



