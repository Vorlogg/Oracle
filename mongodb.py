from pymongo import MongoClient
import random
import csv

def add_student(id: int, name: str, hostel, room) -> dict:
    """Генерация студента
    :rtype: object
    """
    tryfalse = [True, False]
    typerooms = ['На 3 человек', 'На 2 человек']
    mani = ['бюджет', 'контракт']
    payment = [1700, 2000, 1500, 1400]
    exit = ['Зашёл в общежитие', 'Вышел из общежитие']
    date = ['01.09.2018-31.08.2021', '01.09.2019-31.08.2021', '01.09.2020-31.08.2021', '01.09.2017-31.08.2021',
            '01.09.2016-31.08.2021']
    post_data = {
        'Идентификатор': id,
        'ФИО': name,
        'Льготы': random.choice(tryfalse),
        'Вид обучения': random.choice(mani),
        'Сумма оплаты': random.choice(payment),
        'Период посещения': random.choice(exit),
        'Общежитие': hostel,
        'Комната': room,
        'Период с/по': random.choice(date)
    }

    return post_data


def hostel(n):
    hostel = {}
    for i in range(1, n + 1):
        hostel[i] = {'hostel_id':i,'Местоположение': "Общежитие " + str(i), 'Количество комнат в здании': 180}
    return hostel


def room(room,id):
    rooms = {}
    tryfalse = [True, False]
    nomer = 1
    floop = 1
    for i in range(1, 180):
        if room ==3:
            liferoom = 3
            room = 'На 3 человек'
        else:
            liferoom = 2
            room = 'На 2 человек'
        nomer += 1
        if i % 20 == 0:
            floop += 1
            nomer = 1
        if nomer >= 10:
            nomerstr = nomer
        else:
            nomerstr = "0" + str(nomer)
        rooms[i] = {'room_id':id+i,'Номер комнаты': str(floop) + str(nomerstr), 'Тип комнаты': room,
                    'Проживающих': liferoom,
                    'Когда проводили дезинфекцию дата': str(random.randint(1, 31)) + '.05.2021',
                    'Клопы': random.choice(tryfalse),
                    'Предупреждения': random.randint(0, 3)}
    return rooms
    # print(rooms)


if __name__ == "__main__":
    client = MongoClient()
    db = client['MongoDB']
    roomers = db['students_hostel']
    file = "res.csv"
    home = 1
    nomer = 1
    floop = 1
    data = []
    i = 1
    hostels = hostel(250)
    # print(len(hostels))
    host = 1
    # print(rooms)
    # i = 1
    fl=1
    res = []
    x=random.randint(2, 3)
    room_id=0
    rooms = room(x,room_id)
    with open(file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=',')
        for row in reader:
            dat = add_student(int(row["STUDENT_ID"]), row["NAME"], hostels[host], rooms[nomer])
            i+=1
            if i==x:
                i=0
                nomer+=1
            if nomer == 179:

                host += 1
                nomer = 1
                x = random.randint(2, 3)
                room_id += 200
                rooms = room(x,room_id)

            roomers.insert_one(dat)
