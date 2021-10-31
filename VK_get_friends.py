import sys
import time
import json
import logging
import requests
import pandas as pd


def sort_report(filename, filetype):
    '''
       Функция сортировки отчетов. 
       Документ загружается в оперативную память 
       и сортируется по полю first_name.
    '''
    if filetype == 'csv':
        df = pd.read_csv(f'{filename}.csv')
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.csv', index=False)
    elif filetype == 'tsv':
        df = pd.read_csv(f'{filename}.tsv', sep='\t')
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.tsv', sep='\t', index=False)
    elif filetype == 'json':
        json_data = []
        with open(f'{filename}.json', 'r') as reader:
            json_data = json.load(reader)
        json_data_sorted = sorted(json_data, key=lambda user: user['first_name'])
        with open(f'{filename}.json', 'w') as writer:
            json.dump(json_data_sorted, writer)


def write_to_file(data, mode, filename, filetype, offset, header):
    '''
       Функция записи данных в файл.
       Данные предварительно сортируются 
       и записываются в файл. В итоге получается 
       отчет с пилообразной сортировкой.
    '''
    if filetype == 'csv':
        df = pd.DataFrame(data)
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.csv', mode=mode, index=False, header=header)
    elif filetype == 'tsv':
        df = pd.DataFrame(data)
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.tsv', sep='\t', mode=mode, index=False, header=header)
    elif filetype == 'json':
        json_data = []
        for i in range(len(data['first_name'])):
            json_data.append({'first_name': data['first_name'][i], 
                              'last_name': data['last_name'][i], 
                              'country': data['country'][i], 
                              'city': data['city'][i], 
                              'bdate': data['bdate'][i], 
                              'sex':data['sex'][i]
                             })
        if mode == 'w':
            with open(f'{filename}.json', mode) as writer:
                json.dump(json_data, writer)
        elif mode == 'a':
            with open(f'{filename}.json', f'{mode}+') as f:
                f.seek(f.truncate(f.tell()-1)) # обрезали ]
                f.write(', ')
                temp = f.tell() # запомним позицию. Мы еще к ней вернемся
                json.dump(json_data, f)
                f.seek(temp+1) # вернулись к запомненной позиции + 1
                temp_data = f.read() # записали все, кроме [
                f.seek(f.truncate(f.tell()-len(temp_data)-1)) # обрезали до [ включительно
                f.write(temp_data) # обратно записали вынесенные данные
    else:
        logging.error(f"Program cannot create report "
                      f"file of {filetype} format")
    

def data_collector(response):
    '''
       Функция сбора данных. На вход получает 
       ответ API VK и формирует словарь, 
       который передает на выход.
    '''
    sex_dict = {0: 'undefined', 1: 'female', 2: 'male'}
    data = {'first_name': [], 'last_name': [], 'country': [], 
            'city': [], 'bdate': [], 'sex': []}
    friends = response.json()['response']['items']
    for friend in friends:
        fkeys = friend.keys()
        if 'deactivated' not in fkeys:
            if 'first_name' in fkeys:
                data['first_name'].append(friend['first_name'])
            else:
                data['first_name'].append('')
                
            if 'last_name' in fkeys:
                data['last_name'].append(friend['last_name'])
            else:
                data['last_name'].append('')
            
            if 'country' in fkeys:
                data['country'].append(friend['country']['title'])
            else:
                data['country'].append('')
            
            if 'city' in fkeys:
                data['city'].append(friend['city']['title'])
            else:
                data['city'].append('')
                
            if 'bdate' in fkeys:
                bdate_iso = ''
                if len(friend['bdate'].split('.')) == 3:
                    bdate = time.strptime(friend['bdate'], '%d.%m.%Y')
                    bdate_iso = time.strftime('%Y-%m-%d', bdate)
                else:
                    bdate = time.strptime(friend['bdate'], '%d.%m')
                    bdate_iso = time.strftime('%m-%d', bdate)
                data['bdate'].append(bdate_iso)
            else:
                data['bdate'].append('')
                
            if 'sex' in fkeys:
                data['sex'].append(sex_dict[friend['sex']])
            else:
                data['sex'].append('')
    return data
    

def request_processing(access_token, user_id, count, 
                       offset, fname, ftype, flag=False,
                       fields='country,city,bdate,sex', v='5.81'):
    '''
       Функция, которая передает запросы API VK, 
       получает данные и записывает их в файл.
    '''
    offset = offset
    response = requests.get(f'https://api.vk.com/method/friends.get?'
                        f'v={v}&access_token={access_token}&'
                        f'user_id={user_id}&fields={fields}'
                        f'&count={count}&offset={offset}')
    try:
        if response.json()['response']['items']:
            data = data_collector(response)
            if flag:
                write_to_file(data, mode='a', filename=fname, 
                              filetype=ftype, offset=offset, header=False)
            else:
                write_to_file(data, mode='w', filename=fname, 
                              filetype=ftype, offset=offset, header=True)
            offset += count
            # to avoid error response 
            # (error_code: 6, Too many requests per second)
            time.sleep(1)
            request_processing(access_token, user_id, count, 
                               offset, fname, ftype, flag=True)
        else:
            if flag:
                print('Sending requests completed')
            else:
                print(f'User (id: {user_id}) has no friends')
    except KeyError:
        print("KeyError: 'response'")
        print(response.json())


def main(access_token, user_id, fname='report', ftype='csv'):
    '''
       Главная функция. Выполняются все процессы 
       сбора данных и проводится их сортировка.
    '''
    count = 1000 # размер выборки
    offset = 0 # сдвиг по генеральной совокупности
    request_processing(access_token, user_id, count, 
                       offset, fname, ftype)
    sort_report(fname, ftype)
    
    
if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', 
                        level=logging.DEBUG)

    if len(sys.argv) < 2:
        logging.error(f'VK_get_friends program takes at least 2 arguments '
                      f'(acess_token, user_id), '
                      f'but {len(sys.argv)} were given')
    elif len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        i = -1
        while 'filename' not in sys.argv[i+1]:
            i += 1
        j = -1
        while 'filetype' not in sys.argv[j+1]:
            j += 1

        if i == -1 and j == -1:
            main(sys.argv[1], sys.argv[2], sys.argv[3])
        elif (i > 0 and i < 3) or (j > 0 and j < 3):
            logging.error("filename and filetype arguments must be after "
                          "access_token and user_id")
        else:
            if i != -1:
                if sys.argv[i+1].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[i+1].split('=')[1])
                else:
                    logging.error("Empty filename")
            elif i != -1:
                if sys.argv[j+1].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         ftype=sys.argv[j+1].split('=')[1])
                else:
                    logging.error("Empty filetype")
    elif len(sys.argv) == 5:
        i = 0
        j = 0
        i_flag = False # для определения наличия строки filename
        j_flag = False # для определения наличия строки filetype
        while i < 4 and 'filename' not in sys.argv[i]:
            if 'filename' in sys.argv[i]:
                i_flag = True
                break
            i += 1
            
        while j < 4 and 'filetype' not in sys.argv[j]:
            if 'filetype' in sys.argv[j]:
                j_flag = True
                break
            j += 1

        if i_flag == False and j_flag == False:
            main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        elif (i > 0 and i < 3) or (j > 0 and j < 3):
            logging.error("filename and filetype arguments must be after "
                          "access_token and user_id")
        else:
            if i_flag and j_flag:
                if (sys.argv[i].split('=')[1] != '' 
                    and sys.argv[j].split('=')[1] != ''):
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[i].split('=')[1], 
                         ftype=sys.argv[j].split('=')[1])
                else:
                    logging.error("Empty filetype or filetype")
            elif i_flag:
                if sys.argv[i].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[i].split('=')[1], 
                         ftype=sys.argv[4 if 4 > i else 3])
                else:
                    logging.error("Empty filename")
            elif j_flag:
                if sys.argv[j].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[4 if 4 > j else 3], 
                         ftype=sys.argv[j].split('=')[1])
                else:
                    logging.error("Empty filetype")
    else:
        logging.error(f'VK_get_friends program takes 4 arguments '
                      f'(acess_token, user_id, filename, file_format),'
                      f'but {len(sys.argv)} were given')