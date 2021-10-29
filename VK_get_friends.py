import sys
import time
import json
import logging
import requests
import pandas as pd


def write_to_file(data, mode, filename, filetype, offset):
    if filetype=='csv':
        df = pd.DataFrame(data)
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.csv', mode=mode, index=False)
    elif filetype=='tsv':
        df = pd.DataFrame(data)
        df.sort_values(by=['first_name'], inplace=True)
        df.reset_index(inplace=True, drop=True)
        df.to_csv(f'{filename}.tsv', sep='\t', mode=mode, index=False)
    elif filetype=='json':
        json_data = {}
        for i in range(len(data['first_name'])):
            json_data[str(i+offset)] = {'first_name': data['first_name'][i], 
                                        'last_name': data['last_name'][i], 
                                        'country': data['country'][i], 
                                        'city': data['city'][i], 
                                        'bdate': data['bdate'][i], 
                                        'sex':data['sex'][i]
                                       }
        with open(f'{filename}.json', mode) as writer:
            json.dump(json_data, writer)
    else:
        logging.error(f"Program cannot create report "
                      f"file of {filetype} format")
    

def data_handler(response):
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
    offset = offset
    response = requests.get(f'https://api.vk.com/method/friends.get?'
                        f'v={v}&access_token={access_token}&'
                        f'user_id={user_id}&fields={fields}'
                        f'&count={count}&offset={offset}')
    try:
        if response.json()['response']['items']:
            data = data_handler(response)
            if flag:
                write_to_file(data, mode='a', filename=fname, 
                              filetype=ftype, offset=offset)
            else:
                write_to_file(data, mode='w', filename=fname, 
                              filetype=ftype, offset=offset)
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
    count = 1000 # размер выборки
    offset = 0 # сдвиг по генеральной совокупности
    request_processing(access_token, user_id, count, 
                       offset, fname, ftype)
    
    
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
        i = -1
        while 'filename' not in sys.argv[i+1]:
            i += 1
        j = -1
        while 'filetype' not in sys.argv[j+1]:
            j += 1

        if i == -1 and j == -1:
            main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
        elif (i+1 > 0 and i+1 < 3) or (j+1 > 0 and j+1 < 3):
            logging.error("filename and filetype arguments must be after "
                          "access_token and user_id")
        else:
            if i != -1 and j != -1:
                if (sys.argv[i+1].split('=')[1] != '' 
                    and sys.argv[j+1].split('=')[1] != ''):
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[i+1].split('=')[1], 
                         ftype=sys.argv[j+1].split('=')[1])
                else:
                    logging.error("Empty filetype or filetype")
            elif i != -1:
                if sys.argv[i+1].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[i+1].split('=')[1], 
                         ftype=sys.argv[4 if 4 > i+1 else 3])
                else:
                    logging.error("Empty filename")
            elif j != -1:
                if sys.argv[j+1].split('=')[1] != '':
                    main(sys.argv[1], sys.argv[2], 
                         fname=sys.argv[4 if 4 > j+1 else 3], 
                         ftype=sys.argv[j+1].split('=')[1])
                else:
                    logging.error("Empty filetype")
    else:
        logging.error(f'VK_get_friends program takes 4 arguments '
                      f'(acess_token, user_id, filename, file_format),'
                      f'but {len(sys.argv)} were given')