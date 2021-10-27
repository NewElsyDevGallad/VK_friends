# v=5.81
# client_id=7984179
# access_token=85b3240de340e9939ed30629aa8cdf6306ef59dad13651732f08fa3c51e7cdc3947272c1cece8f9e2d3f5
# expires_in=86400
# user_id=100179132 (me)
# user_id=137728934 (Anton)
# fields=country,city,bdate,sex
# api.vk.com/method/friends.get?v=5.81&access_token=85b3240de340e9939ed30629aa8cdf6306ef59dad13651732f08fa3c51e7cdc3947272c1cece8f9e2d3f5&user_id=137728934&fields=country,city,bdate,sex


import sys
import requests
import logging
import pandas as pd
import datetime


def write_to_file(df, filename='report', ftype='csv'):
    if ftype=='csv':
        df.to_csv(f'{filename}.csv', index=False)
    elif ftype=='tsv':
        df.to_csv(f'{filename}.tsv', sep='\t', index=False)
    elif ftype=='json':
        df.to_json(f'{filename}.json', orient='index')
    

def data_handler(access_token, user_id, fields='country,city,bdate,sex', v='5.81'):
    response = requests.get(f'https://api.vk.com/method/friends.get?v={v}&access_token={access_token}&user_id={user_id}&fields={fields}')
    friends = response.json()['response']['items']   
    data = {'first_name': [], 'last_name': [], 'country': [], 'city': [], 'bdate': [], 'sex': []}
    sex_dict = {1: 'female', 2: 'male'}
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
                # bdate_iso = parser.parse(friend['bdate']).isoformat()
                bdate_iso = ''
                if len(friend['bdate'].split('.')) == 3:
                    bdate_iso = datetime.datetime.strptime(friend['bdate'], '%d.%m.%Y').isoformat()
                else:
                    bdate_iso = datetime.datetime.strptime(friend['bdate'], '%d.%m').isoformat()
                data['bdate'].append(bdate_iso)
            else:
                data['bdate'].append('')
                
            if 'sex' in fkeys:
                data['sex'].append(sex_dict[friend['sex']])
            else:
                data['sex'].append('')
    df = pd.DataFrame(data)
    df.sort_values(by=['first_name'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    return df
    
    
if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    print(sys.argv)
    if len(sys.argv) > 5:
        logging.error(f'VK_get_friends program takes 4 arguments (acess_token, user_id, filename, file_format), but {len(sys.argv)} were given')
    elif len(sys.argv) < 2:
        logging.error(f'VK_get_friends program takes at least 2 arguments (acess_token, user_id), but {len(sys.argv)} were given')
        
    elif len(sys.argv) == 3:
        df = data_handler(sys.argv[1], sys.argv[2])
        write_to_file(df)
    elif len(sys.argv) == 4:
        if '=' in sys.argv[3]:
            if sys.argv[3].split('=')[0] == 'file_format':
                df = data_handler(sys.argv[1], sys.argv[2])
                write_to_file(df, ftype=sys.argv[3].split('=')[1])
            elif sys.argv[3].split('=')[0] == 'filename':
                df = data_handler(sys.argv[1], sys.argv[2])
                write_to_file(df, filename=sys.argv[3].split('=')[1])
            else:
                logging.error(f"{sys.argv[3].split('=')[0]} is not name argument. Maybe you mean file_format or filename?")
        else:
            df = data_handler(sys.argv[1], sys.argv[2])
            write_to_file(df, sys.argv[3])
    elif len(sys.argv) == 5:
        df = data_handler(sys.argv[1], sys.argv[2])
        write_to_file(df, sys_argv[3], sys_argv[4])