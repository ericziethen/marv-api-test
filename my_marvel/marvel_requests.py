
import http
import json
import logging
import os
import time
import urllib
import urllib3

import marvel
import requests

from my_marvel import stats

NAME_ORDER_MAPPING = {
    'events': 'name',
    'creators': 'lastName,firstName',
    'comics': 'title',
    'characters': 'name',
}


def get_order_type_fron_caller_func(func_name):
    '''
        Functions either have a single entity
            -> characters.all
        Or multiple entities (we want the last one)
            -> events.creators
    '''

    name_list = func_name.split('.')
    for name in name_list[::-1]:
        if name.lower() in NAME_ORDER_MAPPING:
            return NAME_ORDER_MAPPING[name.lower()]

    raise ValueError(F'Cannot find ordertype for {func_name}')


def find_file_with_prefix(prefix, search_dir):
    if os.path.exists(search_dir):
        for file_name in os.listdir(search_dir):
            if file_name.lower().startswith(prefix.lower()) and file_name.lower().endswith('.json'):
                file_path = os.path.join(search_dir, file_name)
                return file_path
    return None


def get_till_end(*, caller_func, result_limit, start_offset, target_dir,
                 base_file_name, order_type, sub_section_func_dict=None, get_id=None,
                 stop_after_count=None):
    logging.info(F'Processing "{base_file_name}"')
    next_offset=start_offset

    result_count = 0
    while True:
        logging.info(F'Processing Offset "{next_offset}"')
        results = None

        guess_file_name = F'{base_file_name}_{next_offset + 1}_'

        found_file_path = find_file_with_prefix(guess_file_name, target_dir)
        if found_file_path:
            logging.info(F'Found file to skip request "{found_file_path}"')
            with open(found_file_path, 'r', encoding='utf-8') as file_ptr:
                results = json.load(file_ptr)
        else:
            logging.info('Sleep 5 Seconds before request')
            time.sleep(5)
            if not order_type:
                order_type = get_order_type_fron_caller_func(caller_func.__name__)
            if get_id:
                results = caller_func(get_id, limit=result_limit, offset=next_offset, orderBy=order_type)
            else:
                results = caller_func(limit=result_limit, offset=next_offset, orderBy=order_type)

        if results and results['data']['count'] > 0:
            result_count += results['data']['count']

            # Store the Main Data
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            result_start = results['data']['offset'] + 1
            result_end = results['data']['offset'] + results['data']['count']
            file_name = F'''{base_file_name}_{result_start}_{result_end}.json'''

            file_path = os.path.join(target_dir, file_name)
            if found_file_path is None:
                with open(file_path, 'w', encoding='utf-8') as file_ptr:
                    json.dump(results, file_ptr, indent=4, sort_keys=False, ensure_ascii=False)

            # Check for Subsections we need
            if sub_section_func_dict:
                logging.info('Check Subsections', sub_section_func_dict)
                for result in results['data']['results']:
                    result_id = result['id']
                    #logging.info('check_result id', result_id)
                    for sub_name, sub_func in sub_section_func_dict.items():
                        key = sub_name.lower().strip()
                        #logging.info(F'Sub Key Name: "{key}", in result: "{key in result}"')
                        if key in result and (result[key]['returned'] < result[key]['available']):
                            logging.info(F'  >> Need to Process Sub Section "{sub_name}" for id "{result_id}"')

                            get_till_end(
                                caller_func=sub_func,
                                result_limit=result_limit,
                                start_offset=0,
                                target_dir=os.path.join(target_dir, sub_name),
                                base_file_name=F'{base_file_name}__{result_id}__{sub_name}',
                                order_type=None,
                                sub_section_func_dict=None,
                                get_id=result_id,
                            )
            if (results['data']['count'] < result_limit) or\
               (results['data']['total'] <= results['data']['count']):
                logging.info(F'''>>> FINISHED, JSON count "{results['data']['count']} less than max "{result_limit}"''')
                break
        else:
            logging.info('>>> FINISHED, No More Results')
            break

        # Next offset with some overlap to capture some movement
        next_offset += result_limit
        if result_limit > 10:
            next_offset -= 6

        if stop_after_count:
            if result_count >= stop_after_count:
                logging.info(F'>>> FINISHED, Only asked for "{stop_after_count}" results, got "{result_count}"')
                break


def add_stats(base_dir):
    dir_dict = {}

    dir_dict['COMICS'] = [
        os.path.join(base_dir, 'COMICS'),
        os.path.join(base_dir, 'COMICS', 'RecentlyModified'),
    ]

    dir_dict['CHARACTERS'] = [
        os.path.join(base_dir, 'CHARACTERS'),
        os.path.join(base_dir, 'COMICS/CHARACTERS'),
        os.path.join(base_dir, 'EVENTS/CHARACTERS'),
    ]

    dir_dict['CREATORS'] = [
        os.path.join(base_dir, 'CREATORS'),
        os.path.join(base_dir, 'COMICS/CREATORS'),
        os.path.join(base_dir, 'EVENTS/CREATORS'),
    ]

    dir_dict['EVENTS'] = [
        os.path.join(base_dir, 'EVENTS'),
    ]

    stats.get_stats(os.path.join(base_dir, 'stats.json'), dir_dict)


def get_marvel_data(*, public_key, private_key, target_dir):
    max_results = 100

    m = marvel.Marvel(public_key, private_key)

    # Get Characters
    characters = m.characters
    get_till_end(
        caller_func=characters.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'CHARACTERS'),
        base_file_name='CHARACTERS',
        order_type='name',
        sub_section_func_dict=None
    )

    # Get Events (with Characters and Creators)
    events = m.events
    get_till_end(
        caller_func=events.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'EVENTS'),
        base_file_name='EVENTS',
        order_type='name',
        sub_section_func_dict={
            'CHARACTERS': events.characters,
            'CREATORS': events.creators,
        }
    )

    # Get Creators
    creators = m.creators
    get_till_end(
        caller_func=creators.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'CREATORS'),
        base_file_name='CREATORS',
        order_type='lastName,firstName',
        sub_section_func_dict=None
    )

    # Get Comics (with Characters and Creators)
    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'COMICS'),
        base_file_name='COMICS',
        order_type='title',
        sub_section_func_dict=None
    )

    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'COMICS'),
        base_file_name='COMICS',
        order_type='title',
        sub_section_func_dict={
            'CHARACTERS': comics.characters,
        }
    )

    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'COMICS'),
        base_file_name='COMICS',
        order_type='title',
        sub_section_func_dict={
            'CREATORS': comics.creators,
        }
    )

    # Get the last view modified items in case we missed some changes while scraping
    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=os.path.join(target_dir, 'COMICS', 'RecentlyModified'),
        base_file_name='COMICS',
        order_type='-modified',
        sub_section_func_dict=None,
        stop_after_count=300,
    )


def get_data(*, log_path, public_key, private_key, target_dir):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler(log_path),
                                  logging.StreamHandler()])

    # Sometimes requests throws an exception, so we can keep tryng a few times
    max_retries = 20
    tries = 1

    while tries <= max_retries:
        logging.info(F'Try "{tries}"')
        try:
            get_marvel_data(public_key=public_key, private_key=private_key, target_dir=target_dir)
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError,
                requests.exceptions.SSLError,
                requests.exceptions.Timeout,
                requests.RequestException,
                urllib.error.HTTPError,
                urllib.error.URLError,
                urllib3.exceptions.ProtocolError,
                http.client.RemoteDisconnected
                ) as error:
            logging.error(F'EXCEPTION: {type(error).__name__} - "{error}"')

        tries += 1

    logging.info(F'Finnished after "{tries}" tries')

    # Find Duplicate IDs
    possible_sub_dirs = ['CHARACTERS', 'EVENTS', 'CREATORS', 'COMICS']
    for sub_dir in possible_sub_dirs:
        sub_dir_path = os.path.join(target_dir, sub_dir)
        if os.path.exists(sub_dir_path):
            duplicate_dir_path = os.path.join(target_dir, 'Duplicates', sub_dir)
            export_duplicate_ids(sub_dir_path, duplicate_dir_path, F'{sub_dir}_DUPLICATE__')

    # Get the stats
    add_stats(target_dir)



def export_duplicate_ids(search_dir, save_dir, file_prefix):
    id_file_path_dict = {}

    # Build a list of ids with file_path
    for file_name in os.listdir(search_dir):
        if not file_name.lower().endswith('.json'):
            continue

        file_path = os.path.join(search_dir, file_name)
        with open(file_path, 'r', encoding='utf-8') as file_ptr:
            results = json.load(file_ptr)

            for entry in results['data']['results']:
                entry_id = entry['id']
                if entry_id not in id_file_path_dict:
                    id_file_path_dict[entry_id] = []
                id_file_path_dict[entry_id].append(file_path)

    # Build up Lists of Duplicate ID's as well as files to process
    duplicate_id_set = set()
    files_to_check_set = set()

    for myid, path_list in id_file_path_dict.items():
        if len(path_list) > 1:
            #print(F'Add "{myid}" ({type(myid)}) to ({type(duplicate_id_set)}) - "{duplicate_id_set}"')
            duplicate_id_set.add(myid)
            for mypath in path_list:
                files_to_check_set.add(mypath)

    # Create the duplicate_dict
    duplicate_id_dict = {}
    if duplicate_id_set and not os.path.exists(save_dir):
        os.makedirs(save_dir)

    for file_path in files_to_check_set:
        with open(file_path, 'r', encoding='utf-8') as file_ptr:
            results = json.load(file_ptr)
            for entry in results['data']['results']:
                entry_id = entry['id']
                if entry_id in duplicate_id_set:
                    if entry_id not in duplicate_id_dict:
                        duplicate_id_dict[entry_id] = []
                    duplicate_id_dict[entry_id].append(entry)

    # Create the new files
    for myid, data_list in duplicate_id_dict.items():
        for count, entry in enumerate(data_list, start=1):
            new_file_name = F'{file_prefix}__ID_{myid}_{count}.json'
            new_file_path = os.path.join(save_dir, new_file_name)

            with open(new_file_path, 'w', encoding='utf-8') as file_ptr:
                json.dump(data_list, file_ptr, indent=4, sort_keys=False, ensure_ascii=False)


if __name__ == '__main__':
    export_duplicate_ids(
        R'D:\temp\zipTest\data\tmp\COMICS',
        R'D:\temp\zipTest\data\tmp\# Duplicates\COMICS_ID_DUPLICATES',
        'COMICS_DUPLICATE__')
