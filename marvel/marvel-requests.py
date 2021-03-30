
import http
import json
import logging
import os
import time
import urllib

import marvel


def find_file_with_prefix(prefix, search_dir):
    for file_name in os.listdir(search_dir):
        if file_name.lower().startswith(prefix.lower()) and file_name.lower().endswith('.json'):
            file_path = os.path.join(search_dir, file_name)
            return file_path
    return None


def get_till_end(*, caller_func, result_limit, start_offset, target_dir,
                 base_file_name, sub_section_func_dict=None, get_id=None):
    logging.info(F'Processing "{base_file_name}"')
    next_offset=start_offset

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
            if get_id:
                results = caller_func(get_id, limit=result_limit, offset=next_offset)
            else:
                results = caller_func(limit=result_limit, offset=next_offset)

        if results and results['data']['count'] > 0:
            # Store the Main Data
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            result_start = results['data']['offset'] + 1
            result_end = results['data']['offset'] + results['data']['count']
            file_name = F'''{base_file_name}_{result_start}_{result_end}.json'''

            file_path = os.path.join(target_dir, file_name)
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
                                sub_section_func_dict=None,
                                get_id=result_id,
                            )
            if results['data']['count'] < result_limit:
                logging.info(F'''>>> FINISHED, JSON count "{results['data']['count']} less than max "{result_limit}"''')
                break
        else:
            logging.info('>>> FINISHED, No More Results')
            break

        next_offset += result_limit


def get_marvel_data():
    max_results = 100

    m = marvel.Marvel(PUBLIC_KEY, PRIVATE_KEY)

    # Get Characters
    characters = m.characters
    get_till_end(
        caller_func=characters.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=R'DATA_TEST\CHARACTERS',
        base_file_name='CHARACTERS',
        sub_section_func_dict=None
    )

    # Get Events (with Characters and Creators)
    events = m.events
    get_till_end(
        caller_func=events.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=R'DATA_TEST\EVENTS',
        base_file_name='EVENTS',
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
        target_dir=R'DATA_TEST\CREATORS',
        base_file_name='CREATORS',
        sub_section_func_dict=None
    )

    # Get Comics (with Characters and Creators)
    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=R'DATA_TEST\COMICS',
        base_file_name='COMICS',
        sub_section_func_dict=None
    )

    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=R'DATA_TEST\COMICS',
        base_file_name='COMICS',
        sub_section_func_dict={
            'CHARACTERS': comics.characters,
        }
    )

    comics = m.comics
    get_till_end(
        caller_func=comics.all,
        result_limit=max_results,
        start_offset=0,
        target_dir=R'DATA_TEST\COMICS',
        base_file_name='COMICS',
        sub_section_func_dict={
            'CREATORS': comics.creators,
        }
    )


def main():
    #logging.basicConfig(filename='log.log', encoding='utf-8', level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler("log.log"),
                                  logging.StreamHandler()])

    # Sometimes requests throws an exception, so we can keep tryng a few times
    max_retries = 20
    tries = 1

    while tries <= max_retries:
        logging.info(F'Try "{tries}"')
        try:
            get_marvel_data()
            break
        except urllib.error.HTTPError as error:
            logging.error(F'HTTPError: "{error.code}" - "{error}"')
        except urllib.error.URLError as error:
            logging.error(F'URLError: "{error.code}" - "{error}"')
        except http.client.RemoteDisconnected as error:
            logging.error(F'RemoteDisconnected: "{error.code}" - "{error}"')

        tries += 1

    logging.info(F'Finnished after "{tries}" tries')


if __name__ == '__main__':
    PUBLIC_KEY = os.getenv('PUBLIC_KEY')
    assert PUBLIC_KEY

    PRIVATE_KEY = os.getenv('PRIVATE_KEY')
    assert PRIVATE_KEY

    main()
