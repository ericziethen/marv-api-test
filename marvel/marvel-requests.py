
import json
import logging
import os
import time

import marvel

def get_till_end(*, caller_func, result_limit, start_offset, target_dir,
                 base_file_name, sub_section_func_dict=None, get_id=None):
    logging.info(F'Processing "{base_file_name}"')
    next_offset=start_offset

    while True:
        logging.info(F'Processing Offset "{next_offset}"')
        if get_id:
            results = caller_func(get_id, limit=result_limit, offset=next_offset)
        else:
            results = caller_func(limit=result_limit, offset=next_offset)

        if results and results['data']['count'] > 0:
            # Store the Main Data
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)

            file_name = F'{base_file_name}_{next_offset}_{next_offset + result_limit}.json'
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
        else:
            logging.info('>>> FINISHED, No More Results')
            break

        next_offset += result_limit
        logging.info('Sleep 5 Seconds')
        time.sleep(5)


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
        sub_section_func_dict={
            'CHARACTERS': comics.characters,
            'CREATORS': comics.creators,
        }
    )


def main():
    #logging.basicConfig(filename='log.log', encoding='utf-8', level=logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        handlers=[logging.FileHandler("log.log"),
                                  logging.StreamHandler()])

    get_marvel_data()

if __name__ == '__main__':
    PUBLIC_KEY = os.getenv('PUBLIC_KEY')
    assert PUBLIC_KEY

    PRIVATE_KEY = os.getenv('PRIVATE_KEY')
    assert PRIVATE_KEY

    main()
