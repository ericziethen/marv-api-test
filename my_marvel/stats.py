
import json
import logging
import os


def get_stats(dest_file, dir_dict):
    stats_dict = {}

    logging.info('### START ###')

    for entity, dir_list in dir_dict.items():
        id_set = set()
        totals_set = set()
        entity_dict = {}

        if entity in stats_dict:
            raise ValueError('Duplicate Stats configuration')

        # Get the data
        for my_dir in dir_list:
            if not os.path.exists(my_dir):
                logging.info('Cannot find Dir', my_dir)

            for file_name in os.listdir(my_dir):
                if not file_name.lower().endswith('json'):
                    continue
                file_path = os.path.join(my_dir, file_name)
                with open(file_path, 'r', encoding='utf-8') as file_ptr:
                    results = json.load(file_ptr)
                    totals_set.add(results['data']['total'])
                    for result in results['data']['results']:
                        id_set.add(result['id'])

        entity_dict['unique_id_count'] = len(id_set)
        entity_dict['totals'] = list(totals_set)
        stats_dict[entity] = entity_dict

    logging.info('### DONE ###')

    with open(dest_file, 'w', encoding='utf-8') as file_ptr:
        json.dump(stats_dict, file_ptr, indent=4, sort_keys=False, ensure_ascii=False)


def main():
    base_dir = R'D:\# Eric Projects\marv-api-test\DATA_TEST\2021.04.03\data'

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

    get_stats(os.path.join(base_dir, 'stats.json'), dir_dict)

if __name__ == '__main__':
    main()
