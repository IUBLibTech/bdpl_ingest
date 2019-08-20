import pickle
import os
import datetime

def premis_dict(timestamp, event_type, event_outcome, event_detail, event_detail_note, agent_id):
    temp_dict = {}
    temp_dict['eventType'] = event_type
    temp_dict['eventOutcomeDetail'] = event_outcome
    temp_dict['timestamp'] = timestamp
    temp_dict['eventDetailInfo'] = event_detail
    temp_dict['eventDetailInfo_additional'] = event_detail_note
    temp_dict['linkingAgentIDvalue'] = agent_id
    return temp_dict

def main():
    print('\nNOTE: timestamp should be recorded immediately before running manual operation.')
    timestamp = input('\nEnter timestamp, path to log file, or hit "Enter" to record timestamp: ')

    if timestamp == '':
        timestamp = str(datetime.datetime.now())
    elif os.path.isfile(timestamp):
        timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(timestamp)).isoformat()

    event_list = ['replication', 'disk image creation', 'normalization', 'forensic feature analysis', 'format identification', 'message digest calculation', 'metadata extraction', 'forensic feature analysis', 'virus check']

    while True:
        
        event_type = input('\nEnter "event type"; be sure to use a term defined in PREMIS data dictionary: ')
        
        if not event_type in event_list:
            continue
        else:
            break
        
        
    event_outcome = 0

    event_detail = input('\nEnter command line option or note GUI operation: ')

    event_notes_dict = {'replication' : 'Created a copy of an object that is, bit-wise, identical to the original.', 'disk image creation' : 'Extracted a disk image from the physical information carrier.', 'normalization' : 'Transformed object to an institutionally supported preservation format.', 'virus check' : 'Scanned files for malicious programs.', 'format identification' : 'Determined file format and version numbers for content recorded in the PRONOM format registry.', 'metadata extraction' : '',  'forensic feature analysis' : '', 'message digest calculation' : 'Extracted information about the structure and characteristics of content, including file checksums.'}

    if event_type == 'metadata extraction' or event_type == 'forensic feature analysis':
        event_detail_note = inpit('\nEnter description of event: ')
    else:
        event_detail_note = event_notes_dict[event_type]
        
    agent_id = input('\nEnter software name and version: ')

    while True:
        target = input('\nEnter path to barcode folder: ')
        
        if os.path.exists(target):
            break

    temp_dir = os.path.join(target, 'temp')
    if not os.path.exists(temp_dir):
        os.mkdir(temp_dir)
    
    temp_file = os.path.join(temp_dir, 'premis_list.txt')
    
    premis_list = []
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, 'rb') as file:
            premis_list = pickle.load(file)

    
    manual_info = premis_dict(timestamp, event_type, event_outcome, event_detail, event_detail_note, agent_id)
    
    premis_list.append(manual_info)
    
    with open(temp_file, 'wb') as file:
        pickle.dump(premis_list, file)

if __name__ == '__main__':
    main()

