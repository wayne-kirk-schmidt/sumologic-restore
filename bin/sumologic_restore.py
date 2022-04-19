#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Exaplanation: Sumo Logic Restore! Recovering content from a Sumo Logic backup

Usage:
   $ python  sumologic_restore  [ options ]

Style:
   Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html

    @name           sumologic_restore
    @version        2.00
    @author-name    Wayne Schmidt
    @author-email   wschmidt@sumologic.com
    @license-name   Apache 2.0
    @license-url    https://www.apache.org/licenses/LICENSE-2.0
"""

__version__ = 2.00
__author__ = "Wayne Schmidt (wschmidt@sumologic.com)"

### beginning ###
import json
import os
import sys
import time
import datetime
import argparse
import configparser
import http
import pandas
import requests

sys.dont_write_bytecode = 1

MY_CFG = 'undefined'
PARSER = argparse.ArgumentParser(description="""

Allows you to restore content from a backup folder

""")

PARSER.add_argument("-a", metavar='<secret>', dest='MY_SECRET', \
                    help="set api (format: <key>:<secret>) ")

PARSER.add_argument("-k", metavar='<client>', dest='MY_CLIENT', \
                    help="set key (format: <site>_<orgid>) ")

PARSER.add_argument("-c", metavar='<configfile>', dest='CONFIG', \
                    help="Specify config file")

PARSER.add_argument("-r", metavar='<restorepoint>', dest='RESTOREPOINT', \
                    help="Specify specific folder")

PARSER.add_argument("-b", metavar='<backupdir>', dest='BACKUPDIR', \
                    help="Specify backup directory")

PARSER.add_argument("-v", type=int, default=0, metavar='<verbose>', \
                    dest='verbose', help="increase verbosity")

ARGS = PARSER.parse_args()

DELAY_TIME = .5

RESTOREMAP = dict()

RESTORERECORD = dict()

REPORTTAG = 'sumologic-restore'

RESTORELOGDIR = '/var/tmp'

RIGHTNOW = datetime.datetime.now()

DATESTAMP = RIGHTNOW.strftime('%Y%m%d')

TIMESTAMP = RIGHTNOW.strftime('%H%M%S')

def resolve_option_variables():
    """
    Validates and confirms all necessary variables for the script
    """

    if ARGS.MY_SECRET:
        (keyname, keysecret) = ARGS.MY_SECRET.split(':')
        os.environ['SUMO_UID'] = keyname
        os.environ['SUMO_KEY'] = keysecret

    if ARGS.MY_CLIENT:
        (deployment, organizationid) = ARGS.MY_CLIENT.split('_')
        os.environ['SUMO_LOC'] = deployment
        os.environ['SUMO_ORG'] = organizationid

def resolve_config_variables():
    """
    Validates and confirms all necessary variables for the script
    """

    if ARGS.CONFIG:
        cfgfile = os.path.abspath(ARGS.CONFIG)
        configobj = configparser.ConfigParser()
        configobj.optionxform = str
        configobj.read(cfgfile)

        if ARGS.verbose > 8:
            print('Displaying Config Contents:')
            print(dict(configobj.items('Default')))

        if configobj.has_option("Default", "SUMO_TAG"):
            os.environ['SUMO_TAG'] = configobj.get("Default", "SUMO_TAG")

        if configobj.has_option("Default", "SUMO_UID"):
            os.environ['SUMO_UID'] = configobj.get("Default", "SUMO_UID")

        if configobj.has_option("Default", "SUMO_KEY"):
            os.environ['SUMO_KEY'] = configobj.get("Default", "SUMO_KEY")

        if configobj.has_option("Default", "SUMO_LOC"):
            os.environ['SUMO_LOC'] = configobj.get("Default", "SUMO_LOC")

        if configobj.has_option("Default", "SUMO_END"):
            os.environ['SUMO_END'] = configobj.get("Default", "SUMO_END")

        if configobj.has_option("Default", "SUMO_ORG"):
            os.environ['SUMO_ORG'] = configobj.get("Default", "SUMO_ORG")

def initialize_variables():
    """
    Validates and confirms all necessary variables for the script
    """

    resolve_option_variables()

    resolve_config_variables()

    try:
        my_uid = os.environ['SUMO_UID']
        my_key = os.environ['SUMO_KEY']

    except KeyError as myerror:
        print('Environment Variable Not Set :: {} '.format(myerror.args[0]))

    return my_uid, my_key

( sumo_uid, sumo_key ) = initialize_variables()

def create_restore_point(source):
    """
    Create a restore folder for all of the content restored
    """

    restore_folder = '.'.join((REPORTTAG, DATESTAMP, TIMESTAMP))
    personal_folder_id = source.get_myfolders()['id']
    restore_folder_id = source.make_folder(restore_folder, personal_folder_id)['id']

    return restore_folder_id

def read_backup_manifest(backups):
    """
    Read the backup manifest to determine restore targets
    """

    manifestfile = '{}/manifest/sumologic-backup.csv'.format(backups)

    dataframe = pandas.read_csv(manifestfile, header=0)

    return dataframe

def create_restore_folders(source, restoredf, restoreid):
    """
    Create all of the intermediary folders to restore content
    """

    folderdataframe = restoredf.loc[restoredf['my_type'] == 'Folder']

    bpathframe = folderdataframe[['backup_path']]

    bplist = bpathframe.values.tolist()

    for bpelement in bplist:
        counter = 0
        createdid = restoreid
        pathlist = list()
        for bpid in bpelement[0].split('/'):
            dirnamedf = restoredf.loc[restoredf['uid_myself'] == bpid]
            foldername = dirnamedf['my_name'].values.tolist()[0]
            foldername = str(foldername)
            foldername = foldername.replace("/","")
            if counter == 0:
                parentid = restoreid
                restorepath = foldername
                pathlist.append(foldername)
            else:
                restorepath = '/'.join(pathlist)
                parentid = RESTOREMAP[restorepath]
                pathlist.append(foldername)
                restorepath = '/'.join(pathlist)
            if restorepath not in RESTOREMAP:
                createdid = source.make_folder(foldername, parentid)['id']
                if ARGS.verbose > 6:
                    print('Created Restore-Point: {}'.format(restorepath))
                RESTOREMAP[restorepath] = createdid
            counter = counter + 1

def restore_content(source, backupdf):
    """
    Run through the manifest again to restore content to the right folders
    """

    folderdataframe = backupdf.loc[backupdf['my_type'] != 'Folder']

    bpathframe = folderdataframe[['my_path']]

    bplist = bpathframe.values.tolist()

    for bpelement in bplist:
        manifestpath = bpelement[0]
        backuppartdf = backupdf.loc[backupdf['my_path'] == manifestpath ]
        backuppartchunk = backuppartdf[['backup_path']].values.tolist()[0][0]
        sourcefile = '{}/content/{}.json'.format(ARGS.BACKUPDIR,backuppartchunk)
        contentdir = os.path.dirname(manifestpath.lstrip("/"))
        parentid = RESTOREMAP[contentdir]
        if ARGS.verbose > 6:
            print('Destination: {} Source: {}'.format(parentid, sourcefile))
            load_restore_content(source, parentid, sourcefile)

def load_restore_content(source, parentid, sourcefile):
    """
    Load the identified content into Sumo Logic
    """
    with open(sourcefile, "r") as sourceobject:
        jsonpayload = json.load(sourceobject)
        result = source.start_import_job(parentid, jsonpayload)
        jobid = result['id']
        status = source.check_import_job_status(parentid, jobid)
        if ARGS.verbose > 8:
            print('STATUS: {}'.format(status['status']))
        while status['status'] == 'InProgress':
            status = source.check_import_job_status(parentid, jobid)
            if ARGS.verbose > 8:
                print('STATUS: {}'.format(status['status']))
            time.sleep(DELAY_TIME)

def build_details(source, parent_name, parent_oid_path, child):
    """
    Build the details for the client entry. If a folder recurse
    """

    my_type = child['itemType']
    uid_myself = child['id']
    uid_parent = child['parentId']

    my_name = child['name']

    if ARGS.verbose > 6:
        print('Cataloging Restored Content: {}'.format(my_name))

    my_path_list = ( parent_name, my_name )
    my_path_name = '/'.join(my_path_list)

    my_oid_list = ( parent_oid_path, uid_myself )
    my_oid_path = '/'.join(my_oid_list)

    if my_type == "Folder":
        content_list = source.get_myfolder(uid_myself)
        for content_child in content_list['children']:
            build_details(source, my_path_name, my_oid_path, content_child)

    RESTORERECORD[uid_myself] = dict()
    RESTORERECORD[uid_myself]["parent"] = uid_parent
    RESTORERECORD[uid_myself]["myself"] = uid_myself
    RESTORERECORD[uid_myself]["name"] = my_name
    RESTORERECORD[uid_myself]["path"] = my_path_name
    RESTORERECORD[uid_myself]["backupname"] = uid_myself
    RESTORERECORD[uid_myself]["backuppath"] = my_oid_path
    RESTORERECORD[uid_myself]["type"] = my_type

def create_restore_manifest_file():
    """
    Now display the output we want from the RESTORERECORD data structure we made.
    """
    manifestname = '{}.{}.{}.csv'.format(REPORTTAG, DATESTAMP, TIMESTAMP)
    manifestfile = os.path.join(RESTORELOGDIR, manifestname)

    if ARGS.verbose > 6:
        print('Creating Restore-Manifest: {}'.format(manifestfile))

    with open(manifestfile, 'a') as manifestobject:
        manifestobject.write('{},{},{},{},{},{},{}\n'.format("uid_myself", "uid_parent", \
                             "my_type", "my_name", "my_path", "backup_oid", "backup_path"))

        for content_item in RESTORERECORD:
            uid_parent = RESTORERECORD[content_item]["parent"]
            uid_myself = RESTORERECORD[content_item]["myself"]
            my_name = RESTORERECORD[content_item]["name"]
            my_path = RESTORERECORD[content_item]["path"]
            my_type = RESTORERECORD[content_item]["type"]
            my_backupname = RESTORERECORD[content_item]["backupname"]
            my_backuppath = RESTORERECORD[content_item]["backuppath"]

            manifestobject.write('{},{},{},\"{}\",\"{}\",{},{}\n'.format(uid_myself, uid_parent, \
                                 my_type, my_name, my_path, my_backupname, my_backuppath))

def create_restore_manifest(source,restoreid):
    """
    Create a manifest of the materials restored
    """

    content_list = source.get_myfolder(restoreid)
    parent_base_path = content_list['id']
    uid_myself = content_list['id']
    uid_parent = content_list['parentId']
    parent_name = "/" + content_list['name']

    RESTORERECORD[uid_myself] = dict()
    RESTORERECORD[uid_myself]["parent"] = uid_parent
    RESTORERECORD[uid_myself]["myself"] = uid_myself
    RESTORERECORD[uid_myself]["name"] = parent_name
    RESTORERECORD[uid_myself]["path"] = "/" + parent_name
    RESTORERECORD[uid_myself]["backupname"] = parent_name
    RESTORERECORD[uid_myself]["backuppath"] = parent_base_path
    RESTORERECORD[uid_myself]["type"] = 'Folder'

    for child in content_list['children']:
        build_details(source, parent_name, parent_base_path, child)

def main():
    """
    Setup the Sumo API connection, using the required tuple of region, id, and key.
    Once done, then run through the commands required
    """

    if ARGS.verbose > 3:
        print("Step-01: - Authenticating")

    source = SumoApiClient(sumo_uid, sumo_key)

    if ARGS.verbose > 3:
        print("Step-02: - Creating Restore Point Folder")

    restoreid = create_restore_point(source)

    if ARGS.verbose > 3:
        print("Step-03: - Reading Manifest")

    manifestdf = read_backup_manifest(ARGS.BACKUPDIR)

    if ARGS.verbose > 3:
        print("Step-04: - Creating Intermediate Backup Folders")

    create_restore_folders(source,manifestdf,restoreid)

    if ARGS.verbose > 3:
        print("Step-05: - Restoring Content to Backup Folders")

    restore_content(source,manifestdf)

    if ARGS.verbose > 3:
        print("Step-06: - Write Out Restored Content Manifest")

    create_restore_manifest(source,restoreid)
    create_restore_manifest_file()

### class ###

class SumoApiClient():
    """
    This is defined SumoLogic API Client
    The class includes the HTTP methods, cmdlets, and init methods
    """

    def __init__(self, access_id, access_key, endpoint=None, cookieFile='cookies.txt'):
        """
        Initializes the Sumo Logic object
        """
        self.session = requests.Session()
        self.session.auth = (access_id, access_key)
        self.session.headers = {'content-type': 'application/json', \
            'accept': 'application/json'}
        cookiejar = http.cookiejar.FileCookieJar(cookieFile)
        self.session.cookies = cookiejar
        if endpoint is None:
            self.endpoint = self._get_endpoint()
        elif len(endpoint) < 3:
            self.endpoint = 'https://api.' + endpoint + '.sumologic.com/api'
        else:
            self.endpoint = endpoint
        if self.endpoint[-1:] == "/":
            raise Exception("Endpoint should not end with a slash character")

    def _get_endpoint(self):
        """
        SumoLogic REST API endpoint changes based on the geo location of the client.
        It contacts the default REST endpoint and resolves the 401 to get the right endpoint.
        """
        self.endpoint = 'https://api.sumologic.com/api'
        self.response = self.session.get('https://api.sumologic.com/api/v1/collectors')
        endpoint = self.response.url.replace('/v1/collectors', '')
        return endpoint

    def delete(self, method, params=None, headers=None, data=None):
        """
        Defines a Sumo Logic Delete operation
        """
        response = self.session.delete(self.endpoint + method, \
            params=params, headers=headers, data=data)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def get(self, method, params=None, headers=None):
        """
        Defines a Sumo Logic Get operation
        """
        response = self.session.get(self.endpoint + method, \
            params=params, headers=headers)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def post(self, method, data, headers=None, params=None):
        """
        Defines a Sumo Logic Post operation
        """
        response = self.session.post(self.endpoint + method, \
            data=json.dumps(data), headers=headers, params=params)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

    def put(self, method, data, headers=None, params=None):
        """
        Defines a Sumo Logic Put operation
        """
        response = self.session.put(self.endpoint + method, \
            data=json.dumps(data), headers=headers, params=params)
        if response.status_code != 200:
            response.reason = response.text
        response.raise_for_status()
        return response

### class ###

### methods ###

    def get_myfolders(self):
        """
        This uses a GET to retrieve all connection information.
        """
        url = "/v2/content/folders/personal/"
        body = self.get(url).text
        results = json.loads(body)
        return results

    def get_myfolder(self, myself):
        """
        This uses a GET to retrieve single connection information.
        """
        url = "/v2/content/folders/" + str(myself)
        body = self.get(url).text
        results = json.loads(body)
        time.sleep(DELAY_TIME)
        return results

    def make_folder(self, myname, myparent):
        """
        Create a folder
        """

        folderpayload = dict()
        folderpayload['name'] = str(myname)
        folderpayload['description'] = str(myname)
        folderpayload['parentId'] = str(myparent)

        url = "/v2/content/folders"
        body = self.post(url,data=folderpayload).text
        results = json.loads(body)
        time.sleep(DELAY_TIME)
        return results

    def get_globalfolders(self):
        """
        This uses a GET to retrieve all connection information.
        """
        url = "/v2/content/folders/global"
        body = self.get(url).text
        results = json.loads(body)
        return results

    def get_globalfolder(self, myself):
        """
        This uses a GET to retrieve single connection information.
        """
        url = "/v2/content/folders/global/" + str(myself)
        body = self.get(url).text
        results = json.loads(body)
        return results

    def start_export_job(self, myself):
        """
        This starts an export job by passing in the content ID
        """
        url = "/v2/content/" + str(myself) + "/export"
        body = self.post(url, data=str(myself)).text
        results = json.loads(body)
        return results

    def check_export_job_status(self, myself,jobid):
        """
        This starts an export job by passing in the content ID
        """
        url = "/v2/content/" + str(myself) + "/export/" + str(jobid) + "/status"
        time.sleep(DELAY_TIME)
        body = self.get(url).text
        results = json.loads(body)
        return results

    def check_export_job_result(self, myself,jobid):
        """
        This starts an export job by passing in the content ID
        """
        url = "/v2/content/" + str(myself) + "/export/" + str(jobid) + "/result"
        time.sleep(DELAY_TIME)
        body = self.get(url).text
        results = json.loads(body)
        return results

    def start_import_job(self, folderid, content, adminmode=False, overwrite=False):
        """
        This starts an import job by passing in the content ID and content
        """
        headers = {'isAdminMode': str(adminmode).lower()}
        params = {'overwrite': str(overwrite).lower()}
        url = "/v2/content/folders/" + str(folderid) + "/import"
        time.sleep(DELAY_TIME)
        body = self.post(url, content, headers=headers, params=params).text
        results = json.loads(body)
        return results

    def check_import_job_status(self, folderid, jobid, adminmode=False):
        """
        This checks on the status of an import content job
        """
        headers = {'isAdminMode': str(adminmode).lower()}
        url = "/v2/content/folders/" + str(folderid) + "/import/" + str(jobid) + "/status"
        time.sleep(DELAY_TIME)
        body = self.get(url, headers=headers).text
        results = json.loads(body)
        return results

### methods ###

if __name__ == '__main__':
    main()
