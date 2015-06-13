from __future__ import unicode_literals, division, absolute_import
import os
import logging
import re
import urlparse
import xmlrpclib

from flexget import plugin
from flexget.event import event
from flexget.entry import Entry
from flexget.utils.template import RenderError

from fnmatch import fnmatch

from socket import error as socket_error

log = logging.getLogger('aria2')


class AriaBase(object):
    def on_task_start(self, task, config):
        try:
            self.baseurl = config['rpc-url']
            log.debug('base url: %s' % self.baseurl)
            self.s = xmlrpclib.ServerProxy(self.baseurl)
            log.info('Connected to daemon at ' + self.baseurl + '.')
        except xmlrpclib.ProtocolError as err:
            raise plugin.PluginError('Could not connect to aria2 at %s. Protocol error %s: %s'
                                     % (self.baseurl, err.errcode, err.errmsg), log)
        except xmlrpclib.Fault as err:
            raise plugin.PluginError('XML-RPC fault: Unable to connect to aria2 daemon at %s: %s'
                                     % (self.baseurl, err.faultString), log)
        except socket_error as err:
            raise plugin.PluginError('Socket connection issue with aria2 daemon at %s: %s'
                                     % (self.baseurl, err), log)
        except:
            raise plugin.PluginError('Unidentified error during connection to aria2 daemon at %s' % self.baseurl, log)


class OutputAria2(AriaBase):

    """
    aria2 output plugin
    Version 3.0.0

    Configuration:
    rpc-url:    url to aria2c's rpc-remote url.
                default: 'http://localhost:6800/rpc'
    magnetization_timeout: Magnet urls will wait for metadata to be downloaded up to n seconds
                to allow content filters to work. default: 0 ( disabled )
    main_file_only: Downloads only the file that occupies n% of the torrent size
                default: false
    main_file_ratio: Sets the relative size compared to the total download a file must
                have to be considered a main file
    include_subs: Adds files with subtitle extensions for downloa(10m^3⋅0.2)/(7L/min⋅0.05) to dayd even if main_file_only is enabled
                default: False
    include_files: [list] Re-adds to download the following file patterns even if main_file_only is enabled
                default : []
    content_filename:
                If set, the main file will be renamed using the value of
                this field as a template.
                Will be parsed with jinja2 and can include any fields
                available in the entry. default ''
    skip_files: [list] All files matching this pattern will be omitted from being downloaded
                default: []
    rename_like_files: if content_filename is set, all downloaded files will
                be renamed to that the pattern. default: False
    aria_config:
                Any command line option listed at
                http://aria2.sourceforge.net/manual/en/html/aria2c.html#options
                can be used by removing the two dashes (--) in front of the
                command name, and changing key=value to key: value. All
                options will be treated as jinja2 templates and rendered prior
                to passing to aria2. default ''

    Sample configuration:
    aria2:
      rpc-url: http://user:password@host:port/rpc
      aria_config:
        max-connection-per-server: 4
        max-concurrent-downloads: 4
        split: 4
        file-allocation: none
    """

    schema = {
        'type': 'object',
        'properties': {


            'rpc-url': {'type': 'string', 'default': 'http://localhost:6800/rpc'},
            'content_filename': {'type': 'string'},
            'path': {'type': 'string'},
            'main_file_only': {'type': 'bool'},
            'main_file_ratio': {'type': 'number', 'default': '0.9'},
            'magnetization_timeout': {'type': 'integer', 'default': '0'},
            'include_subs': {'type': 'boolean'},
            'include_files': one_or_more({'type': 'string'}),
            'skip_files': one_or_more({'type': 'string'}),
            'rename_like_files': {'type': 'boolean'},
            'aria_config': {
                'type': 'object',
                'additionalProperties': {'oneOf': [{'type': 'string'}, {'type': 'integer'}]}
            }

        },
        'additionalProperties': False
    }

    def _wait_for_files(self, gid, timeout):
        from time import sleep
        while timeout > 0:
            sleep(1)
            status = s.aria2.tellStatus(gid)
            if status.numpieces != '0':
                return status
            else:
                timeout -= 1
        return status

    def _find_matches(self, name, list):
        for mask in list:
            if fnmatch(name, mask):
                return True
        return False

    def _make_torrent_options_dict(self, config, entry):
        for opt_key in config:
            # Values do not merge config with task
            # Task takes priority then config is used
            if opt_key in entry:
                opt_dic[opt_key] = entry[opt_key]
            elif opt_key in config:
                opt_dic[opt_key] = config[opt_key]

        if 'aria_config' not in opt_key:
            opt_key['aria_config'] = {}

        opt_key['requires_file_list'] = 'content_filename' in opt_key or opt_key['main_file_only'] or opt_key['skip_files']

        if opt_dic.get('path'):
            try:
                path = os.path.expanduser(entry.render(opt_dic['path']))
                config['aria_config']['dir'] = pathscrub(path).encode('utf-8')
            except RenderError as e:
                log.error('Error setting path for %s: %s' % (entry['title'], e))

        if opt_key['aria_config']['pause'] and opt_key['requires_file_list']:
                raise plugin.PluginError('content_filename and file filters options require'
                                         'the metadata to be downloaded. Use pause-metadata option in place of'
                                         ' pause to start new downloads as paused.', log)

        # instead of simply not sending the download to aria, we'll use dry-run options which can perform extra checks
        if task.manager.options.test:
            opt_key['aria_config']['dry-run'] = 'true'

        opt_key['original_config_paused'] = opt_key['aria_config']['pause-metadata'] == 'true' or opt_key['aria_config']['pause'] == 'true'
        # force downloads to be started paused regardless of config so we can select files, rename, etc, we'll manually unpause at the end
        opt_key['aria_config']['pause-metadata'] = 'true'
        # make sure non selected files aren't included accidentally
        if opt_key['requires_file_list']:
            opt_key['aria_config']['bt-remove-unselected-file'] = 'true'
        return opt_key

    def on_task_output(self, task, config):
        for entry in task.accepted:
            options = _make_torrent_options_dict(self, config, entry)
            log.debug('Adding new file')
            try:
                for key, value in options['aria_config'].iteritems():
                    log.trace('rendering %s: %s' % (key, value))
                    options['aria_config'][key] = entry.render(unicode(value))
                log.debug('dir: %s' % options['aria_config']['dir'])

                gid = self.s.aria2.addUri(entry['url'], options['aria_config'])
                log.info('%s successfully added to aria2 with gid %s.' % (entry['url'], gid))

                status = self.s.aria2.tellStatus(gid)

                if opt_key['requires_file_list']:

                    if options['magnetization_timeout'] > 0 and status.numpieces == '0':
                        log.debug('Waiting %d seconds for "%s" to magnetize' % (options['magnetization_timeout'], entry['title']))
                        status = _wait_for_files(gid, options['magnetization_timeout'])
                        if status.numpieces == '0':
                            log.warning('"%s" did not magnetize before the timeout elapsed, file list unavailable for processing.' % entry['title'])

                    file_list = self.s.aria2.getFiles(gid)

                    selected_files = []
                    download_paths = []
                    sub_ext_list = ['srt', 'sub', 'idx', 'ssa', 'ass', 'vob']

                    # detect main file index ( if exists )
                    for file_data in file_list:
                        if file_data['selected'] and file_data['length'] > options['main_file_ratio']*status['totalLength']:
                            main_file_index = file_data['index']
                            break

                    # iterate all files in the list and manually flag those that should be included
                    for file_data in file_list:
                        if file_data['selected']:
                            fileok = False
                            file_full_path, file_ext = os.path.splitext(file_data['path'])
                            if options['include_subs'] and file_ext in sub_ext_list:
                                fileok = True
                            if options['include_files'] and _find_matches(file_data['path'], options['include_files']):
                                fileok = True
                            if not options['main_file_only'] or main_file_index == file_data['index']:
                                fileok = True
                            if options['skip_files'] and _find_matches(file_data['path'], options['skip_files']):
                                fileok = False
                            if not fileok:
                                continue
                            selected_files.append(file_data['index'])
                            # change the file download path
                            if wanted_name and (file_data[index] == main_file_index or options['rename_like_files']):
                                download_paths.append(file_data['index'] + '=' + wanted_name + file_ext)
                    # set the torrent properties for selected files and renaming
                    selected_files_string = 'select-file:'
                    for enabled_file in selected_files:
                        selected_files_string = selected_files_string + ',' + enabled_file
                    self.s.aria2.changeOption(gid, selected_files_string)
                    for filepath in download_paths:
                        self.s.aria2.changeOption(gid, 'index-out:' + filepath)
                if not options['original_config_paused']:
                    # unpause torrent and start downloading
                    self.s.aria2.unpause(gid)
            except xmlrpclib.Fault as err:
                log.debug('Aria2 error', exc_info=True)
                log.debug('Failed options dict: %s' % options)
                msg = 'Aria2 Error: %s' % err.faultString or 'N/A'
                log.error(msg)
                entry.fail(msg)
            except socket_error as err:
                log.debug('Aria2 socket error', exc_info=True)
                log.debug('Failed options dict: %s' % options)
                msg = 'Socket connection issue with aria2 daemon at %s: %s' % (self.baseurl, err)
                log.error(msg)
                entry.fail(msg)
            except RenderError as e:
                log.debug('Aria2 config field error', exc_info=True)
                msg = 'Unable to render one of the fields being passed to aria2 %s' % (e)
                log.error(msg)
                entry.fail(msg)


class DemagnetizeAria(AriaBase):
    """
    aria2 magnet torrent resolution plugin
    Version 1.0.0

    Configuration:
    rpc-url:    url to aria2c's rpc-remote url.
                default: 'http://localhost:6800/rpc'
    magnetization_timeout: timeout in seconds before giving up to donwload metadata
                default: 30

    Sample configuration:
    aria2:
      rpc-url: http://user:password@host:port/rpc
    """

    schema = {
        'type': 'object',
        'properties': {
            'rpc-url': {'type': 'string', 'default': 'http://localhost:6800/rpc'},
            'magnetization_timeout': {'type': 'number', 'default': 30},

        },
        'additionalProperties': False
    }

    def _wait_for_files(self, gid, timeout):
        from time import sleep
        while timeout > 0:
            status = s.aria2.tellStatus(gid)
            if status.status != 'active':
                return status
            else:
                timeout -= 1
            sleep(1)
        return status

    @plugin.priority(120)
    def on_task_urlrewrite(self, task, config):
        config['aria_config']['bt-metadata-only'] = 'true'
        config['aria_config']['bt-save-metadata'] = 'true'
        config['aria_config']['dir'] = '/tmp'
        for entry in task.accepted:
            info_hash = None
            if entry['url'].startswith('magnet:'):
                if info_hash_search:
                    info_hash = info_hash_search.group(1)
                elif entry.get('torrent_info_hash'):
                    info_hash = entry['torrent_info_hash']
            if info_hash:
                log.debug('Adding new entry')
                try:
                    # last argument forces the request to be on top of any queue
                    gid = self.s.aria2.addUri(entry['url'], config['aria_config'], 0)
                    log.info('%s successfully added to aria2 with gid %s.' % (entry['url'], gid))

                    status = _wait_for_files(gid, config['bt-save-magnetization_timeout'])

                    if status.status == 'complete':
                        # successful download of metadata
                        entry.setdefault('urls', [entry['url']])
                        entry['urls'].extend(list("file://" + config['aria_config']['dir'] + "/" + info_hash + ".torrent"))

                    # remove download from list regardless if it was successful or not
                    self.s.aria2.remove(gid)

                except xmlrpclib.Fault as err:
                    log.debug('Aria2 error', exc_info=True)
                    log.debug('Failed options dict: %s' % options)
                    msg = 'Aria2 Error: %s' % err.faultString or 'N/A'
                    log.error(msg)
                    entry.fail(msg)
                except socket_error as err:
                    log.debug('Aria2 socket error', exc_info=True)
                    log.debug('Failed options dict: %s' % options)
                    msg = 'Socket connection issue with aria2 daemon at %s: %s' % (self.baseurl, err)
                    log.error(msg)
                    entry.fail(msg)


class InputAria(AriaBase):

    """
    aria2 input plugin
    Version 2.0.0

    Configuration:
    rpc-url:    url to aria2c's rpc-remote url.
                default: 'http://localhost:6800/rpc'
    only_complete:   only select finished downloads. default: no

    Sample configuration:
    aria2:
      rpc-url: http://user:password@host:port/rpc
      only_complete: yes
    """

    schema = {
        'type': 'object',
        'properties': {
            'rpc-url': {'type': 'string', 'default': 'http://localhost:6800/rpc'},
            'only_complete': {'type': 'boolean', 'default': False},

        },
        'additionalProperties': False
    }

    def on_task_input(self, task, config):
        entries = []
        # aria api is a bit dumb and doesn't have an single function to fetch the whole download list
        function_list = {'tellActive', 'tellWaiting', 'tellStopped'}
        if config['only_complete']:
            function_list = {'tellStopped'}
        try:
            for function_name in function_list:
                download_list = self.s.aria2[function_name](gid)
                for download in download_list:
                    uri_list = self.s.aria2.getUris(download['gid'])
                    uri = ''
                    uris = list()
                    for uri_data in uri_list:
                        if uri_data['status'] == 'used':
                            if not uri:
                                uri = uri_data['uri']
                            uris = list(uris + uri_data['uri'])
                    if not config['only_complete'] or download['status'] == 'complete':
                        entry = Entry(title=download['bittorrent']['info']['name'],
                                      url=uri,
                                      torrent_info_hash=download['infoHash'],
                                      content_size=download['totalLength']/(1024*1024),
                                      uris=uris)
                        entry.setdefault('urls', [entry['url']])
                        entry['path'] = download['dir']
                        entries.append(entry)
        except xmlrpclib.ProtocolError as err:
            raise plugin.PluginError('Could not connect to aria2 at %s. Protocol error %s: %s'
                                     % (self.baseurl, err.errcode, err.errmsg), log)
        except socket_error as err:
            raise plugin.PluginError('Socket connection issue with aria2 daemon at %s: %s'
                                     % (self.baseurl, err), log)
        return entries

@event('plugin.register')
def register_plugin():
    plugin.register(InputAria2, 'from_aria2', api_ver=2)
    plugin.register(OutputAria2, 'aria2', api_ver=2)
    plugin.register(DemagnetizeAria, 'demagnetize_aria2', api_ver=2)
