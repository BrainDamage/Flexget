from __future__ import unicode_literals, division, absolute_import
import logging

from flexget import plugin
from flexget.event import event

log = logging.getLogger('builtins')


def all_builtins():
    """Helper function to return an iterator over all builtin plugins."""
    return (plugin for plugin in plugin.plugins.itervalues() if plugin.builtin)


class PluginDisableBuiltins(object):
    """Disables all (or specific) builtin plugins from a task."""

    def __init__(self):
        # cannot trust that on_task_start would have been executed
        self.disabled = []

    @property
    def schema(self):
        return {
            'oneOf': [
                {'type': 'boolean'},
                {'type': 'array', 'items': {'type': 'string', 'enum': [p.name for p in all_builtins()]}}
            ]
        }

    def debug(self):
        log.debug('Builtin plugins: %s' % ', '.join(plugin.name for plugin in all_builtins()))

    @plugin.priority(255)
    def on_task_start(self, task, config):
        self.disabled = []
        if not config:
            return

        for plugin in all_builtins():
            if config is True or plugin.name in config:
                plugin.builtin = False
                self.disabled.append(plugin.name)
        log.debug('Disabled builtin plugin(s): %s' % ', '.join(self.disabled))

    @plugin.priority(-255)
    def on_task_exit(self, task, config):
        if not self.disabled:
            return

        for name in self.disabled:
            plugin.plugins[name].builtin = True
        log.debug('Enabled builtin plugin(s): %s' % ', '.join(self.disabled))
        self.disabled = []

    on_task_abort = on_task_exit

@event('plugin.register')
def register_plugin():
    plugin.register(PluginDisableBuiltins, 'disable_builtins', api_ver=2)
