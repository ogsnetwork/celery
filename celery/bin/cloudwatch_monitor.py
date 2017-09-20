from __future__ import absolute_import, unicode_literals

import sys

from functools import partial

from celery.platforms import detached, set_process_title, strargv
from celery.bin.base import Command, Option, daemon_options

__all__ = ['cloudwatch_monitor']


class cloudwatch_monitor(Command):  # noqa

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel='INFO', logfile=None, prog_name='celery events',
            pidfile=None, uid=None, gid=None, umask=None,
            working_directory=None, detach=False, **kwargs):

        self.prog_name = prog_name
        return self.run_monitor(
            loglevel=loglevel, logfile=logfile,
            pidfile=pidfile, uid=uid, gid=gid, umask=umask,
            workdir=working_directory, detach=detach)

    def set_process_status(self, prog, info=''):
        prog = '{0}:{1}'.format(self.prog_name, prog)
        info = '{0} {1}'.format(info, strargv(sys.argv))
        return set_process_title(prog, info=info)

    def run_monitor(
            self, logfile=None, pidfile=None, uid=None,
            gid=None, umask=None, workdir=None, detach=False, **kwargs):
        from celery.events.cloudwatch import evtop
        self.set_process_status('top')
        # return evtop(app=self.app)

        mon = partial(
            evtop, self.app, logfile=logfile, pidfile=pidfile, **kwargs)

        if detach:
            with detached(logfile, pidfile, uid, gid, umask, workdir):
                return mon()
        else:
            return mon()

    def get_options(self):
        return (
            (
                Option('--detach', action='store_true'),
                Option('-r', '--maxrate'),
                Option('-l', '--loglevel', default='INFO')
            ) +
            daemon_options(default_pidfile='celeryev.pid') +
            tuple(self.app.user_options['cloudwatch_monitor'])
        )


def main():
    ev = cloudwatch_monitor()
    ev.execute_from_commandline()


if __name__ == '__main__':
    main()
