import luigi
import os
from run_ranker import FindPlates, BatchCheckRanker
from transfer_images import CheckImageDirs
import datetime as dt
from config_classes import SentryConfig


class StartPipe(luigi.Task):
    production = luigi.Parameter(default=False)
    def requires(self):

        required_dirs = ['transfers', 'SubwellImages', 'Data', 'transfers', 'ranker_jobs', 'LogFiles']

        for req in required_dirs:
            if not os.path.isdir(os.path.join(os.getcwd(), req)):
                os.mkdir(os.path.join(os.getcwd(), req))

        if self.production:
            import sentry_sdk
            from sentry_sdk import capture_exception
            from sentry_sdk import configure_scope

            # set sentry key url from config
            sentry_string = str(
                "https://" + SentryConfig().key + "@sentry.io/" + SentryConfig().ident
            )
            # initiate sentry sdk
            sentry_sdk.init(sentry_string)

            # custom handler for luigi exception
            @luigi.Task.event_handler(luigi.Event.FAILURE)
            def send_failure_to_sentry(task, exception):
                # add additional information to sentry scope (about task)
                with configure_scope() as scope:
                    scope.set_extra("os_pid", os.getpid())
                    scope.set_extra("task_id", task.task_id)
                    scope.set_extra("task_family", task.task_family)
                    scope.set_extra("param_args", task.param_args)
                    scope.set_extra("param_kwargs", task.param_kwargs)
                # send error to sentry
                capture_exception()

        now = dt.datetime.now()
        ago = now - dt.timedelta(minutes=10)
        directories = ["barcodes_2drop", "barcodes_3drop", "barcodes_mitegen"]
        for directory in directories:
            for root, dirs, files in os.walk(os.path.join(os.getcwd(), directory)):
                for fname in files:
                    path = os.path.join(root, fname)
                    st = os.stat(path)
                    mtime = dt.datetime.fromtimestamp(st.st_mtime)

                    if mtime < ago:
                        os.remove(os.path.join(root, fname))

        yield FindPlates()
        yield CheckImageDirs()
        yield BatchCheckRanker()

    def run(self):
        try:
            os.remove("plates.done")
        except:
            pass
        try:
            os.remove("checkrank.done")
        except:
            pass
        try:
            os.remove("findplates.done")
        except:
            pass
        try:
            os.remove("checkdirs.done")
        except:
            pass


if __name__ == "__main__":
    luigi.build([StartPipe()], workers=10, no_lock=False)
