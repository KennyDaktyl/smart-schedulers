import logging


class TaskLogger(logging.Logger):
    def makeRecord(
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: str,
        args: tuple,
        exc_info: tuple | None,
        func: str | None = None,
        extra: dict | None = None,
        sinfo: str | None = None,
    ) -> logging.LogRecord:
        extra_copy = dict(extra or {})
        task_name = extra_copy.pop("taskName", None)
        record = super().makeRecord(
            name=name,
            level=level,
            fn=fn,
            lno=lno,
            msg=msg,
            args=args,
            exc_info=exc_info,
            func=func,
            extra=extra_copy,
            sinfo=sinfo,
        )
        if task_name is not None:
            record.taskName = task_name
        return record


def install_task_logger() -> None:
    logging.setLoggerClass(TaskLogger)

