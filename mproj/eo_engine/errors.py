class AfriCultuReSError(BaseException):
    pass


class AfriCultuReSFileNotExist(AfriCultuReSError):
    """The file does not exist"""


class AfriCultuReSFileInUse(AfriCultuReSError):
    """The file is in Use in an operation"""


class AfriCultuReSFileInvalidDataType(AfriCultuReSError):
    """It could not identified nor as Product or Source"""


class AfriCultuReSMisconfiguration(AfriCultuReSError):
    """Misconfiguration"""


class AfriCultuReSRetriableError(AfriCultuReSError):
    """Yep, retry the task"""
