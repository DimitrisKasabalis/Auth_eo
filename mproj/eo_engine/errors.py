class AfriCultuReSError(BaseException):
    pass


class AfriCultuReSFileNotExist(AfriCultuReSError):
    """The file does not exist"""


class AfriCultuReSFileInUse(AfriCultuReSError):
    """The file is in Use in an operation"""
