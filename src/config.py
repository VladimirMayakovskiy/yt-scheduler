from copy import deepcopy
from typing import TypedDict, Optional

from yt.wrapper.common import update, update_inplace
import yt.wrapper.default_config as yt_default_config
from yt.wrapper.mappings import VerifiedDict

class DefaultConfigType(TypedDict, total=False):
    class DefaultConfigProxyType(TypedDict, total=False):
        url: str

    proxy: DefaultConfigProxyType
    config_path: str
    default_work_dir: str

    # class DefaultConfigExecutorType(TypedDict, total=False):
    #     poll_interval: float
    #     task_start_interval: float
    #
    # executor: DefaultConfigExecutorType
    #
    # class DefaultConfigSchedulerType(TypedDict, total=False):
    #     poll_interval: float
    #
    # scheduler: DefaultConfigSchedulerType
    #
    # class DefaultConfigPoolType(TypedDict, total=False):
    #     max_thread_count: int
    #     unordered: bool # todo
    #
    # pool: DefaultConfigPoolType

default_config = {
    "proxy": {
        "url": None,
    },
    "default_work_dir": "//tmp/",
} # todo add fields

def get_default_config() -> VerifiedDict:
    default_template = deepcopy(yt_default_config.get_default_config())
    patch_template = deepcopy(default_config)
    template_dict = yt_default_config.VerifiedDict({**default_template, **patch_template})
    yt_default_config.update_config_from_env(template_dict)
    config = yt_default_config.VerifiedDict(
        template_dict=template_dict,
        transform_func=yt_default_config.transform_value)
    return config

class Config:
    def __init__(self):
        self.config = None
        self.default_config_module = yt_default_config
        self._init_from_env()

    def _init_from_env(self):
        if self.config is not None:
            self.default_config_module.update_config_from_env(self.config)
        else:
            self.config = self.default_config_module.update_config_from_env(get_default_config())

    def update_config(self, patch):
        update_inplace(self.config, patch)

    def get_config(self):
        return self.config

    def get_proxy(self):
        return self._get("proxy/url")

    def set_proxy(self, value):
        self._set("proxy/url", value)

    def __getattr__(self, key):
        try:
            return self._get(key)
        except KeyError:
            raise AttributeError(f"Config has no attribute {key}")

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def _get(self, key):
        d = self.config
        parts = key.split("/")
        for k in parts:
            d = d.get(k)
        return d

    def _set(self, key, value):
        d = self.config
        parts = key.split("/")
        for k in parts[:-1]:
            d = d[k]
        d[parts[-1]] = value