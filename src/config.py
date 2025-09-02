from copy import deepcopy
from typing import TypedDict, Optional

import yt.wrapper as yt

class DefaultConfigType(TypedDict, total=False):
    class DefaultConfigProxyType(TypedDict, total=False):
        url: str = ""

    proxy: DefaultConfigProxyType
    config_path: Optional[str]
    default_work_dir: Optional[str]
    # todo

default_config = {
    "proxy": {
        "url": None,
    },
    "config_path": None,
    "default_work_dir": None,
}

def get_default_config() -> DefaultConfigType:
    template_dict = deepcopy(default_config)
    config = yt.default_config.VerifiedDict(
        template_dict=template_dict,
        transform_func=yt.default_config.transform_value)
    return config

ENV_VARS_SHORTCUTS = {
    "YT_PROXY": "proxy/url",
}

def _update_from_env_vars(
    config: yt.default_config.VerifiedDict,
    shortcuts: Optional[dict] = None,
):
    if shortcuts is None:
        shortcuts = ENV_VARS_SHORTCUTS

    def _set(d, key, value):
        parts = key.split("/")
        for k in parts[:-1]:
            d = d[k]
        d[parts[-1]] = value

    import os
    for key, path in shortcuts.items():
        if value := os.getenv(key):
            _set(config, path, value)

    return config

class Config:
    def __init__(self):
        self.config = None
        self._reload()
        # todo version

    def _reload(self):
        if self.config is None:
            self.config = _update_from_env_vars(get_default_config())
        else:
            self.config = _update_from_env_vars(self.config)

    def get_proxy(self):
        return self._get("proxy/url")

    def set_proxy(self, value):
        self._set("proxy/url", value)

    def __getattr__(self, key):
        try:
            d = self._get(key)
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