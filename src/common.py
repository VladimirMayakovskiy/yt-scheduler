from typing import Any, Optional

class classproperty(property):
    def __get__(self, obj: Any, objtype: Optional[type] = None) -> Any:
        return self.fget(objtype)