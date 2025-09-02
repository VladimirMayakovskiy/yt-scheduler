from __future__ import annotations

import json
import math
import datetime
import logging
import enum
from enum import Enum, unique
from typing import Any, Callable

from dag import DAG
from yt_wrapper import ContextWrapper
from yt_operator import Operator

import yt.wrapper as yt

class Encoding(str, Enum):
    TYPE = "__type"
    VAR = "__var"

@unique
class AttrTypes(str, Enum):
    DAG = "dag"
    OP = "operator"
    DATETIME = "datetime"
    TIMEDELTA = "timedelta"
    DICT = "dict"
    SET = "set"
    TUPLE = "tuple"

class SerializedObject:
    _primitive_types = (int, bool, float, str)
    _datetime_types = (datetime.datetime,)
    _excluded_types = (logging.Logger, type, property)

    @staticmethod
    def _encode(x: Any, type_: Any) -> dict[str, Any]:
        return {Encoding.VAR.value: x, Encoding.TYPE.value: type_}
    @classmethod
    def _is_primitive(cls, var: Any) -> bool:
        return var is None or isinstance(var, cls._primitive_types)
    @classmethod
    def _is_excluded(cls, var: Any) -> bool:
        return var is None or isinstance(var, cls._excluded_types)

    @classmethod
    def serialize_to_json(
            cls,
            object_to_serialize: Operator | DAG,
            decorated_fields: set,
            keys_to_serialize: list[str] = None,
    ) -> dict[str, Any]:
        serialized_object: dict[str, Any] = {}
        for key in keys_to_serialize:
            value = getattr(object_to_serialize, key, None)
            if cls._is_excluded(value):
                continue
            elif key in decorated_fields:
                serialized_object[key] = cls.serialize(value)
            else:
                value = cls.serialize(value)
                if isinstance(value, dict) and Encoding.TYPE in value:
                    value = value[Encoding.VAR]
                serialized_object[key] = value
        return serialized_object

    @classmethod
    def serialize(cls, var: Any) -> Any:
        if cls._is_primitive(var):
            if isinstance(var, enum.Enum):
                return var.value
            if isinstance(var, float) and (math.isnan(var) or math.isinf(var)):
                return str(var)
            return var
        elif isinstance(var, cls._datetime_types):
            return cls._encode(var.timestamp(), type_=AttrTypes.DATETIME)
        elif isinstance(var, datetime.timedelta):
            return cls._encode(var.total_seconds(), type_=AttrTypes.TIMEDELTA)
        elif isinstance(var, dict):
            return cls._encode({str(k): cls.serialize(v) for k, v in var.items()}, type_=AttrTypes.DICT,)
        elif isinstance(var, list):
            return [cls.serialize(v) for v in var]
        elif isinstance(var, set):
            try:
                return cls._encode(sorted(cls.serialize(v) for v in var), type_=AttrTypes.SET,)
            except TypeError:
                return cls._encode([cls.serialize(v) for v in var], type_=AttrTypes.SET,)
        elif isinstance(var, tuple):
            return cls._encode([cls.serialize(v) for v in var], type_=AttrTypes.TUPLE,)
        elif isinstance(var, DAG):
            return cls._encode(SerializedDag.serialize_dag(var), type_=AttrTypes.DAG,)
        elif isinstance(var, Operator):
            return cls._encode(SerializedOperator.serialize_operator(var), type_=AttrTypes.OP,)
        else:
            return str(var)

    @classmethod
    def deserialize(cls, encoded_var: Any) -> Any:
        if cls._is_primitive(encoded_var):
            return encoded_var
        elif isinstance(encoded_var, list):
            return [cls.deserialize(v) for v in encoded_var]

        if not isinstance(encoded_var, dict):
            raise ValueError(f"The encoded_var should be dict and is {type(encoded_var)}")

        var = encoded_var[Encoding.VAR]
        type_ = encoded_var[Encoding.TYPE]

        if type_ == AttrTypes.DICT:
            return {k: cls.deserialize(v) for k, v in var.items()}
        # elif type_ == AttrTypes.DAG:
        #     return SerializedDAG.deserialize_dag(var)
        # elif type_ == AttrTypes.OP:
        #     return SerializedOperator.deserialize_operator(var)
        elif type_ == AttrTypes.DATETIME:
            return datetime.datetime.fromisoformat(var)
        elif type_ == AttrTypes.TIMEDELTA:
            return datetime.timedelta(seconds=var)
        elif type_ == AttrTypes.SET:
            return {cls.deserialize(v) for v in var}
        elif type_ == AttrTypes.TUPLE:
            return tuple(cls.deserialize(v) for v in var)
        else:
            raise TypeError(f"Invalid type {type_!s} in deserialization.")


    @classmethod
    def to_json(cls, var: DAG | Operator | dict | list | set | tuple, *, sort_keys: bool = True, separators=(",", ":")) -> str:
        return json.dumps(cls.serialize(var), ensure_ascii=False, sort_keys=sort_keys, separators=separators)

    @classmethod
    def from_json(cls, serialized_obj: str) -> SerializedObject | dict | list | set | tuple:
        return cls.from_dict(json.loads(serialized_obj))

    @classmethod
    def from_dict(cls, serialized_obj: dict[Encoding, Any]) -> SerializedObject | dict | list | set | tuple:
        return cls.deserialize(serialized_obj)

class SerializedOperator(Operator, SerializedObject):
    _constructor_initialize_fields = [("succeeding_task_ids", set()), ("preceding_task_ids", set())]
    _decorated_fields = {}
    _serialize_fields = ["task_id", "operation_type", "spec"]

    @classmethod
    def serialize_operator(cls, op: Operator) -> dict[str, Any]:
        return cls.serialize_to_json(op, set(cls._decorated_fields), cls._serialize_fields)

    @classmethod
    def deserialize_operator(cls, encoded_op: dict[str, Any], context: Callable) -> Operator:
        operation_type = encoded_op.get("operation_type")
        operator = cls.__new__(cls)
        for k, v in encoded_op.items():
            if k == "spec":
                spec = {k_: cls.deserialize(v_) for k_, v_ in v.items()}
                spec_builder_cls = dict({
                    builder_cls().operation_type: builder_cls
                    for builder_cls in yt.spec_builders.SpecBuilder.__subclasses__()
                }).get(operation_type)
                if spec_builder_cls is None:
                    raise RuntimeError

                spec_builder = spec_builder_cls()
                spec_builder.spec(spec)

                k = "spec_builder"
                v = spec_builder
            elif k == "operation_type":
                continue
            elif k == "task_id":
                k = "id"
                v = cls.deserialize(v)
            elif k in cls._decorated_fields:
                v = cls.deserialize(v)
            try:
                object.__setattr__(operator, k, v)
            except:
                print(k, v)
                raise

        keys_to_set_none = cls._serialize_fields - encoded_op.keys()
        for k in keys_to_set_none:
            object.__setattr__(operator, k, None)

        for k, v in cls._constructor_initialize_fields:
            if k in encoded_op:
                continue
            object.__setattr__(operator, k, v)

        operator._contextualize = context
        return operator

class SerializedDag(DAG, SerializedObject):
    _decorated_fields = {"defaults_args"}
    _serialize_fields = ["work_dir"]
    @classmethod
    def serialize_dag(cls, dag: DAG) -> dict:
        try:
            serialized_dag = cls.serialize_to_json(dag, cls._decorated_fields, cls._serialize_fields)
            serialized_dag["tasks"] = [cls.serialize(task) for _, task in dag.task_dict.items()]
            return serialized_dag
        except Exception as e:
            raise Exception(f"Failed to serialize DAG {dag.dag_id!r}: {e}")

    @classmethod
    def deserialize_dag(cls, encoded_dag: dict, dag_id: str, context_wrapper: ContextWrapper) -> SerializedDag:
        context = context_wrapper.bind(work_dir=encoded_dag.get("work_dir"))
        dag = cls.__new__(cls)
        dag.dag_id = dag_id

        for k, v in encoded_dag.items():
            if k == "tasks":
                tasks = {}
                for obj in v:
                    if obj.get(Encoding.TYPE) == AttrTypes.OP:
                        deser_operator = SerializedOperator.deserialize_operator(obj[Encoding.VAR], context)
                        deser_operator.prepare_user_spec()
                        deser_operator.dag_id = dag_id
                        tasks[deser_operator.task_id] = deser_operator
                k = "task_dict"
                v = tasks
            elif k in cls._decorated_fields:
                v = cls.deserialize(v)
            object.__setattr__(dag, k, v)

        keys_to_set_none = cls._serialize_fields - encoded_dag.keys()
        for k in keys_to_set_none:
            object.__setattr__(dag, k, None)

        return dag