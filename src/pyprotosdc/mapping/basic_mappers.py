import decimal
from functools import reduce

from google.protobuf.duration_pb2 import Duration
from google.protobuf.wrappers_pb2 import StringValue
from org.somda.protosdc.proto.model.common import common_types_pb2

from .mapping_helpers import attr_name_to_p


def string_value_to_p(s: (str, None), p: (StringValue, None)) -> StringValue:
    """modify protobuf StringValue
    if p is None, a new StringValue is returned; otherwise the parameter is modified
    """
    if p is None:
        p = StringValue()
    if s is not None:
        p.value = s
        return p


def string_value_from_p(p, attr_name) -> (str, None):
    if p.HasField(attr_name):
        opt = getattr(p, attr_name)
        return opt.value
    return None


def duration_to_p(f: [float, decimal.Decimal], p: Duration) -> None:
    """modify protobuf Duration value inline """
    if f is None:
        return
    p.seconds = int(f)
    p.nanos = int((f - int(f)) * 1e9)


def duration_from_p(p: Duration) -> float:
    """read protobuf Duration """
    ret = float(p.seconds) + float(p.nanos) / 1e9
    return ret


def decimal_to_p(pm_value: decimal.Decimal,
                 # pm_property_type: type,
                 p_dest: common_types_pb2.Decimal):
    t = pm_value.as_tuple()
    abs_value = reduce(lambda x, y: x * 10 + y, t.digits)
    p_dest.value = abs_value * -1 if t.sign else abs_value
    p_dest.scale = -t.exponent


def decimal_from_p(p: common_types_pb2.Decimal) -> decimal.Decimal:
    # return decimal.Decimal(p.value) / decimal.Decimal(int(math.pow(10, p.scale)))
    return decimal.Decimal(p.value) / decimal.Decimal(10 ** p.scale)


def _enum_name_to_p(name):
    # insert an underscore when changing from an lowercase to an uppercase char
    tmp = []
    tmp.append(name[0])
    for c in name[1:]:
        if c.isupper() and tmp[-1].islower():
            tmp.append('_')
        tmp.append(c)
    return ''.join(tmp).upper()


def enum_attr_to_p(s: str, p) -> None:
    """modify protobuf EnumType inline """
    if s is not None:
        s = _enum_name_to_p(s)  # protobuf is always upper case
        index = p.EnumType._enum_type.values_by_name[s].index
        p.enum_type = index


def enum_attr_from_p_func(p_src, p_attr_name, dest, dest_attr_name):
    """
    returns the converted value or None
    """
    p = getattr(p_src, p_attr_name)
    index = p.enum_type
    enum_string = p.EnumType._enum_type.values[index].name
    prop = getattr(dest.__class__, dest_attr_name)
    enum_cls = prop._converter._klass
    for name, member in enum_cls.__members__.items():
        if _enum_name_to_p(member.value) == enum_string:
            return member
    raise ValueError(f'unknown enum "{enum_string}" for {dest_attr_name}, type={enum_cls.__name__}')


def enum_attr_from_p(p_src, pm_attr_name, enum_cls):
    """
    returns the converted value or None
    """
    return enum_from_p(p_src, attr_name_to_p(pm_attr_name), enum_cls)
    p = getattr(p_src, attr_name_to_p(pm_attr_name))
    index = p.enum_type
    enum_string = p.EnumType._enum_type.values[index].name
    prop = getattr(dest.__class__, dest_attr_name)
    enum_cls = prop._converter._klass
    for name, member in enum_cls.__members__.items():
        if _enum_name_to_p(member.value) == enum_string:
            return member
    raise ValueError(f'unknown enum "{enum_string}" for {dest_attr_name}, type={enum_cls.__name__}')


def enum_from_p(p_src, p_attr_name, enum_cls):
    """
    returns the converted value or None
    """
    p = getattr(p_src, p_attr_name)
    index = p.enum_type
    enum_string = p.EnumType._enum_type.values[index].name
    for name, member in enum_cls.__members__.items():
        if _enum_name_to_p(member.value) == enum_string:
            return member
    raise ValueError(f'unknown enum for {p_attr_name}, type={enum_cls.__name__}')
