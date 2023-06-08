from deltalake._internal import ArrayType, Field, MapType, PrimitiveType, StructType
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import S3Generator

from ..logger import logger

DELTA_TO_ODD_TYPE_MAP = {
    "string": Type.TYPE_STRING,
}


def map_to_odd_type(delta_type: str) -> Type:
    return DELTA_TO_ODD_TYPE_MAP.get(delta_type, Type.TYPE_UNKNOWN)


def unknown_field(generator: S3Generator, field: Field) -> DataSetField:
    generator.set_oddrn_paths(columns=field.name)
    return DataSetField(
        oddrn=generator.get_oddrn_by_path("columns"),
        name=field.name,
        type=DataSetFieldType(
            type=Type.TYPE_UNKNOWN,
            logical_type=field.type,
            is_nullable=field.nullable,
        ),
    )


def map_primitive(generator: S3Generator, field: Field) -> DataSetField:
    generator.set_oddrn_paths(columns=field.name)
    return DataSetField(
        oddrn=generator.get_oddrn_by_path("columns"),
        name=field.name,
        type=DataSetFieldType(
            type=map_to_odd_type(field.type.type),
            logical_type=field.type.type,
            is_nullable=field.nullable,
        ),
    )


def map_map(generator: S3Generator, field: Field) -> DataSetField:
    ...


def map_struct(generator: S3Generator, field: Field) -> DataSetField:
    ...


def map_array(generator: S3Generator, field: Field) -> DataSetField:
    ...


def map_field(generator: S3Generator, field: Field) -> DataSetField:
    type_ = field.type

    if isinstance(type_, PrimitiveType):
        return map_primitive(generator, field)
    else:
        logger.error(f"Unknown field type: {field.type}")
