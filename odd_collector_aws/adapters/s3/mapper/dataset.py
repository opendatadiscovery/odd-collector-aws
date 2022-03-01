from typing import Dict, List, Any

from odd_models.models import DataSetField, Type, DataSetFieldType, DataEntity, DataSet, DataEntityType
from oddrn_generator.generators import S3Generator
from pyarrow import Field, Schema, lib
from more_itertools import flatten
from lark import Lark, LarkError
from .s3_field_type_transformer import S3FieldTypeTransformer
from lark import Transformer, Token
from typing import List, Tuple, Dict, Any, Iterable

SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/" \
                  "main/specification/extensions/s3.json"

TYPE_MAP: Dict[str, Type] = {
    'int8': Type.TYPE_INTEGER,
    'int16': Type.TYPE_INTEGER,
    'int32': Type.TYPE_INTEGER,
    'int64': Type.TYPE_INTEGER,
    'uint8': Type.TYPE_INTEGER,
    'uint16': Type.TYPE_INTEGER,
    'uint32': Type.TYPE_INTEGER,
    'uint64': Type.TYPE_INTEGER,
    'float8': Type.TYPE_NUMBER,
    'float16': Type.TYPE_NUMBER,
    'float32': Type.TYPE_NUMBER,
    'float64': Type.TYPE_NUMBER,
    'time32': Type.TYPE_TIME,
    'time64': Type.TYPE_TIME,
    'timestamp': Type.TYPE_DATETIME,
    'date32': Type.TYPE_DATETIME,
    'date64': Type.TYPE_DATETIME,
    'duration': Type.TYPE_DURATION,
    'month_day_nano_interval': Type.TYPE_DURATION,
    'binary': Type.TYPE_BINARY,
    'string': Type.TYPE_STRING,
    'utf8': Type.TYPE_STRING,
    'large_binary': Type.TYPE_BINARY,
    'large_string': Type.TYPE_STRING,
    'large_utf8': Type.TYPE_STRING,
    'decimal128': Type.TYPE_NUMBER,
    'list': Type.TYPE_LIST,
    'map': Type.TYPE_MAP,
    'struct': Type.TYPE_STRUCT,
    'union': Type.TYPE_UNION,
    "double": Type.TYPE_NUMBER
}
field_type_transformer = S3FieldTypeTransformer()
parser = Lark.open('grammar/s3_field_type_grammar.lark', rel_to=__file__, parser="lalr", start='type')

def __parse(field_type: str) -> Dict[str, Any]:
    column_tree = parser.parse(field_type)
    return field_type_transformer.transform(column_tree)

def map_dataset(name, schema: Schema, metadata: Dict, oddrn_gen: S3Generator, rows_number) -> DataEntity:
    name = ':'.join(name.split('/')[1:])

    oddrn_gen.set_oddrn_paths(keys=name) 
    metadata = [{'schema_url': f'{SCHEMA_FILE_URL}#/definitions/S3DataSetExtension',
                'metadata': metadata}]
    
    columns = map_columns(schema, oddrn_gen)

    return DataEntity(
        name=name,
        oddrn=oddrn_gen.get_oddrn_by_path('keys', name),
        metadata=metadata,
        # TODO for updated_at and Created_at we need first and last files mod date from s3, arrow file info shows only size  
        updated_at=None,
        created_at=None,
        type=DataEntityType.FILE,
        dataset=DataSet(
            rows_number=rows_number,
            field_list=columns
        )
    )


def map_column(oddrn_gen: S3Generator, 
                type_parsed: Dict[str, Any], 
                column_name: str = None, 
                parent_oddrn: str = None,
                column_description: str = None,
                stats: DataSetField = None,
                is_key: bool = None,
                is_value: bool = None) -> List[DataSetField]:
    result = []
    ds_type = type_parsed['type']
    name = column_name if column_name is not None \
        else type_parsed["field_name"] if "field_name" in type_parsed \
        else ds_type

    resource_name = "keys" if is_key \
        else "values" if is_value \
        else "subcolumns"

    dsf =  DataSetField(
        name=name,
        oddrn=oddrn_gen.get_oddrn_by_path('columns', name) if parent_oddrn is None else f'{parent_oddrn}/{resource_name}/{name}',
        parent_field_oddrn=parent_oddrn,
        type=DataSetFieldType(
            type= TYPE_MAP.get(ds_type, Type.TYPE_UNKNOWN),              #TYPE_MAP.get(str(field.type), TYPE_MAP.get(type(field.type), Type.TYPE_UNKNOWN)),
            logical_type=str(ds_type),
            is_nullable=True
        ),
        is_key=bool(is_key),
        is_value=bool(is_value),
        owner=None,
        metadata=[],
        stats=stats or None,
        default_value=None,
        description=column_description
    )
    result.append(dsf)

    if ds_type in ['struct', 'union']:
        for children in type_parsed['children']:
            result.extend(map_column(oddrn_gen=oddrn_gen,
                                       parent_oddrn=dsf.oddrn,
                                       type_parsed=children))


    if ds_type == 'list':
        for children in type_parsed['children']:
            result.extend(map_column(oddrn_gen=oddrn_gen,
                                        parent_oddrn=dsf.oddrn,
                                        type_parsed=children,
                                        is_value=True))

    if ds_type == 'map':
        result.extend(map_column(
            oddrn_gen=oddrn_gen,
            parent_oddrn=dsf.oddrn,
            type_parsed=type_parsed['key_type'],
            is_key=True)
        )

        result.extend(map_column(
            oddrn_gen=oddrn_gen,
            parent_oddrn=dsf.oddrn,
            type_parsed=type_parsed['value_type'],
            is_value=True)
        )

    
    return result


def map_columns(schema: Schema, oddrn_gen: S3Generator) -> List[DataSetField]:
    flat_list = []
    for i in range(0, len(schema)):
        for item in map_column(oddrn_gen=oddrn_gen, type_parsed=__parse(str(schema.field(i).type)),column_name= schema.field(i).name):
            flat_list.append(item)
  
    return flat_list
