from lark import Transformer, Token


class AthenaFieldTypeTransformer(Transformer):
    def field_definition(self, items):
        return items[1] | {"field_name": str(items[0])}

    def struct_type(self, items):
        return {"type": "struct", "children": items}

    def type(self, items):
        obj = items[0]
        return (
            {"type": str(obj).lower()}
            if isinstance(obj, Token) and obj.type == "SIMPLE_TYPE"
            else obj
        )

    def iterable_type(self, items):
        return {"type": "list", "children": items}

    def map_type(self, items):
        return {
            "type": "map",
            "key_type": items[0],
            "value_type": items[1],
        }

    def union_type(self, items):
        return {"type": "union", "children": items}
