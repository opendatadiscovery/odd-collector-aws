from typing import Optional, TypeVar, List

T = TypeVar('T')


def safe_list_get(xs: List[T], idx: int) -> Optional[T]:
    try:
        return xs[idx]
    except IndexError:
        return None
