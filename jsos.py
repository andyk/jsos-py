import re
import hashlib
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple, Optional
import asyncio

FUNCTION_KEY = "~#jF"
DATE_KEY = "~#jD"
REGEXP_KEY = "~#jR"

BUILTIN_MAP_KEY = "~#bM"
BUILTIN_SET_KEY = "~#bS"

def is_primitive(obj):
    return obj is None or isinstance(obj, (int, float, str, bool))

class JsonStore(ABC):
    @abstractmethod
    async def has_json(self, sha1: str) -> bool:
        pass

    async def has_jsons(self, sha1_array: List[str]) -> List[Tuple[str, bool]]:
        return [(sha1, await self.has_json(sha1)) for sha1 in sha1_array]

    @abstractmethod
    async def get_json(self, sha1: str) -> Optional[Dict]:
        pass

    async def get_jsons(self, sha1_array: List[str]) -> List[Tuple[str, Optional[Dict]]]:
        return [(sha1, await self.get_json(sha1)) for sha1 in sha1_array]

    async def get_json_entries(self, sha1_array: List[str]) -> Dict[str, Optional[Dict]]:
        return {sha1: await self.get_json(sha1) for sha1 in sha1_array}

    @abstractmethod
    async def put_json(self, obj: Dict) -> str:
        pass

    async def put_jsons(self, objects: List[Dict]) -> List[Tuple[str, Dict]]:
        return [(await self.put_json(obj), obj) for obj in objects]

    @abstractmethod
    async def delete_json(self, sha1: str) -> None:
        pass

    async def delete_jsons(self, sha1_array: List[str]) -> None:
        await asyncio.gather(*(self.delete_json(sha1) for sha1 in sha1_array))


class MultiJsonStore(JsonStore):
    def __init__(self, json_stores: List[JsonStore], auto_cache: bool = True):
        if not json_stores:
            raise ValueError("MultiJsonStore must have at least one JsonStore")
        self.json_stores = json_stores
        self.auto_cache = auto_cache

    async def has_json(self, sha1: str) -> bool:
        return any(await json_store.has_json(sha1) for json_store in self.json_stores)

    async def get_json(self, sha1: str) -> Optional[Dict]:
        missing_sha1 = []
        got_val = None
        for json_store in self.json_stores:
            got_val = await json_store.get_json(sha1)
            if got_val is not None:
                break
            else:
                missing_sha1.append(json_store)

        if self.auto_cache and got_val is not None:
            await asyncio.gather(*(json_store.put_json(got_val) for json_store in missing_sha1))

        return got_val

    async def get_jsons(self, sha1_array: List[str]) -> List[Tuple[str, Optional[Dict]]]:
        resultMap = {sha1: None for sha1 in sha1_array}
        missedSha1sPerStore = {}

        for json_store in self.json_stores:
            still_missing = [sha1 for sha1 in resultMap if resultMap[sha1] is None]
            if not still_missing:
                break

            got_vals = await json_store.get_jsons(still_missing)
            missed_sha1s = []

            for sha1, val in got_vals:
                if val is not None:
                    resultMap[sha1] = val
                else:
                    missed_sha1s.append(sha1)

            if missed_sha1s:
                missedSha1sPerStore[json_store] = missed_sha1s

        if self.auto_cache:
            for json_store, missed_sha1s in missedSha1sPerStore.items():
                jsons_to_cache = [resultMap[sha1] for sha1 in missed_sha1s if resultMap[sha1] is not None]
                if jsons_to_cache:
                    await json_store.put_jsons(jsons_to_cache)

        return [(sha1, resultMap[sha1]) for sha1 in sha1_array]

    async def put_json(self, obj: Dict) -> str:
        results = await asyncio.gather(*(json_store.put_json(obj) for json_store in self.json_stores))
        if any(result != results[0] for result in results):
            raise ValueError("Inconsistent results when putting the same object.")
        return results[0]

    async def put_jsons(self, objects: List[Dict]) -> List[Tuple[str, Dict]]:
        all_store_put_results = await asyncio.gather(*(store.put_jsons(objects) for store in self.json_stores))
        first_store_results = all_store_put_results[0]
        
        for store_put_results in all_store_put_results[1:]:
            for i, (hash, json) in enumerate(store_put_results):
                if first_store_results[i][0] != hash:
                    raise ValueError(f"Inconsistent results when putting object {json}.")

        return first_store_results

    async def delete_json(self, sha1: str) -> None:
        await asyncio.gather(*(json_store.delete_json(sha1) for json_store in self.json_stores))

class ValStore:
    def __init__(self, json_store: JsonStore):
        self.json_store = json_store

    async def put_val(self, obj: Any) -> Tuple[str, Any]:
        put_normalized = await self.encode_normalize_put_val(obj)
        encoded_normalized = self.encode_normalized([pair[0] for pair in put_normalized])
        encoded_normalized_sha1 = await self.json_store.put_json(encoded_normalized)
        if not put_normalized:
            raise ValueError("normalize_encode_put_val() returned 0 objects but should have returned >0.")
        return encoded_normalized_sha1, encoded_normalized

    async def encode_normalize_put_val(self, obj: Any) -> List[Tuple[str, Any]]:
        encoded = self.encode(obj)
        normalized = self.normalize(encoded)
        manifest = [sha1 for sha1, _ in await self.json_store.put_jsons(normalized)]
        if len(normalized) != len(manifest):
            raise ValueError(f"put_jsons() returned {len(manifest)} sha1 strings, but expected {len(normalized)} objects to be put.")
        return [(manifest[i], normalized[i]) for i in range(len(normalized))]

    async def put_vals(self, objects: List[Any]) -> List[Tuple[str, Any]]:
        return [await self.put_val(obj) for obj in objects]

    async def get_val(self, sha1: str, freeze: bool = True) -> Any:
        encoded_normalized = await self.json_store.get_json(sha1)
        if encoded_normalized is None:
            return None
        # Assuming is_encoded_normalized is a function you have to check if the object is encoded and normalized
        normalized = await self.decode_normalized(encoded_normalized)
        denormalized = await self.denormalize(normalized)
        return self.decode(denormalized, freeze)

    async def get_vals(self, sha1array: List[str]) -> List[Any]:
        return [await self.get_val(sha1) for sha1 in sha1array]

    async def get_val_entries(self, sha1array: List[str]) -> Dict[str, Any]:
        tuples = [await self.get_val(sha1) for sha1 in sha1array]
        return dict(zip(sha1array, tuples))

    async def delete_val(self, sha1: str) -> None:
        await self.json_store.delete_json(sha1)

    async def delete_vals(self, sha1_array: List[str]) -> None:
        await self.json_store.delete_jsons(sha1_array)

    def encode(self, obj: Any) -> Any:
        return self.recursive_encode(obj, {})

    def decode(self, obj: Any, freeze: bool = True) -> Any:
        return self.recursive_decode(obj, freeze)
    
    def recursive_encode(self, obj: Any, visited: Dict) -> Any:
        if id(obj) in visited:
            return visited[id(obj)]

        encoded = self.shallow_encode(obj)
        visited[id(obj)] = encoded

        if isinstance(encoded, dict):
            return {k: self.recursive_encode(v, visited) for k, v in encoded.items()}
        elif isinstance(encoded, list):
            return [self.recursive_encode(i, visited) for i in encoded]
        else:
            return encoded

    def recursive_decode(self, obj: Any, freeze: bool = True) -> Any:
        if isinstance(obj, dict):
            decoded_obj = {k: self.recursive_decode(v, freeze) for k, v in obj.items()}
        elif isinstance(obj, list):
            decoded_obj = [self.recursive_decode(i, freeze) for i in obj]
        else:
            decoded_obj = obj

        decoded = self.shallow_decode(decoded_obj)
        return decoded

    def shallow_encode(obj):
        if callable(obj):
            raise "Cannot encode functions"
        
        if isinstance(obj, set):
            return [BUILTIN_SET_KEY, list(obj)]

        if isinstance(obj, list):
            # Ensure it's not the list type itself
            return obj.copy() if obj is not list else list(obj)
        
        if isinstance(obj, re.Pattern):
            return [REGEXP_KEY, [obj.pattern, obj.flags]]

        if not is_primitive(obj):
            raise ValueError(f"shallowEncode can only handle primitive types, received: {obj}")

        return obj

class InMemoryJsonStore(JsonStore):
    def __init__(self):
        super().__init__()
        self.val_map = {}

    async def has_json(self, sha1: str) -> bool:
        return sha1 in self.val_map

    async def get_json(self, sha1: str):
        return self.val_map.get(sha1)

    async def put_json(self, obj) -> str:
        sha1 = self.get_sha1(obj)
        self.val_map[sha1] = obj
        return sha1

    async def delete_json(self, sha1: str):
        if sha1 in self.val_map:
            del self.val_map[sha1]

    @staticmethod
    def get_sha1(obj) -> str:
        # Convert the object to a JSON string and encode it to bytes
        obj_str = json.dumps(obj, sort_keys=True)
        return hashlib.sha1(obj_str.encode()).hexdigest()