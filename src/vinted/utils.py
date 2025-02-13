import re

from .exceptions import InvalidUrlException
from urllib.parse import unquote


def parse_url_to_params(url: str):
    try:
        decoded_url = unquote(url)

        matched_params = re.match(r"^https:\/\/www\.vinted\.([a-z]+)", decoded_url)
        if not matched_params:
            raise InvalidUrlException

        missing_ids_params = ["catalog", "status"]

        params = re.findall(r"([a-z_]+)(\[\])?=([a-zA-Z 0-9._À-ú+%]*)&?", decoded_url)
        if not isinstance(matched_params.groups(), tuple):
            raise InvalidUrlException

        mapped_params = {}

        for param_name, is_array, param_value in params:
            if " " in param_value:
                param_value = param_value.replace(" ", "+")

            if is_array:
                if param_name in missing_ids_params:
                    param_name = f"{param_name}_id"

                if param_name + "s" in mapped_params:
                    mapped_params[param_name + "s"].append(param_value)
                else:
                    mapped_params[param_name + "s"] = [param_value]
            else:
                mapped_params[param_name] = param_value

        final_params = {}
        for key, value in mapped_params.items():
            if isinstance(value, list):
                final_params[key] = ",".join(value)
            else:
                final_params[key] = value

        [
            final_params.pop(key)
            for key in ["time", "page", "per_page"]
            if key in final_params.keys()
        ]

        return final_params
    except Exception as e:
        print(e)
        raise InvalidUrlException
