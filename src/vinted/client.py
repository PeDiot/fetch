from typing import List, Literal, Dict

import requests
import time

from .endpoints import Endpoints
from .utils import parse_url_to_params
from .models import VintedResponse
from .enums import Domain, SortOption, USER_AGENT


class Vinted:
    def __init__(self, domain: Domain = "fr") -> None:
        self.base_url = f"https://www.vinted.{domain}"
        self.api_url = f"{self.base_url}/api/v2"
        self.headers = {"User-Agent": USER_AGENT}
        self.cookies = self.fetch_cookies()

    def fetch_cookies(self):
        response = requests.get(self.base_url, headers=self.headers)
        return response.cookies

    def _call(self, method: Literal["get"], *args, **kwargs):
        return requests.request(
            method=method, headers=self.headers, cookies=self.cookies, *args, **kwargs
        )

    def _get(
        self,
        endpoint: Endpoints,
        format_values=None,
        *args,
        **kwargs,
    ) -> VintedResponse:
        if format_values:
            url = self.api_url + endpoint.value.format(format_values)
        else:
            url = self.api_url + endpoint.value

        response = self._call(method="get", url=url, *args, **kwargs)

        if response.status_code == 200:
            try:
                return VintedResponse(
                    status_code=response.status_code, data=response.json()
                )
            except requests.exceptions.JSONDecodeError:
                return VintedResponse(status_code=response.status_code)
        else:
            return VintedResponse(status_code=response.status_code)

    def search(
        self,
        url: str = None,
        page: int = 1,
        per_page: int = 96,
        query: str = None,
        price_from: float = None,
        price_to: float = None,
        order: SortOption = "newest_first",
        catalog_ids: int | List[int] = None,
        size_ids: int | List[int] = None,
        brand_ids: int | List[int] = None,
        status_ids: int | List[int] = None,
        color_ids: int | List[int] = None,
        patterns_ids: int | List[int] = None,
        material_ids: int | List[int] = None,
    ) -> VintedResponse:
        params = {
            "page": page,
            "per_page": per_page,
            "time": time.time(),
            "search_text": query,
            "price_from": price_from,
            "price_to": price_to,
            "catalog_ids": catalog_ids,
            "order": order,
            "size_ids": size_ids,
            "brand_ids": brand_ids,
            "status_ids": status_ids,
            "color_ids": color_ids,
            "patterns_ids": patterns_ids,
            "material_ids": material_ids,
        }
        if url:
            params.update(parse_url_to_params(url))

        return self._get(Endpoints.CATALOG_ITEMS, params=params)

    def search_users(
        self, query: str, page: int = 1, per_page: int = 36
    ) -> VintedResponse:
        params = {"page": page, "per_page": per_page, "search_text": query}
        return self._get(Endpoints.USERS, params=params)

    def item_info(self, item_id: int) -> VintedResponse:
        return self._get(Endpoints.ITEMS, item_id)

    def user_info(self, user_id: int, localize: bool = False) -> VintedResponse:
        params = {"localize": localize}
        return self._get(Endpoints.USER, user_id, params=params)

    def user_items(
        self,
        user_id: int,
        page: int = 1,
        per_page: int = 96,
        order: SortOption = "newest_first",
    ) -> VintedResponse:
        params = {"page": page, "per_page": per_page, "order": order}
        return self._get(Endpoints.USER_ITEMS, user_id, params=params)

    def user_feedbacks(
        self,
        user_id: int,
        page: int = 1,
        per_page: int = 20,
        by: Literal["all", "user", "system"] = "all",
    ) -> VintedResponse:
        params = {"user_id": user_id, "page": page, "per_page": per_page, "by": by}
        return self._get(Endpoints.USER_FEEDBACKS, params=params)

    def user_feedbacks_summary(
        self,
        user_id: int,
    ) -> VintedResponse:
        params = {"user_id": user_id}
        return self._get(
            Endpoints.USER_FEEDBACKS_SUMMARY,
            params=params,
        )

    def search_suggestions(self, query: str) -> VintedResponse:
        return self._get(
            Endpoints.SEARCH_SUGGESTIONS,
            params={"query": query},
        )

    def catalog_filters(
        self,
        query: str = None,
        catalog_ids: int = None,
        brand_ids: int | List[int] = None,
        status_ids: int | List[int] = None,
        color_ids: int | List[int] = None,
    ) -> VintedResponse:
        params = {
            "search_text": query,
            "catalog_ids": catalog_ids,
            "time": time.time(),
            "brand_ids": brand_ids,
            "status_ids": status_ids,
            "color_ids": color_ids,
        }
        return self._get(Endpoints.CATALOG_FILTERS, params=params)

    def catalogs_list(self) -> VintedResponse:
        return self._get(
            Endpoints.CATALOG_INITIALIZERS,
            params={"page": 1, "time": time.time()},
        )
