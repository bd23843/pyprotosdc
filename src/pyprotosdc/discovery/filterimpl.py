from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

if TYPE_CHECKING:
    from org.somda.protosdc.proto.model.discovery import discovery_messages_pb2
    from .service import Service
    from collections.abc import Iterable


def _string_matches(string_list: list[str], candidate: str):
    """Plain case-sensitive string comparison."""
    return candidate in string_list


def _rfc3986_string_matches(string_list: list[str], candidate: str):
    """ RFC 3986 comparison of URI:
    case-insensitive for scheme and authority; case-sensitive for path segments;
    other URI components are excluded.
    """
    candidate_scope = urlsplit(candidate)
    for scope_string in string_list:
        scope = urlsplit(scope_string)
        if candidate_scope.scheme.lower() != scope.scheme.lower() \
                or candidate_scope.netloc.lower() != scope.netloc.lower():
            return False
        if candidate_scope.path == scope.path:
            return True
        cand_path_elements = candidate_scope.path.split('/')
        scope_path_elements = scope.path.split('/')
        cand_path_elements = [unquote(elem) for elem in cand_path_elements]
        scope_path_elements = [unquote(elem) for elem in scope_path_elements]
        if len(cand_path_elements) != len(scope_path_elements):
            return False
        return all(cand_path_elements[i] == elem for i, elem in enumerate(scope_path_elements))


def _matches_filter(service: Service,
                    search_filters: list[discovery_messages_pb2.SearchFilter]) -> bool:
    """Check if service matches the filters.

    - An empty list seeks every endpoint on the network.
    - Scope matchers are linked logically by AND (every scope shall match)
    - Endpoint identifiers are linked logically by OR (any endpoint shall match)
    - Matching scopes and endpoint identifiers are logically linked by OR (which results in the intersection of
      matched scopes and endpoint identifiers)
    """
    matches_scope = True
    matches_epr = False

    for search_filter in search_filters:
        which = search_filter.WhichOneof(search_filter.DESCRIPTOR.oneofs[0].name)
        if which == 'scope_matcher':
            if search_filter.scope_matcher.algorithm == search_filter.scope_matcher.RFC_3986:
                if not _rfc3986_string_matches(service.scopes, search_filter.scope_matcher.scope.value):
                    matches_scope = False
            else:
                if not _string_matches(service.scopes, search_filter.scope_matcher.scope.value):
                    matches_scope = False
        else:
            if search_filter.endpoint_identifier == service.epr:
                matches_epr = True
    return matches_scope or matches_epr


def filter_services(services: Iterable[Service],
                    search_filters: list[discovery_messages_pb2.SearchFilter]) -> Iterable[Service]:
    """Filter services that match types and scopes.

    - An empty list seeks every endpoint on the network.
    - Scope matchers are linked logically by AND (every scope shall match)
    - Endpoint identifiers are linked logically by OR (any endpoint shall match)
    - Matching scopes and endpoint identifiers are logically linked by OR (which results in the intersection of
      matched scopes and endpoint identifiers)

    """
    if not search_filters:
        return services
    return [service for service in services if _matches_filter(service, search_filters)]
