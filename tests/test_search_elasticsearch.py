from app.search_elasticsearch import ESIndexCapabilities, ElasticsearchSearchClient
from app.search_es_bootstrap import SearchESBootstrapper


def test_es_index_capabilities_detects_optional_fields():
    mapping = {
        "movies_bm25": {
            "mappings": {
                "properties": {
                    "title_suggest": {"type": "completion"},
                    "title_sort": {"type": "keyword"},
                    "alternative_titles": {
                        "type": "text",
                        "fields": {
                            "korean": {"type": "text", "analyzer": "korean_analyzer"},
                        },
                    },
                }
            }
        }
    }

    capabilities = ESIndexCapabilities.from_mapping(mapping, index_name="movies_bm25")

    assert capabilities.has_title_suggest is True
    assert capabilities.has_title_sort is True
    assert capabilities.has_alternative_titles_korean is True


def test_build_movie_query_skips_unmapped_optional_field():
    client = ElasticsearchSearchClient()
    capabilities = ESIndexCapabilities()

    query = client._build_movie_query(
        keyword="인터스텔라",
        search_type="all",
        genre=None,
        year_from=None,
        year_to=None,
        rating_min=None,
        rating_max=None,
        vote_count_min=None,
        capabilities=capabilities,
    )

    fields = query["bool"]["must"][0]["multi_match"]["fields"]
    assert "alternative_titles.korean^2" not in fields
    assert "alternative_titles^1.8" in fields


def test_build_suggest_body_skips_completion_when_title_suggest_unavailable():
    client = ElasticsearchSearchClient()
    capabilities = ESIndexCapabilities(has_title_suggest=False)

    suggest = client._build_suggest_body("인터", capabilities)

    assert "title_completion" not in suggest
    assert "title_phrase_ko" in suggest
    assert "title_phrase_en" in suggest


def test_bootstrapper_builds_missing_mapping_only():
    bootstrapper = SearchESBootstrapper()
    mapping = {
        "movies_bm25": {
            "mappings": {
                "properties": {
                    "alternative_titles": {"type": "text"},
                }
            }
        }
    }

    mapping_update = bootstrapper._build_mapping_update(mapping, ESIndexCapabilities())

    assert mapping_update["properties"]["title_suggest"] == {"type": "completion"}
    assert mapping_update["properties"]["title_sort"] == {"type": "keyword"}
    assert "alternative_titles" in mapping_update["properties"]
