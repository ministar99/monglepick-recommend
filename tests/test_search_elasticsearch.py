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


def test_build_genre_discovery_query_scores_selected_genre_groups():
    client = ElasticsearchSearchClient()

    query = client._build_genre_discovery_query(
        genres=["액션", "드라마"],
        genre_match_groups=[["액션", "액숀"], ["드라마"]],
        year_from=None,
        year_to=None,
        rating_min=None,
        rating_max=None,
        popularity_min=None,
        popularity_max=None,
        vote_count_min=None,
    )

    bool_query = query["bool"]
    assert bool_query["minimum_should_match"] == 1
    assert len(bool_query["should"]) == 2
    first_group_terms = bool_query["should"][0]["constant_score"]["filter"]["bool"]["should"]
    assert {"term": {"genres": "액션"}} in first_group_terms
    assert {"term": {"genres": "액숀"}} in first_group_terms


def test_build_sort_prioritizes_score_for_genre_discovery_rating():
    client = ElasticsearchSearchClient()
    capabilities = ESIndexCapabilities()

    sort = client._build_sort(
        sort_by="rating",
        sort_order="desc",
        capabilities=capabilities,
        prioritize_score=True,
    )

    assert sort[0] == {"_score": {"order": "desc"}}
    assert sort[1] == {"rating": {"order": "desc", "missing": "_last"}}


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
