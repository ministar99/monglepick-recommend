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


def test_build_related_movie_search_body_combines_text_and_metadata_signals():
    client = ElasticsearchSearchClient()

    body = client._build_related_movie_search_body(
        movie_id="10",
        title="기생충",
        title_en="Parasite",
        overview="반지하 가족이 상류층 가정에 침투하며 벌어지는 이야기",
        director="봉준호",
        cast_members=["송강호", "이선균", "조여정"],
        genres=["드라마", "스릴러"],
        collection_name="봉준호 대표작",
        limit=15,
    )

    bool_query = body["query"]["bool"]
    should = bool_query["should"]

    assert body["size"] == 30
    assert any("more_like_this" in clause for clause in should)
    assert any(
        clause.get("match_phrase", {}).get("director", {}).get("query") == "봉준호"
        for clause in should
    )
    assert any(
        clause.get("constant_score", {}).get("filter") == {"term": {"genres": "드라마"}}
        for clause in should
    )
    assert any(
        clause.get("match_phrase", {}).get("cast", {}).get("query") == "송강호"
        for clause in should
    )
    assert {"term": {"id": "10"}} in bool_query["must_not"]


def test_build_collection_movie_search_body_filters_by_collection_name():
    client = ElasticsearchSearchClient()

    body = client._build_collection_movie_search_body(
        movie_id="10",
        collection_name="다크 나이트 트릴로지",
        page=1,
        page_size=50,
    )

    assert body["from"] == 50
    assert body["size"] == 50
    assert body["query"]["bool"]["must"][0] == {
        "match_phrase": {
            "collection_name": {
                "query": "다크 나이트 트릴로지",
            }
        }
    }
    assert {"term": {"id": "10"}} in body["query"]["bool"]["must_not"]
    assert body["sort"][0] == {"release_year": {"order": "asc", "missing": "_last"}}


def test_to_movie_item_parses_related_metadata_fields():
    client = ElasticsearchSearchClient()

    movie = client._to_movie_item(
        {
            "_source": {
                "id": "11",
                "title": "살인의 추억",
                "title_en": "Memories of Murder",
                "genres": ["범죄", "드라마"],
                "release_year": 2003,
                "rating": 8.1,
                "vote_count": 2200,
                "poster_path": "/poster.jpg",
                "trailer_url": "https://example.com/trailer",
                "overview": "연쇄살인범을 쫓는 형사들",
                "director": "봉준호",
                "cast": ["송강호", "김상경"],
                "keywords": ["연쇄살인", "형사"],
                "collection_name": "봉준호 대표작",
            },
            "_score": 12.3456,
        }
    )

    assert movie.movie_id == "11"
    assert movie.director == "봉준호"
    assert movie.cast == ["송강호", "김상경"]
    assert movie.keywords == ["연쇄살인", "형사"]
    assert movie.collection_name == "봉준호 대표작"
    assert movie.score == 12.3456
